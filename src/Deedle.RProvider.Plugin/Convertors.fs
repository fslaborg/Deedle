module internal Deedle.RProvider.Plugin.Conversions

open System
open Deedle
open Deedle.Internal
open Deedle.Indices
open RDotNet
open RProvider
open RProvider.``base``
open Microsoft.FSharp.Reflection

// ------------------------------------------------------------------------------------------------
// Conversion helpers
// ------------------------------------------------------------------------------------------------

/// Convert Deedle frame key (or multi-level tuple) to string that can be passed to R
let convertKey (key:obj) =
  if Object.ReferenceEquals(key, null) then ""
  elif FSharpType.IsTuple(key.GetType()) then
    FSharpValue.GetTupleFields(key)
    |> Array.map string
    |> String.concat " - "
  else string key

/// Turn columns/rows into an index with either int or string keys
let (|NumericIndex|StringIndex|) (items:string[]) =
  let res = items |> Seq.map Int32.TryParse |> Seq.takeWhile fst |> Seq.map snd |> Array.ofSeq
  if res.Length = items.Length then NumericIndex (Index.ofKeys res)
  else StringIndex (Index.ofKeys items)

/// Turn columns/rows into an index with the specified type of keys
let convertIndex (names:string[]) : option<IIndex<'R>> =
  try 
    names 
    |> Array.map (Convert.changeType<'R>)
    |> Index.ofKeys |> Some
  with _ -> None


/// Convert vector to a boxed array that can be passed to the R provider
let convertVector : IVector -> obj =
  { new VectorHelpers.VectorCallSite1<obj> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        // Figure out how to handle missing values - if we can pass NA or NaN to R
        // then we just fill missing values with 'missingVal'
        let missingVal = 
          if typeof<'T> = typeof<string> then Some (Unchecked.defaultof<'T>)
          elif typeof<'T> = typeof<double> then Some (unbox<'T> Double.NaN)
          else None

        // If there are missing values and we do not have filler, try converting to float
        let hasMissing = col.DataSequence |> Seq.exists (fun v -> not v.HasValue)
        if hasMissing && missingVal.IsNone then
          let colNum = VectorHelpers.tryChangeType<float> col
          if colNum.HasValue then
            box [| for v in colNum.Value.DataSequence -> if v.HasValue then v.Value else nan  |]
          else
            invalidOp (sprintf "Cannot pass column with missing values to R. Missing values of type %s are not supported" (typeof<'T>.Name))

        // Either there are no missing values, or we can fill them 
        else 
          box [| for v in col.DataSequence -> if v.HasValue then v.Value else missingVal.Value |] }
  |> VectorHelpers.createVectorDispatcher 


/// Creates data frame with the specified row & col indices
let constructFrame (df:DataFrame) rowIndex colIndex =
  let rows = df.GetRows() |> Array.ofSeq
  // TODO: Do not always create column with objects - pick int/string/something
  let data = Array.init df.ColumnCount (fun colIndex ->
    let colData = rows |> Array.map (fun r -> r.[colIndex])
    (Vector.ofValues colData) :> IVector)
  Some(Frame<_,_>(rowIndex, colIndex, Vector.ofValues data))

let createDefaultFrame (symExpr:SymbolicExpression) =
  match symExpr with
  | RDotNet.ActivePatterns.DataFrame df ->
      // Create data frame with some row/column index
      match df.RowNames, df.ColumnNames with
      | NumericIndex rows, NumericIndex cols -> constructFrame df rows cols |> Option.map box
      | NumericIndex rows, StringIndex cols -> constructFrame df rows cols |> Option.map box
      | StringIndex rows, NumericIndex cols -> constructFrame df rows cols |> Option.map box
      | StringIndex rows, StringIndex cols -> constructFrame df rows cols |> Option.map box
  | _ -> None

let tryCreateFrame (symExpr:SymbolicExpression) : option<Frame<'R, 'C>> =
  match symExpr with
  | RDotNet.ActivePatterns.DataFrame df ->
      match convertIndex df.RowNames, convertIndex df.ColumnNames with
      | Some rowIndex, Some colIndex -> constructFrame df rowIndex colIndex
      | _ -> None
  | _ -> None