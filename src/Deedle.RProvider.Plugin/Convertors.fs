namespace Deedle.RProvider.Plugin

open System
open Deedle
open Deedle.Indices
open RDotNet
open RProvider
open RProvider.``base``
open Microsoft.FSharp.Reflection
open System.ComponentModel.Composition

[<Export(typeof<IDefaultConvertFromR>)>]
type DataFrameToR() = 
  
  /// Turn columns/rows into an index with either int or string keys
  let (|NumericIndex|StringIndex|) (items:string[]) =
    let res = items |> Seq.map Int32.TryParse |> Seq.takeWhile fst |> Seq.map snd |> Array.ofSeq
    if res.Length = items.Length then 
      NumericIndex (Index.ofKeys res)
    else 
      StringIndex (Index.ofKeys items)

  /// Creates data frame with the specified row & col indices
  let createFrame (df:DataFrame) rowIndex colIndex =
    let rows = df.GetRows() |> Array.ofSeq
    // TODO: Do not always create column with objects - pick int/string/something
    let data = Array.init df.ColumnCount (fun colIndex ->
      let colData = rows |> Array.map (fun r -> r.[colIndex])
      (Vector.ofValues colData) :> IVector)
    Some(box (Frame<_,_>(rowIndex, colIndex, Vector.ofValues data)))

  interface IDefaultConvertFromR with
    member x.Convert(symExpr) =
      match symExpr with
      | RDotNet.ActivePatterns.DataFrame df ->
          // Create data frame with some row/column index
          match df.RowNames, df.ColumnNames with
          | NumericIndex rows, NumericIndex cols -> createFrame df rows cols
          | NumericIndex rows, StringIndex cols -> createFrame df rows cols
          | StringIndex rows, NumericIndex cols -> createFrame df rows cols
          | StringIndex rows, StringIndex cols -> createFrame df rows cols
      | _ -> None

[<Export(typedefof<IConvertToR<Frame<_,_>>>)>]
type DataFrameFromR<'R, 'C when 'R : equality and 'C : equality>() =
  
  // Convert key - or multi-level tuple - into a string
  let convertKey (key:obj) =
    if Object.ReferenceEquals(key, null) then ""
    elif FSharpType.IsTuple(key.GetType()) then
      FSharpValue.GetTupleFields(key)
      |> Array.map string
      |> String.concat " - "
    else string key

  interface IConvertToR<Frame<'R, 'C>> with
    member x.Convert(engine, input:Frame<'R, 'C>) =
      let args = 
        [ for r, c in input.Columns |> Series.observations do
            yield convertKey r, [| 0 |] ] 
      let rowNames = "row.names", input.RowKeys |> Seq.map convertKey
      R.data_frame(namedParams (rowNames::args))
