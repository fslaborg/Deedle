module internal Deedle.RPlugin.Conversions

open System
open Deedle
open Deedle.Internal
open Deedle.Indices
open RDotNet
open RDotNet.ActivePatterns
open RProvider
open RProvider.``base``
open RProvider.zoo
open Microsoft.FSharp.Reflection

// ------------------------------------------------------------------------------------------------
// Conversion helpers
// ------------------------------------------------------------------------------------------------

let invcult = System.Globalization.CultureInfo.InvariantCulture
let dateFmt = "yyyy-MM-dd HH:mm:ss.ffffff"

let dateTimeOffsetToStr (dt:DateTimeOffset) =
    dt.ToUniversalTime().ToString(dateFmt, invcult)

let dateTimeToStr (dt:DateTime) =
    dt.ToUniversalTime().ToString(dateFmt, invcult)

/// Convert Deedle frame key (or multi-level tuple) to string that can be passed to R
let convertKey (key:obj) =
  if Object.ReferenceEquals(key, null) then ""
  elif FSharpType.IsTuple(key.GetType()) then
    FSharpValue.GetTupleFields(key)
    |> Array.map string
    |> String.concat " - "
  elif key :? DateTime then
    dateTimeToStr (key :?> DateTime)
  elif key :? DateTimeOffset then
    dateTimeOffsetToStr (key :?> DateTimeOffset)
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
    |> Array.map (Convert.convertType<'R> ConversionKind.Flexible)
    |> Index.ofKeys |> Some
  with _ -> None

/// Convert vector to a boxed array that can be passed to the R provider
let convertVector (vector:IVector) : obj =
  { new VectorCallSite<obj> with
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
          let colNum = VectorHelpers.tryConvertType<float> ConversionKind.Flexible col
          if colNum.HasValue then
            box [| for v in colNum.Value.DataSequence -> if v.HasValue then v.Value else nan  |]
          else
            invalidOp (sprintf "Cannot pass column with missing values to R. Missing values of type %s are not supported" (typeof<'T>.Name))

        // Either there are no missing values, or we can fill them 
        else 
          box [| for v in col.DataSequence -> if v.HasValue then v.Value else missingVal.Value |] }
  |> vector.Invoke


/// Creates data frame with the specified row & col indices
let constructFrame (df:DataFrame) rowIndex colIndex =
  let rows = df.GetRows() |> Array.ofSeq
  // TODO: Do not always create column with objects - pick int/string/something
  let data = Array.init df.ColumnCount (fun colIndex ->
    let colData = rows |> Array.map (fun r -> r.[colIndex])
    VectorHelpers.createInferredTypeVector Vectors.ArrayVector.ArrayVectorBuilder.Instance colData)
  Some(Frame<_,_>(rowIndex, colIndex, Vector.ofValues data, IndexBuilder.Instance, VectorBuilder.Instance))

/// Convert R expression to a data frame and return frame of an
/// appropriate type, based on the values of the indices
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

/// Convert R expression to a data frame of the specified (expected) type
let tryCreateFrame (symExpr:SymbolicExpression) : option<Frame<'R, 'C>> =
  match R.as_data_frame(symExpr) with
  | RDotNet.ActivePatterns.DataFrame df ->
      match convertIndex df.RowNames, convertIndex df.ColumnNames with
      | Some rowIndex, Some colIndex -> constructFrame df rowIndex colIndex
      | _ -> None
  | _ -> None

// ------------------------------------------------------------------------------------------------
// Time series operations
// ------------------------------------------------------------------------------------------------

/// Try converting symbolic expression to a Zoo series.
/// This works for almost everything, but not quite (e.g. lambda functions)
let tryAsZooSeries (symExpr:SymbolicExpression) =
  if Array.exists ((=) "zoo") symExpr.Class then Some symExpr
  else 
    try Some(R.as_zoo(symExpr))
    with :? RDotNet.ParseException -> None

/// Try convert the keys of a specified zoo time series to DateTime
let tryGetDateTimeKeys (zoo:SymbolicExpression) fromDateTime =
  try
    R.strftime(R.index(zoo), "%Y-%m-%d %H:%M:%S").AsCharacter()
    |> Seq.map (fun v -> DateTime.ParseExact(v, "yyyy-MM-dd HH:mm:ss", invcult))
    |> Seq.map fromDateTime
    |> Some
  with :? RDotNet.ParseException | :? RDotNet.EvaluationException -> None

/// Try converting the specified symbolic expression to a time series
let tryCreateTimeSeries fromDateTime (symExpr:SymbolicExpression) : option<Series<'K, 'V>> = 
  tryAsZooSeries symExpr |> Option.bind (fun zoo ->
    // Format the keys as string and turn them into DateTimes
    let keys = tryGetDateTimeKeys zoo fromDateTime
    // If converting keys to datetime worked, return series
    keys |> Option.bind (fun keys ->
      let values = zoo.GetValue<'V[]>()
      Some(Series(keys, values)) ))

/// Try converting the specified symbolic expression to a series with arbitrary keys
let tryCreateSeries (symExpr:SymbolicExpression) : option<Series<'K, 'V>> = 
  tryAsZooSeries symExpr |> Option.map (fun zoo ->
    // Format the keys as string and turn them into DateTimes
    let keys = R.index(zoo).GetValue<'K[]>()
    let values = zoo.GetValue<'V[]>()
    Series(keys, values) )

/// Given symbolic expression, convert it to a time series.
/// Pick the most appropriate key/value type, based on the data.
let createDefaultSeries (symExpr:SymbolicExpression) = 
  tryAsZooSeries symExpr |> Option.bind (fun zoo ->
    let dateKeys = tryGetDateTimeKeys zoo id
    let index = R.index(zoo)
    match zoo, dateKeys, index with
    // First handle the case when keys are date times
    | IntegerVector ints, Some dateKeys, _ -> Some(box (Series(dateKeys, ints)))
    | NumericVector nums, Some dateKeys, _ -> Some(box (Series(dateKeys, nums)))
    | CharacterVector chars, Some dateKeys, _ -> Some(box (Series(dateKeys, chars)))
    // Convert the keys to integer
    | IntegerVector ints, _, IntegerVector keys -> Some(box (Series(keys, ints)))
    | NumericVector nums, _, IntegerVector keys -> Some(box (Series(keys, nums)))
    | CharacterVector chars, _, IntegerVector keys -> Some(box (Series(keys, chars)))
    // Convert the keys to float
    | IntegerVector ints, _, NumericVector keys -> Some(box (Series(keys, ints)))
    | NumericVector nums, _, NumericVector keys -> Some(box (Series(keys, nums)))
    | CharacterVector chars, _, NumericVector keys -> Some(box (Series(keys, chars)))
    // Convert the keys to string
    | IntegerVector ints, _, CharacterVector keys -> Some(box (Series(keys, ints)))
    | NumericVector nums, _, CharacterVector keys -> Some(box (Series(keys, nums)))
    | CharacterVector chars, _, CharacterVector keys -> Some(box (Series(keys, chars)))
    | _ -> None )
