#nowarn "10001"
namespace FSharp.DataFrame

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

open System
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open System.Collections.Generic

open FSharp.DataFrame.Keys
open FSharp.DataFrame.Vectors 

type Frame =
  /// Load data frame from a CSV file. The operation automatically reads column names from the 
  /// CSV file (if they are present) and infers the type of values for each column. Columns
  /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
  /// types (such as dates) are not converted automatically.
  ///
  /// ## Parameters
  ///
  ///  * `location` - Specifies a file name or an web location of the resource.
  ///  * `skipTypeInference` - Specifies whether the method should skip inferring types
  ///    of columns automatically (when set to `true` you need to provide explicit `schema`)
  ///  * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
  ///    rows to use for type inference. The default value is 0, meaninig all rows.
  ///  * `schema` - A string that specifies CSV schema. See the documentation for 
  ///    information about the schema format.
  ///  * `separators` - A string that specifies one or more (single character) separators
  ///    that are used to separate columns in the CSV file. Use for example `";"` to 
  ///    parse semicolon separated files.
  ///  * `culture` - Specifies the name of the culture that is used when parsing 
  ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member ReadCsv
    ( location:string, [<Optional>] skipTypeInference, [<Optional>] inferRows, 
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture) =
    FrameUtils.readCsv 
      location (Some (not skipTypeInference)) (Some inferRows) (Some schema) "NaN,NA,#N/A,:" 
      (if separators = null then None else Some separators) (Some culture)

  /// Creates a data frame with ordinal Integer index from a sequence of rows.
  /// The column indices of individual rows are unioned, so if a row has fewer
  /// columns, it will be successfully added, but there will be missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<Series<'ColKey,'V>>) = 
    FrameUtils.fromRows(Series(rows |> Seq.mapi (fun i _ -> i), rows))

//  static member FromColumns(cols) = 
//    FrameUtils.fromColumns(cols)

  /// Creates a data frame with ordinal Integer index from a sequence of rows.
  /// The column indices of individual rows are unioned, so if a row has fewer
  /// columns, it will be successfully added, but there will be missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(keys:seq<'RowKey>, columns:seq<KeyValuePair<'ColKey, Series<'RowKey, 'V>>>) = 
    let rowIndex = FrameUtils.indexBuilder.Create(keys, None)
    let colIndex = FrameUtils.indexBuilder.Create([], None)
    let df = Frame<_, _>(rowIndex, colIndex, FrameUtils.vectorBuilder.Create [||])
    let other = Frame.FromColumns(columns)
    df.Join(other, kind=JoinKind.Left)

  // TODO: Add the above to F# API

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns<'RowKey,'ColKey, 'V when 'RowKey: equality and 'ColKey: equality>(cols:seq<KeyValuePair<'ColKey, Series<'RowKey, 'V>>>) = 
    let colKeys = cols |> Seq.map (fun kvp -> kvp.Key)
    let colSeries = cols |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromColumns(Series(colKeys, colSeries))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRowKeys<'K when 'K : equality>(keys:seq<'K>) =
    let rowIndex = FrameUtils.indexBuilder.Create(keys, None)
    let colIndex = FrameUtils.indexBuilder.Create([], None)
    Frame<_, string>(rowIndex, colIndex, FrameUtils.vectorBuilder.Create [||])

[<AutoOpen>]
module FSharpFrameExtensions =

  /// Custom operator that can be used when constructing series from observations
  /// or frames from key-row or key-column pairs. The operator simply returns a 
  /// tuple, but it provides a more convenient syntax. For example:
  ///
  ///     Series.ofObservations [ "k1" => 1; "k2" => 15 ]
  ///
  let (=>) a b = a, b

  let (=?>) a (b:ISeries<_>) = a, b
  
  /// Custom operator that can be used for applying fuction to all elements of 
  /// a series. This provides a nicer syntactic sugar for the `Series.mapValues` 
  /// function. For example:
  ///
  ///     // Given a float series and a function on floats
  ///     let s1 = Series.ofValues [ 1.0 .. 10.0 ]
  ///     let adjust v = max 10.0 v
  ///
  ///     // Apply "adjust (v + v)" to all elements
  ///     adjust $ (s1 + s1)
  ///
  let ($) f series = Series.mapValues f series

  type Frame with
    // NOTE: When changing the parameters below, do not forget to update 'features.fsx'!

    /// Load data frame from a CSV file. The operation automatically reads column names from the 
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    ///
    /// ## Parameters
    ///
    ///  * `location` - Specifies a file name or an web location of the resource.
    ///  * `inferTypes` - Specifies whether the method should attempt to infer types
    ///    of columns automatically (set this to `false` if you want to specify schema)
    ///  * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
    ///    rows to use for type inference. The default value is 0, meaninig all rows.
    ///  * `schema` - A string that specifies CSV schema. See the documentation for 
    ///    information about the schema format.
    ///  * `separators` - A string that specifies one or more (single character) separators
    ///    that are used to separate columns in the CSV file. Use for example `";"` to 
    ///    parse semicolon separated files.
    ///  * `culture` - Specifies the name of the culture that is used when parsing 
    ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
    static member readCsv(location:string, ?inferTypes, ?inferRows, ?schema, ?separators, ?culture) =
      FrameUtils.readCsv location inferTypes inferRows schema "NaN,NA,#N/A,:" separators culture

    /// Creates a data frame with ordinal Integer index from a sequence of rows.
    /// The column indices of individual rows are unioned, so if a row has fewer
    /// columns, it will be successfully added, but there will be missing values.
    static member ofRowsOrdinal(rows:seq<#Series<_, _>>) = 
      FrameUtils.fromRows(Series(rows |> Seq.mapi (fun i _ -> i), rows))

    static member ofRows(rows:seq<_ * #ISeries<_>>) = 
      let names, values = rows |> List.ofSeq |> List.unzip
      FrameUtils.fromRows(Series(names, values))

    static member ofRows(rows) = 
      FrameUtils.fromRows(rows)

    static member ofRowKeys(keys) = 
      Frame.FromRowKeys(keys)
    
    static member ofColumns(cols) = 
      FrameUtils.fromColumns(cols)

    static member ofColumns(cols:seq<_ * #ISeries<'K>>) = 
      let names, values = cols |> List.ofSeq |> List.unzip
      FrameUtils.fromColumns(Series(names, values))
    
    static member ofValues(values) =
      values 
      |> Seq.groupBy (fun (row, col, value) -> col)
      |> Seq.map (fun (col, items) -> 
          let keys, _, values = Array.ofSeq items |> Array.unzip3
          col, Series(keys, values) )
      |> Frame.ofColumns

    static member ofRecords (series:Series<'K, 'R>) =
      let keyValuePairs = 
        seq { for k, v in Series.observationsAll series do 
                if v.IsSome then yield k, v.Value }
      let recordsToConvert = Seq.map snd keyValuePairs
      let frame = Reflection.convertRecordSequence<'R>(recordsToConvert)
      let frame = frame.ReplaceRowIndexKeys(Seq.map fst keyValuePairs)
      frame.ReindexRowKeys(series.Keys)

    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)    


  type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
    member frame.Append(rowKey, row) = frame.Append(Frame.ofRows [ rowKey => row ])
    member frame.WithMissing(value) = Frame.withMissingVal value frame
    member frame.WithColumnIndex(columnKeys:seq<'TNewColumnKey>) = Frame.renameCols columnKeys frame
    member frame.WithRowIndex<'TNewRowIndex when 'TNewRowIndex : equality>(col) : Frame<'TNewRowIndex, _> = 
      Frame.indexRows col frame

    // Grouping
    member frame.GroupRowsBy<'TGroup when 'TGroup : equality>(key) =
      frame.Rows 
      |> Series.groupInto (fun _ v -> v.GetAs<'TGroup>(key)) (fun k g -> g |> Frame.ofRows)      
      |> Frame.collapseRows

    member frame.GroupRowsInto<'TGroup when 'TGroup : equality>(key, f:System.Func<_, _, _>) =
      frame.Rows 
      |> Series.groupInto (fun _ v -> v.GetAs<'TGroup>(key)) (fun k g -> f.Invoke(k, g |> Frame.ofRows))
      |> Frame.collapseRows

    member frame.GroupRowsUsing<'TGroup when 'TGroup : equality>(f:System.Func<_, _, 'TGroup>) =
      frame.Rows 
      |> Series.groupInto (fun k v -> f.Invoke(k, v)) (fun k g -> g |> Frame.ofRows)
      |> Frame.collapseRows


[<Extension>]
type FrameExtensions =
  /// Filters frame rows using the specified condtion. Returns a new data frame
  /// that contains rows for which the provided function returned false. The function
  /// is called with `KeyValuePair` containing the row key as the `Key` and `Value`
  /// gives access to the row series.
  ///
  /// ## Parameters
  ///
  ///  * `frame` - A data frame to invoke the filtering function on.
  ///  * `condition` - A delegate that specifies the filtering condition.
  [<Extension>]
  static member Where(frame:Frame<'TRowKey, 'TColumnKey>, condition) = 
    frame.Rows.Where(condition) |> Frame.ofRows

  [<Extension>]
  static member Select(frame:Frame<'TRowKey, 'TColumnKey>, projection) = 
    frame.Rows.Select(projection) |> Frame.ofRows

  [<Extension>]
  static member SelectRowKeys(frame:Frame<'TRowKey, 'TColumnKey>, projection) = 
    frame.Rows.SelectKeys(projection) |> Frame.ofRows

  [<Extension>]
  static member SelectColumnKeys(frame:Frame<'TRowKey, 'TColumnKey>, projection) = 
    frame.Columns.SelectKeys(projection) |> Frame.ofColumns

  [<Extension>]
  static member Append(frame:Frame<'TRowKey, 'TColumnKey>, rowKey, row) = 
    frame.Append(Frame.ofRows [ rowKey => row ])

  [<Extension>]
  static member OrderRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.orderRows frame

  [<Extension>]
  static member OrderColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.orderCols frame

  [<Extension>]
  static member Transpose(frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> Frame.ofRows


  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:ColumnSeries<'TRowKey, 'TColKey1 * 'TColKey2>, lo1:option<'TColKey1>, hi1:option<'TColKey1>, lo2:option<'TColKey2>, hi2:option<'TColKey2>) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Option.map box lo2|]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:ColumnSeries<'TRowKey, 'TColKey1 * 'TColKey2>, lo1:option<'TColKey1>, hi1:option<'TColKey1>, k2:'TColKey2) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Some (box k2) |]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:ColumnSeries<'TRowKey, 'TColKey1 * 'TColKey2>, k1:'TColKey1, lo2:option<'TColKey2>, hi2:option<'TColKey2>) =
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Some (box k1); Option.map box lo2|]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:ColumnSeries<'TRowKey, 'TColKey1 * 'TColKey2>, lo1:option<'K1>, hi1:option<'K1>, lo2:option<'K2>, hi2:option<'K2>) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Option.map box lo2|]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:RowSeries<'TRowKey1 * 'TRowKey2, 'TColKey>, lo1:option<'TRowKey1>, hi1:option<'TRowKey1>, lo2:option<'TRowKey2>, hi2:option<'TRowKey2>) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Option.map box lo2|]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:RowSeries<'TRowKey1 * 'TRowKey2, 'TColKey>, lo1:option<'TRowKey1>, hi1:option<'TRowKey1>, k2:'TRowKey2) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Some (box k2) |]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:RowSeries<'TRowKey1 * 'TRowKey2, 'TColKey>, k1:'TRowKey1, lo2:option<'TRowKey2>, hi2:option<'TRowKey2>) =
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Some (box k1); Option.map box lo2|]

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:RowSeries<'TRowKey1 * 'TRowKey2, 'TColKey>, lo1:option<'K1>, hi1:option<'K1>, lo2:option<'K2>, hi2:option<'K2>) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel <| SimpleLookup [|Option.map box lo1; Option.map box lo2|]

type KeyValue =
  static member Create<'K, 'V>(key:'K, value:'V) = KeyValuePair(key, value)