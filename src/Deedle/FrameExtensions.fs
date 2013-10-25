#nowarn "10001"
namespace Deedle

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

open System
open System.IO
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open System.Collections.Generic

open Deedle.Keys
open Deedle.Vectors 

type Frame =
  // ----------------------------------------------------------------------------------------------
  // Reading CSV files
  // ----------------------------------------------------------------------------------------------
  

  /// Load data frame from a CSV file. The operation automatically reads column names from the 
  /// CSV file (if they are present) and infers the type of values for each column. Columns
  /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
  /// types (such as dates) are not converted automatically.
  ///
  /// ## Parameters
  ///
  ///  * `location` - Specifies a file name or an web location of the resource.
  ///  * `hasHeaders` - Specifies whether the input CSV file has header row
  ///     (when not set, the default value is `true`)
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
    ( location:string, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] skipTypeInference, [<Optional>] inferRows, 
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture) =
    use reader = new StreamReader(location)
    FrameUtils.readCsv 
      reader (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (Some (not skipTypeInference)) (Some inferRows) (Some schema) "NaN,NA,#N/A,:" 
      (if separators = null then None else Some separators) (Some culture)

  /// Load data frame from a CSV file. The operation automatically reads column names from the 
  /// CSV file (if they are present) and infers the type of values for each column. Columns
  /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
  /// types (such as dates) are not converted automatically.
  ///
  /// ## Parameters
  ///
  ///  * `stream` - Specifies the input stream, opened at the beginning of CSV data
  ///  * `hasHeaders` - Specifies whether the input CSV file has header row
  ///     (when not set, the default value is `true`)
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
    ( stream:Stream, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] skipTypeInference, [<Optional>] inferRows, 
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture) =
    FrameUtils.readCsv 
      (new StreamReader(stream)) (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (Some (not skipTypeInference)) (Some inferRows) (Some schema) "NaN,NA,#N/A,:" 
      (if separators = null then None else Some separators) (Some culture)

  // ----------------------------------------------------------------------------------------------
  // Creating from rows or from columns
  // ----------------------------------------------------------------------------------------------

  // Creates a data frame with ordinal Integer index from a sequence of rows.
  // The column indices of individual rows are unioned, so if a row has fewer
  // columns, it will be successfully added, but there will be missing values.

  // sequence of (series / kvps / kvps with object series)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(rows:seq<Series<'ColKey,'V>>) = 
    FrameUtils.fromColumns(Series(rows |> Seq.mapi (fun i _ -> i), rows))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(columns:seq<KeyValuePair<'ColKey, Series<'RowKey, 'V>>>) = 
    let colKeys = columns |> Seq.map (fun kvp -> kvp.Key)
    let colSeries = columns |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromColumns(Series(colKeys, colSeries))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(columns:seq<KeyValuePair<'ColKey, ObjectSeries<'RowKey>>>) = 
    let colKeys = columns |> Seq.map (fun kvp -> kvp.Key)
    let colSeries = columns |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromColumns(Series(colKeys, colSeries))

  // series of (series / object series)
  
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(cols:Series<'TColKey, ObjectSeries<'TRowKey>>) = 
    FrameUtils.fromColumns(cols)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(cols:Series<'TColKey, Series<'TRowKey, 'V>>) = 
    FrameUtils.fromColumns(cols)

  // sequence of series / sequence of kvps / sequence of kvps with object series

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<Series<'ColKey,'V>>) = 
    FrameUtils.fromRows(Series(rows |> Seq.mapi (fun i _ -> i), rows))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<KeyValuePair<'RowKey, Series<'ColKey, 'V>>>) = 
    let rowKeys = rows |> Seq.map (fun kvp -> kvp.Key)
    let rowSeries = rows |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromRows(Series(rowKeys, rowSeries))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<KeyValuePair<'RowKey, ObjectSeries<'ColKey>>>) = 
    let rowKeys = rows |> Seq.map (fun kvp -> kvp.Key)
    let rowSeries = rows |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromRows(Series(rowKeys, rowSeries))

  // series of (series / object series)
  
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:Series<'TColKey, ObjectSeries<'TRowKey>>) = 
    FrameUtils.fromRows(rows)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:Series<'TColKey, Series<'TRowKey, 'V>>) = 
    FrameUtils.fromRows(rows)

  // ----------------------------------------------------------------------------------------------
  // Creating frame from values or from records
  // ----------------------------------------------------------------------------------------------

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromValues(values, colSel:Func<_, _>, rowSel:Func<_, _>, valSel:Func<_, _>) =
    FrameUtils.fromValues values colSel.Invoke rowSel.Invoke valSel.Invoke

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromValues (values) =
    FrameUtils.fromValues values (fun (_, col, _) -> col) (fun (row, _, _) -> row) (fun (_, _, v) -> v)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRecords (series:Series<'K, 'R>) =
    let keyValuePairs = 
      seq { for k, v in Series.observationsAll series do 
              if v.IsSome then yield k, v.Value }
    let recordsToConvert = Seq.map snd keyValuePairs
    let frame = Reflection.convertRecordSequence<'R>(recordsToConvert)
    frame |> Frame.indexRowsWith (Seq.map fst keyValuePairs)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRecords (values:seq<'T>) =
    Reflection.convertRecordSequence<'T>(values)    

  // Also used from F#

  static member ReadReader (reader) =
    FrameUtils.readReader reader


  // ----------------------------------------------------------------------------------------------
  // Creating other frames
  // ----------------------------------------------------------------------------------------------


  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member CreateEmpty() =
    Frame<'R, 'C>(Index.ofKeys [], Index.ofKeys [], Vector.ofValues [])

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
  ///     series [ "k1" => 1; "k2" => 15 ]
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

  /// A function for constructing data frame from a sequence of name - column pairs.
  /// This provides a nicer syntactic sugar for `Frame.ofColumns`.
  ///
  /// ## Example
  /// To create a simple frame with two columns, you can write:
  /// 
  ///     frame [ "A" => series [ 1 => 30.0; 2 => 35.0 ]
  ///             "B" => series [ 1 => 30.0; 3 => 40.0 ] ]
  ///
  let frame columns = 
    let names, values = columns |> Array.ofSeq |> Array.unzip
    FrameUtils.fromColumns(Series(names, values))

  type Frame with
    // NOTE: When changing the parameters below, do not forget to update 'features.fsx'!

    /// Load data frame from a CSV file. The operation automatically reads column names from the 
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    ///
    /// ## Parameters
    ///
    ///  * `path` - Specifies a file name or an web location of the resource.
    ///  * `hasHeaders` - Specifies whether the input CSV file has header row
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
    static member ReadCsv(path:string, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators, ?culture) =
      use reader = new StreamReader(path)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema "NaN,NA,#N/A,:" separators culture

    /// Load data frame from a CSV file. The operation automatically reads column names from the 
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    ///
    /// ## Parameters
    ///
    ///  * `stream` - Specifies the input stream, opened at the beginning of CSV data
    ///  * `hasHeaders` - Specifies whether the input CSV file has header row
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
    static member ReadCsv(stream:Stream, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators, ?culture) =
      FrameUtils.readCsv (new StreamReader(stream)) hasHeaders inferTypes inferRows schema "NaN,NA,#N/A,:" separators culture
      
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
      Frame.FromValues(values)

    static member ofRecords (series:Series<'K, 'R>) =
      Frame.FromRecords(series)

    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)    

  type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
    /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    ///
    /// ## Parameters
    ///  - `stream` - Specifies the output stream where the CSV data should be written
    ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
    ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
    ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
    ///    TSV files and `,` for CSV files)
    ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
    ///
    /// [category:Input and output]
    member frame.SaveCsv(stream:Stream, ?includeRowKeys, ?keyNames, ?separator, ?culture) = 
      FrameUtils.writeCsv (new StreamWriter(stream)) None separator culture includeRowKeys keyNames frame

    /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    ///
    /// ## Parameters
    ///  - `path` - Specifies the output file name where the CSV data should be written
    ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
    ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
    ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
    ///    TSV files and `,` for CSV files)
    ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
    ///
    /// [category:Input and output]
    member frame.SaveCsv(path:string, ?includeRowKeys, ?keyNames, ?separator, ?culture) = 
      use writer = new StreamWriter(path)
      FrameUtils.writeCsv writer (Some path) separator culture includeRowKeys keyNames frame

    /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    ///
    /// ## Parameters
    ///  - `path` - Specifies the output file name where the CSV data should be written
    ///  - `keyNames` - Specifies the CSV headers for row key (or keys, for multi-level index)
    ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
    ///    TSV files and `,` for CSV files)
    ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
    ///
    /// [category:Input and output]
    member frame.SaveCsv(path:string, keyNames) = 
      use writer = new StreamWriter(path)
      FrameUtils.writeCsv writer (Some path) None None (Some true) (Some keyNames) frame

    member frame.ToDataTable(rowKeyNames) = 
      FrameUtils.toDataTable rowKeyNames frame

module FrameBuilder =
  type Columns<'R, 'C when 'C : equality and 'R : equality>() = 
    let mutable series = []
    member x.Add(key:'C, value:ISeries<'R>) =
      series <- (key, value)::series
    member x.Frame = Frame.ofColumns series
    interface System.Collections.IEnumerable with
      member x.GetEnumerator() = (x :> seq<_>).GetEnumerator() :> Collections.IEnumerator
    interface seq<KeyValuePair<'C, ISeries<'R>>> with
      member x.GetEnumerator() = 
        (series |> List.rev |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()

  type Rows<'R, 'C when 'C : equality and 'R : equality>() = 
    let mutable series = []
    member x.Add(key:'R, value:ISeries<'C>) =
      series <- (key, value)::series
    member x.Frame = Frame.ofRows series
    interface System.Collections.IEnumerable with
      member x.GetEnumerator() = (x :> seq<_>).GetEnumerator() :> Collections.IEnumerator
    interface seq<KeyValuePair<'R, ISeries<'C>>> with
      member x.GetEnumerator() = 
        (series |> List.rev |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()


type KeyValue =
  static member Create<'K, 'V>(key:'K, value:'V) = KeyValuePair(key, value)


/// Some comment
///
/// ## Index manipulation
/// Summary 1
///
/// ## Input and output
/// Summary 2
///
/// ## Missing values
/// Summary 3
[<Extension>]
type FrameExtensions =
  // ----------------------------------------------------------------------------------------------
  // Index manipulation
  // ----------------------------------------------------------------------------------------------

  /// Align the existing data to a specified collection of row keys. Values in the data frame
  /// that do not match any new key are dropped, new keys (that were not in the original data 
  /// frame) are assigned missing values.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame that is to be realigned.
  ///  - `keys` - A sequence of new row keys. The keys must have the same type as the original
  ///    frame keys (because the rows are realigned).
  ///
  /// [category:Index manipulation]
  [<Extension>]
  static member RealignRows(frame:Frame<'R, 'C>, keys) = 
    frame |> Frame.realignRows keys

  /// Replace the row index of the frame with ordinarilly generated integers starting from zero.
  /// The rows of the frame are assigned index according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///
  /// [category:Index manipulation]
  [<Extension>]
  static member IndexRowsOrdinally(frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame |> Frame.indexRowsOrdinally

  /// Replace the row index of the frame with the provided sequence of row keys.
  /// The rows of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///  - `keys` - A collection of new row keys.
  ///
  /// [category:Index manipulation]
  [<Extension>]
  static member IndexRowsWith(frame:Frame<'R, 'C>, keys:seq<'TNewRowIndex>) =
    frame |> Frame.indexRowsWith keys

  /// Replace the column index of the frame with the provided sequence of column keys.
  /// The columns of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current column index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose column index are to be replaced.
  ///  - `keys` - A collection of new column keys.
  ///
  /// [category:Index manipulation]
  [<Extension>]
  static member IndexColumnsWith(frame:Frame<'R, 'C>, keys:seq<'TNewRowIndex>) =
    frame |> Frame.indexColsWith keys

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Index manipulation]
  [<Extension>]
  static member OrderRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.orderRows frame

  /// Returns a data frame that contains the same data as the input, 
  /// but whose columns are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Index manipulation]
  [<Extension>]
  static member OrderColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.orderCols frame

  /// Returns a transposed data frame. The rows of the original data frame are used as the
  /// columns of the new one (and vice versa). Use this operation if you have a data frame
  /// and you mostly need to access its rows as a series (because accessing columns as a 
  /// series is more efficient).
  /// 
  /// ## Parameters
  ///  - `frame` - Source data frame to be transposed.
  /// 
  /// [category:Index manipulation]
  [<Extension>]
  static member Transpose(frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> Frame.ofRows

  // ----------------------------------------------------------------------------------------------
  // Input and output
  // ----------------------------------------------------------------------------------------------

  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  ///
  /// ## Parameters
  ///  - `stream` - Specifies the output stream where the CSV data should be written
  ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
  ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
  ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
  ///    TSV files and `,` for CSV files)
  ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
  ///
  /// [category:Input and output]
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, stream:Stream, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) = 
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    FrameUtils.writeCsv (new StreamWriter(stream)) None separator culture (Some includeRowKeys) keyNames frame

  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  ///
  /// ## Parameters
  ///  - `path` - Specifies the output file name where the CSV data should be written
  ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
  ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
  ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
  ///    TSV files and `,` for CSV files)
  ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
  ///
  /// [category:Input and output]
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, path:string, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) = 
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    use writer = new StreamWriter(path)
    FrameUtils.writeCsv writer (Some path) separator culture (Some includeRowKeys) keyNames frame

  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  ///
  /// ## Parameters
  ///  - `path` - Specifies the output file name where the CSV data should be written
  ///  - `keyNames` - Specifies the CSV headers for row key (or keys, for multi-level index)
  ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
  ///    TSV files and `,` for CSV files)
  ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
  ///
  /// [category:Input and output]
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, path:string, keyNames, [<Optional>] separator, [<Optional>] culture) = 
    use writer = new StreamWriter(path)
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    FrameUtils.writeCsv writer (Some path) separator culture (Some true) (Some keyNames) frame

  [<Extension>]
  static member ToDataTable(frame:Frame<'R, 'C>, rowKeyNames) = 
      FrameUtils.toDataTable rowKeyNames frame

  // ----------------------------------------------------------------------------------------------
  // Assorted stuff
  // ----------------------------------------------------------------------------------------------

  [<Extension>]
  static member Sum(frame:Frame<'R, 'C>) = Frame.sum frame

  [<Extension>]
  static member Window(frame:Frame<'R, 'C>, size) = Frame.window size frame

  [<Extension>]
  static member Window(frame:Frame<'R, 'C>, size, aggregate:Func<_, _>) = 
    Frame.windowInto size aggregate.Invoke frame

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  [<Extension>]
  static member CountRows(frame:Frame<'R, 'C>) = frame.RowIndex.Mappings |> Seq.length

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  [<Extension>]
  static member CountColumns(frame:Frame<'R, 'C>) = frame.ColumnIndex.Mappings |> Seq.length

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
  static member Where(frame:Frame<'TRowKey, 'TColumnKey>, condition:Func<_, _>) = 
    frame.Rows.Where(condition) |> Frame.ofRows

  /// Filters frame rows using the specified condtion. Returns a new data frame
  /// that contains rows for which the provided function returned false. The function
  /// is called with `KeyValuePair` containing the row key as the `Key` and `Value`
  /// gives access to the row series and a row index.
  ///
  /// ## Parameters
  ///
  ///  * `frame` - A data frame to invoke the filtering function on.
  ///  * `condition` - A delegate that specifies the filtering condition.
  [<Extension>]
  static member Where(frame:Frame<'TRowKey, 'TColumnKey>, condition:Func<_, _, _>) = 
    frame.Rows.Where(condition) |> Frame.ofRows

  [<Extension>]
  static member Select(frame:Frame<'TRowKey, 'TColumnKey>, projection:Func<_, _>) = 
    frame.Rows.Select(projection) |> Frame.ofRows

  [<Extension>]
  static member Select(frame:Frame<'TRowKey, 'TColumnKey>, projection:Func<_, _, _>) = 
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
  static member Shift(frame:Frame<'TRowKey, 'TColumnKey>, offset) = 
    frame |> Frame.shift offset

  [<Extension>]
  static member Diff(frame:Frame<'TRowKey, 'TColumnKey>, offset) = 
    frame.SeriesApply<float>(false, fun s -> Series.diff offset s :> ISeries<_>)

  [<Extension>]
  static member Reduce(frame:Frame<'TRowKey, 'TColumnKey>, aggregation:Func<'T, 'T, 'T>) = 
    frame |> Frame.reduce (fun a b -> aggregation.Invoke(a, b))


  // hierarchical indexing 

  [<Extension>]
  static member Flatten(frame:Frame<'R1, 'C1>, keySelector:Func<_, 'K>, valueSelector:Func<_, 'V>) =
    frame |> Frame.flatten keySelector.Invoke valueSelector.Invoke

  [<Extension>]
  static member FlattenRows(frame:Frame<'R, 'C>, keySelector:Func<'R, 'K>, op:Func<_, 'V>) =
    frame |> Frame.flattenRows keySelector.Invoke op.Invoke

  /// [category:Fancy accessors]
  [<Extension>]
  static member GetRows(frame:Frame<'TRowKey, 'TColumnKey>, [<ParamArray>] rowKeys:_[]) = 
    frame.Rows.GetItems(rowKeys) |> Frame.ofRows

  [<Extension>]
  static member GetRowsAt(frame:Frame<'TRowKey, 'TColumnKey>, [<ParamArray>] indices:int[]) = 
    let keys = indices |> Array.map frame.Rows.GetKeyAt
    let values = indices |> Array.map (fun i -> frame.Rows.GetAt(i) :> ISeries<_>)
    Seq.zip keys values |> Frame.ofRows

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

  // ----------------------------------------------------------------------------------------------
  // Missing values
  // ----------------------------------------------------------------------------------------------

  /// Fill missing values of a given type in the frame with a constant value.
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the provided filling value. The operation does not attempt to 
  /// convert between numeric values (so a series containing `float` will not be
  /// converted to a series of `int`).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `value` - A constant value that is used to fill all missing values
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, value:'T) = 
    Frame.fillMissingWith value frame

  /// Fill missing values in the data frame with the nearest available value
  /// (using the specified direction). Note that the frame may still contain
  /// missing values after call to this function (e.g. if the first value is not available
  /// and we attempt to fill series with previous values). This operation can only be
  /// used on ordered frames.
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, direction) = 
    Frame.fillMissing direction frame

  /// Fill missing values in the frame using the specified function. The specified
  /// function is called with all series and keys for which the frame does not 
  /// contain value and the result of the call is used in place of the missing value.
  ///
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the return type of the provided filling function. The operation 
  /// does not attempt to convert between numeric values (so a series containing 
  /// `float` will not be converted to a series of `int`).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `f` - A function that takes a series `Series<R, T>` together with a key `K` 
  ///    in the series and generates a value to be used in a place where the original 
  ///    series contains a missing value.
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, f:Func<_, _, 'T>) = 
    Frame.fillMissingUsing (fun s k -> f.Invoke(s, k)) frame

  /// Creates a new data frame that contains only those rows of the original 
  /// data frame that are _dense_, meaning that they have a value for each column.
  /// The resulting data frame has the same number of columns, but may have 
  /// fewer rows (or no rows at all).
  /// 
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filtered
  ///
  /// [category:Missing values]
  [<Extension>]
  static member DropSparseRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropSparseRows frame

  /// Creates a new data frame that contains only those columns of the original 
  /// data frame that are _dense_, meaning that they have a value for each row.
  /// The resulting data frame has the same number of rows, but may have 
  /// fewer columns (or no columns at all).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filtered
  ///
  /// [category:Missing values]
  [<Extension>]
  static member DropSparseColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropSparseCols frame
  