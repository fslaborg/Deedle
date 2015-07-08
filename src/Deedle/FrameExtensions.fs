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
open FSharp.Data.Runtime
open Deedle.Keys
open Deedle.Vectors 
open FSharp.Data

/// Provides static methods for creating frames, reading frame data
/// from CSV files and database (via IDataReader). The type also provides
/// global configuration for reflection-based expansion.
///
/// [category:Frame and series operations]
type Frame =

  // ----------------------------------------------------------------------------------------------
  // Configuration
  // ----------------------------------------------------------------------------------------------
  
  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection specifies additional primitive (but reference)
  /// types that should not be expaneded. By default, this includes DateTime, string, etc.
  ///
  /// [category:Configuration]
  static member NonExpandableTypes = Reflection.additionalPrimitiveTypes

  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection specifies interfaces whose implementations
  /// should not be expanded. By default, this includes collections such as IList.
  ///
  /// [category:Configuration]
  static member NonExpandableInterfaces = Reflection.nonFlattenedTypes

  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection lets you specify custom expansion behavior
  /// for any type. This is a dictionary with types as keys and functions that implement the
  /// expansion as values.
  ///
  /// ## Example
  /// For example, say you have a type `MyPair` with propreties `Item1` of type `int` and
  /// `Item2` of type `string` (and perhaps other properties which makes the default behavior
  /// inappropriate). You can register custom expander as:
  ///
  ///     Frame.CustomExpanders.Add(typeof<MyPair>, fun v -> 
  ///       let a = v :?> MyPair
  ///       [ "First", typeof<int>, box a.Item1; 
  ///         "Second", typeof<string>, box a.Item2 ] :> seq<_> )
  ///
  /// [category:Configuration]
  static member CustomExpanders = Reflection.customExpanders

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
  ///  * `inferTypes` - Specifies whether the method should attempt to infer types
  ///    of columns automatically (set this to `false` if you want to specify schema)
  ///  * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
  ///    rows to use for type inference. The default value is 100.
  ///  * `schema` - A string that specifies CSV schema. See the documentation for 
  ///    information about the schema format.
  ///  * `separators` - A string that specifies one or more (single character) separators
  ///    that are used to separate columns in the CSV file. Use for example `";"` to 
  ///    parse semicolon separated files.
  ///  * `culture` - Specifies the name of the culture that is used when parsing 
  ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
  ///  * `maxRows` - Specifies the maximum number of rows that will be read from the CSV file
  ///
  /// [category:Input and output]
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member ReadCsv
    ( location:string, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] inferTypes:Nullable<bool>, [<Optional>] inferRows:Nullable<int>,
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture, [<Optional>] maxRows:Nullable<int>,
      [<Optional>] missingValues ) =
    use reader = new StreamReader(location)
    FrameUtils.readCsv 
      reader 
      (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (if inferTypes.HasValue then Some inferTypes.Value else None)
      (if inferRows.HasValue then Some inferRows.Value else None) 
      (Some schema) (Some missingValues)
      (if separators = null then None else Some separators) (Some culture)
      (if maxRows.HasValue then Some maxRows.Value else None)


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
  ///  * `inferTypes` - Specifies whether the method should attempt to infer types
  ///    of columns automatically (set this to `false` if you want to specify schema)
  ///  * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
  ///    rows to use for type inference. The default value is 100.
  ///  * `schema` - A string that specifies CSV schema. See the documentation for 
  ///    information about the schema format.
  ///  * `separators` - A string that specifies one or more (single character) separators
  ///    that are used to separate columns in the CSV file. Use for example `";"` to 
  ///    parse semicolon separated files.
  ///  * `culture` - Specifies the name of the culture that is used when parsing 
  ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
  ///  * `maxRows` - The maximal number of rows that should be read from the CSV file.
  ///  * `missingValues` - An array of strings that contains values which should be treated
  ///    as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".
  ///
  /// [category:Input and output]
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member ReadCsv
    ( stream:Stream, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] inferTypes:Nullable<bool>, [<Optional>] inferRows:Nullable<int>, 
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture, [<Optional>] maxRows:Nullable<int>,
      [<Optional>] missingValues) =
    FrameUtils.readCsv 
      (new StreamReader(stream)) 
      (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (if inferTypes.HasValue then Some inferTypes.Value else None)
      (if inferRows.HasValue then Some inferRows.Value else None) 
      (Some schema) (Some missingValues) 
      (if separators = null then None else Some separators) (Some culture)
      (if maxRows.HasValue then Some maxRows.Value else None)

  // Note: The following is also used from F#

  /// Read data from `IDataReader`. The method reads all rows from the data reader
  /// and for each row, gets all the columns. When a value is `DBNull`, it is treated
  /// as missing. The types of created vectors are determined by the field types reported
  /// by the data reader.
  /// 
  /// [category:Input and output]
  static member ReadReader (reader) =
    FrameUtils.readReader reader

  // ----------------------------------------------------------------------------------------------
  // Creating from rows or from columns
  // ----------------------------------------------------------------------------------------------

  // Creates a data frame with ordinal Integer index from a sequence of rows.
  // The column indices of individual rows are unioned, so if a row has fewer
  // columns, it will be successfully added, but there will be missing values.

  // sequence of (series / kvps / kvps with object series)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(cols:seq<Series<'ColKey,'V>>) = 
    FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance (Series(cols |> Seq.mapi (fun i _ -> i), cols))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(columns:seq<KeyValuePair<'ColKey, Series<'RowKey, 'V>>>) = 
    let colKeys = columns |> Seq.map (fun kvp -> kvp.Key)
    let colSeries = columns |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance (Series(colKeys, colSeries))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(columns:seq<KeyValuePair<'ColKey, ObjectSeries<'RowKey>>>) = 
    let colKeys = columns |> Seq.map (fun kvp -> kvp.Key)
    let colSeries = columns |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance (Series(colKeys, colSeries))

  // series of (series / object series)
  
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(cols:Series<'TColKey, ObjectSeries<'TRowKey>>) = 
    FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance cols

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromColumns(cols:Series<'TColKey, Series<'TRowKey, 'V>>) = 
    FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance cols

  // sequence of series / sequence of kvps / sequence of kvps with object series

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<Series<'ColKey,'V>>) = 
    FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(rows |> Seq.mapi (fun i _ -> i), rows))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<KeyValuePair<'RowKey, Series<'ColKey, 'V>>>) = 
    let rowKeys = rows |> Seq.map (fun kvp -> kvp.Key)
    let rowSeries = rows |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(rowKeys, rowSeries))

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:seq<KeyValuePair<'RowKey, ObjectSeries<'ColKey>>>) = 
    let rowKeys = rows |> Seq.map (fun kvp -> kvp.Key)
    let rowSeries = rows |> Seq.map (fun kvp -> kvp.Value)
    FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(rowKeys, rowSeries))

  // series of (series / object series)
  
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:Series<'TColKey, ObjectSeries<'TRowKey>>) = 
    FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance rows

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRows(rows:Series<'TColKey, Series<'TRowKey, 'V>>) = 
    FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance rows

  // ----------------------------------------------------------------------------------------------
  // Creating frame from values, records or from 2D array
  // ----------------------------------------------------------------------------------------------

  /// Create a data frame from a sequence of objects and functions that return
  /// row key, column key and value for each object in the input sequence.
  ///
  /// ## Parameters
  ///  - `values` - Input sequence of objects 
  ///  - `colSel` - A function that returns the column key of an object
  ///  - `rowSel` - A function that returns the row key of an object
  ///  - `valSel` - A function that returns the value of an object
  ///
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromValues(values:seq<'T>, colSel:Func<_, 'C>, rowSel:Func<_, 'R>, valSel:Func<_, 'V>) =
    FrameUtils.fromValues values colSel.Invoke rowSel.Invoke valSel.Invoke

  /// Create a data frame from a sequence of tuples containing row key, column key and a value
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromValues (values) =
    FrameUtils.fromValues values (fun (_, col, _) -> col) (fun (row, _, _) -> row) (fun (_, _, v) -> v)

  /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
  /// over the specified type parameter `'T` and turns its properties to columns.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRecords (series:Series<'K, 'R>) =
    let keyValuePairs = 
      seq { for k, v in Series.observationsAll series do 
              if v.IsSome then yield k, v.Value }
    let recordsToConvert = Seq.map snd keyValuePairs
    let frame = Reflection.convertRecordSequence<'R>(recordsToConvert)
    frame |> Frame.indexRowsWith (Seq.map fst keyValuePairs)

  /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
  /// over the specified type parameter `'T` and turns its properties to columns. The
  /// rows of the resulting frame are automatically indexed by `int`.
  ///
  /// ## Example
  /// The method can be nicely used to create a data frame using C# anonymous types
  /// (the result is a data frame with columns "A" and "B" containing two rows).
  ///
  ///    [lang=csharp]
  ///    var df = Frame.FromRecords(new[] {
  ///      new { A = 1, B = "Test" },
  ///       new { A = 2, B = "Another"}
  ///    });
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRecords (values:seq<'T>) =
    Reflection.convertRecordSequence<'T>(values)    

  /// Create data frame from a 2D array of values. The first dimension of the array
  /// is used as rows and the second dimension is treated as columns. Rows and columns
  /// of the returned frame are indexed with the element's offset in the array.
  ///
  /// ## Parameters
  ///  - `array` - A two-dimensional array to be converted into a data frame
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromArray2D(array:'T[,]) =
    // Generate row index (int offsets) and column index (int offsets)
    let rowIndex = IndexBuilder.Instance.Create(Array.init (array.GetLength(0)) id, Some true)
    let colIndex = IndexBuilder.Instance.Create(Array.init (array.GetLength(1)) id, Some true)
    // Generate vectors with column-based data
    let vectors = Array.zeroCreate (array.GetLength(1))
    for c = 0 to vectors.Length - 1 do
      let col = Array.init (array.GetLength(0)) (fun r -> array.[r,c])
      vectors.[c] <- VectorBuilder.Instance.Create(col) :> IVector
    let data = VectorBuilder.Instance.Create(vectors)
    Frame(rowIndex, colIndex, data, IndexBuilder.Instance, VectorBuilder.Instance)

  // ----------------------------------------------------------------------------------------------
  // Creating other frames
  // ----------------------------------------------------------------------------------------------

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member CreateEmpty() =
    Frame<'R, 'C>(Index.ofKeys [], Index.ofKeys [], Vector.ofValues [], IndexBuilder.Instance, VectorBuilder.Instance)

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRowKeys<'K when 'K : equality>(keys:seq<'K>) =
    let rowIndex = FrameUtils.indexBuilder.Create(keys, None)
    let colIndex = FrameUtils.indexBuilder.Create([], None)
    Frame<_, string>(rowIndex, colIndex, FrameUtils.vectorBuilder.Create [||], IndexBuilder.Instance, VectorBuilder.Instance)


/// This module contains F# functions and extensions for working with frames. This
/// includes operations for creating frames such as the `frame` function, `=>` operator
/// and `Frame.ofRows`, `Frame.ofColumns` and `Frame.ofRowKeys` functions. The module
/// also provides additional F# extension methods including `ReadCsv`, `SaveCsv` and `PivotTable`.
///
/// ## Frame construction
/// The functions and methods in this group can be used to create frames. If you are creating
/// a frame from a number of sample values, you can use `frame` and the `=>` operator (or the
/// `=?>` opreator which is useful if you have multiple series of distinct types):
///
///     frame [ "Column 1" => series [ 1 => 1.0; 2 => 2.0 ]
///             "Column 2" => series [ 3 => 3.0 ] ]
///
/// Aside from this, the various type extensions let you write `Frame.ofXyz` to construct frames
/// from data in various formats - `Frame.ofRows` and `Frame.ofColumns` create frame from a series
/// or a sequence of rows or columns; `Frame.ofRecords` creates a frame from .NET objects using
/// Reflection and `Frame.ofRowKeys` creates an empty frame with the specified keys.
///
/// ## Frame operations
/// The group contains two overloads of the F#-friendly version of the `PivotTable` method.
///
/// ## Input and output
/// This group of extensions includes a number of overloads for the `ReadCsv` and `SaveCsv` 
/// methods. The methods here are designed to be used from F# and so they are F#-style extensions
/// and they use F#-style optional arguments. In general, the overlads take either a path or
/// `TextReader`/`TextWriter`. Also note that `ReadCsv<'R>(path, indexCol, ...)` lets you specify
/// the column to be used as the index.
///
/// [category:Frame and series operations]
[<AutoOpen>]
module ``F# Frame extensions`` =

  /// Custom operator that can be used when constructing series from observations
  /// or frames from key-row or key-column pairs. The operator simply returns a 
  /// tuple, but it provides a more convenient syntax. For example:
  ///
  ///     series [ "k1" => 1; "k2" => 15 ]
  ///
  /// [category:Frame construction]
  let (=>) a b = a, b

  /// Custom operator that can be used when constructing a frame from observations
  /// of series. The operator simply returns a tuple, but it upcasts the series 
  /// argument so you don't have to do manual casting. For example:
  ///
  ///     frame [ "k1" =?> series [0 => "a"]; "k2" =?> series ["x" => "y"] ]
  ///
  /// [category:Frame construction]
  let (=?>) a (b:ISeries<_>) = a, b

  /// A function for constructing data frame from a sequence of name - column pairs.
  /// This provides a nicer syntactic sugar for `Frame.ofColumns`.
  ///
  /// ## Example
  /// To create a simple frame with two columns, you can write:
  /// 
  ///     frame [ "A" => series [ 1 => 30.0; 2 => 35.0 ]
  ///             "B" => series [ 1 => 30.0; 3 => 40.0 ] ]
  ///
  /// [category:Frame construction]
  let frame columns = 
    let names, values = columns |> Array.ofSeq |> Array.unzip
    
    // If all the series have the same builders, then use those for the frame
    let vbs = [ for s in values -> (s :> ISeries<_>).VectorBuilder ] |> Seq.distinct |> List.ofSeq
    let ibs = [ for s in values -> (s :> ISeries<_>).Index.Builder ] |> Seq.distinct |> List.ofSeq
    let vb, ib =
      match vbs, ibs with
      | [vb], [ib] -> vb, ib
      | _ -> VectorBuilder.Instance, IndexBuilder.Instance

    FrameUtils.fromColumns ib vb (Series(names, values))

  type Frame with
    // NOTE: When changing the parameters below, do not forget to update 'frame.fsx'!

    /// Load data frame from a CSV file. The operation automatically reads column names from the 
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    ///
    /// ## Parameters
    ///
    ///  * `path` - Specifies a file name or an web location of the resource.
    ///  * `indexCol` - Specifies the column that should be used as an index in the 
    ///     resulting frame. The type is specified via a type parameter, e.g. use
    ///     `Frame.ReadCsv<int>("file.csv", indexCol="Day")`.
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
    ///  * `maxRows` - The maximal number of rows that should be read from the CSV file.
    ///  * `missingValues` - An array of strings that contains values which should be treated
    ///    as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".
    ///
    /// [category:Input and output]
    static member ReadCsv<'R when 'R : equality>
        ( path:string, indexCol, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators, 
          ?culture, ?maxRows, ?missingValues ) : Frame<'R, _> =
      use reader = new StreamReader(path)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows
      |> Frame.indexRows indexCol

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
    ///    rows to use for type inference. The default value is 100.
    ///  * `schema` - A string that specifies CSV schema. See the documentation for 
    ///    information about the schema format.
    ///  * `separators` - A string that specifies one or more (single character) separators
    ///    that are used to separate columns in the CSV file. Use for example `";"` to 
    ///    parse semicolon separated files.
    ///  * `culture` - Specifies the name of the culture that is used when parsing 
    ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
    ///  * `maxRows` - The maximal number of rows that should be read from the CSV file.
    ///  * `missingValues` - An array of strings that contains values which should be treated
    ///    as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".
    ///
    /// [category:Input and output]
    static member ReadCsv
        ( path:string, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators, 
          ?culture, ?maxRows, ?missingValues ) =
      use reader = new StreamReader(path)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows

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
    ///    rows to use for type inference. The default value is 100.
    ///  * `schema` - A string that specifies CSV schema. See the documentation for 
    ///    information about the schema format.
    ///  * `separators` - A string that specifies one or more (single character) separators
    ///    that are used to separate columns in the CSV file. Use for example `";"` to 
    ///    parse semicolon separated files.
    ///  * `culture` - Specifies the name of the culture that is used when parsing 
    ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
    ///  * `maxRows` - The maximal number of rows that should be read from the CSV file.
    ///  * `missingValues` - An array of strings that contains values which should be treated
    ///    as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".
    ///
    /// [category:Input and output]
    static member ReadCsv
        ( stream:Stream, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators, 
          ?culture, ?maxRows, ?missingValues ) =
      FrameUtils.readCsv (new StreamReader(stream)) hasHeaders inferTypes inferRows schema missingValues separators culture maxRows

    /// Load data frame from a CSV file. The operation automatically reads column names from the 
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    ///
    /// ## Parameters
    ///
    ///  * `reader` - Specifies the `TextReader`, positioned at the beginning of CSV data
    ///  * `hasHeaders` - Specifies whether the input CSV file has header row
    ///  * `inferTypes` - Specifies whether the method should attempt to infer types
    ///    of columns automatically (set this to `false` if you want to specify schema)
    ///  * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
    ///    rows to use for type inference. The default value is 100.
    ///  * `schema` - A string that specifies CSV schema. See the documentation for 
    ///    information about the schema format.
    ///  * `separators` - A string that specifies one or more (single character) separators
    ///    that are used to separate columns in the CSV file. Use for example `";"` to 
    ///    parse semicolon separated files.
    ///  * `culture` - Specifies the name of the culture that is used when parsing 
    ///    values in the CSV file (such as `"en-US"`). The default is invariant culture. 
    ///  * `maxRows` - The maximal number of rows that should be read from the CSV file.
    ///  * `missingValues` - An array of strings that contains values which should be treated
    ///    as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".
    ///
    /// [category:Input and output]
    static member ReadCsv
        ( reader:TextReader, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, 
          ?separators, ?culture, ?maxRows, ?missingValues ) =
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows

    /// Creates a frame with ordinal Integer index from a sequence of rows.
    /// The column indices of individual rows are unioned, so if a row has fewer
    /// columns, it will be successfully added, but there will be missing values.
    ///
    /// [category:Frame construction]
    static member ofRowsOrdinal(rows:seq<#Series<'K, 'V>>) = 
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(rows |> Seq.mapi (fun i _ -> i), rows))

    /// Creates a frame from a sequence of row keys and row series pairs. 
    /// The row series can contain values of any type, but it has to be the same 
    /// for all the series - if you have heterogenously typed series, use `=?>`.
    ///
    /// [category:Frame construction]
    static member ofRows(rows:seq<'R * #ISeries<'C>>) = 
      let names, values = rows |> List.ofSeq |> List.unzip
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(names, values))

    /// Creates a frame from a series that maps row keys to a nested series 
    /// containing values for each row.
    ///
    /// [category:Frame construction]
    static member ofRows(rows) : Frame<'R, 'C> = 
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance rows

    /// Creates a frame with the specified row keys, but no columns (and no data).
    /// This is useful if you want to build a frame gradually and restrict all the
    /// later added data to a sequence of row keys known in advance.
    ///
    /// [category:Frame construction]
    static member ofRowKeys(keys:seq<'R>) = 
      Frame.FromRowKeys(keys)
    
    /// Creates a frame from a series that maps column keys to a nested series 
    /// containing values for each column.
    ///
    /// [category:Frame construction]
    static member ofColumns(cols) : Frame<'R, 'C> = 
      FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance cols

    /// Creates a frame from a sequence of column keys and column series pairs. 
    /// The column series can contain values of any type, but it has to be the same 
    /// for all the series - if you have heterogenously typed series, use `=?>`.
    ///
    /// [category:Frame construction]
    static member ofColumns(cols:seq<'C * #ISeries<'R>>) = 
      let names, values = cols |> List.ofSeq |> List.unzip
      FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance (Series(names, values))
    
    /// Create a data frame from a sequence of tuples containing row key, column key and a value.
    ///
    /// [category:Frame construction]
    static member ofValues(values:seq<'R * 'C * 'V>) =
      Frame.FromValues(values)

    /// Creates a data frame from a series containing any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    ///
    /// [category:Frame construction]
    static member ofRecords (series:Series<'K, 'R>) =
      Frame.FromRecords(series)

    /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    ///
    /// [category:Frame construction]
    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)    

    /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    ///
    /// [category:Frame construction]
    static member ofRecords<'R when 'R : equality> (values:System.Collections.IEnumerable, indexCol:string) =
      Reflection.convertRecordSequenceUntyped(values).IndexRows<'R>(indexCol)

    /// Create data frame from a 2D array of values. The first dimension of the array
    /// is used as rows and the second dimension is treated as columns. Rows and columns
    /// of the returned frame are indexed with the element's offset in the array.
    ///
    /// ## Parameters
    ///  - `array` - A two-dimensional array to be converted into a data frame
    ///
    /// [category:Frame construction]
    static member ofArray2D (array:'T[,]) = 
      Frame.FromArray2D(array)

  type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
    /// Creates a new data frame resulting from a 'pivot' operation. Consider a denormalized data 
    /// frame representing a table: column labels are field names & table values are observations
    /// of those fields. pivotTable buckets the rows along two axes, according to the values of 
    /// the columns `r` and `c`; and then computes a value for the frame of rows that land in each 
    /// bucket.
    ///
    /// ## Parameters
    ///  - `r` - A column key to group on for the resulting row index
    ///  - `c` - A column key to group on for the resulting col index
    ///  - `op` - A function computing a value from the corresponding bucket frame 
    ///
    /// [category:Frame operations]
    member frame.PivotTable<'R, 'C, 'T when 'R : equality and 'C : equality>(r:'TColumnKey, c:'TColumnKey, op:Frame<'TRowKey,'TColumnKey> -> 'T) =
      frame |> Frame.pivotTable (fun k os -> os.GetAs<'R>(r)) (fun k os -> os.GetAs<'C>(c)) op

    /// Save data frame to a CSV file or a `TextWriter`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    ///
    /// ## Parameters
    ///  - `writer` - Specifies the TextWriter to which the CSV data should be written
    ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
    ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
    ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
    ///    TSV files and `,` for CSV files)
    ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
    ///
    /// [category:Input and output]
    member frame.SaveCsv(writer:TextWriter, ?includeRowKeys, ?keyNames, ?separator, ?culture) = 
      FrameUtils.writeCsv (writer) None separator culture includeRowKeys keyNames frame

    /// Save data frame to a CSV file or a `TextWriter`. When calling the operation,
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

    /// Save data frame to a CSV file or to a `TextWriter`. When calling the operation,
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

    /// Returns the data of the frame as a .NET `DataTable` object. The column keys are
    /// automatically converted to strings that are used as column names. The row index is
    /// turned into an additional column with the specified name (the function takes the name
    /// as a sequence to support hierarchical keys, but typically you can write just
    /// `frame.ToDataTable(["KeyName"])`.
    ///
    /// ## Parameters
    ///  - `rowKeyNames` - Specifies the names of the row key components (or just a single
    ///    row key name if the row index is not hierarchical).
    ///
    /// [category:Input and output]
    member frame.ToDataTable(rowKeyNames) = 
      FrameUtils.toDataTable rowKeyNames frame

    /// [omit]
    [<Obsolete("Use overload taking TextWriter instead")>] 
    member frame.SaveCsv(stream:Stream, ?includeRowKeys, ?keyNames, ?separator, ?culture) = 
      use writer = new StreamWriter(stream)
      FrameUtils.writeCsv (writer) None separator culture includeRowKeys keyNames frame

/// Type that can be used for creating frames using the C# collection initializer syntax.
/// You can use `new FrameBuilder.Columns<...>` to create a new frame from columns or you
/// can use `new FrameBuilder.Rows<...>` to create a new frame from rows.
///
/// ## Example
/// The following creates a new frame with columns `Foo` and `Bar`:
/// 
///     var sampleFrame =
///       new FrameBuilder.Columns<int, string> {
///         { "Foo", new SeriesBuilder<int> { {1,11.1}, {2,22.4} }.Series }
///         { "Bar", new SeriesBuilder<int> { {1,42.42} }.Series }
///       }.Frame;
///
/// [category:Frame and series operations]
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

/// A type with extension method for `KeyValuePair<'K, 'V>` that makes
/// it possible to create values using just `KeyValue.Create`.
///
/// [category:Primitive types and values]
type KeyValue =
  static member Create<'K, 'V>(key:'K, value:'V) = KeyValuePair(key, value)


/// Contains C# and F# extension methods for the `Frame<'R, 'C>` type. The members are 
/// automatically available when you import the `Deedle` namespace. The type contains 
/// object-oriented counterparts to most of the functionality from the `Frame` module.
///
/// ## Data structure manipulation
/// Summary 1
///
/// ## Input and output
/// Summary 2
///
/// ## Missing values
/// Summary 3
///
/// [category:Frame and series operations]
[<Extension>]
type FrameExtensions =
  // ----------------------------------------------------------------------------------------------
  // Data structure manipulation
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
  /// [category:Data structure manipulation]
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
  /// [category:Data structure manipulation]
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
  /// [category:Data structure manipulation]
  [<Extension>]
  static member IndexRowsWith(frame:Frame<'R, 'C>, keys:seq<'TNewRowIndex>) =
    frame |> Frame.indexRowsWith keys

  /// Replace the row index of the frame with a sequence of row keys generated using
  /// a function invoked on each row.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///  - `f` - A function from row (as object series) to new row key value
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member IndexRowsUsing(frame:Frame<'R, 'C>, f:Func<ObjectSeries<'C>,'R2>) =
    frame |> Frame.indexRowsUsing f.Invoke

  /// Replace the column index of the frame with the provided sequence of column keys.
  /// The columns of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current column index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose column index are to be replaced.
  ///  - `keys` - A collection of new column keys.
  ///
  /// [category:Data structure manipulation]
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
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortRowsByKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame

  /// Returns a data frame that contains the same data as the input, 
  /// but whose columns are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortColumnsByKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are sorted by some column.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortRows(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey) = 
    frame |> Frame.sortRows key

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are sorted by some column.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortRowsWith(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey, cmp: Comparer<'V>) = 
    frame |> Frame.sortRowsWith key (fun a b -> cmp.Compare(a,b))

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are sorted by some column.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortRowsBy(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey, f: Func<'V,'V2>) = 
    frame |> Frame.sortRowsBy key f.Invoke 

  /// Returns a transposed data frame. The rows of the original data frame are used as the
  /// columns of the new one (and vice versa). Use this operation if you have a data frame
  /// and you mostly need to access its rows as a series (because accessing columns as a 
  /// series is more efficient).
  /// 
  /// ## Parameters
  ///  - `frame` - Source data frame to be transposed.
  /// 
  /// [category:Data structure manipulation]
  [<Extension>]
  static member Transpose(frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  /// Creates a new data frame where all columns are expanded based on runtime
  /// structure of the objects they store. The expansion is performed recrusively
  /// to the specified depth. A column can be expanded if it is `Series<string, T>` 
  /// or `IDictionary<K, V>` or if it is any .NET object with readable
  /// properties. 
  ///
  /// ## Parameters
  ///  - `nesting` - The nesting level for expansion. When set to 0, nothing is done.
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member ExpandColumns(frame:Frame<'R, string>, nesting, [<Optional>] dynamic) =
    FrameUtils.expandVectors nesting dynamic frame

  /// Creates a new data frame where the specified columns are expanded based on runtime
  /// structure of the objects they store. A column can be expanded if it is 
  /// `Series<string, T>` or `IDictionary<K, V>` or if it is any .NET object with readable
  /// properties. 
  ///
  /// ## Example
  /// Given a data frame with a series that contains tuples, you can expand the
  /// tuple members and get a frame with columns `S.Item1` and `S.Item2`:
  /// 
  ///     let df = frame [ "S" => series [ 1 => (1, "One"); 2 => (2, "Two") ] ]  
  ///     df.ExpandColumns ["S"]
  ///
  /// ## Parameters
  ///  - `names` - Names of columns in the original data frame to be expanded
  ///  - `frame` - Input data frame whose columns will be expanded
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member ExpandColumns(frame:Frame<'R, string>, names) =
    FrameUtils.expandColumns (set names) frame

  /// Given a data frame whose row index has two levels, create a series
  /// whose keys are the unique first level keys, and whose values are 
  /// those corresponding frames selected from the original data.  
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member Nest(frame:Frame<Tuple<'TRowKey1, 'TRowKey2>, 'TColumnKey>) =
    frame |> Frame.mapRowKeys (fun t -> (t.Item1, t.Item2)) |> Frame.nest

  /// Given a data frame whose row index has two levels, create a series
  /// whose keys are the unique results of the keyselector projection, and 
  /// whose values are those corresponding frames selected from the original 
  /// data.  
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member NestBy(frame:Frame<'TRowKey1, 'TColumnKey>, keyselector:Func<'TRowKey1, 'TRowKey2>) =
    frame |> Frame.nestBy keyselector.Invoke

  /// Given a series whose values are frames, create a frame resulting
  /// from the concatenation of all the frames' rows, with the resulting 
  /// keys having two levels. This is the inverse operation to nest.
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member Unnest(series:Series<'TRowKey1, Frame<'TRowKey2, 'TColumnKey>>) =
    series |> Frame.unnest |> Frame.mapRowKeys (fun (k1, k2) -> Tuple<'TRowKey1, 'TRowKey2>(k1, k2))

  // ----------------------------------------------------------------------------------------------
  // Input and output
  // ----------------------------------------------------------------------------------------------

  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  ///
  /// ## Parameters
  ///  - `writer` - Specifies the text writer to which the CSV data should be written
  ///  - `includeRowKeys` - When set to `true`, the row key is also written to the output file
  ///  - `keyNames` - Can be used to specify the CSV headers for row key (or keys, for multi-level index)
  ///  - `separator` - Specify the column separator in the file (the default is `\t` for 
  ///    TSV files and `,` for CSV files)
  ///  - `culture` - Specify the `CultureInfo` object used for formatting numerical data
  ///
  /// [category:Input and output]
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, writer: TextWriter, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) = 
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    FrameUtils.writeCsv (writer) None separator culture (Some includeRowKeys) keyNames frame

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

  /// Returns the data of the frame as a .NET `DataTable` object. The column keys are
  /// automatically converted to strings that are used as column names. The row index is
  /// turned into an additional column with the specified name (the function takes the name
  /// as a sequence to support hierarchical keys, but typically you can write just
  /// `frame.ToDataTable(["KeyName"])`.
  ///
  /// ## Parameters
  ///  - `rowKeyNames` - Specifies the names of the row key components (or just a single
  ///    row key name if the row index is not hierarchical).
  ///
  /// [category:Input and output]
  [<Extension>]
  static member ToDataTable(frame:Frame<'R, 'C>, rowKeyNames) = 
    FrameUtils.toDataTable rowKeyNames frame

  /// Creates a new data frame resulting from a 'pivot' operation. Consider a denormalized data 
  /// frame representing a table: column labels are field names & table values are observations
  /// of those fields. pivotTable buckets the rows along two axes, according to the values of 
  /// the columns `r` and `c`; and then computes a value for the frame of rows that land in each 
  /// bucket.
  ///
  /// ## Parameters
  ///  - `r` - A column key to group on for the resulting row index
  ///  - `c` - A column key to group on for the resulting col index
  ///  - `op` - A function computing a value from the corresponding bucket frame 
  ///
  /// [category:Frame operations]
  [<Extension>]
  static member PivotTable<'R, 'C, 'RNew, 'CNew, 'T when 'R : equality and 'C : equality and 'RNew : equality and 'CNew : equality>(frame: Frame<'R, 'C>, r:'C, c:'C, op:Func<Frame<'R,'C>,'T>) =
      frame |> Frame.pivotTable (fun k os -> os.GetAs<'RNew>(r)) (fun k os -> os.GetAs<'CNew>(c)) op.Invoke

  // ----------------------------------------------------------------------------------------------
  // Assorted stuff
  // ----------------------------------------------------------------------------------------------

  [<Extension>]
  static member Print(frame:Frame<'K, 'V>) = Console.WriteLine(frame.Format());

  [<Extension>]
  static member Print(frame:Frame<'K, 'V>, printTypes:bool) = Console.WriteLine(frame.Format(printTypes));

  [<Extension>]
  static member Sum(frame:Frame<'R, 'C>) = Stats.sum frame

  [<Extension>]
  static member Window(frame:Frame<'R, 'C>, size) = Frame.window size frame

  [<Extension>]
  static member Window(frame:Frame<'R, 'C>, size, aggregate:Func<_, _>) = 
    Frame.windowInto size aggregate.Invoke frame

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  [<Extension; Obsolete("Use df.RowCount")>]
  static member CountRows(frame:Frame<'R, 'C>) = frame.RowIndex.Mappings |> Seq.length

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  [<Extension; Obsolete("Use df.ColumnCount")>]
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
    frame.Rows.Where(condition) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

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
    frame.Rows.Where(condition) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member Select(frame:Frame<'TRowKey, 'TColumnKey>, projection:Func<_, _>) = 
    frame.Rows.Select(projection) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member Select(frame:Frame<'TRowKey, 'TColumnKey>, projection:Func<_, _, _>) = 
    frame.Rows.Select(projection) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member SelectRowKeys(frame:Frame<'TRowKey, 'TColumnKey>, projection) = 
    frame.Rows.SelectKeys(projection) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member SelectColumnKeys(frame:Frame<'TRowKey, 'TColumnKey>, projection) = 
    frame.Columns.SelectKeys(projection) |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member Merge(frame:Frame<'TRowKey, 'TColumnKey>, rowKey, row) = 
    frame.Merge(Frame.ofRows [ rowKey => row ])

  /// Returns a frame with columns shifted by the specified offset. When the offset is 
  /// positive, the values are shifted forward and first `offset` keys are dropped. When the
  /// offset is negative, the values are shifted backwards and the last `offset` keys are dropped.
  /// Expressed in pseudo-code:
  ///
  ///     result[k] = series[k - offset]
  ///
  /// ## Parameters
  ///  - `offset` - Can be both positive and negative number.
  ///  - `frame` - The input frame whose columns are to be shifted.
  ///
  /// ## Remarks
  /// If you want to calculate the difference, e.g. `df - (Frame.shift 1 df)`, you can
  /// use `Frame.diff` which will be a little bit faster.
  [<Extension>]
  static member Shift(frame:Frame<'TRowKey, 'TColumnKey>, offset) = 
    frame |> Frame.shift offset

  /// Returns a frame with columns containing difference between an original value and
  /// a value at the specified offset. For example, calling `Frame.diff 1 s` returns a 
  /// frame where previous column values is subtracted from the current ones. In pseudo-code, the
  /// function behaves as follows:
  ///
  ///     result[k] = series[k] - series[k - offset]
  ///
  /// Columns that cannot be converted to `float` are left without a change.
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `frame` - The input frame containing at least some `float` columns.
  ///
  [<Extension>]
  static member Diff(frame:Frame<'TRowKey, 'TColumnKey>, offset) = 
    frame |> Frame.diff offset 

  [<Extension>]
  static member Reduce(frame:Frame<'TRowKey, 'TColumnKey>, aggregation:Func<'T, 'T, 'T>) = 
    frame |> Frame.reduceValues (fun a b -> aggregation.Invoke(a, b))

  /// [category:Fancy accessors]
  [<Extension>]
  static member GetRows(frame:Frame<'TRowKey, 'TColumnKey>, [<ParamArray>] rowKeys:_[]) = 
    frame.Rows.GetItems(rowKeys) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member FilterRowsBy<'TRowKey, 'TColumnKey, 'V when 'TRowKey : equality and 'TColumnKey : equality>
      (frame:Frame<'TRowKey, 'TColumnKey>, column, value) = 
    Frame.filterRowsBy column value frame

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

  // ----------------------------------------------------------------------------------------------
  // Obsolete - kept for temporary compatibility
  // ----------------------------------------------------------------------------------------------

  /// [omit]
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member OrderRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame
  /// [omit]
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member SortByRowKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame
  /// [omit]
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member OrderColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame
  /// [omit]
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member SortByColKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame
  /// [omit]
  [<Extension; Obsolete("Use overload taking TextWriter instead")>] 
  static member SaveCsv(frame:Frame<'R, 'C>, stream:Stream, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) = 
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    use writer = new StreamWriter(stream)
    FrameUtils.writeCsv (writer) None separator culture (Some includeRowKeys) keyNames frame
  