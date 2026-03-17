#nowarn "10001"
namespace Deedle

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

open System
open System.IO
open System.Text
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open Deedle.Keys
open Deedle.Vectors


/// <summary>
/// Provides static methods for creating frames, reading frame data
/// from CSV files and database (via IDataReader). The type also provides
/// global configuration for reflection-based expansion.
/// </summary>
///
/// <category>Frame and series operations</category>
type Frame =

  // ----------------------------------------------------------------------------------------------
  // Configuration
  // ----------------------------------------------------------------------------------------------

  /// <summary>
  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection specifies additional primitive (but reference)
  /// types that should not be expaneded. By default, this includes DateTime, string, etc.
  /// </summary>
  ///
  /// <category>Configuration</category>
  static member NonExpandableTypes = Reflection.additionalPrimitiveTypes

  /// <summary>
  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection specifies interfaces whose implementations
  /// should not be expanded. By default, this includes collections such as IList.
  /// </summary>
  ///
  /// <category>Configuration</category>
  static member NonExpandableInterfaces = Reflection.nonFlattenedTypes

  /// <summary>
  /// Configures how reflection-based expansion behaves - see also `df.ExpandColumns`.
  /// This (mutable, non-thread-safe) collection lets you specify custom expansion behavior
  /// for any type. This is a dictionary with types as keys and functions that implement the
  /// expansion as values.
  /// </summary>
  /// <example>
  /// For example, say you have a type `MyPair` with propreties `Item1` of type `int` and
  /// `Item2` of type `string` (and perhaps other properties which makes the default behavior
  /// inappropriate). You can register custom expander as:
  /// <code>
  /// Frame.CustomExpanders.Add(typeof&lt;MyPair&gt;, fun v -&gt;
  ///   let a = v :?&gt; MyPair
  ///   [ "First", typeof&lt;int&gt;, box a.Item1;
  ///     "Second", typeof&lt;string&gt;, box a.Item2 ] :&gt; seq&lt;_&gt; )
  /// </code>
  /// </example>
  /// <category>Configuration</category>
  static member CustomExpanders = Reflection.customExpanders

  // ----------------------------------------------------------------------------------------------
  // Reading CSV files
  // ----------------------------------------------------------------------------------------------

  /// <summary>
  /// Load data frame from a CSV file. The operation automatically reads column names from the
  /// CSV file (if they are present) and infers the type of values for each column. Columns
  /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
  /// types (such as dates) are not converted automatically.
  /// </summary>
  /// <param name="location">Specifies a file name or an web location of the resource.</param>
  /// <param name="hasHeaders">Specifies whether the input CSV file has header row (when not set, the default value is `true`)</param>
  /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically. Set to `false` to treat all columns as strings. When a `schema` is also provided and `inferTypes=false`, the schema overrides are still applied.</param>
  /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
  /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format. Schema overrides are respected even when `inferTypes=false`.</param>
  /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
  /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
  /// <param name="maxRows">Specifies the maximum number of rows that will be read from the CSV file</param>
  /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
  /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
  /// <param name="encoding">Specifies the character encoding to use when reading the CSV file. When not set, UTF-8 with BOM detection is used.</param>
  /// <category>Input and output</category>
  static member ReadCsv
    ( location:string, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] inferTypes:Nullable<bool>, [<Optional>] inferRows:Nullable<int>,
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture, [<Optional>] maxRows:Nullable<int>,
      [<Optional>] missingValues, [<Optional>] preferOptions, [<Optional>] encoding:Encoding ) =
    use reader = if encoding = null then new StreamReader(location) else new StreamReader(location, encoding)
    FrameUtils.readCsv
      reader
      (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (if inferTypes.HasValue then Some inferTypes.Value else None)
      (if inferRows.HasValue then Some inferRows.Value else None)
      (Some schema) (Some missingValues)
      (if separators = null then None else Some separators) (Some culture)
      (if maxRows.HasValue then Some maxRows.Value else None)
      (Some preferOptions)


  /// <summary>
  /// Load data frame from a CSV file. The operation automatically reads column names from the
  /// CSV file (if they are present) and infers the type of values for each column. Columns
  /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
  /// types (such as dates) are not converted automatically.
  /// </summary>
  /// <param name="stream">Specifies the input stream, opened at the beginning of CSV data</param>
  /// <param name="hasHeaders">Specifies whether the input CSV file has header row (when not set, the default value is `true`)</param>
  /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
  /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
  /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
  /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
  /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
  /// <param name="maxRows">The maximal number of rows that should be read from the CSV file.</param>
  /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
  /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
  /// <param name="encoding">Specifies the character encoding to use when reading the CSV stream. When not set, UTF-8 with BOM detection is used.</param>
  /// <category>Input and output</category>
  static member ReadCsv
    ( stream:Stream, [<Optional>] hasHeaders:Nullable<bool>, [<Optional>] inferTypes:Nullable<bool>, [<Optional>] inferRows:Nullable<int>,
      [<Optional>] schema, [<Optional>] separators, [<Optional>] culture, [<Optional>] maxRows:Nullable<int>,
      [<Optional>] missingValues, [<Optional>] preferOptions:Nullable<bool>, [<Optional>] encoding:Encoding) =
    FrameUtils.readCsv
      (if encoding = null then new StreamReader(stream) else new StreamReader(stream, encoding))
      (if hasHeaders.HasValue then Some hasHeaders.Value else None)
      (if inferTypes.HasValue then Some inferTypes.Value else None)
      (if inferRows.HasValue then Some inferRows.Value else None)
      (Some schema) (Some missingValues)
      (if separators = null then None else Some separators) (Some culture)
      (if maxRows.HasValue then Some maxRows.Value else None)
      (if preferOptions.HasValue then Some preferOptions.Value else None)

  // Note: The following is also used from F#

  /// <summary>
  /// Read data from `IDataReader`. The method reads all rows from the data reader
  /// and for each row, gets all the columns. When a value is `DBNull`, it is treated
  /// as missing. The types of created vectors are determined by the field types reported
  /// by the data reader.
  /// </summary>
  ///
  /// <category>Input and output</category>
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

  /// <summary>
  /// Create a data frame from a sequence of objects and functions that return
  /// row key, column key and value for each object in the input sequence.
  /// </summary>
  /// <param name="values">Input sequence of objects</param>
  /// <param name="colSel">A function that returns the column key of an object</param>
  /// <param name="rowSel">A function that returns the row key of an object</param>
  /// <param name="valSel">A function that returns the value of an object</param>
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

  /// <summary>
  /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
  /// over the specified type parameter `'T` and turns its properties to columns. The
  /// rows of the resulting frame are automatically indexed by `int`.
  /// </summary>
  /// <example>
  /// The method can be nicely used to create a data frame using C# anonymous types
  /// (the result is a data frame with columns "A" and "B" containing two rows).
  /// <code lang="csharp">
  /// var df = Frame.FromRecords(new[] {
  ///   new { A = 1, B = "Test" },
  ///   new { A = 2, B = "Another"}
  /// });
  /// </code>
  /// </example>
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromRecords (values:seq<'T>) =
    Reflection.convertRecordSequence<'T>(values)

  /// <summary>
  /// Create data frame from a 2D array of values. The first dimension of the array
  /// is used as rows and the second dimension is treated as columns. Rows and columns
  /// of the returned frame are indexed with the element's offset in the array.
  /// </summary>
  /// <param name="array">A two-dimensional array to be converted into a data frame</param>
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

  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromJaggedArray(jArray:'T [][]) =
    if Array.exists (fun (x:'T []) -> x.Length <> jArray.[0].Length) jArray then
      invalidOp "FromJaggedArray: The input jagged array must have the same dimensions in all inner arrays."
    else
      let rowCount, colCount = jArray.Length, jArray.[0].Length
      // Generate row index (int offsets) and column index (int offsets)
      let rowIndex = IndexBuilder.Instance.Create(Array.init rowCount id, Some true)
      let colIndex = IndexBuilder.Instance.Create(Array.init colCount id, Some true)
      // Generate vectors with column-based data
      let vectors = Array.zeroCreate colCount
      for c = 0 to vectors.Length - 1 do
        let col = Array.init rowCount (fun r -> jArray.[r].[c])
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


/// <summary>
/// This module contains F# functions and extensions for working with frames. This
/// includes operations for creating frames such as the `frame` function, `=>` operator
/// and `Frame.ofRows`, `Frame.ofColumns` and `Frame.ofRowKeys` functions. The module
/// also provides additional F# extension methods including `ReadCsv`, `SaveCsv` and `PivotTable`.
/// </summary>
/// <remarks>
/// <para>Frame construction:</para>
/// <para>
/// The functions and methods in this group can be used to create frames. If you are creating
/// a frame from a number of sample values, you can use `frame` and the `=>` operator (or the
/// `=?>` opreator which is useful if you have multiple series of distinct types):
/// </para>
/// <para>
///     frame [ "Column 1" => series [ 1 => 1.0; 2 => 2.0 ]
///             "Column 2" => series [ 3 => 3.0 ] ]
/// </para>
/// <para>
/// Aside from this, the various type extensions let you write `Frame.ofXyz` to construct frames
/// from data in various formats - `Frame.ofRows` and `Frame.ofColumns` create frame from a series
/// or a sequence of rows or columns; `Frame.ofRecords` creates a frame from .NET objects using
/// Reflection and `Frame.ofRowKeys` creates an empty frame with the specified keys.
/// </para>
/// <para>Frame operations:</para>
/// <para>
/// The group contains two overloads of the F#-friendly version of the `PivotTable` method.
/// </para>
/// <para>Input and output:</para>
/// <para>
/// This group of extensions includes a number of overloads for the `ReadCsv` and `SaveCsv`
/// methods. The methods here are designed to be used from F# and so they are F#-style extensions
/// and they use F#-style optional arguments. In general, the overlads take either a path or
/// `TextReader`/`TextWriter`. Also note that `ReadCsv&lt;'R&gt;(path, indexCol, ...)` lets you specify
/// the column to be used as the index.
/// </para>
/// </remarks>
/// <category>Frame and series operations</category>
[<AutoOpen>]
module ``F# Frame extensions`` =

  /// <summary>
  /// Custom operator that can be used when constructing series from observations
  /// or frames from key-row or key-column pairs. The operator simply returns a
  /// tuple, but it provides a more convenient syntax. For example:
  ///
  ///     series [ "k1" => 1; "k2" => 15 ]
  /// </summary>
  ///
  /// <category>Frame construction</category>
  let (=>) a b = a, b

  /// <summary>
  /// Custom operator that can be used when constructing a frame from observations
  /// of series. The operator simply returns a tuple, but it upcasts the series
  /// argument so you don't have to do manual casting. For example:
  ///
  ///     frame [ "k1" =?> series [0 => "a"]; "k2" =?> series ["x" => "y"] ]
  /// </summary>
  ///
  /// <category>Frame construction</category>
  let (=?>) a (b:ISeries<_>) = a, b

  /// <summary>
  /// A function for constructing data frame from a sequence of name - column pairs.
  /// This provides a nicer syntactic sugar for `Frame.ofColumns`.
  /// </summary>
  /// <example>
  /// To create a simple frame with two columns, you can write:
  /// <code>
  /// frame [ "A" =&gt; series [ 1 =&gt; 30.0; 2 =&gt; 35.0 ]
  ///         "B" =&gt; series [ 1 =&gt; 30.0; 3 =&gt; 40.0 ] ]
  /// </code>
  /// </example>
  /// <category>Frame construction</category>
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

    /// <summary>
    /// Load data frame from a CSV file. The operation automatically reads column names from the
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    /// </summary>
    /// <param name="path">Specifies a file name or an web location of the resource.</param>
    /// <param name="indexCol">Specifies the column that should be used as an index in the resulting frame. The type is specified via a type parameter, e.g. use <c>Frame.ReadCsv&lt;int&gt;("file.csv", indexCol="Day")</c>.</param>
    /// <param name="hasHeaders">Specifies whether the input CSV file has header row</param>
    /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
    /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100. Value 0 means all rows.</param>
    /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
    /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
    /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
    /// <param name="maxRows">The maximal number of rows that should be read from the CSV file.</param>
    /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
    /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
    /// <param name="encoding">Specifies the character encoding to use when reading the CSV file. When not set, UTF-8 with BOM detection is used.</param>
    /// <category>Input and output</category>
    static member ReadCsv<'R when 'R : equality>
        ( path:string, indexCol, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators,
          ?culture, ?maxRows, ?missingValues, ?preferOptions, ?encoding: Encoding ) : Frame<'R, _> =
      use reader = match encoding with Some e -> new StreamReader(path, e) | None -> new StreamReader(path)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows preferOptions
      |> Frame.indexRows indexCol

    /// <summary>
    /// Load data frame from a CSV file. The operation automatically reads column names from the
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    /// </summary>
    /// <param name="path">Specifies a file name or an web location of the resource.</param>
    /// <param name="hasHeaders">Specifies whether the input CSV file has header row</param>
    /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
    /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
    /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
    /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
    /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
    /// <param name="maxRows">The maximal number of rows that should be read from the CSV file.</param>
    /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
    /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
    /// <param name="encoding">Specifies the character encoding to use when reading the CSV file. When not set, UTF-8 with BOM detection is used.</param>
    /// <category>Input and output</category>
    static member ReadCsv
        ( path:string, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators,
          ?culture, ?maxRows, ?missingValues, ?preferOptions, ?encoding: Encoding ) =
      use reader = match encoding with Some e -> new StreamReader(path, e) | None -> new StreamReader(path)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows preferOptions

    /// <summary>
    /// Load data frame from a CSV file. The operation automatically reads column names from the
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    /// </summary>
    /// <param name="stream">Specifies the input stream, opened at the beginning of CSV data</param>
    /// <param name="hasHeaders">Specifies whether the input CSV file has header row</param>
    /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
    /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
    /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
    /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
    /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
    /// <param name="maxRows">The maximal number of rows that should be read from the CSV file.</param>
    /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
    /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
    /// <param name="encoding">Specifies the character encoding to use when reading the CSV stream. When not set, UTF-8 with BOM detection is used.</param>
    /// <category>Input and output</category>
    static member ReadCsv
        ( stream:Stream, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators,
          ?culture, ?maxRows, ?missingValues, ?preferOptions, ?encoding: Encoding ) =
      let reader = match encoding with Some e -> new StreamReader(stream, e) | None -> new StreamReader(stream)
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows preferOptions

    /// <summary>
    /// Load data frame from a CSV file. The operation automatically reads column names from the
    /// CSV file (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    /// </summary>
    /// <param name="reader">Specifies the `TextReader`, positioned at the beginning of CSV data</param>
    /// <param name="hasHeaders">Specifies whether the input CSV file has header row</param>
    /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
    /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
    /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
    /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV file. Use for example `";"` to parse semicolon separated files.</param>
    /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV file (such as `"en-US"`). The default is invariant culture.</param>
    /// <param name="maxRows">The maximal number of rows that should be read from the CSV file.</param>
    /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
    /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
    /// <category>Input and output</category>
    static member ReadCsv
        ( reader:TextReader, ?hasHeaders, ?inferTypes, ?inferRows, ?schema,
          ?separators, ?culture, ?maxRows, ?missingValues, ?preferOptions ) =
      FrameUtils.readCsv reader hasHeaders inferTypes inferRows schema missingValues separators culture maxRows preferOptions

    /// <summary>
    /// Load data frame from a string representing a UTF8-encoded CSV file. The operation automatically
    /// reads column names from the string (if they are present) and infers the type of values for each column. Columns
    /// of primitive types (`int`, `float`, etc.) are converted to the right type. Columns of other
    /// types (such as dates) are not converted automatically.
    /// </summary>
    /// <param name="csvString">Specifies the input string containing the CSV</param>
    /// <param name="hasHeaders">Specifies whether the input CSV string has header row</param>
    /// <param name="inferTypes">Specifies whether the method should attempt to infer types of columns automatically (set this to `false` if you want to specify schema)</param>
    /// <param name="inferRows">If `inferTypes=true`, this parameter specifies the number of rows to use for type inference. The default value is 100.</param>
    /// <param name="schema">A string that specifies CSV schema. See the documentation for information about the schema format.</param>
    /// <param name="separators">A string that specifies one or more (single character) separators that are used to separate columns in the CSV string. Use for example `";"` to parse semicolon separated files.</param>
    /// <param name="culture">Specifies the name of the culture that is used when parsing values in the CSV string (such as `"en-US"`). The default is invariant culture.</param>
    /// <param name="maxRows">The maximal number of rows that should be read from the CSV string.</param>
    /// <param name="missingValues">An array of strings that contains values which should be treated as missing when reading the file. The default value is: "NaN"; "NA"; "#N/A"; ":"; "-"; "TBA"; "TBD".</param>
    /// <param name="preferOptions">Specifies whether to prefer optional values when parsing CSV data.</param>
    /// <category>Input and output</category>
    static member ReadCsvString
        ( csvString:string, ?hasHeaders, ?inferTypes, ?inferRows, ?schema, ?separators,
          ?culture, ?maxRows, ?missingValues, ?preferOptions ) =
      FrameUtils.readString csvString hasHeaders inferTypes inferRows schema missingValues separators culture maxRows preferOptions


    /// <summary>
    /// Creates a frame with ordinal Integer index from a sequence of rows.
    /// The column indices of individual rows are unioned, so if a row has fewer
    /// columns, it will be successfully added, but there will be missing values.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRowsOrdinal(rows:seq<#Series<'K, 'V>>) =
      let vector = rows |> Array.ofSeq |> VectorBuilder.Instance.Create
      let index = IndexBuilder.Instance.Create(seq { 0L .. vector.Length-1L }, Some true)
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(index, vector, VectorBuilder.Instance, IndexBuilder.Instance))


    /// <summary>
    /// Creates a frame from a sequence of row keys and row series pairs.
    /// The row series can contain values of any type, but it has to be the same
    /// for all the series - if you have heterogenously typed series, use `=?>`.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRows(rows:seq<'R * #ISeries<'C>>) =
      let names, values = rows |> List.ofSeq |> List.unzip
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance (Series(names, values))

    /// <summary>
    /// Creates a frame from a series that maps row keys to a nested series
    /// containing values for each row.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRows(rows) : Frame<'R, 'C> =
      FrameUtils.fromRows IndexBuilder.Instance VectorBuilder.Instance rows

    /// <summary>
    /// Creates a frame with the specified row keys, but no columns (and no data).
    /// This is useful if you want to build a frame gradually and restrict all the
    /// later added data to a sequence of row keys known in advance.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRowKeys(keys:seq<'R>) =
      Frame.FromRowKeys(keys)

    /// <summary>
    /// Creates a frame from a series that maps column keys to a nested series
    /// containing values for each column.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofColumns(cols) : Frame<'R, 'C> =
      FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance cols

    /// <summary>
    /// Creates a frame from a sequence of column keys and column series pairs.
    /// The column series can contain values of any type, but it has to be the same
    /// for all the series - if you have heterogenously typed series, use `=?>`.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofColumns(cols:seq<'C * #ISeries<'R>>) =
      let names, values = cols |> List.ofSeq |> List.unzip
      FrameUtils.fromColumns IndexBuilder.Instance VectorBuilder.Instance (Series(names, values))

    /// <summary>
    /// Create a data frame from a sequence of tuples containing row key, column key and a value.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofValues(values:seq<'R * 'C * 'V>) =
      Frame.FromValues(values)

    /// <summary>
    /// Creates a data frame from a series containing any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRecords (series:Series<'K, 'R>) =
      Frame.FromRecords(series)

    /// <summary>
    /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)

    /// <summary>
    /// Creates a data frame from a sequence of any .NET objects. The method uses reflection
    /// over the specified type parameter `'T` and turns its properties to columns.
    /// </summary>
    ///
    /// <category>Frame construction</category>
    static member ofRecords<'R when 'R : equality> (values:System.Collections.IEnumerable, indexCol:string) =
      Reflection.convertRecordSequenceUntyped(values).IndexRows<'R>(indexCol)

    /// <summary>
    /// Create data frame from a 2D array of values. The first dimension of the array
    /// is used as rows and the second dimension is treated as columns. Rows and columns
    /// of the returned frame are indexed with the element's offset in the array.
    /// </summary>
    /// <param name="array">A two-dimensional array to be converted into a data frame</param>
    /// <category>Frame construction</category>
    static member ofArray2D (array:'T[,]) =
      Frame.FromArray2D(array)

    /// <summary>
    /// Create data frame from a jagged array of values. The first dimension of the array
    /// is used as rows and the second dimension is treated as columns. Rows and columns
    /// of the returned frame are indexed with the element's offset in the array.
    /// </summary>
    /// <remarks>
    /// Please note that this function will fail when the inner arrays of the input do not have the same lengths.
    /// </remarks>
    /// <param name="jArray">A jagged array to be converted into a data frame</param>
    /// <category>Frame construction</category>
    static member ofJaggedArray (jArray:'T[][]) =
      Frame.FromJaggedArray(jArray)

  type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
    /// <summary>
    /// Creates a new data frame resulting from a 'pivot' operation. Consider a denormalized data
    /// frame representing a table: column labels are field names &amp; table values are observations
    /// of those fields. pivotTable buckets the rows along two axes, according to the values of
    /// the columns `r` and `c`; and then computes a value for the frame of rows that land in each
    /// bucket.
    /// </summary>
    /// <param name="r">A column key to group on for the resulting row index</param>
    /// <param name="c">A column key to group on for the resulting col index</param>
    /// <param name="op">A function computing a value from the corresponding bucket frame</param>
    /// <category>Frame operations</category>
    member frame.PivotTable<'R, 'C, 'T when 'R : equality and 'C : equality>(r:'TColumnKey, c:'TColumnKey, op:Frame<'TRowKey,'TColumnKey> -> 'T) =
      frame |> Frame.pivotTable (fun k os -> os.GetAs<'R>(r)) (fun k os -> os.GetAs<'C>(c)) op

    /// <summary>
    /// Save data frame to a CSV file or a `TextWriter`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    /// </summary>
    /// <param name="writer">Specifies the TextWriter to which the CSV data should be written</param>
    /// <param name="includeRowKeys">When set to `true`, the row key is also written to the output file</param>
    /// <param name="keyNames">Can be used to specify the CSV headers for row key (or keys, for multi-level index)</param>
    /// <param name="separator">Specify the column separator in the file (the default is `\t` for TSV files and `,` for CSV files)</param>
    /// <param name="culture">Specify the `CultureInfo` object used for formatting numerical data</param>
    /// <category>Input and output</category>
    member frame.SaveCsv(writer:TextWriter, ?includeRowKeys, ?keyNames, ?separator, ?culture) =
      FrameUtils.writeCsv (writer) None separator culture includeRowKeys keyNames frame

    /// <summary>
    /// Save data frame to a CSV file or a `TextWriter`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    /// </summary>
    /// <param name="path">Specifies the output file name where the CSV data should be written</param>
    /// <param name="includeRowKeys">When set to `true`, the row key is also written to the output file</param>
    /// <param name="keyNames">Can be used to specify the CSV headers for row key (or keys, for multi-level index)</param>
    /// <param name="separator">Specify the column separator in the file (the default is `\t` for TSV files and `,` for CSV files)</param>
    /// <param name="culture">Specify the `CultureInfo` object used for formatting numerical data</param>
    /// <category>Input and output</category>
    member frame.SaveCsv(path:string, ?includeRowKeys, ?keyNames, ?separator, ?culture) =
      use writer = new StreamWriter(path)
      FrameUtils.writeCsv writer (Some path) separator culture includeRowKeys keyNames frame

    /// <summary>
    /// Save data frame to a CSV file or to a `TextWriter`. When calling the operation,
    /// you can specify whether you want to save the row keys or not (and headers for the keys)
    /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
    /// file name ending with `.tsv`, the `\t` separator is used automatically.
    /// </summary>
    /// <param name="path">Specifies the output file name where the CSV data should be written</param>
    /// <param name="keyNames">Specifies the CSV headers for row key (or keys, for multi-level index)</param>
    /// <category>Input and output</category>
    member frame.SaveCsv(path:string, keyNames) =
      use writer = new StreamWriter(path)
      FrameUtils.writeCsv writer (Some path) None None (Some true) (Some keyNames) frame

    /// <summary>
    /// Returns the data of the frame as a .NET `DataTable` object. The column keys are
    /// automatically converted to strings that are used as column names. The row index is
    /// turned into an additional column with the specified name (the function takes the name
    /// as a sequence to support hierarchical keys, but typically you can write just
    /// `frame.ToDataTable(["KeyName"])`.
    /// </summary>
    /// <param name="rowKeyNames">Specifies the names of the row key components (or just a single row key name if the row index is not hierarchical).</param>
    /// <category>Input and output</category>
    member frame.ToDataTable(rowKeyNames) =
      FrameUtils.toDataTable rowKeyNames frame

    /// <exclude />
    [<Obsolete("Use overload taking TextWriter instead")>]
    member frame.SaveCsv(stream:Stream, ?includeRowKeys, ?keyNames, ?separator, ?culture) =
      use writer = new StreamWriter(stream)
      FrameUtils.writeCsv (writer) None separator culture includeRowKeys keyNames frame

    /// <summary>
    /// Serialize the data frame to a JSON string.
    /// </summary>
    /// <param name="orient">
    /// Controls the JSON layout. Allowed values:
    /// <c>"columns"</c> (default) — column-major <c>{"col":{"row":v}}</c>;
    /// <c>"index"</c> — row-major <c>{"row":{"col":v}}</c>;
    /// <c>"records"</c> — array of row objects <c>[{"col":v}]</c>.
    /// </param>
    /// <category>Input and output</category>
    member frame.ToJson(?orient) =
      FrameUtils.toJson (defaultArg orient "columns") frame

    /// <summary>
    /// Save the data frame as a JSON file.
    /// </summary>
    /// <param name="writer">The <c>TextWriter</c> to write the JSON to.</param>
    /// <param name="orient">Controls the JSON layout (see <c>ToJson</c>).</param>
    /// <category>Input and output</category>
    member frame.SaveJson(writer:TextWriter, ?orient) =
      FrameUtils.writeJson writer (defaultArg orient "columns") frame

    /// <summary>
    /// Save the data frame as a JSON file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="orient">Controls the JSON layout (see <c>ToJson</c>).</param>
    /// <category>Input and output</category>
    member frame.SaveJson(path:string, ?orient) =
      use writer = new StreamWriter(path)
      FrameUtils.writeJson writer (defaultArg orient "columns") frame

/// <summary>
/// Type that can be used for creating frames using the C# collection initializer syntax.
/// You can use <c>new FrameBuilder.Columns&lt;...&gt;</c> to create a new frame from columns or you
/// can use <c>new FrameBuilder.Rows&lt;...&gt;</c> to create a new frame from rows.
/// </summary>
/// <example>
/// The following creates a new frame with columns `Foo` and `Bar`:
///
///     var sampleFrame =
///       new <c>FrameBuilder.Columns&lt;int, string&gt;</c> {
///         { "Foo", new <c>SeriesBuilder&lt;int&gt;</c> { {1,11.1}, {2,22.4} }.Series }
///         { "Bar", new <c>SeriesBuilder&lt;int&gt;</c> { {1,42.42} }.Series }
///       }.Frame;
/// </example>
/// <category>Frame and series operations</category>
module FrameBuilder =
  type Columns<'R, 'C when 'C : equality and 'R : equality>() =
    let mutable series = []
    member x.Add(key:'C, value:ISeries<'R>) =
      series <- (key, value)::series
    member x.Frame = series |> List.rev |> Frame.ofColumns
    interface System.Collections.IEnumerable with
      member x.GetEnumerator() = (x :> seq<_>).GetEnumerator() :> Collections.IEnumerator
    interface seq<KeyValuePair<'C, ISeries<'R>>> with
      member x.GetEnumerator() =
        (series |> List.rev |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()

  type Rows<'R, 'C when 'C : equality and 'R : equality>() =
    let mutable series = []
    member x.Add(key:'R, value:ISeries<'C>) =
      series <- (key, value)::series
    member x.Frame = series |> List.rev |> Frame.ofRows
    interface System.Collections.IEnumerable with
      member x.GetEnumerator() = (x :> seq<_>).GetEnumerator() :> Collections.IEnumerator
    interface seq<KeyValuePair<'R, ISeries<'C>>> with
      member x.GetEnumerator() =
        (series |> List.rev |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()

/// <summary>
/// A type with extension method for <c>KeyValuePair&lt;'K, 'V&gt;</c> that makes
/// it possible to create values using just `KeyValue.Create`.
/// </summary>
///
/// <category>Primitive types and values</category>
type KeyValue =
  static member Create<'K, 'V>(key:'K, value:'V) = KeyValuePair(key, value)


/// <summary>
/// Contains C# and F# extension methods for the `Frame&lt;'R, 'C&gt;` type. The members are
/// automatically available when you import the `Deedle` namespace. The type contains
/// object-oriented counterparts to most of the functionality from the `Frame` module.
/// </summary>
/// <remarks>
/// <para>Data structure manipulation:</para>
/// <para>
/// Summary 1
/// </para>
/// <para>Input and output:</para>
/// <para>
/// Summary 2
/// </para>
/// <para>Missing values:</para>
/// <para>
/// Summary 3
/// </para>
/// </remarks>
/// <category>Frame and series operations</category>
[<Extension>]
type FrameExtensions =
  // ----------------------------------------------------------------------------------------------
  // Data structure manipulation
  // ----------------------------------------------------------------------------------------------

  /// <summary>
  /// Align the existing data to a specified collection of row keys. Values in the data frame
  /// that do not match any new key are dropped, new keys (that were not in the original data
  /// frame) are assigned missing values.
  /// </summary>
  /// <param name="frame">Source data frame that is to be realigned.</param>
  /// <param name="keys">A sequence of new row keys. The keys must have the same type as the original frame keys (because the rows are realigned).</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member RealignRows(frame:Frame<'R, 'C>, keys) =
    frame |> Frame.realignRows keys

  /// <summary>
  /// Replace the row index of the frame with ordinarilly generated integers starting from zero.
  /// The rows of the frame are assigned index according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  /// </summary>
  /// <param name="frame">Source data frame whose row index are to be replaced.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member IndexRowsOrdinally(frame:Frame<'TRowKey, 'TColumnKey>) =
    frame |> Frame.indexRowsOrdinally

  /// <summary>
  /// Replace the row index of the frame with the provided sequence of row keys.
  /// The rows of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  /// </summary>
  /// <param name="frame">Source data frame whose row index are to be replaced.</param>
  /// <param name="keys">A collection of new row keys.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member IndexRowsWith(frame:Frame<'R, 'C>, keys:seq<'TNewRowIndex>) =
    frame |> Frame.indexRowsWith keys

  /// <summary>
  /// Replace the row index of the frame with a sequence of row keys generated using
  /// a function invoked on each row.
  /// </summary>
  /// <param name="frame">Source data frame whose row index are to be replaced.</param>
  /// <param name="f">A function from row (as object series) to new row key value</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member IndexRowsUsing(frame:Frame<'R, 'C>, f:Func<ObjectSeries<'C>,'R2>) =
    frame |> Frame.indexRowsUsing f.Invoke

  /// <summary>
  /// Replace the column index of the frame with the provided sequence of column keys.
  /// The columns of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current column index is not ordered.
  /// </summary>
  /// <param name="frame">Source data frame whose column index are to be replaced.</param>
  /// <param name="keys">A collection of new column keys.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member IndexColumnsWith(frame:Frame<'R, 'C>, keys:seq<'TNewRowIndex>) =
    frame |> Frame.indexColsWith keys

  /// <summary>
  /// Returns a data frame that contains the same data as the input,
  /// but whose rows are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  /// </summary>
  /// <param name="frame">Source data frame to be ordered.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member SortRowsByKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame

  /// <summary>
  /// Returns a data frame that contains the same data as the input,
  /// but whose columns are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  /// </summary>
  /// <param name="frame">Source data frame to be ordered.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member SortColumnsByKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame

  /// <summary>
  /// Returns a data frame that contains the same data as the input,
  /// but whose rows are sorted by some column.
  /// </summary>
  /// <param name="frame">Source data frame to be ordered.</param>
  /// <param name="key">The column key to sort by.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member SortRows(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey) =
    frame |> Frame.sortRows key

  /// <summary>
  /// Returns a data frame that contains the same data as the input,
  /// but whose rows are sorted by some column.
  /// </summary>
  /// <param name="frame">Source data frame to be ordered.</param>
  /// <param name="key">The column key to sort by.</param>
  /// <param name="cmp">The comparer to use for sorting values.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member SortRowsWith(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey, cmp: Comparer<'V>) =
    frame |> Frame.sortRowsWith key (fun a b -> cmp.Compare(a,b))

  /// <summary>
  /// Returns a data frame that contains the same data as the input,
  /// but whose rows are sorted by some column.
  /// </summary>
  /// <param name="frame">Source data frame to be ordered.</param>
  /// <param name="key">The column key to sort by.</param>
  /// <param name="f">A function to transform values before comparison.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member SortRowsBy(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey, f: Func<'V,'V2>) =
    frame |> Frame.sortRowsBy key f.Invoke

  /// <summary>
  /// Returns a series containing the dense rank of each row in the data frame, based on
  /// values in the specified column. The rank is 1-based: the row with the smallest
  /// column value receives rank 1. Rows with equal values receive the same rank.
  /// Rows where the column has a missing value receive a missing rank.
  /// </summary>
  /// <param name="frame">Source data frame.</param>
  /// <param name="key">The column key to rank by.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member RankRowsBy(frame:Frame<'TRowKey, 'TColumnKey>, key: 'TColumnKey) =
    frame |> Frame.rankRowsBy key

  /// <summary>
  /// Returns a transposed data frame. The rows of the original data frame are used as the
  /// columns of the new one (and vice versa). Use this operation if you have a data frame
  /// and you mostly need to access its rows as a series (because accessing columns as a
  /// series is more efficient).
  /// </summary>
  /// <param name="frame">Source data frame to be transposed.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member Transpose(frame:Frame<'TRowKey, 'TColumnKey>) =
    frame.Columns |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  /// <summary>
  /// Creates a new data frame where all columns are expanded based on runtime
  /// structure of the objects they store. The expansion is performed recrusively
  /// to the specified depth. A column can be expanded if it is <c>Series&lt;string, T&gt;</c>
  /// or <c>IDictionary&lt;K, V&gt;</c> or if it is any .NET object with readable
  /// properties.
  /// </summary>
  /// <param name="frame">Input data frame to be expanded.</param>
  /// <param name="nesting">The nesting level for expansion. When set to 0, nothing is done.</param>
  /// <param name="dynamic">Specifies whether to use dynamic expansion.</param>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member ExpandColumns(frame:Frame<'R, string>, nesting, [<Optional>] dynamic) =
    FrameUtils.expandVectors nesting dynamic frame

  /// <summary>
  /// Creates a new data frame where the specified columns are expanded based on runtime
  /// structure of the objects they store. A column can be expanded if it is
  /// <c>Series&lt;string, T&gt;</c> or <c>IDictionary&lt;K, V&gt;</c> or if it is any .NET object with readable
  /// properties.
  /// </summary>
  /// <param name="names">Names of columns in the original data frame to be expanded</param>
  /// <param name="frame">Input data frame whose columns will be expanded</param>
  /// <remarks>
  /// <example>
  /// Given a data frame with a series that contains tuples, you can expand the
  /// tuple members and get a frame with columns `S.Item1` and `S.Item2`:
  /// <code>
  /// let df = frame [ "S" =&gt; series [ 1 =&gt; (1, "One"); 2 =&gt; (2, "Two") ] ]
  /// df.ExpandColumns ["S"]
  /// </code>
  /// </example>
  /// </remarks>
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member ExpandColumns(frame:Frame<'R, string>, names) =
    FrameUtils.expandColumns (set names) frame

  /// <summary>
  /// Given a data frame whose row index has two levels, create a series
  /// whose keys are the unique first level keys, and whose values are
  /// those corresponding frames selected from the original data.
  /// </summary>
  ///
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member Nest(frame:Frame<Tuple<'TRowKey1, 'TRowKey2>, 'TColumnKey>) =
    frame |> Frame.mapRowKeys (fun t -> t) |> Frame.nest

  /// <summary>
  /// Given a data frame whose row index has two levels, create a series
  /// whose keys are the unique results of the keyselector projection, and
  /// whose values are those corresponding frames selected from the original
  /// data.
  /// </summary>
  ///
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member NestBy(frame:Frame<'TRowKey1, 'TColumnKey>, keyselector:Func<'TRowKey1, 'TRowKey2>) =
    frame |> Frame.nestBy keyselector.Invoke

  /// <summary>
  /// Given a series whose values are frames, create a frame resulting
  /// from the concatenation of all the frames' rows, with the resulting
  /// keys having two levels. This is the inverse operation to nest.
  /// </summary>
  ///
  /// <category>Data structure manipulation</category>
  [<Extension>]
  static member Unnest(series:Series<'TRowKey1, Frame<'TRowKey2, 'TColumnKey>>) =
    series |> Frame.unnest

  // ----------------------------------------------------------------------------------------------
  // Input and output
  // ----------------------------------------------------------------------------------------------

  /// <summary>
  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  /// </summary>
  /// <param name="frame">The input data frame to be saved.</param>
  /// <param name="writer">Specifies the text writer to which the CSV data should be written</param>
  /// <param name="includeRowKeys">When set to `true`, the row key is also written to the output file</param>
  /// <param name="keyNames">Can be used to specify the CSV headers for row key (or keys, for multi-level index)</param>
  /// <param name="separator">Specify the column separator in the file (the default is `\t` for TSV files and `,` for CSV files)</param>
  /// <param name="culture">Specify the `CultureInfo` object used for formatting numerical data</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, writer: TextWriter, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) =
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    FrameUtils.writeCsv (writer) None separator culture (Some includeRowKeys) keyNames frame

  /// <summary>
  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  /// </summary>
  /// <param name="frame">The input data frame to be saved.</param>
  /// <param name="path">Specifies the output file name where the CSV data should be written</param>
  /// <param name="includeRowKeys">When set to `true`, the row key is also written to the output file</param>
  /// <param name="keyNames">Can be used to specify the CSV headers for row key (or keys, for multi-level index)</param>
  /// <param name="separator">Specify the column separator in the file (the default is `\t` for TSV files and `,` for CSV files)</param>
  /// <param name="culture">Specify the `CultureInfo` object used for formatting numerical data</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, path:string, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) =
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    use writer = new StreamWriter(path)
    FrameUtils.writeCsv writer (Some path) separator culture (Some includeRowKeys) keyNames frame

  /// <summary>
  /// Save data frame to a CSV file or to a `Stream`. When calling the operation,
  /// you can specify whether you want to save the row keys or not (and headers for the keys)
  /// and you can also specify the separator (use `\t` for writing TSV files). When specifying
  /// file name ending with `.tsv`, the `\t` separator is used automatically.
  /// </summary>
  /// <param name="frame">The input data frame to be saved.</param>
  /// <param name="path">Specifies the output file name where the CSV data should be written</param>
  /// <param name="keyNames">Specifies the CSV headers for row key (or keys, for multi-level index)</param>
  /// <param name="separator">Specify the column separator in the file (the default is `\t` for TSV files and `,` for CSV files)</param>
  /// <param name="culture">Specify the `CultureInfo` object used for formatting numerical data</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member SaveCsv(frame:Frame<'R, 'C>, path:string, keyNames, [<Optional>] separator, [<Optional>] culture) =
    use writer = new StreamWriter(path)
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    FrameUtils.writeCsv writer (Some path) separator culture (Some true) (Some keyNames) frame

  /// <summary>
  /// Returns the data of the frame as a .NET `DataTable` object. The column keys are
  /// automatically converted to strings that are used as column names. The row index is
  /// turned into an additional column with the specified name (the function takes the name
  /// as a sequence to support hierarchical keys, but typically you can write just
  /// `frame.ToDataTable(["KeyName"])`.
  /// </summary>
  /// <param name="frame">The input data frame to be converted.</param>
  /// <param name="rowKeyNames">Specifies the names of the row key components (or just a single row key name if the row index is not hierarchical).</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member ToDataTable(frame:Frame<'R, 'C>, rowKeyNames) =
    FrameUtils.toDataTable rowKeyNames frame

  /// <summary>
  /// Serialize the data frame to a JSON string.
  /// </summary>
  /// <param name="frame">The input data frame to serialize.</param>
  /// <param name="orient">
  /// Controls the JSON layout. Allowed values:
  /// <c>"columns"</c> (default) — column-major <c>{"col":{"row":v}}</c>;
  /// <c>"index"</c> — row-major <c>{"row":{"col":v}}</c>;
  /// <c>"records"</c> — array of row objects <c>[{"col":v}]</c>.
  /// </param>
  /// <category>Input and output</category>
  [<Extension>]
  static member ToJson(frame:Frame<'R, 'C>, [<Optional>] orient) =
    FrameUtils.toJson (if orient = null then "columns" else orient) frame

  /// <summary>
  /// Save the data frame as JSON to the specified <c>TextWriter</c>.
  /// </summary>
  /// <param name="frame">The input data frame to serialize.</param>
  /// <param name="writer">The <c>TextWriter</c> to write JSON to.</param>
  /// <param name="orient">Controls the JSON layout (see <c>ToJson</c>).</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member SaveJson(frame:Frame<'R, 'C>, writer:TextWriter, [<Optional>] orient) =
    FrameUtils.writeJson writer (if orient = null then "columns" else orient) frame

  /// <summary>
  /// Save the data frame as a JSON file at the specified path.
  /// </summary>
  /// <param name="frame">The input data frame to serialize.</param>
  /// <param name="path">The output file path.</param>
  /// <param name="orient">Controls the JSON layout (see <c>ToJson</c>).</param>
  /// <category>Input and output</category>
  [<Extension>]
  static member SaveJson(frame:Frame<'R, 'C>, path:string, [<Optional>] orient) =
    use writer = new StreamWriter(path)
    FrameUtils.writeJson writer (if orient = null then "columns" else orient) frame

  /// <summary>
  /// Creates a new data frame resulting from a 'pivot' operation. Consider a denormalized data
  /// frame representing a table: column labels are field names &amp; table values are observations
  /// of those fields. pivotTable buckets the rows along two axes, according to the values of
  /// the columns `r` and `c`; and then computes a value for the frame of rows that land in each
  /// bucket.
  /// </summary>
  /// <param name="frame">The input data frame to pivot.</param>
  /// <param name="r">A column key to group on for the resulting row index</param>
  /// <param name="c">A column key to group on for the resulting col index</param>
  /// <param name="op">A function computing a value from the corresponding bucket frame</param>
  /// <category>Frame operations</category>
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

  /// <summary>
  /// Filters frame rows using the specified condition. Returns a new data frame
  /// that contains rows for which the provided function returned false. The function
  /// is called with `KeyValuePair` containing the row key as the `Key` and `Value`
  /// gives access to the row series.
  /// </summary>
  /// <param name="frame">A data frame to invoke the filtering function on.</param>
  /// <param name="condition">A delegate that specifies the filtering condition.</param>
  [<Extension>]
  static member Where(frame:Frame<'TRowKey, 'TColumnKey>, condition:Func<_, _>) =
    // Rebuild frame column-wise to preserve column ElementType when some columns are all-missing.
    let filteredRows = frame.Rows.Where(condition)
    let newIdx = filteredRows.Index
    let relocs = frame.IndexBuilder.Reindex(frame.RowIndex, newIdx, Lookup.Exact, VectorConstruction.Return 0, fun _ -> true)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newIdx.AddressingScheme relocs)
    Frame<_, _>(newIdx, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// <summary>
  /// Filters frame rows using the specified condtion. Returns a new data frame
  /// that contains rows for which the provided function returned false. The function
  /// is called with `KeyValuePair` containing the row key as the `Key` and `Value`
  /// gives access to the row series and a row index.
  /// </summary>
  /// <param name="frame">A data frame to invoke the filtering function on.</param>
  /// <param name="condition">A delegate that specifies the filtering condition.</param>
  [<Extension>]
  static member Where(frame:Frame<'TRowKey, 'TColumnKey>, condition:Func<_, _, _>) =
    // Rebuild frame column-wise to preserve column ElementType when some columns are all-missing.
    let filteredRows = frame.Rows.Where(condition)
    let newIdx = filteredRows.Index
    let relocs = frame.IndexBuilder.Reindex(frame.RowIndex, newIdx, Lookup.Exact, VectorConstruction.Return 0, fun _ -> true)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newIdx.AddressingScheme relocs)
    Frame<_, _>(newIdx, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

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

  /// <summary>
  /// Returns a frame with columns shifted by the specified offset. When the offset is
  /// positive, the values are shifted forward and first `offset` keys are dropped. When the
  /// offset is negative, the values are shifted backwards and the last `offset` keys are dropped.
  /// Expressed in pseudo-code:
  ///
  ///     result[k] = series[k - offset]
  /// </summary>
  /// <param name="offset">Can be both positive and negative number.</param>
  /// <param name="frame">The input frame whose columns are to be shifted.</param>
  /// <remarks>
  /// If you want to calculate the difference, e.g. `df - (Frame.shift 1 df)`, you can
  /// use `Frame.diff` which will be a little bit faster.
  /// </remarks>
  [<Extension>]
  static member Shift(frame:Frame<'TRowKey, 'TColumnKey>, offset) =
    frame |> Frame.shift offset

  /// <summary>
  /// Returns a frame with columns containing difference between an original value and
  /// a value at the specified offset. For example, calling `Frame.diff 1 s` returns a
  /// frame where previous column values is subtracted from the current ones. In pseudo-code, the
  /// function behaves as follows:
  ///
  ///     result[k] = series[k] - series[k - offset]
  ///
  /// Columns that cannot be converted to `float` are left without a change.
  /// </summary>
  /// <param name="offset">When positive, subtracts the past values from the current values; when negative, subtracts the future values from the current values.</param>
  /// <param name="frame">The input frame containing at least some `float` columns.</param>
  [<Extension>]
  static member Diff(frame:Frame<'TRowKey, 'TColumnKey>, offset) =
    frame |> Frame.diff offset

  /// <summary>
  /// Returns a frame where each value is the percentage change relative to the value at the
  /// specified offset. For example, calling <c>PctChange(1)</c> returns a frame where each
  /// value represents the relative change from the previous row's value. In pseudo-code:
  ///
  ///     result[k] = (frame[k] - frame[k - offset]) / frame[k - offset]
  ///
  /// Columns that cannot be converted to <c>float</c> are left without a change.
  /// This is commonly used in financial analysis to compute returns (e.g. daily stock returns).
  /// </summary>
  /// <param name="offset">When positive, computes change from past values; when negative, computes change relative to future values.</param>
  /// <param name="frame">The input frame containing at least some <c>float</c> columns.</param>
  [<Extension>]
  static member PctChange(frame:Frame<'TRowKey, 'TColumnKey>, offset) =
    frame |> Frame.pctChange offset

  [<Extension>]
  static member Reduce(frame:Frame<'TRowKey, 'TColumnKey>, aggregation:Func<'T, 'T, 'T>) =
    frame |> Frame.reduceValues (fun a b -> aggregation.Invoke(a, b))

  /// <category>Fancy accessors</category>
  [<Extension>]
  static member GetRows(frame:Frame<'TRowKey, 'TColumnKey>, [<ParamArray>] rowKeys:_[]) =
    frame.Rows.GetItems(rowKeys) |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  [<Extension>]
  static member FilterRowsBy(frame:Frame<'TRowKey, 'TColumnKey>, column, value) =
    Frame.filterRowsBy column value frame

  /// <summary>
  /// Returns a new data frame containing only the rows that have distinct values in the
  /// specified columns. When multiple rows have the same values in those columns, only
  /// the first row (in index order) is preserved.
  /// </summary>
  /// <param name="frame">Input data frame to be filtered.</param>
  /// <param name="columns">An array of column keys used to determine row uniqueness.</param>
  /// <category>Frame transformations</category>
  [<Extension>]
  static member DistinctRowsBy(frame:Frame<'TRowKey, 'TColumnKey>, [<ParamArray>] columns:'TColumnKey[]) =
    Frame.distinctRowsBy columns frame

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

  /// <summary>
  /// Fill missing values of a given type in the frame with a constant value.
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the provided filling value. The operation does not attempt to
  /// convert between numeric values (so a series containing `float` will not be
  /// converted to a series of `int`).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filled</param>
  /// <param name="value">A constant value that is used to fill all missing values</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, value:'T) =
    Frame.fillMissingWith value frame

  /// <summary>
  /// Fill missing values in the data frame with the nearest available value
  /// (using the specified direction). Note that the frame may still contain
  /// missing values after call to this function (e.g. if the first value is not available
  /// and we attempt to fill series with previous values). This operation can only be
  /// used on ordered frames.
  /// </summary>
  /// <param name="frame">An input data frame that is to be filled</param>
  /// <param name="direction">Specifies the direction used when searching for the nearest available value. `Backward` means that we want to look for the first value with a smaller key while `Forward` searches for the nearest greater key.</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, direction) =
    Frame.fillMissing direction frame

  /// <summary>
  /// Fill missing values in the frame using the specified function. The specified
  /// function is called with all series and keys for which the frame does not
  /// contain value and the result of the call is used in place of the missing value.
  ///
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the return type of the provided filling function. The operation
  /// does not attempt to convert between numeric values (so a series containing
  /// `float` will not be converted to a series of `int`).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filled</param>
  /// <param name="f">A function that takes a series <c>Series&lt;R, T&gt;</c> together with a key <c>K</c> in the series and generates a value to be used in a place where the original series contains a missing value.</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member FillMissing(frame:Frame<'TRowKey, 'TColumnKey>, f:Func<_, _, 'T>) =
    Frame.fillMissingUsing (fun s k -> f.Invoke(s, k)) frame

  /// <summary>
  /// Creates a new data frame that contains only those rows of the original
  /// data frame that are _dense_, meaning that they have a value for each column.
  /// The resulting data frame has the same number of columns, but may have
  /// fewer rows (or no rows at all).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filtered</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member DropSparseRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropSparseRows frame

  /// <summary>
  /// Creates a new data frame that contains only those rows that are empty for each column.
  /// The resulting data frame has the same number of columns, but may have
  /// fewer rows (or no rows at all).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filtered</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member DropEmptyRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropEmptyRows frame

  /// <summary>
  /// Creates a new data frame that contains only those columns of the original
  /// data frame that are _dense_, meaning that they have a value for each row.
  /// The resulting data frame has the same number of rows, but may have
  /// fewer columns (or no columns at all).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filtered</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member DropSparseColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropSparseCols frame

  /// <summary>
  /// Creates a new data frame that drops those columns that are empty for each row.
  /// The resulting data frame has the same number of rows, but may have
  /// fewer columns (or no columns at all).
  /// </summary>
  /// <param name="frame">An input data frame that is to be filtered</param>
  /// <category>Missing values</category>
  [<Extension>]
  static member DropEmptyColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.dropEmptyCols frame

  // ----------------------------------------------------------------------------------------------
  // Obsolete - kept for temporary compatibility
  // ----------------------------------------------------------------------------------------------

  /// <exclude />
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member OrderRows(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame
  /// <exclude />
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member SortByRowKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortRowsByKey frame
  /// <exclude />
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member OrderColumns(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame
  /// <exclude />
  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member SortByColKey(frame:Frame<'TRowKey, 'TColumnKey>) = Frame.sortColsByKey frame
  /// <exclude />
  [<Extension; Obsolete("Use overload taking TextWriter instead")>]
  static member SaveCsv(frame:Frame<'R, 'C>, stream:Stream, [<Optional>] includeRowKeys, [<Optional>] keyNames, [<Optional>] separator, [<Optional>] culture) =
    let separator = if separator = '\000' then None else Some separator
    let culture = if culture = null then None else Some culture
    let keyNames = if keyNames = Unchecked.defaultof<_> then None else Some keyNames
    use writer = new StreamWriter(stream)
    FrameUtils.writeCsv (writer) None separator culture (Some includeRowKeys) keyNames frame

