namespace FSharp.DataFrame

module internal Reflection = 
  open System.Linq
  open System.Linq.Expressions
  open Microsoft.FSharp.Reflection
  open Microsoft.FSharp

  let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance

  let enumerableSelect =
    match <@@ Enumerable.Select([0], fun v -> v) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() | _ -> failwith "Could not find Enumerable.Select"
  let enumerableToArray =
    match <@@ Enumerable.ToArray([0]) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() | _ -> failwith "Could not find Enumerable.ToArray"
  let createNonOpt = 
    match <@@ vectorBuilder.CreateNonOptional([| |]) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() | _ -> failwith "Could not find vectorBuilder.CreateNonOptional"

  let getRecordConvertors<'T>() = 
    let recdTy = typeof<'T>
    let fields = recdTy.GetProperties() |> Seq.filter (fun p -> p.CanRead) 
    let colIndex = indexBuilder.Create<string>([ for f in fields -> f.Name ], Some false)
    let fieldConvertors = 
      [| for f in fields ->
          // Information about the types involved...
          let fldTy = f.PropertyType
          // Build: fun recd -> recd.<Field>
          let recd = Expression.Parameter(recdTy)
          let func = Expression.Lambda(Expression.Call(recd, f.GetGetMethod()), [recd])
          // Build: Enumerable.ToArray(Enumerable.Select(<input>, fun recd -> recd.<Field>))
          let input = Expression.Parameter(typeof<seq<'T>>)
          let selected = Expression.Call(enumerableSelect.MakeGenericMethod [| recdTy; fldTy |], input, func)
          let body = Expression.Call(enumerableToArray.MakeGenericMethod [| fldTy |], selected)
          // Build: vectorBuilder.CreateNonOptional( ... body ... )
          let conv = Expression.Call(Expression.Constant(vectorBuilder), createNonOpt.MakeGenericMethod [| fldTy |], body)
          // Compile & run
          Expression.Lambda(conv, [input]).Compile() |]
    colIndex, fieldConvertors

  let convertRecordSequence<'T>(data:seq<'T>) =
    let colIndex, convertors = getRecordConvertors<'T>()
    let frameData = 
      [| for convFunc in convertors ->
          convFunc.DynamicInvoke( [| box data |] ) :?> IVector |]
      |> vectorBuilder.CreateNonOptional
    Frame<int, string>(Index.Create [0 .. (Seq.length data) - 1], colIndex, frameData)



[<AutoOpen>]
module FSharp =
  open System

  type Frame = 
    static member ofRowsOrdinal(rows:seq<#Series<_, _>>) = 
      let keys = rows |> Seq.mapi (fun i _ -> i)
      Frame.FromRows(Series(keys, rows))
    static member ofRows(rows:seq<_ * #Series<_, _>>) = 
      let names, values = rows |> List.ofSeq |> List.unzip
      Frame.FromRows(Series(names, values))
    static member ofRows(rows) = 
      Frame.FromRows(rows)
    static member ofRowKeys(keys) = 
      let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance // TODO: Capture somewhere
      let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
      Frame<_, string>(indexBuilder.Create(keys, None), indexBuilder.Create([], None), vectorBuilder.CreateNonOptional [||])
    
    static member ofColumns(cols) = 
      Frame.FromColumns(cols)
    static member ofColumns(cols:seq<_ * Series<_, _>>) = 
      let names, values = cols |> List.ofSeq |> List.unzip
      Frame.FromColumns(Series(names, values))
    
    static member ofValues(values) =
      values 
      |> Seq.groupBy (fun (row, col, value) -> col)
      |> Seq.map (fun (col, items) -> 
          let keys, _, values = Array.ofSeq items |> Array.unzip3
          col, Series(keys, values) )
      |> Frame.ofColumns

  let (=>) a b = a, b
  let ($) f series = Series.mapValues f series
      
[<AutoOpen>]
module FrameExtensions =
  open FSharp.Data
  open ProviderImplementation
  open FSharp.DataFrame.Vectors 
  open FSharp.Data.RuntimeImplementation
  open FSharp.Data.RuntimeImplementation.StructuralTypes
  
  type Frame with
    static member ReadCsv(file:string, ?inferTypes, ?schema, ?inferRows, ?separators) =

      let inferRows = defaultArg inferRows 0
      let missingValues = "NaN,NA,#N/A,:"
      let missingValuesArr = [| "NaN"; "NA"; "#N/A"; ":" |]
      let culture = ""
      let cultureInfo = System.Globalization.CultureInfo.InvariantCulture
      
      let safeMode = false // Irrelevant - all DF values can be missing
      let preferOptionals = true // Ignored

      let createVector typ (data:string[]) = 
        if typ = typeof<bool> then Vector.CreateNA (Array.map (fun s -> Operations.ConvertBoolean(culture, Some(s))) data) :> IVector
        elif typ = typeof<decimal> then Vector.CreateNA (Array.map (fun s -> Operations.ConvertDecimal(culture, Some(s))) data) :> IVector
        elif typ = typeof<float> then Vector.CreateNA (Array.map (fun s -> Operations.ConvertFloat(culture, missingValues, Some(s))) data) :> IVector
        elif typ = typeof<int> then Vector.CreateNA (Array.map (fun s -> Operations.ConvertInteger(culture, Some(s))) data) :> IVector
        elif typ = typeof<int64> then Vector.CreateNA (Array.map (fun s -> Operations.ConvertInteger64(culture, Some(s))) data) :> IVector
        else Vector.Create data :> IVector

      // If 'inferTypes' is specified (or by default), use the CSV type inference
      // to load information about types in the CSV file. By default, use the entire
      // content (but inferRows can be set to smaller number). Otherwise we just
      // "infer" all columns as string.
      let inferedProperties = 
        let data = Csv.CsvFile.Load(file, ?separators=separators)
        if not (inferTypes = Some false) then
          CsvInference.inferType 
            data inferRows (missingValuesArr, cultureInfo) (defaultArg schema "") safeMode preferOptionals
          ||> CsvInference.getFields preferOptionals
        else 
          if data.Headers.IsNone then failwith "CSV file is missing headers!"
          [ for c in data.Headers.Value -> 
              PrimitiveInferedProperty.Create(c, typeof<string>, true) ]

      // Load the data and convert the values to the appropriate type
      let data = Csv.CsvFile.Load(file, ?separators=separators).Cache()
      let columnIndex = Index.Create data.Headers.Value
      let columns = 
        [| for name, prop in Seq.zip data.Headers.Value inferedProperties  ->
             [| for row in data.Data -> row.GetColumn(name) |]
             |> createVector prop.RuntimeType |]
      let rowIndex = Index.Create [ 0 .. (Seq.length data.Data) - 1 ]
      Frame(rowIndex, columnIndex, Vector.Create columns)


  type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
    member frame.Where(condition) =
      frame.Rows.Where(condition) |> Frame.ofRows
      
    member frame.Append(rowKey, row) =
      frame.Append(Frame.ofRows [ rowKey => row ])
      
    member frame.WithMissing(value) = 
      let fillFunc = VectorHelpers.fillNA value
      let data = frame.Data.Select fillFunc
      Frame<'TRowKey, 'TColumnKey>(frame.RowIndex, frame.ColumnIndex, data)

    member frame.WithOrderedRows() = 
      let newRowIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.RowIndex, Vectors.Return 0)
      let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder rowCmd)
      Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

    member x.WithColumnIndex(columnKeys:seq<'TNewColumnKey>) =
      let columns = seq { for v in x.Columns.Values -> v :> ISeries<_> }
      Frame<_, _>(columnKeys, columns)

    member x.WithRowIndex<'TNewRowIndex when 'TNewRowIndex : equality>(column) =
      let columnVec = x.GetSeries<'TNewRowIndex>(column)
      let lookup addr = columnVec.Vector.GetValue(addr)
      let newRowIndex, rowCmd = x.IndexBuilder.WithIndex(x.RowIndex, lookup, Vectors.Return 0)
      let newData = x.Data.Select(VectorHelpers.transformColumn x.VectorBuilder rowCmd)
      Frame<_, _>(newRowIndex, x.ColumnIndex, newData)

    member frame.ReplaceRowIndexKeys<'TNewRowIndex when 'TNewRowIndex : equality>(keys:seq<'TNewRowIndex>) =
      let newRowIndex = frame.IndexBuilder.Create(keys, None)
      let getRange = VectorHelpers.getVectorRange frame.VectorBuilder frame.RowIndex.Range
      let newData = frame.Data.Select(getRange)
      Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

    member frame.ReindexRowKeys(keys) = 
      // Create empty frame with the required keys    
      let empty = Frame<_, _>(frame.IndexBuilder.Create(keys, None), frame.IndexBuilder.Create([], None), frame.VectorBuilder.CreateNonOptional [||])
      for key, series in frame.Columns.Observations do 
        empty.AddSeries(key, series)
      empty

    member frame.GroupRowsBy<'TGroup when 'TGroup : equality>(key) =
      frame.Rows |> Series.groupInto (fun _ v -> v.GetAs<'TGroup>(key)) (fun k g -> g |> Frame.ofRows)

    member frame.GroupRowsInto<'TGroup when 'TGroup : equality>(key, f:System.Func<_, _, _>) =
      frame.Rows |> Series.groupInto (fun _ v -> v.GetAs<'TGroup>(key)) (fun k g -> f.Invoke(k, g |> Frame.ofRows))

type column<'T>(value:obj) =
  member x.Value = value

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Frame = 
  open FSharp.DataFrame.Internal

  // ----------------------------------------------------------------------------------------------
  // Grouping
  // ----------------------------------------------------------------------------------------------

  let groupRowsInto column f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupInto 
      (fun _ v -> v.Get(column)) 
      (fun k g -> g |> Frame.ofRows |> f)

  let groupRowsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    groupRowsInto column id frame

  //let shiftRows offset (frame:Frame<'TRowKey, 'TColKey>) = 
  //  frame.Columns 
  //  |> Series.map (fun k col -> Series.shift offset col)
  //  |> Frame.ofColumns

  let takeLast count (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.takeLast count |> Frame.ofRows

  let dropMissing (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowsDense |> Series.dropMissing |> Frame.ofRows


  // ----------------------------------------------------------------------------------------------
  // Wrappers that simply call member functions of the data frame
  // ----------------------------------------------------------------------------------------------

  /// Creates a new data frame that contains all data from 
  /// the original data frame, together with additional series.
  [<CompiledName("AddSeries")>]
  let addSeries column series (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.AddSeries(column, series); f

  /// Append two data frames. The columns of the resulting data frame
  /// will be the union of columns of the two data frames. The row keys
  /// may overlap, but the values must not - if there is a value for a
  /// certain column, at the same row index in both data frames, an exception
  /// is thrown. 
  [<CompiledName("Append")>]
  let append (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Append(frame2)

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing _series_ representing
  /// individual columns of the frame.
  [<CompiledName("Columns")>]
  let columns (frame:Frame<'TRowKey, 'TColKey>) = frame.Columns

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame.
  [<CompiledName("Rows")>]
  let rows (frame:Frame<'TRowKey, 'TColKey>) = frame.Rows

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing _series_ representing
  /// individual columns of the frame. This is similar to `Columns`, but it
  /// skips columns that contain missing value in _any_ row.
  [<CompiledName("ColumnsDense")>]
  let columnsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.ColumnsDense

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame. This is similar to `Rows`, but it
  /// skips rows that contain missing value in _any_ column.
  [<CompiledName("RowsDense")>]
  let rowsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.RowsDense

  /// Creates a new data frame that contains all data from the original
  /// data frame without the specified series (column).
  [<CompiledName("DropSeries")>]
  let dropSeries column (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.DropSeries(column); f

  /// Creates a new data frame where the specified column is repalced
  /// with a new series. (If the series does not exist, only the new
  /// series is added.)
  [<CompiledName("ReplaceSeries")>]
  let replaceSeries column series (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.ReplaceSeries(column, series); f

  /// Returns a specified series (column) from a data frame. This 
  /// function uses exact matching semantics. Use `lookupSeries` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetSeries")>]
  let getSeries column (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column)

  /// Returns a specified row from a data frame. This 
  /// function uses exact matching semantics. Use `lookupRow` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetRow")>]
  let getRow row (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row)

  /// Returns a specified series (column) from a data frame. If the data frame has 
  /// ordered column index, the lookup semantics can be used to get series
  /// with nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupSeries")>]
  let lookupSeries column lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column, lookup)

  /// Returns a specified row from a data frame. If the data frame has 
  /// ordered row index, the lookup semantics can be used to get row with 
  /// nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupRow")>]
  let lookupRow row lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row, lookup)

  /// Returns a data frame that contains the same data as the argument, 
  /// but whose rows are ordered series. This allows using inexact lookup
  /// for rows (e.g. using `lookupRow`) or inexact left/right joins.
  [<CompiledName("OrderRows")>]
  let orderRows (frame:Frame<'TRowKey, 'TColKey>) = frame.WithOrderedRows()

  /// Creates a new data frame that uses the specified column as an row index.
  [<CompiledName("WithRowIndex")>]
  let withRowIndex (column:column<'TNewRowKey>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.WithRowIndex<'TNewRowKey>(unbox<'TColKey> column.Value)

  /// Creates a new data frame that uses the specified list of keys as a new column index.
  [<CompiledName("WithColumnKeys")>]
  let withColumnKeys (keys:seq<'TNewColKey>) (frame:Frame<'TRowKey, 'TColKey>) = frame.WithColumnIndex(keys)

  /// Join two frames using the specified kind of join. This function uses
  /// exact matching on keys. If you want to align nearest smaller or greater
  /// keys in left or outer join, use `joinAlign`. 
  [<CompiledName("Join")>]
  let join kind (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Join(frame2, kind)

  /// Join two frames using the specified kind of join and 
  /// the specified lookup semantics.
  [<CompiledName("JoinAlign")>]
  let joinAlign kind lookup (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Join(frame2, kind, lookup)

  
  /// Fills all missing values in all columns & rows of the data frame with the
  /// specified value (this can only be used when the data is homogeneous)
  [<CompiledName("WithMissing")>]
  let withMissingVal defaultValue (frame:Frame<'TRowKey, 'TColKey>) =
    frame.WithMissing(defaultValue)

  // ----------------------------------------------------------------------------------------------
  // TBD
  // ----------------------------------------------------------------------------------------------

  let inline filterRows f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Where(fun kvp -> f kvp.Key kvp.Value) 

  // ----------------------------------------------------------------------------------------------
  // Additional functions for working with data frames
  // ----------------------------------------------------------------------------------------------

  let inline mean (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.mean)

  let inline sum (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sum)

  let inline countValues (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<obj>() |> Series.map (fun _ -> Series.countValues)

  let countKeys (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowIndex.Keys |> Seq.length

  let inline sdv (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdv)

  let inline diff offset (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.mapValues (fun s -> Series.diff offset (s.As<float>())) |> Frame.ofColumns


[<AutoOpen>]
module FSharp2 =
  type FSharp.Frame with
    static member ofRecords (series:Series<'K, 'R>) =
      let keyValuePairs = 
        seq { for k, v in series.ObservationsOptional do 
                if v.HasValue then yield k, v.Value }
      let recordsToConvert = Seq.map snd keyValuePairs
      let frame = Reflection.convertRecordSequence<'R>(recordsToConvert)
      let frame = frame.ReplaceRowIndexKeys(Seq.map fst keyValuePairs)
      frame.ReindexRowKeys(series.Keys)

    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)    
