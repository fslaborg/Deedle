namespace FSharp.DataFrame

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors

type JoinKind = 
  | Outer = 0
  | Inner = 1
  | Left = 2
  | Right = 3

module internal FrameHelpers =
  // A "generic function" that boxes all values of a vector (IVector<int, 'T> -> IVector<int, obj>)
  let boxVector v = VectorHelpers.boxVector v
  // A "generic function" that transforms a generic vector using specified transformation
  let transformColumn vb cmd = VectorHelpers.transformColumn vb cmd

  // A "generic function" that changes the type of vector elements
  let changeType<'R> : IVector -> IVector<'R> = 
    { new VectorHelpers.VectorCallSite1<IVector<'R>> with
        override x.Invoke<'T>(col:IVector<'T>) = 
          col.Select(fun v -> System.Convert.ChangeType(v, typeof<'R>) :?> 'R) }
    |> VectorHelpers.createDispatcher

  // A "generic function" that fills NA values
  let fillNA (def:obj) : IVector -> IVector = 
    { new VectorHelpers.VectorCallSite1<IVector> with
        override x.Invoke<'T>(col:IVector<'T>) = 
          col.SelectOptional(function
            | OptionalValue.Missing -> OptionalValue(unbox def)
            | OptionalValue.Present v -> OptionalValue(v)) :> IVector }
    |> VectorHelpers.createDispatcher
  
open FrameHelpers

/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>
    internal ( rowIndex:IIndex<'TRowKey>, columnIndex:IIndex<'TColumnKey>, 
               data:IVector<IVector>) =

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  /// Vector builder
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
  let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

  // TODO: Perhaps assert that the 'data' vector has all things required by column index
  // (to simplify various handling below)

  let mutable rowIndex = rowIndex
  let mutable columnIndex = columnIndex
  let mutable data = data

  let createRowReader rowAddress =
    // 'let rec' would be more elegant, but it is slow...
    let virtualVector = ref (Unchecked.defaultof<_>)
    let materializeVector() =
      let data = (virtualVector : ref<IVector<_>>).Value.DataSequence
      virtualVector := Vector.CreateNA(data)
    virtualVector :=
      { new IVector<obj> with
          member x.GetValue(columnAddress) = 
            let vector = data.GetValue(columnAddress)
            if not vector.HasValue then OptionalValue.Missing
            else vector.Value.GetObject(rowAddress) 
          member x.Data = 
            [| for _, addr in columnIndex.Mappings -> x.GetValue(addr) |]
            |> IReadOnlyList.ofArray |> VectorData.SparseList          
          member x.Select(f) = materializeVector(); virtualVector.Value.Select(f)
          member x.SelectOptional(f) = materializeVector(); virtualVector.Value.SelectOptional(f)
        
        interface IVector with
          member x.SuppressPrinting = false
          member x.ElementType = typeof<obj>
          member x.GetObject(i) = (x :?> IVector<obj>).GetValue(i) }
    VectorHelpers.delegatedVector virtualVector

  let safeGetRowVector row = 
    let rowVect = rowIndex.Lookup(row)
    if not rowVect.HasValue then invalidArg "index" (sprintf "The data frame does not contain row with index '%O'" row) 
    else  createRowReader rowVect.Value

  let safeGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then 
      invalidArg "column" (sprintf "Column with a key '%O' does not exist in the data frame" column)
    let columnVector = data.GetValue columnIndex.Value
    if not columnVector.HasValue then
      invalidOp "column" (sprintf "Column with a key '%O' is present, but does not contain a value" column)
    columnVector.Value
  
  member private x.tryGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then OptionalValue.Missing else
    data.GetValue columnIndex.Value
  member private x.indexBuilder = indexBuilder
  member private x.vectorBuilder = vectorBuilder

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  // ----------------------------------------------------------------------------------------------
  // Frame operations - joins
  // ----------------------------------------------------------------------------------------------

  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>, ?kind, ?lookup) =    
    // Union row indices and get transformations to apply to left/right vectors
    let lookup = defaultArg lookup Lookup.Exact
    let newRowIndex, thisRowCmd, otherRowCmd = 
      match kind with 
      | Some JoinKind.Inner ->
          indexBuilder.Intersect(rowIndex, otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)
      | Some JoinKind.Left ->
          let otherRowCmd = indexBuilder.Reindex(otherFrame.RowIndex, rowIndex, lookup, Vectors.Return 0)
          rowIndex, Vectors.Return 0, otherRowCmd
      | Some JoinKind.Right ->
          let thisRowCmd = indexBuilder.Reindex(rowIndex, otherFrame.RowIndex, lookup, Vectors.Return 0)
          otherFrame.RowIndex, thisRowCmd, Vectors.Return 0
      | Some JoinKind.Outer | None | Some _ ->
          indexBuilder.Union(rowIndex, otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)

    // Append the column indices and get transformation to combine them
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newColumnIndex, colCmd = 
      indexBuilder.Append(columnIndex, otherFrame.ColumnIndex, Vectors.Return 0, Vectors.Return 1, VectorValueTransform.LeftOrRight)
    // Apply transformation to both data vectors
    let newThisData = data.Select(transformColumn vectorBuilder thisRowCmd)
    let newOtherData = otherFrame.Data.Select(transformColumn vectorBuilder otherRowCmd)
    // Combine column vectors a single vector & return results
    let newData = vectorBuilder.Build(colCmd, [| newThisData; newOtherData |])
    Frame(newRowIndex, newColumnIndex, newData)

  member frame.Append(otherFrame:Frame<'TRowKey, 'TColumnKey>) = 
    // Union the column indices and get transformations for both
    let newColumnIndex, thisColCmd, otherColCmd = 
      indexBuilder.Union(columnIndex, otherFrame.ColumnIndex, Vectors.Return 0, Vectors.Return 1)

    // Append the row indices and get transformation that combines two column vectors
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newRowIndex, rowCmd = 
      indexBuilder.Append(rowIndex, otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 1, VectorValueTransform.LeftOrRight)

    // Transform columns - if we have both vectors, we need to append them
    let appendVector = 
      { new VectorHelpers.VectorCallSite2<IVector> with
          override x.Invoke<'T>(col1:IVector<'T>, col2:IVector<'T>) = 
            vectorBuilder.Build(rowCmd, [| col1; col2 |]) :> IVector }
    |> VectorHelpers.createTwoArgDispatcher
    // .. if we only have one vector, we need to pad it 
    let padVector isLeft = 
      { new VectorHelpers.VectorCallSite1<IVector> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            let empty = Vector.Create []
            let args = if isLeft then [| col; empty |] else [| empty; col |]
            vectorBuilder.Build(rowCmd, args) :> IVector }
      |> VectorHelpers.createDispatcher
    let padLeftVector, padRightVector = padVector true, padVector false

    let append = VectorValueTransform.Create(fun (l:OptionalValue<IVector>) r ->
      if l.HasValue && r.HasValue then OptionalValue(appendVector (l.Value, r.Value))
      elif l.HasValue then OptionalValue(padLeftVector l.Value)
      elif r.HasValue then OptionalValue(padRightVector r.Value)
      else OptionalValue.Missing )

    let newDataCmd = Vectors.Combine(thisColCmd, otherColCmd, append)
    let newData = vectorBuilder.Build(newDataCmd, [| data; otherFrame.Data |])

    Frame(newRowIndex, newColumnIndex, newData)

  // ----------------------------------------------------------------------------------------------
  // df.Rows and df.Columns
  // ----------------------------------------------------------------------------------------------

  // TODO: These may be accessed often.. we need to cache them?

  member frame.GetColumns<'R>() = 
    Series.Create(columnIndex, data.Select(fun vect -> 
      Series.Create(rowIndex, changeType<'R> vect)))

  member frame.Columns = 
    Series.Create(columnIndex, data.Select(fun vect -> 
      Series.CreateUntyped(rowIndex, boxVector vect)))

  member frame.ColumnsDense = 
    Series.Create(columnIndex, data.SelectOptional(fun vect -> 
      // Assuming that the data has all values - which should be an invariant...
      let all = rowIndex.Mappings |> Seq.forall (fun (key, addr) -> vect.Value.GetObject(addr).HasValue)
      if all then OptionalValue(Series.CreateUntyped(rowIndex, boxVector vect.Value))
      else OptionalValue.Missing ))

  member frame.Rows = 
    let emptySeries = Series<_, _>(rowIndex, Vector.Create [], vectorBuilder, indexBuilder)
    emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key, Lookup.Exact)
      if not rowAddress.HasValue then OptionalValue.Missing
      else OptionalValue(Series.CreateUntyped(columnIndex, createRowReader rowAddress.Value)))

  member frame.RowsDense = 
    let emptySeries = Series<_, _>(rowIndex, Vector.Create [], vectorBuilder, indexBuilder)
    emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key, Lookup.Exact)
      if not rowAddress.HasValue then OptionalValue.Missing else 
        let rowVec = createRowReader rowAddress.Value
        let all = columnIndex.Mappings |> Seq.forall (fun (key, addr) -> rowVec.GetValue(addr).HasValue)
        if all then OptionalValue(Series.CreateUntyped(columnIndex, rowVec))
        else OptionalValue.Missing )

  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  member frame.Clone() =
    Frame<_, _>(rowIndex, columnIndex, data)

  member frame.GetRow<'R>(row:'TRowKey, ?lookup) : Series<'TColumnKey, 'R> = 
    let row = frame.Rows.Get(row, ?lookup = lookup)
    Series.Create(columnIndex, changeType row.Vector)

  member frame.AddSeries(column:'TColumnKey, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.CreateUnsorted [column], Vector.Create [series.Vector :> IVector ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data

  member frame.DropSeries(column:'TColumnKey) = 
    let newColumnIndex, colCmd = indexBuilder.DropItem(columnIndex, column, Vectors.Return 0)    
    columnIndex <- newColumnIndex
    data <- vectorBuilder.Build(colCmd, [| data |])

  member frame.ReplaceSeries(column:'TColumnKey, series:Series<_, _>, ?lookup) = 
    let lookup = defaultArg lookup Lookup.Exact
    if columnIndex.Lookup(column, lookup).HasValue then
      frame.DropSeries(column)
    frame.AddSeries(column, series)

  member frame.GetSeries<'R>(column:'TColumnKey, ?lookup) : Series<'TRowKey, 'R> = 
    let lookup = defaultArg lookup Lookup.Exact
    match safeGetColVector(column, lookup) with
    | :? IVector<'R> as vec -> 
        Series.Create(rowIndex, vec)
    | colVector ->
        Series.Create(rowIndex, changeType colVector)

  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceSeries(column, series)

  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceSeries(column, Series.Create(frame.RowIndex, Vector.Create data))

  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  interface IFormattable with
    member frame.Format() = 
      seq { yield ""::[ for colName, _ in frame.ColumnIndex.Mappings do yield colName.ToString() ]
            let rows = frame.Rows
            for ind, addr in frame.RowIndex.Mappings do
              let row = rows.[ind]
              yield 
                (ind.ToString() + " ->")::
                [ for _, value in row.ObservationsOptional ->  // TODO: is this good?
                    value.ToString() ] }
      |> array2D
      |> Formatting.formatTable

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  new(names:seq<'TColumnKey>, columns:seq<ISeries<'TRowKey>>) =
    let df = Frame(Index.Create [], Index.Create [], Vector.Create [])
    let df = (df, Seq.zip names columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.CreateUnsorted [colKey], Vector.Create [colData.Vector])
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data)

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

and Frame =
  static member internal Register() = 
    Series.SeriesOperations <-
      { new SeriesOperations with
          member x.OuterJoin<'TIndex2, 'TValue2 when 'TIndex2 : equality>
              (series1:Series<'TIndex2, 'TValue2>, series2:Series<'TIndex2, 'TValue2>) = 
            let frame1 = Frame(series1.Index, Index.Create [0], Vector.Create [| series1.Vector :> IVector |])
            let frame2 = Frame(series2.Index, Index.Create [1], Vector.Create [| series2.Vector :> IVector |])
            let joined = frame1.Join(frame2)
            joined.Rows.Select(fun row -> row.Value :> Series<_, _>) }

  static member internal Create<'TColumnKey, 'TRowKey, 'TValue when 'TColumnKey : equality and 'TRowKey : equality>
      (column:'TColumnKey, series:Series<'TRowKey, 'TValue>) = 
    let data = Vector.Create [| series.Vector :> IVector |]
    Frame(series.Index, Index.Create [column], data)

  static member internal CreateRow(row:'TRowKey, series:Series<'TColumnKey, 'TValue>) = 
    let data = series.Vector.SelectOptional(fun v -> 
      let res = Vectors.ArrayVector.ArrayVectorBuilder.Instance.CreateOptional [| v |] 
      OptionalValue(res :> IVector))
    Frame(Index.Create [row], series.Index, data)

  static member internal FromRows<'TRowKey, 'TColumnKey, 'TSeries, 'TValue 
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> Series<'TColumnKey, 'TValue>>
      (nested:Series<'TRowKey, 'TSeries>) =
    // Append all rows in some order
    let initial = Frame(Index.CreateUnsorted [], Index.Create [], Vector.Create [| |])
    let folded =
      (initial, nested.ObservationsOptional) ||> Seq.fold (fun df (name, series) -> 
        if not series.HasValue then df 
        else Frame.CreateRow(name, series.Value).Append(df))

    // Reindex according to the original index
    let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance // TODO: Capture somewhere
    let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

    let rowCmd = indexBuilder.Reindex(folded.RowIndex, nested.Index, Lookup.Exact, Vectors.Return 0)
    let newRowIndex = nested.Index
    let newColumnIndex = folded.ColumnIndex
    let newData = folded.Data.Select(transformColumn vectorBuilder rowCmd)
    Frame(newRowIndex, newColumnIndex, newData)

  static member internal FromColumns<'TRowKey, 'TColumnKey, 'TSeries, 'TValue 
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> Series<'TRowKey, 'TValue>>
      (nested:Series<'TColumnKey, 'TSeries>) =
    let initial = Frame(Index.Create [], Index.CreateUnsorted [], Vector.Create [| |])
    (initial, nested.ObservationsOptional) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.Join(Frame.Create(name, series.Value), JoinKind.Outer))


  // ----------------------------------------------------------------------------------------------
  // Reindexing 
  // ----------------------------------------------------------------------------------------------

type Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with

  member frame.WithMissing(value) = 
    let fillFunc = fillNA value
    let data = frame.Data.Select fillFunc
    Frame(frame.RowIndex, frame.ColumnIndex, data)

  member frame.WithOrderedRows() = 
    let newRowIndex, rowCmd = frame.indexBuilder.OrderIndex(frame.RowIndex, Vectors.Return 0)
    let newData = frame.Data.Select(transformColumn frame.vectorBuilder rowCmd)
    Frame(newRowIndex, frame.ColumnIndex, newData)

  member x.WithColumnIndex(columnKeys:seq<_>) =
    let columns = seq { for v in x.Columns.Values -> v :> ISeries<_> }
    
    // Copy & paste from the new() constructor (oops)
    let df = Frame(Index.Create [], Index.Create [], Vector.Create [])
    let df = (df, Seq.zip columnKeys columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.CreateUnsorted [colKey], Vector.Create [colData.Vector])
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data)


  member x.WithRowIndex<'TNewRowIndex when 'TNewRowIndex : equality>(column) =
    let columnVec = x.GetSeries<'TNewRowIndex>(column)
    let lookup addr = columnVec.Vector.GetValue(addr)
    let newRowIndex, rowCmd = x.indexBuilder.WithIndex(x.RowIndex, lookup, Vectors.Return 0)
    let newData = x.Data.Select(FrameHelpers.transformColumn x.vectorBuilder rowCmd)
    Frame(newRowIndex, x.ColumnIndex, newData)

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

  let convertRecordSequence<'T>(data:seq<'T>) =
    let recdTy = typeof<'T>
    let fields = recdTy.GetProperties() |> Seq.filter (fun p -> p.CanRead) 
    let colIndex = indexBuilder.Create<string>([ for f in fields -> f.Name ], Some false)
    let frameData = 
      [| for f in fields do
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
          let convFunc = Expression.Lambda(conv, [input]).Compile()
          yield convFunc.DynamicInvoke( [| box data |] ) :?> IVector |]
      |> vectorBuilder.CreateNonOptional
    Frame<int, string>(Index.Create [0 .. (Seq.length data) - 1], colIndex, frameData)

[<AutoOpen>]
module FSharp =
  type Series = 
    static member ofObservations(observations) = 
      Series(Seq.map fst observations, Seq.map snd observations)
    static member ofValues(values) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values)
    
  type Frame = 
    static member ofRecords (values:seq<'T>) =
      Reflection.convertRecordSequence<'T>(values)    

    static member ofRowsOrdinal(rows:seq<#Series<_, _>>) = 
      let keys = rows |> Seq.mapi (fun i _ -> i)
      Frame.FromRows(Series(keys, rows))
    static member ofRows(rows:seq<_ * #Series<_, _>>) = 
      let names, values = rows |> List.ofSeq |> List.unzip
      Frame.FromRows(Series(names, values))
    static member ofRows(rows) = 
      Frame.FromRows(rows)
    
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

[<AutoOpen>]
module FrameExtensions =
  open FSharp.Data
  open ProviderImplementation
  open FSharp.Data.RuntimeImplementation
  open FSharp.Data.RuntimeImplementation.StructuralTypes
  
  type Frame with
    static member ReadCsv(file:string, ?inferTypes, ?schema, ?inferRows) =

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
        let data = Csv.CsvFile.Load(file)
        if not (inferTypes = Some false) then
          CsvInference.inferType 
            data inferRows (missingValuesArr, cultureInfo) (defaultArg schema "") safeMode preferOptionals
          ||> CsvInference.getFields preferOptionals
        else 
          if data.Headers.IsNone then failwith "CSV file is missing headers!"
          [ for c in data.Headers.Value -> 
              PrimitiveInferedProperty.Create(c, typeof<string>, true) ]

      // Load the data and convert the values to the appropriate type
      let data = Csv.CsvFile.Load(file).Cache()
      let columnIndex = Index.Create data.Headers.Value
      let columns = 
        [| for name, prop in Seq.zip data.Headers.Value inferedProperties  ->
             [| for row in data.Data -> row.GetColumn(name) |]
             |> createVector prop.RuntimeType |]
      let rowIndex = Index.Create [ 0 .. (Seq.length data.Data) - 1 ]
      Frame(rowIndex, columnIndex, Vector.Create columns)
