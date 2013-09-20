namespace FSharp.DataFrame

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

open FSharp.DataFrame
open System.ComponentModel
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors

open System.Runtime.InteropServices
open VectorHelpers

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
      virtualVector := Vector.ofOptionalValues(data)
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
          member x.SelectMissing(f) = materializeVector(); virtualVector.Value.SelectMissing(f)
        
        interface IVector with
          member x.SuppressPrinting = false
          member x.ElementType = typeof<obj>
          member x.GetObject(i) = (x :?> IVector<obj>).GetValue(i) }
    VectorHelpers.delegatedVector virtualVector

  let safeGetRowVector row = 
    let rowVect = rowIndex.Lookup(row)
    if not rowVect.HasValue then invalidArg "index" (sprintf "The data frame does not contain row with index '%O'" row) 
    else  createRowReader (snd rowVect.Value)

  let safeGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then 
      invalidArg "column" (sprintf "Column with a key '%O' does not exist in the data frame" column)
    let columnVector = data.GetValue (snd columnIndex.Value)
    if not columnVector.HasValue then
      invalidOp "column" (sprintf "Column with a key '%O' is present, but does not contain a value" column)
    columnVector.Value
  
  member private x.tryGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then OptionalValue.Missing else
    data.GetValue (snd columnIndex.Value)
  member internal x.IndexBuilder = indexBuilder
  member internal x.VectorBuilder = vectorBuilder

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  member frame.RowKeys = rowIndex.Keys
  member frame.ColumnKeys = columnIndex.Keys

  // ----------------------------------------------------------------------------------------------
  // Frame operations - joins
  // ----------------------------------------------------------------------------------------------

  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>, ?kind, ?lookup) =    
    let lookup = defaultArg lookup Lookup.Exact

    let restrictToRowIndex (restriction:IIndex<_>) (sourceIndex:IIndex<_>) vector = 
      if restriction.Ordered then
        let min, max = rowIndex.KeyRange
        sourceIndex.Builder.GetRange(sourceIndex, Some(min, BoundaryBehavior.Inclusive), Some(max, BoundaryBehavior.Inclusive), vector)
      else sourceIndex, vector

    // Union row indices and get transformations to apply to left/right vectors
    let newRowIndex, thisRowCmd, otherRowCmd = 
      match kind with 
      | Some JoinKind.Inner ->
          indexBuilder.Intersect(rowIndex, otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)
      | Some JoinKind.Left ->
          let otherRowIndex, vector = restrictToRowIndex rowIndex otherFrame.RowIndex (Vectors.Return 0)
          let otherRowCmd = indexBuilder.Reindex(otherRowIndex, rowIndex, lookup, vector)
          rowIndex, Vectors.Return 0, otherRowCmd
      | Some JoinKind.Right ->
          let thisRowIndex, vector = restrictToRowIndex otherFrame.RowIndex rowIndex (Vectors.Return 0)
          let thisRowCmd = indexBuilder.Reindex(thisRowIndex, otherFrame.RowIndex, lookup, vector)
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
      { new VectorHelpers.VectorCallSite1<IVector -> IVector> with
          override x.Invoke<'T>(col1:IVector<'T>) = (fun col2 ->
            let col2 = VectorHelpers.changeType<'T> col2
            vectorBuilder.Build(rowCmd, [| col1; col2 |]) :> IVector) }
      |> VectorHelpers.createVectorDispatcher
    // .. if we only have one vector, we need to pad it 
    let padVector isLeft = 
      { new VectorHelpers.VectorCallSite1<IVector> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            let empty = Vector.ofValues []
            let args = if isLeft then [| col; empty |] else [| empty; col |]
            vectorBuilder.Build(rowCmd, args) :> IVector }
      |> VectorHelpers.createVectorDispatcher
    let padLeftVector, padRightVector = padVector true, padVector false

    let append = VectorValueTransform.Create(fun (l:OptionalValue<IVector>) r ->
      if l.HasValue && r.HasValue then OptionalValue(appendVector l.Value r.Value)
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
    Series.Create(columnIndex, data.SelectMissing(fun vect -> 
      // Assuming that the data has all values - which should be an invariant...
      let all = rowIndex.Mappings |> Seq.forall (fun (key, addr) -> vect.Value.GetObject(addr).HasValue)
      if all then OptionalValue(Series.CreateUntyped(rowIndex, boxVector vect.Value))
      else OptionalValue.Missing ))

  member frame.Rows = 
    let emptySeries = Series<_, _>(rowIndex, Vector.ofValues [], vectorBuilder, indexBuilder)
    emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key, Lookup.Exact, fun _ -> true)
      if not rowAddress.HasValue then OptionalValue.Missing
      else OptionalValue(Series.CreateUntyped(columnIndex, createRowReader (snd rowAddress.Value))))

  member frame.RowsDense = 
    let emptySeries = Series<_, _>(rowIndex, Vector.ofValues [], vectorBuilder, indexBuilder)
    emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key, Lookup.Exact, fun _ -> true)
      if not rowAddress.HasValue then OptionalValue.Missing else 
        let rowVec = createRowReader (snd rowAddress.Value)
        let all = columnIndex.Mappings |> Seq.forall (fun (key, addr) -> rowVec.GetValue(addr).HasValue)
        if all then OptionalValue(Series.CreateUntyped(columnIndex, rowVec))
        else OptionalValue.Missing )

  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  member frame.Clone() =
    Frame<_, _>(rowIndex, columnIndex, data)

  member frame.GetRow<'R>(row) = frame.GetRow<'R>(row, Lookup.Exact)

  member frame.GetRow<'R>(row, lookup) : Series<'TColumnKey, 'R> = 
    let row = frame.Rows.Get(row, lookup)
    Series.Create(columnIndex, changeType row.Vector)

  member frame.AddSeries(column:'TColumnKey, series:seq<_>) = 
    if (Seq.length frame.RowIndex.Keys) <> (Seq.length series) then invalidArg "series" "Must have the right length"
    let series = Series(frame.RowIndex, Vector.ofValues series, vectorBuilder, indexBuilder)
    frame.AddSeries(column, series)

  member frame.AddSeries(column:'TColumnKey, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.ofUnorderedKeys [column], Vector.ofValues [series.Vector :> IVector ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data

  member frame.DropSeries(column:'TColumnKey) = 
    let newColumnIndex, colCmd = indexBuilder.DropItem(columnIndex, column, Vectors.Return 0)    
    columnIndex <- newColumnIndex
    data <- vectorBuilder.Build(colCmd, [| data |])

  member frame.ReplaceSeries(column:'TColumnKey, series:Series<_, _>, ?lookup) = 
    let lookup = defaultArg lookup Lookup.Exact
    if columnIndex.Lookup(column, lookup, fun _ -> true).HasValue then
      frame.DropSeries(column)
    frame.AddSeries(column, series)

  member frame.GetSeries<'R>(column:'TColumnKey, lookup) : Series<'TRowKey, 'R> = 
    match safeGetColVector(column, lookup, fun _ -> true) with
    | :? IVector<'R> as vec -> 
        Series.Create(rowIndex, vec)
    | colVector ->
        Series.Create(rowIndex, changeType colVector)

  member frame.GetSeries<'R>(column:'TColumnKey) : Series<'TRowKey, 'R> = 
    frame.GetSeries(column, Lookup.Exact)

  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceSeries(column, series)

  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceSeries(column, Series.Create(frame.RowIndex, Vector.ofValues data))

  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  interface IFsiFormattable with
    member frame.Format() = 
      try
        seq { yield ""::[ for colName, _ in frame.ColumnIndex.Mappings do yield colName.ToString() ]
              let rows = frame.Rows
              for item in frame.RowIndex.Mappings |> Seq.startAndEnd Formatting.StartItemCount Formatting.EndItemCount do
                match item with 
                | Choice2Of3() ->
                    yield ":"::[for i in 1 .. data.DataSequence |> Seq.length -> "..."]
                | Choice1Of3(ind, addr) | Choice3Of3(ind, addr) ->
                    let row = rows.[ind]
                    yield 
                      (ind.ToString() + " ->")::
                      [ for KeyValue(_, value) in SeriesExtensions.GetAllObservations(row) ->  // TODO: is this good?
                          value.ToString() ] }
        |> array2D
        |> Formatting.formatTable
      with e -> sprintf "Formatting failed: %A" e

  // ----------------------------------------------------------------------------------------------
  // Some operators
  // ----------------------------------------------------------------------------------------------

  static member inline private PointwiseFrameSeries<'T>(frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, 'T>, op) =
    frame.Columns |> Series.mapValues (fun os ->
      let df = Frame([1;2], [os; series])
      df.Rows |> Series.mapValues (fun row -> op (row.GetAs<'T>(1)) (row.GetAs<'T>(2)))) 

  static member inline private ScalarOperationR<'T>(frame:Frame<'TRowKey, 'TColumnKey>, scalar:'T, op) =
    frame.Columns |> Series.mapValues (fun os -> Series.mapValues (fun v -> op v scalar) (os.As<'T>()))


  static member (/) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, float>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeries<float>(frame, series, (/))

  static member (*) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, (/))

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  new(names:seq<'TColumnKey>, columns:seq<ISeries<'TRowKey>>) =
    let df = Frame(Index.ofKeys [], Index.ofKeys [], Vector.ofValues [])
    let df = (df, Seq.zip names columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.ofUnorderedKeys [colKey], Vector.ofValues [colData.Vector])
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data)


  member frame.ReplaceRowIndexKeys<'TNewRowIndex when 'TNewRowIndex : equality>(keys:seq<'TNewRowIndex>) =
    let newRowIndex = frame.IndexBuilder.Create(keys, None)
    let getRange = VectorHelpers.getVectorRange frame.VectorBuilder frame.RowIndex.Range
    let newData = frame.Data.Select(getRange)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

  member frame.ReindexRowKeys(keys) = 
    // Create empty frame with the required keys    
    let empty = Frame<_, _>(frame.IndexBuilder.Create(keys, None), frame.IndexBuilder.Create([], None), frame.VectorBuilder.Create [||])
    for key, series in frame.Columns |> Series.observations do 
      empty.AddSeries(key, series)
    empty

