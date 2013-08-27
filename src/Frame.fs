#nowarn "40" // 'let rec' values
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
  let boxVector = 
    { new VectorHelpers.VectorCallSite1<int, IVector<int, obj>> with
        override x.Invoke<'T>(col:IVector<int, 'T>) = col.Select(box) }
    |> VectorHelpers.createDispatcher

  // A "generic function" that transforms a generic vector using specified transformation
  let transformColumn (vectorBuilder:IVectorBuilder<'TAddress>) rowCmd = 
    { new VectorHelpers.VectorCallSite1<'TAddress, IVector<'TAddress>> with
        override x.Invoke<'T>(col:IVector<'TAddress, 'T>) = 
          vectorBuilder.Build<'T>(rowCmd, [| col |]) :> IVector<'TAddress> }
    |> VectorHelpers.createDispatcher

open FrameHelpers

/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
type Frame<'TRowIndex, 'TColumnIndex when 'TRowIndex : equality and 'TColumnIndex : equality>
    internal ( rowIndex:IIndex<'TRowIndex, int>, columnIndex:IIndex<'TColumnIndex, int>, 
               data:IVector<int, IVector<int>>) =

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  /// Vector builder
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
 
  // TODO: Perhaps assert that the 'data' vector has all things required by column index
  // (to simplify various handling below)

  let mutable rowIndex = rowIndex
  let mutable columnIndex = columnIndex
  let mutable data = data

  let createRowReader rowAddress =
    let rec materializeVector() =
      let data = (virtualVector : ref<IVector<_, _>>).Value.DataSequence
      virtualVector := upcast Vector.CreateNA(data)
    and virtualVector = 
      { new IVector<int, obj> with
          member x.GetValue(columnAddress) = 
            let vector = data.GetValue(columnAddress)
            if not vector.HasValue then OptionalValue.Missing
            else vector.Value.GetObject(rowAddress) 
          member x.Data = 
            [| for _, addr in columnIndex.Mappings -> x.GetValue(addr) |]
            |> IReadOnlyList.ofArray |> VectorData.SparseList          
          member x.Select(f) = materializeVector(); virtualVector.Value.Select(f)
          member x.SelectMissing(f) = materializeVector(); virtualVector.Value.SelectMissing(f)
        
        interface IVector<int> with
          member x.ElementType = typeof<obj>
          member x.GetObject(i) = (x :?> IVector<int, obj>).GetValue(i) } |> ref
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

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  // ----------------------------------------------------------------------------------------------
  // Frame operations - joins
  // ----------------------------------------------------------------------------------------------

  member frame.Join(otherFrame:Frame<'TRowIndex, 'TColumnIndex>, ?kind) =    
    // Union row indices and get transformations to apply to left/right vectors
    let newRowIndex, thisRowCmd, otherRowCmd = 
      match kind with 
      | Some JoinKind.Inner ->
          rowIndex.IntersectWith(otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)
      | Some JoinKind.Left ->
          let otherRowCmd = otherFrame.RowIndex.Reindex(rowIndex, Vectors.Return 0)
          rowIndex, Vectors.Return 0, otherRowCmd
      | Some JoinKind.Right ->
          let thisRowCmd = rowIndex.Reindex(otherFrame.RowIndex, Vectors.Return 0)
          otherFrame.RowIndex, thisRowCmd, Vectors.Return 0
      | Some JoinKind.Outer | None | Some _ ->
          rowIndex.UnionWith(otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)

    // Append the column indices and get transformation to combine them
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newColumnIndex, colCmd = 
      columnIndex.Append(otherFrame.ColumnIndex, Vectors.Return 0, Vectors.Return 1, VectorValueTransform.LeftOrRight)
    // Apply transformation to both data vectors
    let newThisData = data.Select(transformColumn vectorBuilder thisRowCmd)
    let newOtherData = otherFrame.Data.Select(transformColumn vectorBuilder otherRowCmd)
    // Combine column vectors a single vector & return results
    let newData = vectorBuilder.Build(colCmd, [| newThisData; newOtherData |])
    Frame(newRowIndex, newColumnIndex, newData)

  member frame.Append(otherFrame:Frame<'TRowIndex, 'TColumnIndex>) = 
    // Union the column indices and get transformations for both
    let newColumnIndex, thisColCmd, otherColCmd = 
      columnIndex.UnionWith(otherFrame.ColumnIndex, Vectors.Return 0, Vectors.Return 1)

    // Append the row indices and get transformation that combines two column vectors
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newRowIndex, rowCmd = 
      rowIndex.Append(otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 1, VectorValueTransform.LeftOrRight)

    // Transform columns to align them
    let appendVector = 
      { new VectorHelpers.VectorCallSite2<int, IVector<int>> with
          override x.Invoke<'T>(col1:IVector<int, 'T>, col2:IVector<int, 'T>) = 
            vectorBuilder.Build(rowCmd, [| col1; col2 |]) :> IVector<_> }
    |> VectorHelpers.createTwoArgDispatcher

    let append = VectorValueTransform.Create(fun (l:OptionalValue<IVector<int>>) r ->
      if l.HasValue && r.HasValue then OptionalValue(appendVector (l.Value, r.Value))
      elif l.HasValue then l else r )

    let newDataCmd = Vectors.Combine(thisColCmd, otherColCmd, append)
    let newData = vectorBuilder.Build(newDataCmd, [| data; otherFrame.Data |])

    Frame(newRowIndex, newColumnIndex, newData)
  
  // ----------------------------------------------------------------------------------------------
  // df.Rows and df.Columns
  // ----------------------------------------------------------------------------------------------

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
    let emptySeries = Series(rowIndex, Vector.Create [])
    emptySeries.SelectMissing (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key)
      if not rowAddress.HasValue then OptionalValue.Missing
      else OptionalValue(Series.CreateUntyped(columnIndex, createRowReader rowAddress.Value)))

  member frame.RowsDense = 
    let emptySeries = Series(rowIndex, Vector.Create [])
    emptySeries.SelectMissing (fun row ->
      let rowAddress = rowIndex.Lookup(row.Key)
      if not rowAddress.HasValue then OptionalValue.Missing else 
        let rowVec = createRowReader rowAddress.Value
        let all = columnIndex.Mappings |> Seq.forall (fun (key, addr) -> rowVec.GetValue(addr).HasValue)
        if all then OptionalValue(Series.CreateUntyped(columnIndex, rowVec))
        else OptionalValue.Missing )

  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  member frame.AddSeries(column:'TColumnIndex, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.CreateUnsorted [column], Vector.Create [series.Vector :> IVector<int> ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data

  member frame.DropSeries(column:'TColumnIndex) = 
    let newColumnIndex, colCmd = columnIndex.DropItem(column, Vectors.Return 0)    
    columnIndex <- newColumnIndex
    data <- vectorBuilder.Build(colCmd, [| data |])

  member frame.ReplaceSeries(column:'TColumnIndex, series:Series<_, _>) = 
    if columnIndex.Lookup(column).HasValue then
      frame.DropSeries(column)
    frame.AddSeries(column, series)

  member frame.GetSeries<'R>(column:'TColumnIndex) : Series<'TRowIndex, 'R> = 
    match safeGetColVector column with
    | :? IVector<int, 'R> as vec -> 
        Series.Create(rowIndex, vec)
    | colVector ->
        let changeType = 
          { new VectorHelpers.VectorCallSite1<int, IVector<int, 'R>> with
              override x.Invoke<'T>(col:IVector<int, 'T>) = 
                col.Select(fun v -> System.Convert.ChangeType(v, typeof<'R>) :?> 'R) }
          |> VectorHelpers.createDispatcher
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
            for ind, addr in frame.RowIndex.Mappings do
              let row = frame.Rows.[ind]
              yield 
                (ind.ToString() + " ->")::
                [ for _, value in row.Observations ->  // TODO: is this good?
                    value.ToString() ] }
      |> array2D
      |> Formatting.formatTable

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  new(names:seq<'TColumnIndex>, columns:seq<ISeries<'TRowIndex>>) =
    let df = Frame(Index.Create [], Index.Create [], Vector.Create [])
    let df = (df, Seq.zip names columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.CreateUnsorted [colKey], Vector.Create [colData.Vector])
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data)

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

and Frame =
  static member Register() = 
    Series.SeriesOperations <-
      { new SeriesOperations with
          member x.OuterJoin<'TIndex2, 'TValue2 when 'TIndex2 : equality>
              (series1:Series<'TIndex2, 'TValue2>, series2:Series<'TIndex2, 'TValue2>) = 
            let frame1 = Frame(series1.Index, Index.Create [0], Vector.Create [| series1.Vector :> IVector<int> |])
            let frame2 = Frame(series2.Index, Index.Create [1], Vector.Create [| series2.Vector :> IVector<int> |])
            let joined = frame1.Join(frame2)
            joined.Rows.Select(fun row -> row.Value :> Series<_, _>) }

  static member Create<'TColumnIndex, 'TRowIndex, 'TValue when 'TColumnIndex : equality and 'TRowIndex : equality>
      (column:'TColumnIndex, series:Series<'TRowIndex, 'TValue>) = 
    let data = Vector.Create [| series.Vector :> IVector<int> |]
    Frame(series.Index, Index.Create [column], data)

  static member CreateRow(row:'TRowIndex, series:Series<'TColumnIndex, 'TValue>) = 
    let data = series.Vector.SelectMissing(fun v -> 
      let res = Vectors.ArrayVector.ArrayVectorBuilder.Instance.CreateOptional [| v |] 
      OptionalValue(res :> IVector<_>))
    Frame(Index.Create [row], series.Index, data)

  static member FromRows<'TRowIndex, 'TColumnIndex, 'TSeries, 'TValue 
        when 'TRowIndex : equality and 'TColumnIndex : equality 
        and 'TSeries :> Series<'TColumnIndex, 'TValue>>
      (nested:Series<'TRowIndex, 'TSeries>) =
    // Append all rows in some order
    let initial = Frame(Index.CreateUnsorted [], Index.Create [], Vector.Create [| |])
    let folded =
      (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
        if not series.HasValue then df 
        else Frame.CreateRow(name, series.Value).Append(df))

    // Reindex according to the original index
    let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance // TODO: Capture somewhere
    let rowCmd = folded.RowIndex.Reindex(nested.Index, Vectors.Return 0)
    let newRowIndex = nested.Index
    let newColumnIndex = folded.ColumnIndex
    let newData = folded.Data.Select(transformColumn vectorBuilder rowCmd)
    Frame(newRowIndex, newColumnIndex, newData)

  static member FromColumns<'TRowIndex, 'TColumnIndex, 'TSeries, 'TValue 
        when 'TRowIndex : equality and 'TColumnIndex : equality 
        and 'TSeries :> Series<'TRowIndex, 'TValue>>
      (nested:Series<'TColumnIndex, 'TSeries>) =
    let initial = Frame(Index.Create [], Index.CreateUnsorted [], Vector.Create [| |])
    (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.Join(Frame.Create(name, series.Value), JoinKind.Outer))

[<AutoOpen>]
module FSharp =
  type Frame = 
    static member ofRows(rows:seq<_ * Series<_, _>>) = 
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
  open System.Linq

  type Frame with
    static member ReadCsv(file:string) = 
      let data = Csv.CsvFile.Load(file).Cache()
      let columnIndex = Index.Create data.Headers.Value
      let rowIndex = Index.Create [ 0 .. data.Data.Count() - 1 ]
      let data = 
        [| for name in data.Headers.Value ->
             Vector.Create [| for row in data.Data -> row.GetColumn(name) |] :> IVector<int> |]
        |> Vector.Create
      Frame(rowIndex, columnIndex, data)

