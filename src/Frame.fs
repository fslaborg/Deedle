#nowarn "40" // 'let rec' values
namespace FSharp.DataFrame

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices

type JoinKind = 
  | Outer = 0
  | Inner = 1
  | Left = 2
  | Right = 3
(*
type TransformColumn(vectorBuilder:Vectors.IVectorBuilder<int>, rowCmd) =
  interface VectorHelpers.VectorCallSite<int, IVector<int>> with
              override x.Invoke<'T>(col:IVector<int, 'T>) = 
                let arg = failwith "!"
                ignore ( vectorBuilder.Build<'T>(rowCmd, [| arg |])  )
                failwith "!" :> IVector<int>
*)
/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
type Frame<'TRowIndex, 'TColumnIndex when 'TRowIndex : equality and 'TColumnIndex : equality>
    ( rowIndex:IIndex<'TRowIndex, int>, columnIndex:IIndex<'TColumnIndex, int>, 
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

  let boxVector = 
    { new VectorHelpers.VectorCallSite<int, IVector<int, obj>> with
        override x.Invoke<'T>(col:IVector<int, 'T>) = col.Map(box) }
    |> VectorHelpers.createDispatcher

  let createRowReader rowAddress =
    let rec materializeVector() =
      let data = (virtualVector : ref<IVector<_, _>>).Value.DataSequence
      virtualVector := upcast Vector.CreateNA(data)
    and virtualVector = 
      { new IVector<int, obj> with
          member x.GetValue(columnAddress) = 
            let vector = data.GetValue(columnAddress)
            if not vector.HasValue then OptionalValue.Empty
            else vector.Value.GetObject(rowAddress) 
          member x.Data = 
            [| for _, addr in columnIndex.Mappings -> x.GetValue(addr) |]
            |> IReadOnlyList.ofArray |> SparseList          
          member x.Map(f) = materializeVector(); virtualVector.Value.Map(f)
          member x.MapMissing(f) = materializeVector(); virtualVector.Value.MapMissing(f)
        
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
    if not columnIndex.HasValue then OptionalValue.Empty else
    data.GetValue columnIndex.Value

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  // ----------------------------------------------------------------------------------------------
  // Frame operations - joins
  // ----------------------------------------------------------------------------------------------

  member frame.Join(otherFrame:Frame<'TRowIndex, 'TColumnIndex>, ?kind) =    
    match kind with 
    | Some JoinKind.Outer | None ->
        // Union row indices and get transformations to apply to left/right vectors
        let newRowIndex, thisRowCmd, otherRowCmd = 
          rowIndex.UnionWith(otherFrame.RowIndex, Vectors.Return 0, Vectors.Return 0)
        
        //let newColumnIndex = columnIndex.UnionWith(otherFrame.ColumnIndex, Vectors.Return 0, Vectors.Return 1)
        let newColumnIndex = columnIndex

        let transformColumn rowCmd = 
          { new VectorHelpers.VectorCallSite<int, IVector<int>> with
              override x.Invoke<'T>(col:IVector<int, 'T>) = 
                vectorBuilder.Build(rowCmd, [| col |]) :> IVector<int> }
          |> VectorHelpers.createDispatcher
        
        let newThisData = data.Map(transformColumn thisRowCmd)
        let newOtherData = otherFrame.Data.Map(transformColumn otherRowCmd)
        let newData = newThisData

        Frame(newRowIndex, newColumnIndex, newData)

(*
    | Let rowIndex (newRowIndex, Some JoinKind.Left) 
    | Let otherFrame.RowIndex (newRowIndex, Some JoinKind.Right) ->
        let newColumnIndex = columnIndex.Append(otherFrame.ColumnIndex)
        let newData = data.Map(IndexOps.reindex rowIndex newRowIndex)
        let newData = newData.Append(otherFrame.Data.Map(IndexOps.reindex otherFrame.RowIndex newRowIndex))
        Frame(newRowIndex, newColumnIndex, newData)
*)
    | _ -> failwith "! (join not implemented)"

(*
  member frame.Append(otherFrame:Frame<'TRowIndex, 'TColumnIndex>) = 
    let newColumnIndex = columnIndex.UnionWith(otherFrame.ColumnIndex)
    let newRowIndex = rowIndex.Append(otherFrame.RowIndex)

    let _, hi = newColumnIndex.Range // TODO: Does not scale
    let newData = Array.zeroCreate (hi + 1)
    for colKey, addr in newColumnIndex.Elements do
      let v1, v2 = frame.tryGetColVector colKey, otherFrame.tryGetColVector colKey 
      //let v1 = if v1.HasValue then v1 else rowIndex.Get
      if v1.HasValue && v2.HasValue then
        printfn "%A" colKey
        newData.[addr] <- OptionalValue(v1.Value.Append(v2.Value) :> IVector<_>)

    Frame(newRowIndex, newColumnIndex, Vector.CreateOptional(newData))
*)
  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  member frame.AddSeries(column:'TColumnIndex, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.CreateUnsorted [column], Vector.Create [series.Vector :> IVector<int> ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data
(*
  member frame.DropSeries(column:'TColumnIndex) = 
    let columnAddress = columnIndex.Lookup(column)
    if not columnAddress.HasValue then 
      invalidArg "column" (sprintf "Column with a key '%A' does not exist in the data frame" column) 
    columnIndex <- columnIndex.DropItem(column)
    data <- downcast data.DropRange(columnAddress.Value, columnAddress.Value)

  member frame.ReplaceSeries(column:'TColumnIndex, series:Series<_, _>) = 
    if columnIndex.Lookup(column).HasValue then
      frame.DropSeries(column)
    frame.AddSeries(column, series)
*)
  member frame.GetSeries<'R>(column:'TColumnIndex) : Series<'TRowIndex, 'R> = 
    match safeGetColVector column with
    | :? IVector<int, 'R> as vec -> 
        Series.Create(rowIndex, vec)
    | colVector ->
        let changeType = 
          { new VectorHelpers.VectorCallSite<int, IVector<int, 'R>> with
              override x.Invoke<'T>(col:IVector<int, 'T>) = 
                col.Map(fun v -> System.Convert.ChangeType(v, typeof<'R>) :?> 'R) }
          |> VectorHelpers.createDispatcher
        Series.Create(rowIndex, changeType colVector)
(*
  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceSeries(column, series)

  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceSeries(column, Series.Create(frame.RowIndex, Vector.Create data))
*)
  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  // ----------------------------------------------------------------------------------------------
  // df.Rows and df.Columns
  // ----------------------------------------------------------------------------------------------

  member x.Columns = 
    Series.Create(columnIndex, data.Map(fun vect -> Series.Create(rowIndex, boxVector vect)))

  member x.Rows = 
    let seriesData = 
      [| for rowKey, rowAddress in rowIndex.Mappings ->  // TODO: Does not scale?
            Series.Create(columnIndex, createRowReader rowAddress) |]
    Series.Create(rowIndex, Vector.Create(seriesData)) 

type Frame =
  static member Create(column:'TColumnIndex, series:Series<'TRowIndex, 'TValue>) = 
    let data = Vector.Create [| series.Vector :> IVector<int> |]
    Frame(series.Index, Index.Create [column], data)

  static member CreateRow(row:'TRowIndex, series:Series<'TColumnIndex, 'TValue>) = 
    let data = series.Vector.MapMissing(fun v -> 
      let res = Vectors.ArrayVector.ArrayVectorBuilder.Instance.CreateOptional [| v |] 
      OptionalValue(res :> IVector<_>))
    Frame(Index.Create [row], series.Index, data)

(*  static member FromRows(nested:Series<'TRowIndex, Series<'TColumnIndex, 'TValue>>) =
    // TODO: THis is probably slow
    let columnIndex = 
      nested.Map(fun _ s -> s.Index).Vector.DataSequence |> Seq.fold (fun id1 id2 -> 
        if id2.HasValue then id2.Value.UnionWith(id1) 
        else id1) (upcast Index.Create [])

    let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
    (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.Append(Frame.CreateRow(name, series.Value)))
*)
  static member FromColumns(nested:Series<'TColumnIndex, Series<'TRowIndex, 'TValue>>) =
    let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
    (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.Join(Frame.Create(name, series.Value), JoinKind.Outer))

module PrettyPrint =
  open System

  // Simple functions that pretty-print series and frames
  // (to be integrated as ToString and with F# Interactive)
  let printTable (data:string[,]) =
    let rows = data.GetLength(0)
    let columns = data.GetLength(1)
    let widths = Array.zeroCreate columns 
    data |> Array2D.iteri (fun r c str ->
      widths.[c] <- max (widths.[c]) (str.Length))
    for r in 0 .. rows - 1 do
      for c in 0 .. columns - 1 do
        Console.Write(data.[r, c].PadRight(widths.[c] + 1))
      Console.WriteLine()

  let prettyPrintFrame (f:Frame<_, _>) =
    seq { yield ""::[ for colName, _ in f.ColumnIndex.Mappings do yield colName.ToString() ]
          for ind, addr in f.RowIndex.Mappings do
            let row = f.Rows.[ind]
            yield 
              (ind.ToString() + " ->")::
              [ for _, value in row.Observations ->  // TODO: is this good?
                  value.ToString() ] }
    |> array2D
    |> printTable

  let prettyPrintSeries (s:Series<_, _>) =
    seq { for k, v in s.Observations do
            yield [ k.ToString(); v.ToString() ] }
    |> array2D
    |> printTable

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

