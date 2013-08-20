// --------------------------------------------------------------------------------------
// Initialization
// --------------------------------------------------------------------------------------

// NuGet FSharp.Data
#if NUGET
#I @"C:\dev\NuGet"
#load @"C:\dev\NuGet\nuget.fsx"
NuGet.get __SOURCE_DIRECTORY__ "FSharp.Data"
#endif

#r "lib/FSharp.Data.dll"
open FSharp.Data

open System
open System.Linq
open System.Drawing
open System.Windows.Forms
open System.Collections.Generic
open System.Runtime.CompilerServices

// --------------------------------------------------------------------------------------
// Nullable value
// --------------------------------------------------------------------------------------

let (|Let|) argument input = (argument, input)

// TODO: Rename to N/A
let isNan<'T> () =
  let ty = typeof<'T>
  let nanTest : 'T -> bool = 
    if ty = typeof<float> then unbox Double.IsNaN
    elif ty = typeof<float32> then unbox Single.IsNaN
    elif ty.IsValueType then (fun _ -> false)
    else (fun v -> Object.Equals(null, box v))
  nanTest

[<Struct>]
type OptionalValue<'T>(hasValue:bool, value:'T) = 
  member x.HasValue = hasValue
  member x.Value = 
    if hasValue then value
    else invalidOp "OptionalValue.Value: Value is not available" 
  member x.ValueOrDefault = value
  new (value:'T) = OptionalValue(true, value)
  static member Empty = OptionalValue(false, Unchecked.defaultof<'T>)
  override x.ToString() = 
    if hasValue then value.ToString() else "missing"

module Array = 
  let inline existsAt low high f (data:'T[]) = 
    let rec test i = 
      if i > high then false
      elif f data.[i] then true
      else test (i + 1)
    test low

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OptionalValue = 
  let inline asOption (value:OptionalValue<'T>) = 
    if value.HasValue then Some value.Value else None

  let inline containsNan (data:'T[]) = 
    let isNan = isNan<'T>() // TODO: Optimize using static member constraints
    Array.exists isNan data

  let inline createNanArray (data:'T[]) =   
    let isNan = isNan<'T>() // TODO: Optimize using static member constraints
    data |> Array.map (fun v -> if isNan v then OptionalValue.Empty else OptionalValue(v))


module IReadOnlyList =
  /// Converts an array to IReadOnlyList. In F# 3.0, the language does not
  /// know that array implements IReadOnlyList, so this is just boxing/unboxing.
  let inline ofArray (array:'T[]) : IReadOnlyList<'T> = unbox (box array)

  /// Converts a lazy sequence to fully evaluated IReadOnlyList
  let inline ofSeq (array:seq<'T>) : IReadOnlyList<'T> = unbox (box (Array.ofSeq array))
  
  /// Sum elements of the IReadOnlyList
  let inline sum (list:IReadOnlyList<'T>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do total <- total + list.[i]
    total

  /// Sum elements of the IReadOnlyList
  let inline average (list:IReadOnlyList<'T>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do total <- total + list.[i]
    LanguagePrimitives.DivideByInt total list.Count

  /// Sum elements of the IReadOnlyList
  let inline sumOptional (list:IReadOnlyList<OptionalValue<'T>>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do 
      if list.[i].HasValue then total <- total + list.[i].Value
    total

  /// Sum elements of the IReadOnlyList
  let inline averageOptional (list:IReadOnlyList<OptionalValue<'T>>) = 
    let mutable total = LanguagePrimitives.GenericZero
    let mutable count = 0 
    for i in 0 .. list.Count - 1 do 
      if list.[i].HasValue then 
        total <- total + list.[i].Value
        count <- count + 1
    LanguagePrimitives.DivideByInt total count

module Seq = 
  let takeAtMost count (input:seq<_>) = input.Take(count)

module PrettyPrint = 
  let ItemCount = 10

// --------------------------------------------------------------------------------------
// Interface (generic & non-generic) for representing vectors
// --------------------------------------------------------------------------------------

type VectorData<'T> = 
  | DenseList of IReadOnlyList<'T>
  | SparseList of IReadOnlyList<OptionalValue<'T>>
  | Sequence of seq<OptionalValue<'T>>

type IVector<'TAddress> = 
  /// Reorders elements of the vector. The function takes a new vector length 
  /// (first parameter) and a list of relocations (a single pair of addresses
  /// specifies that an element at an old address should be moved to a new address).
  abstract Reorder : 'TAddress * (seq<'TAddress * 'TAddress>) -> IVector<'TAddress>
  abstract GetRange : 'TAddress * 'TAddress -> IVector<'TAddress>
  abstract GetObject : 'TAddress -> bool * obj
  abstract MapObjects : (obj -> 'TNewValue) -> IVector<'TAddress, 'TNewValue>

and IVector<'TAddress, 'TValue> = 
  inherit IVector<'TAddress>
  abstract GetValue : 'TAddress -> OptionalValue<'TValue>
  abstract Data : VectorData<'TValue>

  abstract Map : ('TValue -> 'TNewValue) -> IVector<'TAddress, 'TNewValue>
  abstract Append : IVector<'TAddress, 'TValue> -> IVector<'TAddress, 'TValue>

let prettyPrintVector (vector:IVector<'TAddress, 'T>) = 
  let printSequence kind (input:seq<string>) dots = 
    let sb = Text.StringBuilder(kind + " [")
    for it in input |> Seq.takeAtMost PrettyPrint.ItemCount do 
      sb.Append(" ").Append(it).Append(";") |> ignore
    sb.Remove(sb.Length - 1, 1) |> ignore
    if dots then sb.Append("; ... ]").ToString() 
    else sb.Append(" ]").ToString()
  match vector.Data with
  | DenseList list -> printSequence "dense" (Seq.map (fun v -> v.ToString()) list) (list.Count > PrettyPrint.ItemCount)
  | SparseList list -> printSequence "sparse" (Seq.map (fun v -> v.ToString()) list) (list.Count > PrettyPrint.ItemCount)
  | Sequence list -> printSequence "seq" (Seq.map (fun v -> v.ToString()) list) (Seq.length list > PrettyPrint.ItemCount)

[<AutoOpen>]
module VectorExtensions = 
  type IVector<'TAddress, 'TValue> with
    member x.DataSequence = 
      match x.Data with
      | Sequence s -> s
      | SparseList s -> upcast s
      | DenseList s -> Seq.map (fun v -> OptionalValue(v)) s

// --------------------------------------------------------------------------------------
// Concrete vector implementations
// --------------------------------------------------------------------------------------

[<RequireQualifiedAccess>]
type Vector<'T> = 
  | VectorOptional of OptionalValue<'T>[]
  | VectorNonOptional of 'T[]

  static member inline internal createVector (data:OptionalValue<_>[]) : Vector<_> =  // TODO: Rename &^ move 
    if data |> Array.exists (fun v -> not v.HasValue) then Vector.VectorOptional(data)
    else Vector.VectorNonOptional(data |> Array.map (fun v -> v.Value))

  override x.ToString() = prettyPrintVector x
  interface IVector<int> with
    
    member vector.GetObject(index) = 
      match vector with
      | VectorOptional data -> let it = data.[index] in it.HasValue, box it.ValueOrDefault
      | VectorNonOptional data -> true, box data.[index]

    member vector.GetRange(first, last) = 
      match vector with
      | VectorOptional data -> 
          if data |> Array.existsAt first last (fun v -> not v.HasValue) 
            then upcast VectorOptional(data.[first .. last])
            else upcast VectorNonOptional(Array.init (last - first + 1) (fun i -> data.[first + i]))
      | VectorNonOptional data -> 
          upcast VectorNonOptional(data.[first .. last])

    member vector.Reorder(length, relocations) = 
      let newData = Array.zeroCreate length
      match vector with 
      | VectorOptional data ->
          for oldIndex, newIndex in relocations do
            newData.[newIndex] <- data.[oldIndex]
      | VectorNonOptional data ->
          for oldIndex, newIndex in relocations do
            newData.[newIndex] <- OptionalValue(data.[oldIndex])
      upcast Vector.createVector newData

    member vector.MapObjects(f:obj -> 'R) : IVector<int, 'R> = (vector :> IVector<int, 'T>).Map<'R>(box >> f)

  interface IVector<int, 'T> with
    member vector.GetValue(index) = 
      match vector with
      | VectorOptional data -> data.[index]
      | VectorNonOptional data -> OptionalValue(data.[index])
    member vector.Data = 
      match vector with 
      | VectorNonOptional data -> DenseList (IReadOnlyList.ofArray data)
      | VectorOptional data -> SparseList (IReadOnlyList.ofArray data)
    member vector.Map<'TNewValue>(f) = 
      let isNan = isNan<'TNewValue>() 
      let newData =
        match vector with
        | VectorNonOptional data ->
            data |> Array.map (fun v -> 
              let res = f v 
              if isNan res then OptionalValue.Empty else OptionalValue(res))
        | VectorOptional data ->
            data |> Array.map (fun v -> 
              if not v.HasValue then OptionalValue.Empty else
                let res = f v.Value
                if isNan res then OptionalValue.Empty else OptionalValue(res))
      upcast Vector.createVector newData

    member vector.Append(other) = 
      let (|AsVectorOptional|) = function
        | VectorOptional d -> d
        | VectorNonOptional d -> Array.map (fun v -> OptionalValue v) d
      match other with
      | :? Vector<'T> as other ->
         match vector, other with
         | VectorNonOptional d1, VectorNonOptional d2 -> upcast VectorNonOptional (Array.append d1 d2)
         | AsVectorOptional d1, AsVectorOptional d2 -> upcast VectorOptional (Array.append d1 d2)
      | _ ->
        upcast Vector.createVector (Array.ofSeq (Seq.append vector.DataSequence other.DataSequence))

and Vector =
  static member inline Create<'T>(data:'T[]) : IVector<int, 'T> = 
    let hasNans = OptionalValue.containsNan data
    if hasNans then upcast Vector.VectorOptional(OptionalValue.createNanArray data) 
    else upcast Vector.VectorNonOptional(data)
  static member inline Create<'T when 'T : equality>(data:seq<'T>) : IVector<int, 'T> = 
    Vector.Create(Array.ofSeq data)

// --------------------------------------------------------------------------------------
// TESTS
// --------------------------------------------------------------------------------------

fsi.AddPrinter (fun (v:IVector<int>) -> v.ToString())

// TODO: Overload for creating [1; missing 2] with ints
Vector.Create [ 1 .. 10 ]
Vector.Create [ 1 .. 100 ]

let nan = Vector.Create [ 1.0; Double.NaN; 10.1 ]

let five = Vector.Create [ 1 .. 5 ]
let ten = five.Reorder(10, seq { for v in 0 .. 4 -> v, 2 * v })
ten.GetObject(2)
ten.GetObject(3)

five.GetValue(0)
nan.GetValue(1)

five.Data
nan.Data

ten.GetRange(2, 6)

five.Map (fun v -> if v = 1 then Double.NaN else float v) 
five.Map float

// Confusing? 
//   nan.Map (fun v -> if Double.IsNaN(v) then -1.0 else v)

// --------------------------------------------------------------------------------------
// Index
// --------------------------------------------------------------------------------------

let generateAddressRange<'TAddress> (count:'TAddress) : seq<'TAddress> =
  let ty = typeof<'TAddress> 
  if ty = typeof<int> then seq { for i in 0 .. (unbox<int> (box count)) - 1 -> unbox (box i) }
  elif ty = typeof<int64> then seq { for i in 0L .. (unbox<int64> (box count)) - 1L -> unbox (box i) }
  else failwith "generateAddressRange: Unknown type"

let getSize<'TAddress, 'T>(arg:seq<'T>) : 'TAddress = 
  let ty = typeof<'TAddress>
  if ty = typeof<int> then unbox (box (arg.Count()))
  elif ty = typeof<int64> then unbox (box (arg.LongCount()))
  else failwith "getSize: Unknown type"

type IIndex<'TKey, 'TAddress> = 
  abstract Lookup : 'TKey -> OptionalValue<'TAddress>  
  abstract Elements : seq<'TKey * 'TAddress>
  abstract Size : 'TAddress
  abstract UnionWith : IIndex<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>
  abstract Append : IIndex<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>

/// A hashtable that maps index values 'TKey to offsets 'TAddress allowing duplicates
type Index<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality>(keys:seq<'TKey>, offsets:seq<'TAddress>) =
  // Build a lookup table
  let dict = Dictionary<'TKey, 'TAddress>()
  do for k, v in Seq.zip keys offsets do 
       match dict.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> dict.[k] <- v  

  interface IIndex<'TKey, 'TAddress> with
    member x.Lookup(key) = 
      match dict.TryGetValue(key) with
      | true, address -> OptionalValue(address)
      | _ -> OptionalValue.Empty

    member x.Elements = 
      seq { for (KeyValue(k, value)) in dict -> k, value }        

    member x.Size = getSize (x :> IIndex<_, _>).Elements

    member this.UnionWith(other) = 
      let keys = (Seq.map fst (this :> IIndex<_, _>).Elements).Union(Seq.map fst other.Elements)
      let offsets = 
        try generateAddressRange<'TAddress> (getSize keys)
        with _ -> invalidOp "Cannot join data frames with incompatible or unknown types of indices"
      Index(keys, offsets) :> IIndex<_, _>

    /// Throws if there is a repetition
    member this.Append(other) =      
      let keys = 
        [ Seq.map fst (this :> IIndex<_, _>).Elements
          Seq.map fst other.Elements ]
        |> Seq.concat
      let offsets = 
        try generateAddressRange<'TAddress> (getSize keys)
        with _ -> invalidOp "Cannot join data frames with incompatible or unknown types of indices"
      Index(keys, offsets) :> IIndex<_, _>

type Index = 
  static member Create(data:seq<'T>) = Index(data, data |> Seq.mapi (fun i _ -> i))

// --------------------------------------------------------------------------------------
// Series
// --------------------------------------------------------------------------------------

/// A series contains one Index and one Vec
type Series<'TIndex, 'TValue when 'TIndex : equality>(index:IIndex<'TIndex, int>, data:IVector<int, 'TValue>) =
  member internal x.Index = index
  member internal x.Data = data
  member x.Observations = seq { for key, address in index.Elements -> key, data.GetValue(address) }
  member x.Values = x.Observations |> Seq.map fst // TODO: .. Keys?
  member x.Item 
    with get(key) = 
      let address = index.Lookup(key) 
      if not address.HasValue then invalidArg "key" (sprintf "The index '%O' is not present in the series." key)
      let value = data.GetValue(address.Value)
      if not value.HasValue then invalidArg "key" (sprintf "The value for index '%O' is empty." key)
      value.Value

type Series = 
  static member Create(data:seq<'TValue>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'TValue>(Index.Create(lookup), Vector.Create(data))
  static member Create(index:seq<'TIndex>, data:seq<'TValue>) =
    Series<'TIndex, 'TValue>(Index.Create(index), Vector.Create(data))

module Extensions = 
  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Series = 
    let inline sum (series:Series<_, _>) = 
      match series.Data.Data with
      | DenseList list -> IReadOnlyList.sum list
      | SparseList list -> IReadOnlyList.sumOptional list
      | Sequence seq -> Seq.sum (Seq.choose OptionalValue.asOption seq)
    let inline mean (series:Series<_, _>) = 
      match series.Data.Data with
      | DenseList list -> IReadOnlyList.average list
      | SparseList list -> IReadOnlyList.averageOptional list
      | Sequence seq -> Seq.average (Seq.choose OptionalValue.asOption seq)

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

/// Assumes that both column and row indices use int for addressing. It takes the current
/// address in the "row" direction and an index that maps column keys to offsets in 
/// 'vectors' (which is used to get the right column vector)
type RowReader<'TColumn>(offset:int, columnIndex:IIndex<'TColumn, int>, vectors:IVector<int, IVector<int>>) =
  let columns = lazy IReadOnlyList.ofSeq (Seq.map fst columnIndex.Elements)
  member x.GetColumn<'R>(columnKey) : option<'R> =     
    let columnIndex = columnIndex.Lookup(columnKey)
    if not columnIndex.HasValue then 
      invalidArg "columnKey" (sprintf "Column with key '%O' does not exist in the data frame." columnKey)
    let vector = vectors.GetValue(columnIndex.Value)
    if not vector.HasValue then
      invalidOp (sprintf "Vector for column with key '%O' is missing in the data frame." columnKey)
    let b, v = vector.Value.GetObject(offset)
    if b then Some (v :?> 'R) else None

  member x.Columns = columns.Value
  member x.GetColumnAt<'R>(index)  =     
    x.GetColumn<'R>(columns.Value |> Seq.nth index) 

type JoinKind = 
  | Outer = 0
  | Inner = 1
  | Left = 2
  | Right = 3

type FrameRows<'TRowIndex, 'TColumnIndex when 'TRowIndex : equality and 'TColumnIndex : equality> =
  abstract Map : ('TRowIndex -> RowReader<'TColumnIndex> -> option<'R>) -> Series<'TRowIndex,'R> when 'R : equality

/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
type Frame<'TRowIndex, 'TColumnIndex when 'TRowIndex : equality and 'TColumnIndex : equality>
    ( rowIndex:IIndex<'TRowIndex, int>, columnIndex:IIndex<'TColumnIndex, int>, 
      data:IVector<int, IVector<int>>) =

  let mutable rowIndex = rowIndex
  let mutable columnIndex = columnIndex
  let mutable data = data

  let reindex (oldIndex:IIndex<'TKey, 'TAddress>) (newIndex:IIndex<'TKey, 'TAddress>) (data:IVector<'TAddress>) =
    let relocations = seq {  
      for key, oldAddress in oldIndex.Elements do
        let newAddress = newIndex.Lookup(key)
        if newAddress.HasValue then yield oldAddress, newAddress.Value }
    data.Reorder(newIndex.Size, relocations)

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  member frame.Join(otherFrame:Frame<'TRowIndex, 'TColumnIndex>, ?kind) =    
    match kind with 
    | Some JoinKind.Outer | None ->
        let newRowIndex = rowIndex.UnionWith(otherFrame.RowIndex)
        let newColumnIndex = columnIndex.Append(otherFrame.ColumnIndex)
        let newData = data.Map(reindex rowIndex newRowIndex).Append(otherFrame.Data.Map(reindex otherFrame.RowIndex newRowIndex))
        Frame(newRowIndex, newColumnIndex, newData)

    | Let rowIndex (newRowIndex, Some JoinKind.Left) 
    | Let otherFrame.RowIndex (newRowIndex, Some JoinKind.Right) ->
        let newColumnIndex = columnIndex.Append(otherFrame.ColumnIndex)
        let newData = data.Map(reindex rowIndex newRowIndex).Append(otherFrame.Data.Map(reindex otherFrame.RowIndex newRowIndex))
        Frame(newRowIndex, newColumnIndex, newData)

    | _ -> failwith "! (join not implemented)"

  member frame.AddSeries(column:'TColumnIndex, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.Create [column], Vector.Create [series.Data :> IVector<int> ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data

  member frame.GetSeries<'R>(column:'TColumnIndex) : Series<'TRowIndex, 'R> = 
    let convertVector (vec:IVector<int>) : IVector<int, 'R> = 
      match vec with
      | :? IVector<int, 'R> as vec -> vec
      | other -> 
          raise (InvalidCastException("The series cannot be converted to the required type."))

    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then 
      invalidArg "column" (sprintf "Column with a key '%A' does not exist in the data frame" column)
    else
      let columnVector = data.GetValue columnIndex.Value
      if not columnVector.HasValue then
        invalidOp "column" (sprintf "Column with a key '%A' is present, but does not contain a value" column)
      Series(rowIndex, convertVector columnVector.Value)

  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.AddSeries(column, series)

  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.AddSeries(column, Series(frame.RowIndex, Vector.Create data))

  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  member x.Rows =
    { new FrameRows<_, _> with
        member x.Map<'R when 'R : equality>(f) = 
          // TODO: Does not scale
          let seriesData = 
            [| for rowKey, rowAddress in rowIndex.Elements ->
                 match f rowKey (RowReader(rowAddress, columnIndex, data)) with
                 | Some v -> OptionalValue(v)
                 | _ -> OptionalValue.Empty |]
          Series<'TRowIndex, 'R>(rowIndex, Vector.createVector(seriesData)) }
(*
  member x.GetSlice(rowStart, rowEnd, colStart, colEnd) =     
    let allColumns = x.Columns |> Seq.toArray
    let findColumn name def = 
      match name with 
      | None -> def
      | Some name -> Seq.findIndex (fst >> ((=) name)) allColumns
            
    let colStart, colEnd = findColumn colStart 0, findColumn colEnd (allColumns.Length - 1)
    let rowStart, rowEnd = defaultArg rowStart 0, defaultArg rowEnd ((Seq.length index.Values) - 1)
    
    let allIndex = x.Index.Values |> Seq.toArray
    let index = Index (allIndex.[rowStart .. rowEnd])
    let columns = 
      [ for i in colStart .. colEnd -> 
          let name, colData = allColumns.[i]
          name, colData.GetRange(rowStart, rowEnd) ]
    Frame(index, columns)
*)
  member x.Item 
    with get(index) =
      let row = rowIndex.Lookup(index)
      if not row.HasValue then invalidArg "index" (sprintf "The data frame does not contain row with index '%O'" index) 
      else RowReader(row.Value, columnIndex, data)

type Frame =
  static member Create(column:'TColumnIndex, series:Series<'TRowIndex, _>) = 
    let data = Vector.Create [| series.Data :> IVector<int> |]
    Frame(series.Index, Index.Create [column], data)

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
  seq { yield ""::[ for colName, _ in f.ColumnIndex.Elements do yield colName.ToString() ]
        for ind, addr in f.RowIndex.Elements do
          let row = f.[ind]
          yield 
            (ind.ToString() + " ->")::
            [ for col in row.Columns -> 
                match row.GetColumn<obj>(col) with
                | None -> "missing"
                | Some v -> v.ToString() ] }
  |> array2D
  |> printTable

let prettyPrintSeries (s:Series<_, _>) =
  seq { for k, v in s.Observations do
          yield [ k.ToString(); v.ToString() ] }
  |> array2D
  |> printTable

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

// --------------------------------------------------------------------------------------
// Some basic examples of using the data frame
// --------------------------------------------------------------------------------------

open Extensions
open Microsoft.FSharp.Linq.NullableOperators

let s1 = Series.Create(["a"; "b"; "c"], [1 .. 3])
s1.Observations

let s2 = Series.Create(["d"; "c"; "b"], [6; 4; 5])
s2.Observations

let snull = Series.Create [1.0; 2.0; Double.NaN ]
snull |> prettyPrintSeries

snull.Observations

let f1 = Frame.Create("S1", s1)
f1.GetSeries<int>("S1") |> prettyPrintSeries

let f2 = Frame.Create("S2", s2)

f1 |> prettyPrintFrame
f2 |> prettyPrintFrame

f1?Another <- f2.GetSeries<int>("S2")

f1?Test0 <- [ "a"; "b" ] // TODO: bug
f1?Test <- [ "a"; "b"; "!" ]
f1?Test2 <- [ "a"; "b"; "!"; "?" ]

f1 |> prettyPrintFrame
f2 |> prettyPrintFrame

let joined = f1.Join(f2, JoinKind.Outer)
joined |> prettyPrintFrame 

let joinedR = f1.Join(f2, JoinKind.Right)
joinedR |> prettyPrintFrame 

// let joinedI = f1.Join(f2, JoinKind.Inner) // TODO!
// joinedI |> prettyPrintFrame 

// TODO: Think what this should do...
// (special case construction OR sum/mean/...)
let zeros = joined.Rows.Map(fun key reader -> if key = "a" then None else Some Double.NaN)
zeros |> prettyPrintSeries

joined?Zeros <- zeros
joined |> prettyPrintFrame 

joined?Sum <- joined.Rows.Map(fun key reader -> 
  match reader.GetColumn<int>("S1"), reader.GetColumn<int>("S2") with
  | Some n1, Some n2 -> Some(n1 + n2)
  | _ -> None)

//
// joined?Sum <- joined.Columns.["S1", "S2"].Rows.Map(fun key reader -> 
//   reader.GetColumn<int>("S1") + reader.GetColumn<int>("S2") )
//

joined.["c"].GetColumn<string>("Test") // TODO: Can this return Series?

    
joined.GetSeries<int>("Sum")
|> prettyPrintSeries

prettyPrintFrame joined

// TODO: Convert - joined.GetSeries<float>("Sum")
//                 joined?Sum

joined?Sum |> Series.sum

// joined?Sum |> Series.mean


// --------------------------------------------------------------------------------------
// Digits
// --------------------------------------------------------------------------------------
(*
/// Given a float array, display it in a form
let showDigit (data:RowReader) = 
  let bmp = new Bitmap(28, 28)
  for i in 0 .. 783 do
    let value = int (data.GetColumnAt<string>(i))
    let color = Color.FromArgb(value, value, value)
    bmp.SetPixel(i % 28, i / 28, color)
  let frm = new Form(Visible = true, ClientSize = Size(280, 280))
  let img = new PictureBox(Image = bmp, Dock = DockStyle.Fill, SizeMode = PictureBoxSizeMode.StretchImage)
  frm.Controls.Add(img)

let data = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "\\data\\digitssample.csv")

data |> prettyPrintFrame

let labels = data.GetSeries<string>("label")
labels.Values

let digitData = data.[0 .., "pixel0" ..]
let digitData = data.[0 .. 20, "pixel0" .. "pixel5"]

joined // .["a" .. "c", "Column" .. ]
|> prettyPrintFrame

digitData 
|> prettyPrintFrame

digitData.[0].Columns.Length
digitData.[0].GetColumn<string>("pixel0")
digitData.[0].GetColumnAt<string>(0)

// Examples
labels.[0]
digitData.[0] |> showDigit

labels.[1]
digitData.[1] |> showDigit

labels.[3]
digitData.[3] |> showDigit

// --------------------------------------------------------
// TODO: Calculate distance between two arrays
// --------------------------------------------------------

let aggreageDistance (sample1:float[]) (sample2:float[]) =
  let sum =
    Array.map2 (fun a b -> sqrt ((a - b) * (a - b))) sample1 sample2
    |> Seq.sum
  sum / (float sample1.Length)

let zero1 = data.Data |> Seq.nth 1 |> getDataArray
let zero2 = data.Data |> Seq.nth 5 |> getDataArray
let one1 = data.Data |> Seq.nth 0 |> getDataArray
let one2 = data.Data |> Seq.nth 2 |> getDataArray

// The first two should be smaller
aggreageDistance zero1 zero2
aggreageDistance one1 one2

// But this one should be larger
aggreageDistance one1 zero1

// --------------------------------------------------------
// TODO: Calculate "average" samples
// --------------------------------------------------------

let samples = 
  data.Data 
  |> Seq.groupBy (fun r -> getLabel r)
  |> Array.ofSeq
  |> Array.Parallel.map (fun (k, v) -> 
      let samples = Array.map getDataArray (Array.ofSeq v)
      let average = 
        Array.init 784 (fun i ->
          let sum = samples  |> Seq.sumBy (fun a -> Array.get a i) 
          sum / float (Seq.length samples) )
      k, average)

// Show the average samples
// (WARNING: Opens lots of new windows!)
for i in 0 .. 9 do
  samples.[i] |> snd |> showDigit

// --------------------------------------------------------
// TODO: Pick the closest average sample
// --------------------------------------------------------

let classify row =
  let data = getDataArray row
  samples 
  |> Seq.map (fun (key, image) -> 
      key, aggreageDistance image data)
  |> Seq.minBy snd
  |> fst

// --------------------------------------------------------
// Test the classifier using the samples
// --------------------------------------------------------

let test = CsvFile.Load(__SOURCE_DIRECTORY__ + "\\data\\digitscheck.csv").Cache()

for input in test.Data |> Seq.take 10 do
  let got = classify input
  let actual = getLabel input
  printfn "Actual %d, got %A" actual got

test.Data |> Seq.countBy (fun input ->
  classify input = getLabel input)


#load @"..\NuGet\test.fsx"


// IReadOnlyList<'T> ???
// 


(*

    let newData = 
      [| for colKey, _ in newColumnIndex.Elements ->
           let column = 
             let thisCol = columnIndex.Lookup(colKey)
             if thisCol.HasValue then data.GetValue(thisCol.Value).Value
             else 
               let otherCol = otherFrame.ColumnIndex.Lookup(colKey)
               otherFrame.Data.GetValue(otherCol.Value).Value

           reindex rowIndex newRowIndex column |]

      |> Vector.Create
    Frame(newRowIndex, newColumnIndex, newData)

    let getColumns (index:Index<_>) (columns:seq<string * IVec>) usedNames = 
      seq { for name, column in columns ->
              let name = 
                if Set.contains name usedNames |> not then name
                else
                  name 
                  |> Seq.unfold (fun name -> Some(name + "'", name + "'"))
                  |> Seq.find (fun el -> Set.contains el usedNames |> not)
              name, column.Reorder(allKeys.Length, fun i -> 
                match index.Lookup.TryGetValue(allKeys.[i]) with
                | true, index -> Some index 
                | _ -> None ) }
    let newColumns = 
      [ getColumns index columns Set.empty; 
        getColumns frame.Index frame.Columns (Seq.map fst columns |> set) ]
      |> Seq.concat               
    Frame(newIndex, newColumns)
*)  
*)