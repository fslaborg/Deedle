#nowarn "40" // Recursive references checked at runtime
#r "packages/FSharp.Data.dll"

// --------------------------------------------------------------------------------------
// Initialization
// --------------------------------------------------------------------------------------

// NuGet FSharp.Data
#if NUGET
#I @"C:\dev\NuGet"
#load @"C:\dev\NuGet\nuget.fsx"
NuGet.get __SOURCE_DIRECTORY__ "FSharp.Data"
#endif

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

let notImplemented msg = raise <| NotImplementedException(msg)
let (|Let|) argument input = (argument, input)

let isNA<'T> () =
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
    if hasValue then 
      if Object.Equals(null, value) then "<null>"
      else value.ToString() 
    else "missing"

module Array = 
  let inline existsAt low high f (data:'T[]) = 
    let rec test i = 
      if i > high then false
      elif f data.[i] then true
      else test (i + 1)
    test low

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OptionalValue = 
  let inline map f (input:OptionalValue<_>) = 
    if input.HasValue then OptionalValue(f input.Value)
    else OptionalValue.Empty

  let inline ofTuple (b, value) =
    if b then OptionalValue(value) else OptionalValue.Empty

  let inline asOption (value:OptionalValue<'T>) = 
    if value.HasValue then Some value.Value else None

  let inline ofOption opt = 
    match opt with
    | None -> OptionalValue.Empty
    | Some v -> OptionalValue(v)

  let inline containsNan (data:'T[]) = 
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    Array.exists isNA data

  let inline createNanArray (data:'T[]) =   
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    data |> Array.map (fun v -> if isNA v then OptionalValue.Empty else OptionalValue(v))


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
  ///
  /// This may be given non-existent address! (when reordering shorter vector accoding to longer index)
  abstract Reorder : ('TAddress * 'TAddress) * (seq<'TAddress * 'TAddress>) -> IVector<'TAddress>
  abstract GetRange : 'TAddress * 'TAddress -> IVector<'TAddress>
  abstract DropRange : 'TAddress * 'TAddress -> IVector<'TAddress>
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

type Vector =
  static member inline Create<'T>(data:'T[]) : IVector<int, 'T> = 
    let hasNans = OptionalValue.containsNan data
    if hasNans then upcast Vector.VectorOptional(OptionalValue.createNanArray data) 
    else upcast Vector.VectorNonOptional(data)

  static member inline Create<'T when 'T : equality>(data:seq<'T>) : IVector<int, 'T> = 
    Vector.Create(Array.ofSeq data)

  static member inline CreateOptional<'T>(data:OptionalValue<'T>[]) : Vector<'T> = 
    if data |> Array.exists (fun v -> not v.HasValue) then Vector.VectorOptional(data)
    else Vector.VectorNonOptional(data |> Array.map (fun v -> v.Value))

and [<RequireQualifiedAccess>] Vector<'T> = 
  | VectorOptional of OptionalValue<'T>[]
  | VectorNonOptional of 'T[]

  override x.ToString() = prettyPrintVector x
  interface IVector<int> with
    
    member vector.GetObject(index) = 
      match vector with
      | VectorOptional data -> let it = data.[index] in it.HasValue, box it.ValueOrDefault
      | VectorNonOptional data -> true, box data.[index]

    member vector.DropRange(first, last) = 
      match vector with 
      | VectorOptional data ->
          upcast Vector.CreateOptional<_>(Array.append (data.[.. first - 1]) (data.[last + 1 ..]))
      | VectorNonOptional data ->
          upcast VectorNonOptional(Array.append (data.[.. first - 1]) (data.[last + 1 ..]))

    member vector.GetRange(first, last) = 
      match vector with
      | VectorOptional data -> 
          if data |> Array.existsAt first last (fun v -> not v.HasValue) 
            then upcast VectorOptional(data.[first .. last])
            else upcast VectorNonOptional(Array.init (last - first + 1) (fun i -> data.[first + i]))
      | VectorNonOptional data -> 
          upcast VectorNonOptional(data.[first .. last])

    member vector.Reorder((lo, hi), relocations) = 
      let newData = Array.zeroCreate (hi - lo + 1)
      match vector with 
      | VectorOptional data ->
          for oldIndex, newIndex in relocations do
            if oldIndex < data.Length && oldIndex >= 0 then
              newData.[newIndex] <- data.[oldIndex]
      | VectorNonOptional data ->
          for oldIndex, newIndex in relocations do
            if oldIndex < data.Length && oldIndex >= 0 then
              newData.[newIndex] <- OptionalValue(data.[oldIndex])
      upcast Vector.CreateOptional<'T>(newData)

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
      let isNA = isNA<'TNewValue>() 
      let newData =
        match vector with
        | VectorNonOptional data ->
            data |> Array.map (fun v -> 
              let res = f v 
              if isNA res then OptionalValue.Empty else OptionalValue(res))
        | VectorOptional data ->
            data |> Array.map (fun v -> 
              if not v.HasValue then OptionalValue.Empty else
                let res = f v.Value
                if isNA res then OptionalValue.Empty else OptionalValue(res))
      upcast Vector.CreateOptional<'TNewValue>(newData)

    member vector.Append(other) : IVector<_, 'T> = 
      let (|AsVectorOptional|) = function
        | VectorOptional d -> d
        | VectorNonOptional d -> Array.map (fun v -> OptionalValue v) d
      match other with
      | :? Vector<'T> as other ->
         match vector, other with
         | VectorNonOptional d1, VectorNonOptional d2 -> upcast VectorNonOptional (Array.append d1 d2)
         | AsVectorOptional d1, AsVectorOptional d2 -> upcast VectorOptional (Array.append d1 d2)
      | _ ->
        let newData = Array.ofSeq (Seq.append vector.DataSequence other.DataSequence)
        upcast Vector.CreateOptional<'T>(newData)

let delegatedVector (vector:IVector<'TAddress, 'TValue> ref) =
  { new IVector<'TAddress, 'TValue> with
      member x.GetValue(a) = vector.Value.GetValue(a)
      member x.Data = vector.Value.Data
      member x.Map(f) = vector.Value.Map(f)
      member x.Append(v) = vector.Value.Append(v)
    interface IVector<'TAddress> with
      member x.Reorder(l, r) = vector.Value.Reorder(l, r)
      member x.GetRange(l, h) = vector.Value.GetRange(l, h)
      member x.DropRange(l, h) = vector.Value.DropRange(l, h)
      member x.GetObject(i) = vector.Value.GetObject(i)
      member x.MapObjects(f) = vector.Value.MapObjects(f) }

// --------------------------------------------------------------------------------------
// TESTS
// --------------------------------------------------------------------------------------

fsi.AddPrinter (fun (v:IVector<int>) -> v.ToString())

// TODO: Overload for creating [1; missing 2] with ints
Vector.Create [ 1 .. 10 ]
Vector.Create [ 1 .. 100 ]

let nan = Vector.Create [ 1.0; Double.NaN; 10.1 ]

let five = Vector.Create [ 1 .. 5 ]
let ten = five.Reorder((0, 10), seq { for v in 0 .. 4 -> v, 2 * v })
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
//   nan.Map (fun v -> if Double.isNA(v) then -1.0 else v)

// --------------------------------------------------------------------------------------
// Index
// --------------------------------------------------------------------------------------

type AddressOperations<'TAddress> = 
  abstract GenerateRange : 'TAddress * 'TAddress -> seq<'TAddress>
  abstract Zero : 'TAddress 
  abstract RangeOf : seq<'T> -> 'TAddress * 'TAddress
  abstract GetRange : seq<'T> * 'TAddress * 'TAddress -> seq<'T>
  
type IIndex<'TKey, 'TAddress> = 
  abstract Lookup : 'TKey -> OptionalValue<'TAddress>  
  abstract Elements : seq<'TKey * 'TAddress>
  abstract Range : 'TAddress * 'TAddress
  abstract UnionWith : IIndex<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>
  abstract Append : IIndex<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>
  abstract DropItem : 'TKey -> IIndex<'TKey, 'TAddress>
  abstract GetRange : option<'TKey> * option<'TKey> -> IIndex<'TKey, 'TAddress>

/// A hashtable that maps index values 'TKey to offsets 'TAddress allowing duplicates
type Index<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> (keys:seq<'TKey>, ops:AddressOperations<'TAddress>) =
  // Build a lookup table
  let dict = Dictionary<'TKey, 'TAddress>()
  do let addresses = ops.GenerateRange(ops.RangeOf(keys))
     for k, v in Seq.zip keys addresses do 
       match dict.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> dict.[k] <- v  

  interface IIndex<'TKey, 'TAddress> with
    member x.GetRange(lo, hi) =
      let (|Lookup|_|) x = match dict.TryGetValue(x) with true, v -> Some v | _ -> None
      let def = lazy ops.RangeOf(keys)
      let getBound offs proj = 
        match offs with 
        | None -> proj def.Value 
        | Some (Lookup i) -> i
        | _ -> invalidArg "lo,hi" "Keys of the range were not found in the data frame."
      upcast Index<_, _>(ops.GetRange(keys, getBound lo fst, getBound hi snd), ops)

    member x.DropItem(key) = 
      upcast Index<_, _>(Seq.filter ((<>) key) keys, ops) 
      
    member x.Lookup(key) = 
      match dict.TryGetValue(key) with
      | true, address -> OptionalValue(address)
      | _ -> OptionalValue.Empty

    member x.Elements = 
      seq { for (KeyValue(k, value)) in dict -> k, value }        

    member x.Range = ops.RangeOf((x :> IIndex<_, _>).Elements)

    member this.UnionWith(other) = 
      let keys = (Seq.map fst (this :> IIndex<_, _>).Elements).Union(Seq.map fst other.Elements)
      Index<_, _>(keys, ops) :> IIndex<_, _>

    /// Throws if there is a repetition
    member this.Append(other) =      
      let keys = 
        [ Seq.map fst (this :> IIndex<_, _>).Elements
          Seq.map fst other.Elements ]
        |> Seq.concat
      upcast Index<_, _>(keys, ops)

let intAddress = 
  { new AddressOperations<int> with
      member x.GenerateRange(lo, hi) : seq<int> = seq { lo .. hi } 
      member x.Zero = 0
      member x.RangeOf(seq) = 0, (Seq.length seq) - 1 
      member x.GetRange(seq, lo, hi) = 
        if hi >= lo then seq |> Seq.skip lo |> Seq.take (hi - lo + 1) 
        else Seq.empty }

type Index = 
  static member Create(data:seq<'T>) = Index(data, intAddress)

let inline reindex (oldIndex:IIndex<'TKey, 'TAddress>) (newIndex:IIndex<'TKey, 'TAddress>) (data:IVector<'TAddress>) =
  let relocations = seq {  
    for key, oldAddress in oldIndex.Elements do
      let newAddress = newIndex.Lookup(key)
      if newAddress.HasValue then 
        yield oldAddress, newAddress.Value }
  data.Reorder(newIndex.Range, relocations)

// --------------------------------------------------------------------------------------
// Series
// --------------------------------------------------------------------------------------

/// A series contains one Index and one Vec
type Series<'TIndex, 'TValue when 'TIndex : equality>(index:IIndex<'TIndex, int>, data:IVector<int, 'TValue>) =
  member internal x.Index = index
  member internal x.Data = data
  member x.Observations = seq { for key, address in index.Elements -> key, data.GetValue(address) }
  member x.Values = x.Observations |> Seq.map fst // TODO: .. Keys?
  member x.TryGet(key) =
    let address = index.Lookup(key) 
    if not address.HasValue then invalidArg "key" (sprintf "The index '%O' is not present in the series." key)
    let value = data.GetValue(address.Value)
    value |> OptionalValue.asOption
  
  member x.GetItems(items) =
    let newIndex = Index.Create items
    let newData = (reindex index newIndex data) :?> IVector<int, 'TValue>  // TODO: Well.. we could have typed Reoreder but... this should always work
    Series<'TIndex,'TValue>(newIndex, newData) 

  member x.GetSlice(lo, hi) =
    let newIndex = index.GetRange(lo, hi)
    let newData = (reindex index newIndex data) :?> IVector<int, 'TValue>  // TODO: Well.. we could have typed Reoreder but... this should always work
    Series<'TIndex,'TValue>(newIndex, newData) 

  member x.GetItem(key) =
    match x.TryGet(key) with
    | None -> invalidArg "key" (sprintf "The value for index '%O' is empty." key)
    | Some v -> v

  static member (?) (series:Series<_, _>, name:string) = series.GetItem(name)

  member x.Map<'R>(f:'TIndex -> 'TValue -> 'R) = 
    let newData =
      [| for key, addr in index.Elements -> // TODO: Does not scale
           data.GetValue(addr) |> OptionalValue.map (f key) |]
    Series<'TIndex, 'R>(index, Vector.CreateOptional newData)

  member x.MapRaw<'R>(f:'TIndex -> option<'TValue> -> option<'R>) = 
    let newData =
      [| for key, addr in index.Elements -> // TODO: Does not scale
           data.GetValue(addr) |> OptionalValue.asOption |> f key |> OptionalValue.ofOption |]
    Series<'TIndex, 'R>(index, Vector.CreateOptional newData)

  // Should be an extension method on Series<_, obj>
  member x.GetAny<'R>(column) : 'R = unbox (x.GetItem(column))
  member x.TryGetAny<'R>(column) : 'R option = x.TryGet(column) |> Option.map unbox

type Series<'TIndex when 'TIndex : equality> = Series<'TIndex, obj>

type Series = 
  static member Create(data:seq<'TValue>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'TValue>(Index.Create(lookup), Vector.Create(data))
  static member Create(index:seq<'TIndex>, data:seq<'TValue>) =
    Series<'TIndex, 'TValue>(Index.Create(index), Vector.Create(data))
  static member Create(index:IIndex<'TIndex, int>, data:IVector<int, 'TValue>) = 
    Series<'TIndex, 'TValue>(index, data)

[<AutoOpen>] 
module SeriesExtensions =

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

  type Series<'TIndex, 'TValue when 'TIndex : equality> with
    member x.Item with get(a) = x.GetItem(a)
    member x.Item with get(a, b) = x.GetItems [a; b] 
    member x.Item with get(a, b, c) = x.GetItems [a; b; c] 
    member x.Item with get(a, b, c, d) = x.GetItems [a; b; c; d]
    member x.Item with get(a, b, c, d, e) = x.GetItems [a; b; c; d; e]
    member x.Item with get(a, b, c, d, e, f) = x.GetItems [a; b; c; d; e; f]
    member x.Item with get(a, b, c, d, e, f, g) = x.GetItems [a; b; c; d; e; f; g]
    member x.Item with get(a, b, c, d, e, f, g, h) = x.GetItems [a; b; c; d; e; f; g; h] 

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

type JoinKind = 
  | Outer = 0
  | Inner = 1
  | Left = 2
  | Right = 3

/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
and Frame<'TRowIndex, 'TColumnIndex when 'TRowIndex : equality and 'TColumnIndex : equality>
    ( rowIndex:IIndex<'TRowIndex, int>, columnIndex:IIndex<'TColumnIndex, int>, 
      data:IVector<int, IVector<int>>) =

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  let mutable rowIndex = rowIndex
  let mutable columnIndex = columnIndex
  let mutable data = data

  let createRowReader rowAddress =
    let rec materializeVector() =
      let data = (virtualVector : ref<IVector<_, _>>).Value.DataSequence
      virtualVector := upcast Vector.CreateOptional(data |> Array.ofSeq)
    and virtualVector = 
      { new IVector<int, obj> with
          member x.GetValue(columnAddress) = 
            let vector = data.GetValue(columnAddress)
            if not vector.HasValue then OptionalValue.Empty
            else
              match vector.Value.GetObject(rowAddress) with
              | true, v -> OptionalValue(v)
              | _ -> OptionalValue.Empty
          member x.Data = 
            [| for _, addr in columnIndex.Elements -> x.GetValue(addr) |]
            |> IReadOnlyList.ofArray |> SparseList          
          member x.Map(f) = materializeVector(); virtualVector.Value.Map(f)
          member x.Append(v) = materializeVector(); virtualVector.Value.Append(v)
        interface IVector<int> with
          member x.Reorder(s, r) = materializeVector(); virtualVector.Value.Reorder(s, r)
          member x.GetRange(l, h) = materializeVector(); virtualVector.Value.GetRange(l, h)
          member x.DropRange(l, h) = materializeVector(); virtualVector.Value.DropRange(l, h)
          member x.GetObject(i) = let v = (x :?> IVector<int, obj>).GetValue(i) in v.HasValue, v.ValueOrDefault
          member x.MapObjects(f) = materializeVector(); virtualVector.Value.MapObjects(f) } |> ref
    delegatedVector virtualVector

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

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

  // ----------------------------------------------------------------------------------------------
  // Frame operations - joins
  // ----------------------------------------------------------------------------------------------

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
        let newData = data.Map(reindex rowIndex newRowIndex)
        let newData = newData.Append(otherFrame.Data.Map(reindex otherFrame.RowIndex newRowIndex))
        Frame(newRowIndex, newColumnIndex, newData)

    | _ -> failwith "! (join not implemented)"

  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  member frame.AddSeries(column:'TColumnIndex, series:Series<_, _>) = 
    let other = Frame(series.Index, Index.Create [column], Vector.Create [series.Data :> IVector<int> ])
    let joined = frame.Join(other, JoinKind.Left)
    columnIndex <- joined.ColumnIndex
    data <- joined.Data

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

  member frame.GetSeries<'R>(column:'TColumnIndex) : Series<'TRowIndex, 'R> = 
    let convertVector (vec:IVector<int>) : IVector<int, 'R> = 
      let typ = typeof<'R>
      match vec with
      | :? IVector<int, 'R> as vec -> vec
      | vec -> vec.MapObjects (fun v -> System.Convert.ChangeType(v, typ) :?> 'R)
    Series.Create(rowIndex, convertVector (safeGetColVector column))

  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceSeries(column, series)

  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceSeries(column, Series.Create(frame.RowIndex, Vector.Create data))

  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  // ----------------------------------------------------------------------------------------------
  // df.Rows and df.Columns
  // ----------------------------------------------------------------------------------------------

  member x.Columns = 
    Series.Create(columnIndex, data.Map(fun vect -> Series.Create(rowIndex, vect.MapObjects id)))

  member x.Rows = 
    let seriesData = 
      [| for rowKey, rowAddress in rowIndex.Elements ->  // TODO: Does not scale?
            Series.Create(columnIndex, createRowReader rowAddress) |]
    Series.Create(rowIndex, Vector.Create(seriesData)) 

type Frame =
  static member Create(column:'TColumnIndex, series:Series<'TRowIndex, 'TValue>) = 
    let data = Vector.Create [| series.Data :> IVector<int> |]
    Frame(series.Index, Index.Create [column], data)
(*
  static member Create(nested:Series<'TRowIndex, Series<'TColumnIndex, 'TValue>>) =
    let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
    (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.(Frame.Create(name, series.Value), JoinKind.Outer))

  static member Create(nested:Series<'TColumnIndex, Series<'TRowIndex, 'TValue>>) =
    let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
    (initial, nested.Observations) ||> Seq.fold (fun df (name, series) -> 
      if not series.HasValue then df 
      else df.Join(Frame.Create(name, series.Value), JoinKind.Outer))
*)

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

f1.Join(f2, JoinKind.Left) |> prettyPrintFrame


f1?Another <- f2.GetSeries<int>("S2")

f1?Test0 <- [ "a"; "b" ] // TODO: bug - fixed
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
let zerosQ = joined.Rows.MapRaw(fun key reader -> if key = "a" then None else Some Double.NaN)
zerosQ |> prettyPrintSeries

let zerosE = joined.Rows.Map(fun key reader -> if key = "a" then None else Some Double.NaN)
zerosE |> prettyPrintSeries

let zeros = joined.Rows.MapRaw(fun key reader -> if key = "a" then None else Some 0.0)
zeros |> prettyPrintSeries

joined?Zeros <- zeros
joined |> prettyPrintFrame 



joined |> prettyPrintFrame 

joined.Rows.["a"] |> prettyPrintSeries
joined.Rows.["a", "c"] |> Frame.Create |> prettyPrintFrame

joined.Rows.["a", "c", "d"] |> Frame.Create |> prettyPrintFrame
joined.Rows.["a" .. "c"] |> Frame.Create |> prettyPrintFrame

let reversed = joined.Rows.["d", "c", "b", "a"] |> Frame.Create
reversed |> prettyPrintFrame
reversed.Rows.["d" .. "d"] |> prettyPrintFrame
reversed.Rows.["d" .. "c"] |> prettyPrintFrame
reversed.Rows.["a" .. "c"] |> prettyPrintFrame // Empty data frame - as expected

let tf = Frame.Create("First", joined.Rows.["a"]) 
tf |> prettyPrintFrame

tf?Second <- joined.Rows.["b"]
tf |> prettyPrintFrame

let a = joined.Rows.["a"] 
a |> prettyPrintSeries

a?S1
a.["S1"]
a.TryGetAny<int>("S1")
a.GetAny<int>("S1")

joined?Sum <- joined.Rows.Map(fun key row -> 
  match row.TryGetAny<int>("S1"), row.TryGetAny<int>("S2") with
  | Some n1, Some n2 -> Some(n1 + n2)
  | _ -> Some -2)

prettyPrintFrame joined

//
// joined?Sum <- joined.Columns.["S1", "S2"].Rows.Map(fun key reader -> 
//   reader.GetColumn<int>("S1") + reader.GetColumn<int>("S2") )
//

joined.Rows.["c"].GetAny<string>("Test")

    
joined.GetSeries<int>("Sum")
|> prettyPrintSeries

prettyPrintFrame joined

// Conversions
joined.GetSeries<int>("Sum") |> prettyPrintSeries
joined.GetSeries<float>("Sum") |> prettyPrintSeries

joined?Sum |> Series.sum


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
