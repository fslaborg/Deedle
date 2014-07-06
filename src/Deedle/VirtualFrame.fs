namespace Deedle.Virtual

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

open Deedle
open Deedle.Vectors
open Deedle.Internal

type VirtualVectorSource =
  abstract ElementType : System.Type
  abstract Length : int64

type VirtualVectorSource<'V> = 
  inherit VirtualVectorSource
  abstract ValueAt : int64 -> OptionalValue<'V>
  abstract GetSubVector : int64 * int64 -> VirtualVectorSource<'V>

type VirtualVector<'V>(source:VirtualVectorSource<'V>) = 
  member vector.Source = source
  interface IVector with
    member val ElementType = typeof<'V>
    member vector.SuppressPrinting = false
    member vector.GetObject(index) = source.ValueAt(index) |> OptionalValue.map box
    member vector.ObjectSequence = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) |> OptionalValue.map box }
    member vector.Invoke(site) = site.Invoke<'V>(vector)
  interface IVector<'V> with
    member vector.GetValue(index) = source.ValueAt(index)
    member vector.Data = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) } |> VectorData.Sequence
    member vector.SelectMissing<'TNew>(f:OptionalValue<'V> -> OptionalValue<'TNew>) = 
      let rec mapSource (source:VirtualVectorSource<'V>) = 
        { new VirtualVectorSource<'TNew> with
            member x.ValueAt(idx) = f (source.ValueAt(idx))
            member x.GetSubVector(lo, hi) = mapSource (source.GetSubVector(lo, hi))
          interface VirtualVectorSource with
            member x.ElementType = typeof<'TNew>
            member x.Length = source.Length }
      VirtualVector(mapSource source) :> _

    member vector.Select(f) = 
      (vector :> IVector<_>).SelectMissing(OptionalValue.map f)

type VirtualVectorBuilder() =
  let baseBuilder = VectorBuilder.Instance
  static let vectorBuilder = VirtualVectorBuilder() :> IVectorBuilder
  let build cmd args = vectorBuilder.Build(cmd, args)

  static member Instance = vectorBuilder

  interface IVectorBuilder with
    member builder.Create(values) = baseBuilder.Create(values)
    member builder.CreateMissing(optValues) = baseBuilder.CreateMissing(optValues)
    member builder.InitMissing<'T>(size, f) = 
      let isNa = MissingValues.isNA<'T> ()
      let rec createSource lo hi =
        { new VirtualVectorSource<'T> with
            member x.ValueAt(idx) = 
              let v = f (lo + idx)
              if v.HasValue && not (isNa v.Value) then v
              else OptionalValue.Missing 
            member x.GetSubVector(nlo, nhi) = createSource (lo+nlo) (lo+nhi)
          interface VirtualVectorSource with
            member x.ElementType = typeof<'T>
            member x.Length = (hi-lo+1L) }
      VirtualVector(createSource 0L (size-1L)) :> _

    member builder.AsyncBuild<'T>(cmd, args) = baseBuilder.AsyncBuild<'T>(cmd, args)
    member builder.Build<'T>(cmd, args) = 
      match cmd with 
      | GetRange(source, (loRange, hiRange)) ->
          match build source args with
          | :? VirtualVector<'T> as source ->
              let subSource = source.Source.GetSubVector(loRange, hiRange)
              VirtualVector<'T>(subSource) :> IVector<'T>
          | source -> 
              let cmd = GetRange(Return 0, (loRange, hiRange))
              baseBuilder.Build(cmd, [| source |])
      | _ ->    
        baseBuilder.Build<'T>(cmd, args)

// ------------------------------------------------------------------------------------------------
// Indices
// ------------------------------------------------------------------------------------------------

open Deedle.Addressing
open Deedle.Indices
open Deedle.Internal
open System.Collections.Generic
open System.Collections.ObjectModel

module Helpers = 
  let inline addrOfKey lo key = Address.ofInt64 (key - lo)
  let inline keyOfAddr lo addr = lo + (Address.asInt64 addr)

open Helpers

type VirtualOrdinalIndex(range) =
  let lo, hi = range
  let size = hi - lo + 1L
  do if size < 0L then invalidArg "range" "Invalid range"
  interface IIndex<int64> with
    member x.KeyAt(addr) = 
      if addr < 0L || addr >= size then invalidArg "addr" "Out of range"
      else keyOfAddr lo addr
    member x.KeyCount = size
    member x.IsEmpty = size = 0L
    member x.Builder = failwith "Builder: TODO!!" :> IIndexBuilder
    member x.KeyRange = range
    member x.Keys = Array.init (int size) (Address.ofInt >> (keyOfAddr lo)) |> ReadOnlyCollection.ofArray
    member x.Mappings = 
      Seq.range 0L (size - 1L) 
      |> Seq.map (fun i -> KeyValuePair(keyOfAddr lo (Address.ofInt64 i), Address.ofInt64 i))
    member x.IsOrdered = true
    member x.Comparer = Comparer<int64>.Default

    member x.Locate(key) = 
      if key >= lo && key <= hi then addrOfKey lo key
      else Address.Invalid

    member x.Lookup(key, semantics, check) = 
      let rec scan step addr =
        if addr < 0L || addr >= size then OptionalValue.Missing
        elif check addr then OptionalValue( (keyOfAddr lo addr, addr) )
        else scan step (step addr)
      if semantics = Lookup.Exact then
        if key >= lo && key <= hi && check (addrOfKey lo key) then
          OptionalValue( (key, addrOfKey lo key) )
        else OptionalValue.Missing
      else
        let step = 
          if semantics &&& Lookup.Greater = Lookup.Greater then (+) 1L
          elif semantics &&& Lookup.Smaller = Lookup.Smaller then (-) 1L
          else invalidArg "semantics" "Invalid lookup semantics"
        let start =
          let addr = addrOfKey key lo
          if semantics = Lookup.Greater || semantics = Lookup.Smaller 
            then step addr else addr
        scan step start

and VirtualOrdinalIndexBuilder() = 
  let baseBuilder = IndexBuilder.Instance

  let emptyConstruction () =
    let newIndex = VirtualOrdinalIndex(0L, -1L)
    unbox<IIndex<'K>> newIndex, Vectors.Empty(0L)

  static let indexBuilder = VirtualOrdinalIndexBuilder()
  static member Instance = indexBuilder

  interface IIndexBuilder with
    member x.Create<'K when 'K : equality>(keys:seq<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Create<'K when 'K : equality>(keys:ReadOnlyCollection<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Aggregate(index, aggregation, vector, selector) = failwith "Aggregate"
    member x.GroupBy(index, keySel, vector) = failwith "GroupBy"
    member x.OrderIndex(sc) = failwith "OrderIndex"
    member x.Shift(sc, offset) = failwith "Shift"
    member x.Union(sc1, sc2) = failwith "Union"
    member x.Intersect(sc1, sc2) = failwith "Intersect"
    member x.Merge(scs, transform) = failwith "Merge"
    member x.LookupLevel(sc, key) = failwith "LookupLevel"
    member x.WithIndex(index1, f, vector) = failwith "WithIndex"
    member x.Reindex(index1, index2, semantics, vector, cond) = failwith "ReIndex"
    member x.DropItem(sc, key) = failwith "DropItem"
    member x.Resample(index, keys, close, vect, selector) = failwith "Resample"

    member x.GetAddressRange<'K when 'K : equality>((index, vector), (lo, hi)) = 
      match index with
      | :? VirtualOrdinalIndex when hi < lo -> emptyConstruction()
      | :? VirtualOrdinalIndex & (:? IIndex<int64> as index) -> 
          // TODO: range checks
          let keyLo, keyHi = index.KeyRange
          let newVector = Vectors.GetRange(vector, (lo, hi))
          let newIndex = VirtualOrdinalIndex(keyLo + lo, keyLo + hi)
          unbox<IIndex<'K>> newIndex, newVector
      | _ -> 
          baseBuilder.GetAddressRange((index, vector), (lo, hi))

    member x.Project(index:IIndex<'K>) = index

    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) = 
      match index with
      | :? VirtualOrdinalIndex -> 
          let newIndex = Linear.LinearIndexBuilder.Instance.Create(index.Keys, Some index.IsOrdered)
          let cmd = Vectors.CustomCommand([vector], fun vectors ->
            { new VectorCallSite<_> with
                member x.Invoke(vector) =
                  vector.DataSequence
                  |> Array.ofSeq
                  |> ArrayVector.ArrayVectorBuilder.Instance.CreateMissing  :> IVector }
            |> (List.head vectors).Invoke)
          async.Return(newIndex), cmd
      | _ -> async.Return(index), vector

    member x.GetRange<'K when 'K : equality>( (index, vector), (optLo:option<'K * _>, optHi:option<'K * _>)) = 
      match index with
      | :? VirtualOrdinalIndex & (:? IIndex<int64> as index) -> 
          let getRangeKey proj next = function
            | None -> proj index.KeyRange 
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) -> next (unbox<int64> k)
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loIdx, hiIdx = loKey - (fst index.KeyRange), hiKey - (fst index.KeyRange)

          // TODO: range checks
          let newVector = Vectors.GetRange(vector, (loIdx, hiIdx))
          let newIndex = VirtualOrdinalIndex(loKey, hiKey)
          unbox<IIndex<'K>> newIndex, newVector
      | _ -> 
          baseBuilder.GetRange((index, vector), (optLo, optHi))

// ------------------------------------------------------------------------------------------------

type VirtualVectorHelper =
  static member Create<'T>(source:VirtualVectorSource<'T>) = 
    VirtualVector<'T>(source)

type Virtual() =
  static let createMi = typeof<VirtualVectorHelper>.GetMethod("Create")

  static member CreateOrdinalSeries(source) =
    let vector = VirtualVector(source)
    let index = VirtualOrdinalIndex(0L, source.Length-1L)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualOrdinalIndexBuilder.Instance)

  static member CreateOrdinalFrame(keys, sources:seq<VirtualVectorSource>) = 
    let columnIndex = Index.ofKeys keys
    let count = sources |> Seq.fold (fun st src ->
      match st with 
      | None -> Some(src.Length) 
      | Some n when n = src.Length -> Some(n)
      | _ -> invalidArg "sources" "Sources should have the same length!" ) None
    if count = None then invalidArg "sources" "At least one column is required"
    let count = count.Value

    let rowIndex = VirtualOrdinalIndex(0L, count-1L)
    let data = 
      sources 
      |> Seq.map (fun source -> 
          createMi.MakeGenericMethod(source.ElementType).Invoke(null, [| source |]) :?> IVector)
      |> Vector.ofValues
    Frame<_, _>(rowIndex, columnIndex, data, VirtualOrdinalIndexBuilder.Instance, VirtualVectorBuilder.Instance)
    
