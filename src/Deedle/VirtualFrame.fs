namespace Deedle.Virtual

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

open Deedle
open Deedle.Vectors
open Deedle.Internal

type VirtualVectorSource<'V> = 
  abstract Length : int64
  abstract ValueAt : int64 -> OptionalValue<'V>
  abstract GetSubVector : int64 * int64 -> VirtualVectorSource<'V>

type VirtualVector<'V>(source:VirtualVectorSource<'V>) = 
  member x.Source = source
  interface IVector with
    member val ElementType = typeof<'V>
    member x.SuppressPrinting = false
    member x.GetObject(index) = source.ValueAt(index) |> OptionalValue.map box
    member x.ObjectSequence = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) |> OptionalValue.map box }
    member x.Invoke(site) = site.Invoke<'V>(x)
  interface IVector<'V> with
    member x.GetValue(index) = source.ValueAt(index)
    member x.Data = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) } |> VectorData.Sequence
    member x.SelectMissing(f) = invalidOp "VirtualVector.Select: Not supported"
    member x.Select(f) = invalidOp "VirtualVector.SelectMissing: Not supported"

type VirtualVectorBuilder() =
  let baseBuilder = VectorBuilder.Instance
  static let vectorBuilder = VirtualVectorBuilder() :> IVectorBuilder
  let build cmd args = vectorBuilder.Build(cmd, args)

  static member Instance = vectorBuilder

  interface IVectorBuilder with
    member builder.Create(values) = baseBuilder.Create(values)
    member builder.CreateMissing(optValues) = baseBuilder.CreateMissing(optValues)
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
      if typeof<'K> <> typeof<int64> then 
        baseBuilder.GetAddressRange((index, vector), (lo, hi))
      else 
        let index = index :?> IIndex<int64>
        let keyLo, keyHi = index.KeyRange
        if hi < lo then 
          let newIndex = VirtualOrdinalIndex(0L, -1L)
          unbox<IIndex<'K>> newIndex, Vectors.Empty(0L)
        else
          // TODO: range checks
          let newVector = Vectors.GetRange(vector, (lo, hi))
          let newIndex = VirtualOrdinalIndex(keyLo + lo, keyLo + hi)
          unbox<IIndex<'K>> newIndex, newVector

    member x.Project(index:IIndex<'K>) = failwith "Project"
    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) = failwith "AsyncMaterialize"
    member x.GetRange(index, optLo:option<'K * _>, optHi:option<'K * _>, vector) = failwith "GetRange"

// ------------------------------------------------------------------------------------------------

type Virtual =
  static member CreateOrdinalSeries(source) =
    let vector = VirtualVector(source)
    let index = VirtualOrdinalIndex(0L, source.Length-1L)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualOrdinalIndexBuilder.Instance)

