// ------------------------------------------------------------------------------------------------
// Indices
// ------------------------------------------------------------------------------------------------

namespace Deedle.Indices.Virtual

open System
open Deedle
open Deedle.Addressing
open Deedle.Indices
open Deedle.Internal
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Vectors.Virtual
open System.Collections.Generic
open System.Collections.ObjectModel

module Helpers2 = 
  let inline addrOfKey lo key = Address.ofInt64 (key - lo)
  let inline keyOfAddr lo addr = lo + (Address.asInt64 addr)

open Helpers2

type VirtualOrderedIndex<'K when 'K : equality>(source:IVirtualVectorSource<'K>) =
  // TODO: Assert that the source is ordered
  // TODO: Another assumption here is that the source contains no NA values
  let keyAtOffset i = source.ValueAt(Address.ofInt64 i).Value
  let keyAtAddr i = source.ValueAt(i).Value
  member x.Source = source

  interface IIndex<'K> with
    member x.KeyAt(addr) = keyAtAddr addr
    member x.KeyCount = source.Length
    member x.IsEmpty = source.Length = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = keyAtOffset 0L, keyAtOffset (source.Length-1L)
    member x.Keys = Array.init (int source.Length) (int64 >> keyAtAddr) |> ReadOnlyCollection.ofArray
    member x.Mappings = 
      Seq.range 0L (source.Length - 1L) 
      |> Seq.map (fun i -> KeyValuePair(keyAtOffset i, Address.ofInt64 i))
    member x.IsOrdered = true
    member x.Comparer = Comparer<'K>.Default

    member x.Locate(key) = 
      let loc = source.LookupValue(key, Lookup.Exact, fun _ -> true)
      if loc.HasValue then snd loc.Value else Address.Invalid
    member x.Lookup(key, semantics, check) = 
      source.LookupValue(key, semantics, Func<_, _>(check))

and VirtualOrdinalIndex(lo, hi) =
  let size = hi - lo + 1L
  do if size < 0L then invalidArg "range" "Invalid range"
  member x.Range = lo, hi
  interface IIndex<int64> with
    member x.KeyAt(addr) = 
      if addr < 0L || addr >= size then invalidArg "addr" "Out of range"
      else keyOfAddr lo addr
    member x.KeyCount = size
    member x.IsEmpty = size = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = lo, hi
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


and VirtualIndexBuilder() = 
  let baseBuilder = IndexBuilder.Instance

  let emptyConstruction () =
    let newIndex = Index.ofKeys []
    unbox<IIndex<'K>> newIndex, Vectors.Empty(0L)

  static let indexBuilder = VirtualIndexBuilder()
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
    member x.Merge(scs:list<SeriesConstruction<'K>>, transform) = 
      let orderedSources = 
        scs |> List.tryChooseBy (function
          | :? VirtualOrderedIndex<'K> as idx, vec -> Some(idx.Source, vec)
          | _ -> None)
      match orderedSources with
      | Some sources ->
          let newIndex = [ for s, _ in sources -> s ]
          let newIndex2 = (List.head newIndex).MergeWith(List.tail newIndex)
          let newVector = sources |> Seq.map snd |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          VirtualOrderedIndex(newIndex2) :> _, newVector

      | _ -> 
        for index, vector in scs do 
          match index with
          | :? VirtualOrderedIndex<'K>
          | :? VirtualOrdinalIndex -> failwith "TODO: Reindex - ordered/ordinal index - avoid materialization!"
          | _ -> ()
        baseBuilder.Merge(scs, transform)

    member x.Search((index:IIndex<'K>, vector), searchVector:IVector<'V>, searchValue) = 
      match index, searchVector with
      | (:? VirtualOrdinalIndex as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(LookupKind.Lookup searchValue)
          let newIndex = VirtualOrdinalIndex(0L, mapping.Count-1L);
          unbox<IIndex<'K>> newIndex, GetRange(vector, mapping)

      | (:? VirtualOrderedIndex<'K> as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(LookupKind.Lookup searchValue)
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(mapping))
          newIndex :> _, GetRange(vector, mapping)

      | _ ->
          failwith "TODO: Search - search would cause materialization"

    member x.LookupLevel(sc, key) = failwith "LookupLevel"

    member x.WithIndex(index1:IIndex<'K>, indexVector:IVector<'TNewKey>, vector) =
      match indexVector with
      | :? VirtualVector<'TNewKey> as indexVector ->
          let newIndex = VirtualOrderedIndex(indexVector.Source)
          // TODO: Assert that indexVector.Source is ordered and can actually be used as an index?
          // TODO: Assert that indexVector.Source has no duplicate keys and no NANs.... (which is impossible to do)
          upcast newIndex, vector
      | _ -> 
        failwith "TODO: WithIndex - not supported vector"

    member x.Reindex(index1:IIndex<'K>, index2:IIndex<'K>, semantics, vector, cond) = 
      match index1, index2 with
      | (:? VirtualOrdinalIndex as index1), (:? VirtualOrdinalIndex as index2) when index1.Range = index2.Range -> 
          vector           
      | :? VirtualOrderedIndex<'K>, _ 
      | _, :? VirtualOrderedIndex<'K>
      | :? VirtualOrdinalIndex, _ 
      | _, :? VirtualOrdinalIndex ->
          failwith "TODO: Reindex - ordered/ordinal index - avoid materialization!"
      | _ -> baseBuilder.Reindex(index1, index2, semantics, vector, cond)

    member x.DropItem((index:IIndex<'K>, vector), key) = 
      match index with
      | :? VirtualOrderedIndex<'K>
      | :? VirtualOrdinalIndex -> failwith "TODO: DropItem - ordered/ordinal index - avoid materialization!"
      | _ -> baseBuilder.DropItem((index, vector), key)

    member x.Resample(index, keys, close, vect, selector) = failwith "Resample"

    member x.GetAddressRange<'K when 'K : equality>((index, vector), (lo, hi)) = 
      match index with
      | :? VirtualOrderedIndex<'K> as index ->
          // TODO: Range checks
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(Range(lo, hi)))
          let newVector = Vectors.GetRange(vector, Vectors.Range(lo, hi))
          newIndex :> IIndex<'K>, newVector

      | :? VirtualOrdinalIndex when hi < lo -> emptyConstruction()
      | :? VirtualOrdinalIndex as index ->
          // TODO: range checks
          let range = Vectors.Range(lo, hi)
          let newVector = Vectors.GetRange(vector, range)
          let keyLo, keyHi = (index :> IIndex<_>).KeyRange
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
      | :? VirtualOrderedIndex<'K> as index ->
          let getRangeKey bound lookup = function
            | None -> bound
            | Some(k, beh) -> 
                let lookup = if beh = BoundaryBehavior.Inclusive then lookup ||| Lookup.Exact else lookup
                match index.Source.LookupValue(k, lookup, fun _ -> true) with
                | OptionalValue.Present(_, addr) -> addr
                | _ -> bound // TODO: Not sure what this means!

          let loIdx, hiIdx = getRangeKey 0L Lookup.Greater optLo, getRangeKey (index.Source.Length-1L) Lookup.Smaller optHi

          // TODO: probably range checks
          let newVector = Vectors.GetRange(vector, Vectors.Range(loIdx, hiIdx))
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(Range(loIdx, hiIdx)))
          unbox<IIndex<'K>> newIndex, newVector

      | :? VirtualOrdinalIndex as ordIndex & (:? IIndex<int64> as index) -> 
          let getRangeKey proj next = function
            | None -> proj index.KeyRange 
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) -> next (unbox<int64> k)
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loIdx, hiIdx = loKey - (fst index.KeyRange), hiKey - (fst index.KeyRange)

          // TODO: range checks
          let range = Vectors.Range(loIdx, hiIdx)
          let newVector = Vectors.GetRange(vector, range)
          let newIndex = VirtualOrdinalIndex(loKey, hiKey) 
          unbox<IIndex<'K>> newIndex, newVector
      | _ -> 
          baseBuilder.GetRange((index, vector), (optLo, optHi))
