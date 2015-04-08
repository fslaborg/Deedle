// ------------------------------------------------------------------------------------------------
// BigDeedle - A virtualized implementation of an index. There are two kinds of indices:
//
//  - `VirtualOrderedIndex` uses an ordered `IVirtualVectorSource` as the source of
//     indices and uses its search funcitonality to find the matching addresses
//
//  - `VirtualOrdinalIndex` represents an ordinal index consisting of sub-ranges from 
//     some bigger index - this can be e.g. [10..99; 200..299].
//
// ------------------------------------------------------------------------------------------------

namespace Deedle.Indices.Virtual

open System
open Deedle
open Deedle.Ranges
open Deedle.Addressing
open Deedle.Indices
open Deedle.Internal
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Vectors.Virtual
open System.Collections.Generic
open System.Collections.ObjectModel

// ------------------------------------------------------------------------------------------------
// VirtualOrderedIndex - index where the keys are defined by the specified sorted source
// ------------------------------------------------------------------------------------------------

type VirtualOrderedIndex<'K when 'K : equality>(source:IVirtualVectorSource<'K>) =
  // TODO: Assert that the source is ordered
  // TODO: Another assumption here is that the source contains no NA values
  let keyAtAddr i = source.ValueAt(i).Value
  member x.Source = source

  interface IIndex<'K> with
    member x.KeyAt(addr) = keyAtAddr (Location.delayed(addr, source.AddressOperations))
    member x.AddressAt(offs) = source.AddressOperations.AddressOf offs
    member x.KeyCount = source.Length
    member x.IsEmpty = source.Length = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = 
      keyAtAddr (Location.known(source.AddressOperations.FirstElement, 0L)),
      keyAtAddr (Location.delayed(source.AddressOperations.LastElement, source.AddressOperations))
    
    member x.Keys : ReadOnlyCollection<'K> = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> keyAtAddr (Location.known(addr, idx)))
      |> ReadOnlyCollection.ofSeq
    member x.Mappings = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> KeyValuePair(keyAtAddr (Location.known(addr, idx)), addr))
    member x.IsOrdered = true
    member x.Comparer = Comparer<'K>.Default

    member x.Locate(key) = 
      let loc = source.LookupValue(key, Lookup.Exact, fun _ -> true)
      if loc.HasValue then snd loc.Value else Address.invalid
    member x.Lookup(key, semantics, check) = 
      source.LookupValue(key, semantics, Func<_, _>(check))

// ------------------------------------------------------------------------------------------------
// VirtualOrdinalIndex - ordinal index where the keys are specified by the given 'Ranges'
// ------------------------------------------------------------------------------------------------

/// 
and VirtualOrdinalIndex(ranges:Ranges<int64>, source:IVirtualVectorSource) =
  member x.Ranges = ranges
  member x.Source = source
  interface IIndex<int64> with
    member x.KeyAt(addr) = ranges |> Ranges.addressOfKey (source.AddressOperations.OffsetOf(addr))
    member x.AddressAt(offs) = ranges |> Ranges.keyOfAddress offs |> source.AddressOperations.AddressOf
    member x.KeyCount = source.Length
    member x.IsEmpty = source.AddressOperations.Range |> Seq.isEmpty
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = Ranges.keyRange ranges
    member x.Keys = ranges |> Ranges.keys |> ReadOnlyCollection.ofSeq
    member x.Mappings = Seq.map2 (fun k v -> KeyValuePair(k, v)) (Ranges.keys ranges) source.AddressOperations.Range
    member x.IsOrdered = true
    member x.Comparer = Comparer<int64>.Default
    member x.Locate(key) = source.AddressOperations.AddressOf(Ranges.addressOfKey key ranges)
    member x.Lookup(key, semantics, check) = 
      failwith "LOokup!"
      //Ranges.lookup key semantics (addressing.AddressOf >> check) ranges 
      //|> OptionalValue.map (fun (key, addr) ->
      //    key, addressing.AddressOf addr)

// ------------------------------------------------------------------------------------------------
// VirtualIndexBuilder - implements operations on VirtualOrderedIndex and VirtualOrdinalIndex
// ------------------------------------------------------------------------------------------------

and VirtualIndexBuilder() = 
  let baseBuilder = IndexBuilder.Instance

  static let indexBuilder = VirtualIndexBuilder()
  static member Instance = indexBuilder

  interface IIndexBuilder with
    member x.Create<'K when 'K : equality>(keys:seq<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Create<'K when 'K : equality>(keys:ReadOnlyCollection<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Aggregate(index, aggregation, vector, selector) = failwith "Aggregate"
    member x.GroupBy(index, keySel, vector) = failwith "GroupBy"
    member x.OrderIndex( (index, vector) ) = 
      if index.IsOrdered then index, vector
      else failwith "OrderIndex"

    member x.Shift(sc, offset) = failwith "Shift"
    member x.Union(sc1, sc2) = failwith "Union"
    member x.Intersect(sc1, sc2) = failwith "Intersect"
    member x.Merge(scs:list<SeriesConstruction<'K>>, transform) = 

      let (|OrderedSources|_|) : list<SeriesConstruction<_>> -> _ = 
        List.tryChooseBy (function
          | :? VirtualOrderedIndex<'K> as idx, vec -> Some(idx.Source, vec)
          | _ -> None)
    
      let (|OrdinalSources|_|) : list<SeriesConstruction<_>> -> _ = 
        List.tryChooseBy (function
          | :? VirtualOrdinalIndex as idx, vec -> Some(idx.Ranges, idx.Source, vec)
          | _ -> None)

      match scs with
      | OrderedSources sources ->
          let newSource = (fst sources.Head).MergeWith(sources.Tail |> List.map fst)
          let newVector = sources |> Seq.map snd |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          VirtualOrderedIndex(newSource) :> _, newVector

      | OrdinalSources sources ->
          let _, firstSource, _ = sources.Head
          let newSource = 
            { new IVirtualVectorSourceOperation<_> with
                member x.Invoke<'T>(source) =
                  source.MergeWith [for _, s, _ in sources.Tail -> s :?> IVirtualVectorSource<'T> ] :> IVirtualVectorSource }                
            |> firstSource.Invoke
          let newRanges = Ranges.combine [ for r, _, _ in sources -> r ]
          let newVector = seq { for _, _, v in sources -> v } |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          unbox (VirtualOrdinalIndex(newRanges, newSource)), newVector
      | _ -> 
          for index, vector in scs do 
            match index with
            | :? VirtualOrderedIndex<'K>
            | :? VirtualOrdinalIndex -> failwith "TODO: Merge - ordered/ordinal index - avoid materialization!"
            | _ -> ()
          baseBuilder.Merge(scs, transform)


    member x.Search((index:IIndex<'K>, vector), searchVector:IVector<'V>, searchValue) = 
      let searchVector = 
        match searchVector with
        | :? IWrappedVector<'V> as wrapped -> wrapped.UnwrapVector()
        | _ -> searchVector

      match index, searchVector with
      | (:? VirtualOrdinalIndex as index), (:? VirtualVector<'V> as searchVector) ->
          failwith "TODO: Search!"
          (*
          let mapping = searchVector.Source.LookupRange(searchValue)
          let count =
            match mapping with
            | VectorRange.Custom(iv) -> iv.Count
            | VectorRange.Range(lo, hi) -> failwith "TODO: (Address.asInt64 hi) - (Address.asInt64 lo) + 1L"
          let newIndex = VirtualOrdinalIndex(Ranges.Create [ 0L, count-1L ])
          unbox<IIndex<'K>> newIndex, GetRange(vector, mapping)
          *)

      | (:? VirtualOrderedIndex<'K> as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(searchValue)
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
      | (:? VirtualOrdinalIndex as index1), (:? VirtualOrdinalIndex as index2) when index1.Ranges = index2.Ranges -> 
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

    member x.GetAddressRange<'K when 'K : equality>((index, vector), range) = 
      match index with
      | :? VirtualOrderedIndex<'K> as index ->
          // TODO: Range checks
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(range))
          let newVector = Vectors.GetRange(vector, range)
          newIndex :> IIndex<'K>, newVector

      | :? VirtualOrdinalIndex as index ->
          let ordinalRestr = 
            range |> RangeRestriction.map (fun addr -> 
              Ranges.keyOfAddress (index.Source.AddressOperations.OffsetOf(addr)) index.Ranges)
          
          // TODO: range checks
          let newIndex = 
            { new IVirtualVectorSourceOperation<_> with 
                member x.Invoke(source) = VirtualOrdinalIndex(Ranges.restrictRanges ordinalRestr index.Ranges, source.GetSubVector(range)) }
            |> index.Source.Invoke          
          let newVector = Vectors.GetRange(vector, range)
          unbox<IIndex<'K>> newIndex, newVector
      | _ -> 
          baseBuilder.GetAddressRange((index, vector), range)

    member x.Project(index:IIndex<'K>) = index

    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) = 
      match index with
      | :? VirtualOrdinalIndex 
      | :? VirtualOrderedIndex<'K> ->
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
            | None -> Some bound
            | Some(k, beh) -> 
                let lookup = if beh = BoundaryBehavior.Inclusive then lookup ||| Lookup.Exact else lookup
                match index.Source.LookupValue(k, lookup, fun _ -> true) with
                | OptionalValue.Present(_, addr) -> Some addr
                | _ -> None

          let loIdx = getRangeKey index.Source.AddressOperations.FirstElement Lookup.Greater optLo
          let hiIdx = getRangeKey index.Source.AddressOperations.LastElement Lookup.Smaller optHi
          match loIdx, hiIdx with
          | Some loIdx, Some hiIdx when loIdx <= hiIdx ->
              let newVector = Vectors.GetRange(vector, RangeRestriction.Fixed(loIdx, hiIdx))
              let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(RangeRestriction.Fixed(loIdx, hiIdx)))
              newIndex :> IIndex<'K>, newVector
          | _ ->
              Index.ofKeys [] :> _, VectorConstruction.Empty(0L)       
              

      | :? VirtualOrdinalIndex as ordIndex & (:? IIndex<int64> as index) -> 
          let getRangeKey proj next = function
            | None -> proj index.KeyRange 
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) -> 
                Ranges.keyOfAddress (next (Ranges.addressOfKey (unbox<int64> k) ordIndex.Ranges)) ordIndex.Ranges
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loAddr, hiAddr = 
            // TODO: Too many silly conversions there and back again
            ordIndex.Source.AddressOperations.AddressOf (Ranges.addressOfKey loKey ordIndex.Ranges),
            ordIndex.Source.AddressOperations.AddressOf (Ranges.addressOfKey hiKey ordIndex.Ranges)

          // TODO: range checks
          let restr = RangeRestriction.Fixed(loKey, hiKey)
          let range = RangeRestriction.Fixed(loAddr, hiAddr)
          let newVector = Vectors.GetRange(vector, range)
          let newSource =
            { new IVirtualVectorSourceOperation<_> with
                member x.Invoke(source) = source.GetSubVector(range) :> IVirtualVectorSource }
            |> ordIndex.Source.Invoke
          let newIndex = VirtualOrdinalIndex(Ranges.restrictRanges restr ordIndex.Ranges , newSource) 
          unbox<IIndex<'K>> newIndex, newVector
          
      | _ -> 
          baseBuilder.GetRange((index, vector), (optLo, optHi))
