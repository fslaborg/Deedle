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
open Deedle.Virtual
open System.Collections.Generic
open System.Collections.ObjectModel

// ------------------------------------------------------------------------------------------------
// VirtualOrderedIndex - index where the keys are defined by the specified sorted source
// ------------------------------------------------------------------------------------------------

/// Represents an ordered index based on data provided by a virtual source.
/// The index can be used by BigDeedle virtual frames and series, without accessing
/// all data from the data source.
///
/// The index only evaluates the full key collection when needed. Most of the actual
/// work is delegated to the `IVirtualVectorSource<'K>` value passed in the constructor.
type VirtualOrderedIndex<'K when 'K : equality>(source:IVirtualVectorSource<'K>) =
  let keyAtAddr i = source.ValueAt(i).Value
  let keys = Lazy<_>.Create(fun () ->
    source.AddressOperations.Range
    |> Seq.mapl (fun idx addr -> keyAtAddr (KnownLocation(addr, idx)))
    |> ReadOnlyCollection.ofSeq )

  /// Returns the underlying source associated with the index
  member x.Source = source

  /// Implements structural equality check against another index
  override index.Equals(another) =
    match another with
    | null -> false
    | :? IIndex<'K> as another -> Seq.structuralEquals (index :> IIndex<_>).KeySequence another.KeySequence
    | _ -> false

  /// Implement structural hashing against another index
  override index.GetHashCode() =
    (index :> IIndex<_>).Keys |> Seq.structuralHash

  // The interface mostly delegates all work to the source
  interface IIndex<'K> with
    member x.AddressingScheme = VirtualAddressingScheme(source.AddressingSchemeID) :> _
    member x.AddressOperations = source.AddressOperations
    member x.KeyAt(addr) = keyAtAddr (DelayedLocation(addr, source.AddressOperations))
    member x.AddressAt(offs) = source.AddressOperations.AddressOf offs
    member x.KeyCount = source.Length
    member x.IsEmpty = source.AddressOperations.Range |> Seq.isEmpty
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.Keys = keys.Value
    member x.IsOrdered = true
    member x.Comparer = Comparer<'K>.Default

    member x.KeyRange =
      keyAtAddr (KnownLocation(source.AddressOperations.FirstElement, 0L)),
      keyAtAddr (DelayedLocation(source.AddressOperations.LastElement, source.AddressOperations))

    member x.KeySequence : seq<'K> =
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> keyAtAddr (KnownLocation(addr, idx)))

    member x.Mappings =
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> KeyValuePair(keyAtAddr (KnownLocation(addr, idx)), addr))

    member x.Locate(key) =
      match source.LookupValue(key, Lookup.Exact, fun _ -> true) with
      | OptionalValue.Present(_, addr) -> addr
      | _ -> Address.invalid

    member x.Lookup(key, semantics, check) =
      source.LookupValue(key, semantics, Func<_, _>(check))

// ------------------------------------------------------------------------------------------------
// VirtualOrdinalIndex - ordinal index where the keys are specified by the given 'Ranges'
// ------------------------------------------------------------------------------------------------

/// Represents an ordinal index based on addressing provided by a virtual source.
/// The index can be used by BigDeedle virtual frames and series, without accessing
/// all data from the data source.
and VirtualOrdinalIndex(ranges:Ranges<int64>, source:IVirtualVectorSource) =

  /// Returns the ranges of data mapped in this index
  member x.Ranges = ranges
  /// Returns the source that is used to identify the index.
  member x.Source = source

  /// Structural equality on the key intervals only (not source identity). Two ordinal
  /// virtual frames of the same length therefore compare equal, which lets Join/Zip
  /// skip Union. Do not use these indices as dictionary keys if source identity matters.
  override x.Equals(another) =
    match another with
    | :? VirtualOrdinalIndex as other -> ranges.Ranges = other.Ranges.Ranges
    | _ -> false

  override x.GetHashCode() = hash ranges.Ranges

  interface IIndex<int64> with
    member x.AddressingScheme = VirtualAddressingScheme(source.AddressingSchemeID) :> _
    member x.AddressOperations = source.AddressOperations
    member x.KeyAt(addr) = ranges |> Ranges.keyAtOffset (source.AddressOperations.OffsetOf(addr))
    member x.AddressAt(offs) = source.AddressOperations.AddressOf offs
    member x.KeyCount = source.Length
    member x.IsEmpty = source.AddressOperations.Range |> Seq.isEmpty
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = Ranges.keyRange ranges
    member x.Keys = ranges |> Ranges.keys |> ReadOnlyCollection.ofSeq
    member x.KeySequence = ranges |> Ranges.keys
    member x.Mappings = Seq.map2 (fun k v -> KeyValuePair(k, v)) (Ranges.keys ranges) source.AddressOperations.Range
    member x.IsOrdered = true
    member x.Comparer = Comparer<int64>.Default
    member x.Locate(key) = source.AddressOperations.AddressOf(Ranges.offsetOfKey key ranges)
    member x.Lookup(key, semantics, check) =
      Ranges.lookup key semantics (fun k _ -> check(source.AddressOperations.AddressOf k)) ranges
      |> OptionalValue.map (fun (k, offs) -> k, source.AddressOperations.AddressOf k)

// ------------------------------------------------------------------------------------------------
// VirtualIndexBuilder - implements operations on VirtualOrderedIndex and VirtualOrdinalIndex
// ------------------------------------------------------------------------------------------------

/// Implements `IIndexBuilder` interface for BigDeedle. This directly implements operations that can
/// be implemented on virtual vectors (mostly merging, slicing) and for other operations, it calls
/// ordinary `LinearIndexBuilder`. The resulting `VectorConstruction` corresponds to the addressing
/// scheme of the returned index (i.e. if we return virtual, we expect to build virtual vector; if
/// we materialize, the vector builder also has to materialize).
and VirtualIndexBuilder() =
  let baseBuilder = IndexBuilder.Instance

  /// Ordered sub-index after an address restriction. `withLinearAddressing` reindexes the
  /// sliced source onto `0 .. n-1` so absolute-address backends align with Combine/diff.
  let orderedIndexFromRange (source: IVirtualVectorSource<'K>) (range: RangeRestriction<Address>) =
    VirtualOrderedIndex(VirtualVectorSource.withLinearAddressing (source.GetSubVector(range)))

  /// Ordinal sub-index that keeps key intervals via `restrictRanges` (used by GetAddressRange /
  /// GetRange). Unlike `filteredOrdinalIndex`, this never renumbers keys to `0 .. n-1`.
  let ordinalIndexFromRange (ordIndex: VirtualOrdinalIndex) (ordinalRestr: RangeRestriction<int64>) (range: RangeRestriction<Address>) =
    { new IVirtualVectorSourceOperation<_> with
        member _.Invoke source =
          let sub = VirtualVectorSource.withLinearAddressing (source.GetSubVector(range))
          VirtualOrdinalIndex(Ranges.restrictRanges ordinalRestr ordIndex.Ranges, sub) }
    |> ordIndex.Source.Invoke

  let shiftSliceRanges (addrOps: IAddressOperations) (n: int64) (offset: int) =
    let absOff = int64 (abs offset)
    let newLen = n - absOff
    let keyStart, valStart =
      match offset > 0 with
      | true -> absOff, 0L
      | false -> 0L, absOff
    let fixedRange start =
      RangeRestriction.Fixed(addrOps.AddressOf start, addrOps.AddressOf (start + newLen - 1L))
    fixedRange keyStart, fixedRange valStart

  /// Build an enumerable `RangeRestriction<int64>` (Custom) over ordinal keys.
  /// Needed because `StepRange` intentionally cannot enumerate / report `Count`.
  let customInt64KeyRestriction (keys: seq<int64>) =
    let arr = keys |> Seq.toArray
    ({ new IRangeRestriction<int64> with
        member _.Count = int64 arr.Length
       interface seq<int64> with
         member _.GetEnumerator() = (arr :> seq<int64>).GetEnumerator()
       interface System.Collections.IEnumerable with
         member _.GetEnumerator() = (arr :> seq<int64>).GetEnumerator() :> _ }
     |> RangeRestriction.Custom)

  let mappingToOrdinalKeyRestriction (ordIndex: VirtualOrdinalIndex) (mapping: RangeRestriction<Address>) =
    let keyFromOffset offset = Ranges.keyAtOffset offset ordIndex.Ranges
    let keyAtAddr addr = keyFromOffset (ordIndex.Source.AddressOperations.OffsetOf(addr))
    match mapping with
    | RangeRestriction.Custom (:? StepRange as sr) when sr.Step > 0 && sr.Offset >= 0 ->
        customInt64KeyRestriction (seq {
          for i in int64 sr.Offset .. int64 sr.Step .. (ordIndex.Source.Length - 1L) ->
            keyFromOffset i })
    | _ -> RangeRestriction.map keyAtAddr mapping

  /// After Search/`LookupRange` + `GetSubVector`, build the filtered ordinal row index.
  /// On a dense `[0 .. n-1]` ordinal source with Fixed/Step mapping, compact to
  /// `0 .. sub.Length-1` (filter result is a new dense domain). Otherwise keep original
  /// key intervals via `restrictRanges`. Do not use for GetAddressRange/GetRange slices
  /// that must preserve absolute key values.
  let filteredOrdinalIndex (ordIndex: VirtualOrdinalIndex) (mapping: RangeRestriction<Address>) (sub: IVirtualVectorSource) =
    match ordIndex.Ranges.Ranges, mapping with
    | [| (0L, hi) |], (RangeRestriction.Custom (:? StepRange as _) | RangeRestriction.Fixed _)
        when hi = ordIndex.Source.Length - 1L ->
        VirtualOrdinalIndex(Ranges.inlineCreate (+) [0L, sub.Length - 1L], sub)
    | _ ->
        let ordinalRestr = mappingToOrdinalKeyRestriction ordIndex mapping
        VirtualOrdinalIndex(Ranges.restrictRanges ordinalRestr ordIndex.Ranges, sub)

  /// Search path: slice source, then `filteredOrdinalIndex` (handles Step without enumerating it).
  let ordinalIndexFromSearchMapping (ordIndex: VirtualOrdinalIndex) (mapping: RangeRestriction<Address>) =
    { new IVirtualVectorSourceOperation<_> with
        member _.Invoke source =
          let sub = VirtualVectorSource.withLinearAddressing (source.GetSubVector(mapping))
          filteredOrdinalIndex ordIndex mapping sub }
    |> ordIndex.Source.Invoke

  let emptyLinearIndex () =
    Linear.LinearIndexBuilder.Instance.Create(Seq.empty, None), Vectors.Empty(0L)

  let virtualWindowSlice (builder: IIndexBuilder) index vector (addrOps: IAddressOperations) (kind, lo, hi) =
    match hi >= lo with
    | true ->
        let range = RangeRestriction.Fixed(addrOps.AddressOf lo, addrOps.AddressOf hi)
        let idx, cmd = builder.GetAddressRange((index, vector), range)
        kind, (idx, cmd)
    | false -> kind, emptyLinearIndex()

  let mergeOrdinalSources (firstSource: IVirtualVectorSource) (rest: IVirtualVectorSource list) =
    { new IVirtualVectorSourceOperation<_> with
        member _.Invoke<'T>(source) =
          source.MergeWith [for s in rest -> s :?> IVirtualVectorSource<'T>] :> IVirtualVectorSource }
    |> firstSource.Invoke

  /// Create linear index from the keys of a given index
  let materializeIndex (index:IIndex<'T>) =
    Linear.LinearIndexBuilder.Instance.Create(index.Keys, Some index.IsOrdered)

  static let indexBuilder = VirtualIndexBuilder()

  /// Returns an instance of the virtual index builder
  static member Instance = indexBuilder

  interface IIndexBuilder with
    // The following are simply delegated to `LinearIndexBuilder`
    member x.Create(keys:seq<_>, ordered) = baseBuilder.Create(keys, ordered)
    member x.Create(keys:ReadOnlyCollection<_>, ordered) = baseBuilder.Create(keys, ordered)
    member x.Recreate(index) = baseBuilder.Recreate(index)

    member x.GroupBy(index, keySel, vector) = baseBuilder.GroupBy(index, keySel, vector)

    member x.Shift((index:IIndex<'K>, vector), offset) =
      match index.AddressingScheme, offset with
      | :? VirtualAddressingScheme, 0 -> index, vector
      | :? VirtualAddressingScheme, offset ->
          let n = index.KeyCount
          let absOff = int64 (abs offset)
          match n = 0L || absOff >= n with
          | true -> baseBuilder.Shift((index, vector), offset)
          | false ->
              // Keys and values are sliced from different ends so that result[k] = series[k - offset].
              // GetAddressRange / GetRange reindex onto linear 0..newLen-1 (needed for Series.diff Combine).
              let indexRange, vectorRange = shiftSliceRanges index.AddressOperations n offset
              let newIndex, _ = (x :> IIndexBuilder).GetAddressRange((index, vector), indexRange)
              newIndex, Vectors.GetRange(vector, vectorRange)
      | _ -> baseBuilder.Shift((index, vector), offset)

    member x.Union((index1:IIndex<'K>, vector1), (index2, vector2)) =
      match index1, index2 with
      | (:? VirtualOrdinalIndex as a), (:? VirtualOrdinalIndex as b)
          when a.Ranges.Ranges = b.Ranges.Ranges ->
          index1, vector1, vector2
      | (:? VirtualOrderedIndex<'K> as a), (:? VirtualOrderedIndex<'K> as b)
          when Object.ReferenceEquals(a.Source, b.Source) && a.Source.Length = b.Source.Length ->
          index1, vector1, vector2
      | _ ->
          baseBuilder.Union((index1, vector1), (index2, vector2))

    member x.Intersect((index1:IIndex<'K>, vector1), (index2, vector2)) =
      let singleBlock (r: Ranges<int64>) = r.Ranges |> Array.tryHead
      match index1, index2 with
      | (:? VirtualOrdinalIndex as a), (:? VirtualOrdinalIndex as b)
          when a.Ranges.Ranges = b.Ranges.Ranges ->
          index1, vector1, vector2
      | (:? VirtualOrdinalIndex as a), (:? VirtualOrdinalIndex as b) ->
          match singleBlock a.Ranges, singleBlock b.Ranges with
          | Some(lo1, hi1), Some(lo2, hi2) ->
              let lo, hi = max lo1 lo2, min hi1 hi2
              match lo > hi with
              | true ->
                  baseBuilder.Create(Seq.empty, Some true), Vectors.Empty(0L), Vectors.Empty(0L)
              | false ->
                  let kLo, kHi = unbox<'K> lo, unbox<'K> hi
                  let keyRange = Some(kLo, BoundaryBehavior.Inclusive), Some(kHi, BoundaryBehavior.Inclusive)
                  let idx1, cmd1 = (x :> IIndexBuilder).GetRange((index1, vector1), keyRange)
                  let _, cmd2 = (x :> IIndexBuilder).GetRange((index2, vector2), keyRange)
                  idx1, cmd1, cmd2
          | _ ->
              baseBuilder.Intersect((index1, vector1), (index2, vector2))
      | (:? VirtualOrderedIndex<'K> as a), (:? VirtualOrderedIndex<'K> as b)
          when Object.ReferenceEquals(a.Source, b.Source) && a.Source.Length = b.Source.Length ->
          index1, vector1, vector2
      | _ ->
          baseBuilder.Intersect((index1, vector1), (index2, vector2))
    member x.LookupLevel(sc, key) = baseBuilder.LookupLevel(sc, key)
    member x.DropItem((index:IIndex<_>, vector), key) =
      baseBuilder.DropItem((index, vector), key)

    member x.Aggregate(index, aggregation, vector, selector) =
      match aggregation with
      | WindowSize _
      | ChunkSize _ when (index.AddressingScheme :? VirtualAddressingScheme) ->
          let builder = (x :> IIndexBuilder)
          let addrOps = index.AddressOperations
          let locations =
            match aggregation with
            | ChunkSize(sz, bnd) -> Seq.chunkRangesWithBounds (int64 sz) bnd index.KeyCount
            | WindowSize(sz, bnd) -> Seq.windowRangesWithBounds (int64 sz) bnd index.KeyCount
            | _ -> Seq.empty
          let keyValuePairs =
            locations
            |> Seq.map (virtualWindowSlice builder index vector addrOps)
            |> Seq.map selector
            |> Array.ofSeq
          let newIndex =
            Linear.LinearIndexBuilder.Instance.Create(
              ReadOnlyCollection.ofArray (Array.map fst keyValuePairs), None)
          let vect = VectorBuilder.Instance.CreateMissing(Array.map snd keyValuePairs)
          newIndex, vect
      | _ ->
          baseBuilder.Aggregate(index, aggregation, vector, selector)
    member x.Resample(chunkBuilder, index, keys, close, vect, selector) =
      baseBuilder.Resample(chunkBuilder, index, keys, close, vect, selector)

    // VirtualVector.Select returns a VirtualVector and so projecting
    // virtual index also needs to return virtual index
    member x.Project(index:IIndex<'K>) = index

    // Materialize the index asynchronously
    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) =
      match index with
      | :? VirtualOrdinalIndex
      | :? VirtualOrderedIndex<'K> ->
          async.Return(materializeIndex index), vector
      | _ ->
          baseBuilder.AsyncMaterialize((index, vector))


    // If the index is ordered, return it as it is. Otherwise, materialize and order
    member x.OrderIndex( (index, vector) ) =
      match index.IsOrdered with
      | true -> index, vector
      | false -> baseBuilder.OrderIndex( (index, vector) )

    // We can merge series as long as they all have `VirtualOrderedIndex` or as long as they
    // all have `VirtualOrdinalIndex`. If they differ or they are fully evaluated, we call the
    // base vector builder and fully materialize them.
    member x.Merge<'K when 'K : equality>(scs:list<SeriesConstruction<'K>>, transform, reorder) =

      /// Succeeds if all the specified indices are `VirtualOrderedIndex`
      let (|OrderedSources|_|) : list<SeriesConstruction<'K>> -> _ =
        List.tryChooseBy (function
          | :? VirtualOrderedIndex<'K> as idx, vec -> Some(idx.Source, vec)
          | _ -> None)

      /// Succeeds if all the specified indices are `VirtualOrdinalIndex`
      let (|OrdinalSources|_|) : list<SeriesConstruction<'K>> -> _ =
        List.tryChooseBy (function
          | :? VirtualOrdinalIndex as idx, vec -> Some(idx.Ranges, idx.Source, vec)
          | _ -> None)

      // Handle the different 3 cases - ordered, ordinal, mixed/other
      match scs with
      | OrderedSources sources ->
          let newSource = (fst sources.Head).MergeWith(sources.Tail |> List.map fst)
          let newVector = sources |> Seq.map snd |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          VirtualOrderedIndex(newSource) :> _, newVector

      | OrdinalSources sources ->
          let _, firstSource, _ = sources.Head
          let newSource =
            mergeOrdinalSources firstSource [for _, s, _ in sources.Tail -> s]
          let newRanges = Ranges.combine [ for r, _, _ in sources -> r ]
          let newVector = sources |> Seq.map (fun (_, _, v) -> v) |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          unbox (VirtualOrdinalIndex(newRanges, newSource)), newVector
      | _ ->
          let scs =
            [ for index, vector in scs ->
                match index with
                | :? VirtualOrderedIndex<'K> | :? VirtualOrdinalIndex -> materializeIndex index, vector
                | _ -> index, vector ]
          baseBuilder.Merge(scs, transform, true)


    // Search the given vector for a given value and return an index that represents only
    // part of the data matching the value. This works efficiently for `VirtualOrderedIndex`
    // and `VirtualOrdinalIndex` values that provide the `LookupRange` operation to do the search.
    member x.Search((index:IIndex<'K>, vector), searchVector:IVector<'V>, searchValue) =
      let searchVector =
        match searchVector with
        | :? IWrappedVector<'V> as wrapped -> wrapped.UnwrapVector()
        | _ -> searchVector

      match index, searchVector with
      | (:? VirtualOrderedIndex<'K> as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(searchValue)
          orderedIndexFromRange index.Source mapping :> IIndex<'K>, GetRange(vector, mapping)

      | (:? VirtualOrdinalIndex as ordIndex), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(searchValue)
          let newIndex = ordinalIndexFromSearchMapping ordIndex mapping
          unbox<IIndex<'K>> newIndex, GetRange(vector, mapping)

      | _ ->
          baseBuilder.Search((index, vector), searchVector, searchValue)


    /// Replace the index with another index (whose keys are specified by a vector)
    member x.WithIndex(index1:IIndex<'K>, indexVector:IVector<'TNewKey>, vector) =
      match indexVector with
      | :? VirtualVector<'TNewKey> as indexVector ->
          // Assumes that indexVector.Source is ordered (impossible to check)
          // Assumes that indexVector.Source has no duplicates/NANs (impossible)
          let newIndex = VirtualOrderedIndex(indexVector.Source)
          upcast newIndex, vector
      | _ ->
        baseBuilder.WithIndex(index1, indexVector, vector)


    // This materializes the index, except when we want to reindex according to
    // the same index (optimization) in which case we just return it
    member x.Reindex(index1:IIndex<'K>, index2:IIndex<'K>, semantics, vector, cond) =
      match index1, index2 with
      | (:? VirtualOrdinalIndex as index1), (:? VirtualOrdinalIndex as index2)
          when index1.Ranges.Ranges = index2.Ranges.Ranges -> vector
      | _ -> baseBuilder.Reindex(index1, index2, semantics, vector, cond)


    // Apply the specified address range restriction - for `VirtualOrderedIndex` and
    // `VirtualOrdinalIndex`, we directly delegate this to the source. Otherwise, materialize.
    member x.GetAddressRange<'K when 'K : equality>((index, vector), range) =
      match index with
      | :? VirtualOrderedIndex<'K> as index ->
          orderedIndexFromRange index.Source range :> IIndex<'K>, Vectors.GetRange(vector, range)

      | :? VirtualOrdinalIndex as index ->
          let ordinalRestr =
            range |> RangeRestriction.map (fun addr ->
              Ranges.keyAtOffset (index.Source.AddressOperations.OffsetOf(addr)) index.Ranges)

          // We know that 'K = int64, but we don't know what is the
          // generic type of the source (it could be anything)
          let newIndex = ordinalIndexFromRange index ordinalRestr range
          unbox<IIndex<'K>> newIndex, Vectors.GetRange(vector, range)
      | _ ->
          baseBuilder.GetAddressRange((index, vector), range)


    // Get a range determined by the specified keys in the index. This is more complex than
    // `GetAddressRange` because we first need to find the addresses corresponding to the keys
    // (or when we want exclusive boundaries, just after/before the keys).
    member x.GetRange<'K when 'K : equality>( (index, vector), (optLo:option<'K * _>, optHi:option<'K * _>)) =
      match index with
      | :? VirtualOrderedIndex<'K> as index ->
          /// Helper to Lookup the address of a key at the boundary
          let getRangeKey bound lookup = function
            | None -> Some bound
            | Some(k, beh) ->
                let lookup = if beh = BoundaryBehavior.Inclusive then lookup ||| Lookup.Exact else lookup
                match index.Source.LookupValue(k, lookup, fun _ -> true) with
                | OptionalValue.Present(_, addr) -> Some addr
                | _ -> None

          // Get the addresses of the lower and upper boundary (handling exclusive lookups too)
          let loIdx = getRangeKey index.Source.AddressOperations.FirstElement Lookup.Greater optLo
          let hiIdx = getRangeKey index.Source.AddressOperations.LastElement Lookup.Smaller optHi
          match loIdx, hiIdx with
          | Some loIdx, Some hiIdx when loIdx <= hiIdx ->
              // If the range was correct, return an address sub-range
              let addrRange = RangeRestriction.Fixed(loIdx, hiIdx)
              orderedIndexFromRange index.Source addrRange :> IIndex<'K>, Vectors.GetRange(vector, addrRange)
          | _ ->
              // Otherwise (e.g. lo > hi) return empty series
              Index.ofKeys [] :> _, VectorConstruction.Empty(0L)

      | :? VirtualOrdinalIndex as ordIndex & (:? IIndex<int64> as index) ->
          /// Helper that gets the key (in the source index) corresponding the the place where the
          /// subrange actually starts (the keys in the index may not be continuous e.g. when
          /// we merge them) and so for non-inclusive lookups, we inc/dec the *offset*.
          let getRangeKey proj next = function
            | None -> proj index.KeyRange
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) ->
                Ranges.keyAtOffset (next (Ranges.offsetOfKey (unbox<int64> k) ordIndex.Ranges)) ordIndex.Ranges

          // Get the addresses, make range restriction
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loAddr, hiAddr = index.Locate(loKey), index.Locate(hiKey)
          let keyRestr = RangeRestriction.Fixed(loKey, hiKey)
          let addrRestr = RangeRestriction.Fixed(loAddr, hiAddr)

          // Make new vector and new index
          let newSource =
            { new IVirtualVectorSourceOperation<_> with
                member _.Invoke source =
                  VirtualVectorSource.withLinearAddressing (source.GetSubVector(addrRestr)) :> IVirtualVectorSource }
            |> ordIndex.Source.Invoke
          let newIndex = VirtualOrdinalIndex(Ranges.restrictRanges keyRestr ordIndex.Ranges, newSource)
          unbox<IIndex<'K>> newIndex, Vectors.GetRange(vector, addrRestr)

      | _ ->
          // Otherwise, materialize the index and perform slicing on it
          baseBuilder.GetRange((index, vector), (optLo, optHi))
