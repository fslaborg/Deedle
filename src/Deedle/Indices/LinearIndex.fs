// --------------------------------------------------------------------------------------
// A concrete implementation of an index. Represents an index where the values are sto-
// red in an array (or similar structure) with linearly ordered addresses without holes.
// --------------------------------------------------------------------------------------

namespace Deedle.Indices.Linear

open System
open System.Linq
open System.Collections.Generic
open Deedle
open Deedle.Keys
open Deedle.Addressing
open Deedle.Internal
open Deedle.Indices
open Deedle.VectorHelpers
open System.Diagnostics
open System.Collections.ObjectModel

/// LinearIndex + ArrayVector use linear addressing (address is just an offset)
module Address = LinearAddress

/// Implements address operations for linear addressing 
type LinearAddressOperations =
  | LinearAddressOperations of int64 * int64
  interface IAddressOperations with
    member x.AdjustBy(addr, offset) = Address.ofInt64 (Address.asInt64 addr + offset)
    member x.AddressOf(offset) = Address.ofInt64 offset
    member x.OffsetOf(addr) = Address.asInt64 addr
    member x.FirstElement = let (LinearAddressOperations(lo, _)) = x in Address.ofInt64 lo
    member x.LastElement = let (LinearAddressOperations(_, hi)) = x in Address.ofInt64 hi
    member x.Range = 
      let (LinearAddressOperations(lo, hi)) = x 
      Seq.range lo hi |> Seq.map Address.ofInt64

/// An index that maps keys `K` to offsets `Address`. The keys cannot be duplicated.
/// The construction checks if the keys are ordered (using the provided or the default
/// comparer for `K`) and disallows certain operations on unordered indices.
type LinearIndex<'K when 'K : equality> 
  internal (keys:ReadOnlyCollection<'K>, builder, ?ordered) =

  let comparer = Comparer<'K>.Default
  let isOrdered () = 
    // If the caller specified whether the series is ordered, or custom comparer, use that;
    // Otherwise, we do check if it is comparable or nullable (for other types
    // comparer.Compare fails).
    match ordered with
    | Some ord -> ord
    | _ when 
          typeof<IComparable>.IsAssignableFrom(typeof<'K>) ||
          typeof<IComparable<'K>>.IsAssignableFrom(typeof<'K>) -> 
            // This can still fail :-( for example, if the type is a tuple
            // with incomparable values. So, let's just have a fallback returning false...
            try Seq.isSorted keys comparer
            with _ -> false
    | _ when
          typeof<'K>.IsGenericTypeDefinition && typeof<'K>.GetGenericTypeDefinition() = typedefof<Nullable<_>> ->
            try Seq.isSorted keys comparer 
            with _ -> false
    | _ -> false 

  let mutable ordered = Unchecked.defaultof<Nullable<bool>>
  let mutable lookup = null

  let ensureLookup () = 
    if lookup = null then
      let dict = Dictionary<'K, Address>()
      let mutable idx = 0L
      for k in keys do 
        match dict.TryGetValue(k) with
        | true, list -> 
            let info = sprintf "Duplicate key '%A'. Duplicate keys are not allowed in the index." k
            invalidArg "keys" info
        | _ -> 
            dict.[k] <- Address.ofInt64 idx  
            idx <- idx + 1L
      lookup <- dict

  member private x.lookupMap = 
    ensureLookup ()
    lookup

  member private x.isOrdered = 
    ensureLookup ()
    if not ordered.HasValue then ordered <- Nullable(isOrdered())
    ordered.Value

  /// Exposes keys array for use in the index builder
  member internal index.KeyCollection = keys

  /// Implements structural equality check against another index
  override index.Equals(another) = 
    match another with
    | null -> false
    | :? IIndex<'K> as another -> Seq.structuralEquals keys another.KeySequence
    | _ -> false

  /// Implement structural hashing against another index
  override index.GetHashCode() =
    keys |> Seq.structuralHash

  interface IIndex<'K> with
    member x.AddressingScheme = LinearAddressingScheme.Instance
    member x.AddressOperations = LinearAddressOperations(0L, int64 keys.Count - 1L) :> _
    member x.Keys = ensureLookup (); keys
    member x.KeySequence = ensureLookup (); keys :> _
    member x.KeyCount = ensureLookup (); int64 keys.Count
    member x.Builder = builder
    
    member x.AddressAt(idx) = Address.ofInt64 idx

    /// Perform reverse lookup and return key for an address
    member x.KeyAt(address) = keys.[Address.asInt address]

    /// Returns whether the specified index is empty
    member x.IsEmpty = keys |> Seq.isEmpty

    /// Returns the range of keys - makes sense only for ordered index
    member x.KeyRange = 
      if not x.isOrdered then invalidOp "KeyRange is not supported for unordered index."
      keys.[0], keys.[keys.Count - 1]

    /// Get the address for the specified key, low on ceremony
    member x.Locate(key) =
       match x.lookupMap.TryGetValue(key) with
       | true, res -> res
       | _         -> Address.invalid

    /// Get the address for the specified key.
    /// The 'semantics' specifies fancy lookup methods.
    member x.Lookup(key, semantics, check) = 
      match x.lookupMap.TryGetValue(key), semantics with

      // First, handle the case when the value exists in the index (return it only if 
      // 'check' returns 'true' and the caller wants Exact, ExactOrSmaller or ExactOrGreater)

      // When the value exists directly and the user requires exact match, we 
      // just return it (ignoring the fact that Vector value may be missing)
      | (true, res), Lookup.Exact -> OptionalValue((key, res))
      // If the caller wants exact (or greater/smaller), then check if the directly
      // found address is OK (e.g. if there is an associated value in the vector)
      | (true, res), (Lookup.ExactOrGreater | Lookup.ExactOrSmaller) when check res -> 
          OptionalValue((key, res))

      // Otherwise continue we need to use binary search to find the right value.
      
      // 
      // Find the index & generate all previous indices so that we can 'check' them
      | _, (Lookup.Smaller | Lookup.ExactOrSmaller) when x.isOrdered ->
          let inclusive = semantics = Lookup.ExactOrSmaller
          let addrOpt = Array.binarySearchNearestSmaller key comparer inclusive keys
          let indices = addrOpt |> Option.map (fun v -> seq { v .. -1 .. 0 })
          let indices = defaultArg indices Seq.empty
          indices 
          |> Seq.filter (Address.ofInt >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption
          |> OptionalValue.map (fun idx -> keys.[idx], Address.ofInt idx)

      // Find the index & generate all next indices so that we can 'check' them
      | _, (Lookup.Greater | Lookup.ExactOrGreater) when x.isOrdered ->
          let inclusive = semantics = Lookup.ExactOrGreater
          let addrOpt = Array.binarySearchNearestGreater key comparer inclusive keys
          let indices = addrOpt |> Option.map (fun v -> Seq.range v (keys.Count - 1) )
          let indices = defaultArg indices Seq.empty
          indices 
          |> Seq.filter (Address.ofInt >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption
          |> OptionalValue.map (fun idx -> keys.[idx], Address.ofInt idx)

      // If we did not find the key (or when we're unsorted & user wants fancy semantics), fail
      | _ -> OptionalValue.Missing

    /// Returns all mappings of the index (key -> address) 
    member x.Mappings = 
      ensureLookup ()
      keys |> Seq.mapi (fun i k -> KeyValuePair(k, Address.ofInt i))
    /// Are the keys of the index ordered?
    member x.IsOrdered = x.isOrdered
    member x.Comparer = comparer

// --------------------------------------------------------------------------------------
// A lazy index that represents subrange of some other index 
// (optimization for windowing and chunking functions)
// --------------------------------------------------------------------------------------

/// A virtual index that represents a subrange of a specified index. This is useful for
/// windowing operations where we do not want to allocate a new index for each window. 
/// This index can be cheaply constructed and it implements many of the standard functions
/// without actually allocating the index (e.g. KeyCount, KeyAt, IsEmpty). For more 
/// complex index manipulations (including lookup), an actual index is constructed lazily.
type LinearRangeIndex<'K when 'K : equality> 
  internal (index:IIndex<'K>, startAddress:int64, endAddress:int64) =

  /// Actual index that is created only when needed
  let initialize () =
    let actualKeys = Array.init (int (endAddress - startAddress + 1L)) (fun i -> 
        index.Keys.[int startAddress + i]) |> ReadOnlyCollection.ofArray
    index.Builder.Create
      ( actualKeys, if index.IsOrdered then Some(true) else None) 

  let mutable actualIndex = None
  let getActualIndex() = 
    if actualIndex = None then actualIndex <- Some (initialize())
    actualIndex.Value
  
  // Equality and hashing delegates to the actual index
  override index.Equals(another) = getActualIndex().Equals(another) 
  override index.GetHashCode() = getActualIndex().GetHashCode()

  interface IIndex<'K> with
    // Operations that can be implemented without evaluating the index
    member x.AddressingScheme = LinearAddressingScheme.Instance
    member x.AddressOperations = LinearAddressOperations(0L, endAddress - startAddress) :> _
    member x.AddressAt(idx) = Address.ofInt64 idx
    member x.KeyCount = endAddress - startAddress + 1L
    member x.KeyAt(address) = index.KeyAt(Address.ofInt64 (startAddress + (Address.asInt64 address)))
    member x.IsEmpty = endAddress < startAddress
    member x.Builder = index.Builder
    member x.Comparer = index.Comparer

    // KeyRange and IsOrdered are easy if we know that the range is sorted
    member x.KeyRange = 
      if not index.IsOrdered then getActualIndex().KeyRange
      else index.KeyAt(Address.ofInt64 startAddress), index.KeyAt(Address.ofInt64 endAddress)
    member x.IsOrdered = index.IsOrdered || getActualIndex().IsOrdered

    // The rest of the functionality is delegated to 'actualIndex'
    member x.Keys = getActualIndex().Keys
    member x.KeySequence = getActualIndex().KeySequence
    member x.Locate(key) = getActualIndex().Locate(key)
    member x.Lookup(key, semantics, check) = getActualIndex().Lookup(key, semantics, check)
    member x.Mappings = getActualIndex().Mappings

// --------------------------------------------------------------------------------------
// Linear index builder - provides operations for indices (like unioning, 
// intersection, appending and reindexing)
// --------------------------------------------------------------------------------------

/// Index builder object that is associated with `LinearIndex<K>` type. The builder
/// provides operations for manipulating linear indices (and the associated vectors).
type LinearIndexBuilder(vectorBuilder:Vectors.IVectorBuilder) =

  /// Add the <address> unit annotation to a relocations array, without doing any hard work
  let unsafeRelocsToAddr (relocs:(int64 * int64)[]) : ((int64<address> * int64<address>)[]) = unbox relocs

  /// Given the result of one of the 'Seq.align[All][Un]Ordered' functions,
  /// build new index and 'Vectors.Relocate' commands for each vector
  let makeSeriesConstructions (keys:'K[], relocations:list<(int64 * int64)[]>) vectors ordered : (IIndex<_> * list<_>) = 
    let newIndex = LinearIndex<_>(ReadOnlyCollection.ofArray keys, LinearIndexBuilder.Instance, ?ordered=ordered)
    let newVectors = (vectors, relocations) ||> List.mapi2 (fun i vec reloc ->
      Vectors.Relocate(vec, int64 keys.Length, unsafeRelocsToAddr reloc))
    ( upcast newIndex, newVectors )

  /// Calls 'makeSeriesConstructions' on two vectors 
  /// (assumes that 'spec' contains two relocation tables)
  let makeTwoSeriesConstructions spec v1 v2 ordered = 
    match makeSeriesConstructions spec [v1; v2] ordered with
    | index, [ v1; v2 ] -> index, v1, v2
    | _ -> failwith "makeTwoSeriesConstructions: Expected two vectors"

  /// Convert any index to a linear index (and relocate vector accordingly)
  let asLinearIndex (index:IIndex<_>) vector =
    match index with
    | :? LinearIndex<_> as lin -> lin, vector
    | _ ->
      let relocs = index.Mappings |> Seq.mapi (fun i (KeyValue(k, a)) -> Address.ofInt i, a)
      let newVector = Vectors.Relocate(vector, index.KeyCount, relocs)
      LinearIndex(index.Mappings |> Seq.map (fun kvp -> kvp.Key) |> ReadOnlyCollection.ofSeq, LinearIndexBuilder.Instance), newVector

  /// Instance of the index builder (specialized to Int32 addresses)
  static let indexBuilder = LinearIndexBuilder(VectorBuilder.Instance)
  /// Provides a global access to an instance of LinearIndexBuilder
  static member Instance = indexBuilder :> IIndexBuilder

  interface IIndexBuilder with
    /// Linear index is always fully evaluated - just return it
    member builder.Project(index) = index

    /// Create an index from another index - if the other index is not LinearIndex, 
    /// it is fully evaluated and we re-create a LinearIndex from its keys.
    member builder.Recreate(index:IIndex<'K>) = 
      match index with 
      | :? LinearIndex<'K> -> index
      | otherIndex -> LinearIndexBuilder.Instance.Create(otherIndex.Keys, Some otherIndex.IsOrdered)

    /// Linear index is always fully evaluated - just return it asynchronously
    member builder.AsyncMaterialize( (index, vector) ) = async.Return(index), vector
    
    /// Create an index from the specified data
    member builder.Create<'K when 'K : equality>(keys:seq<_>, ordered) = 
      LinearIndex<'K>(ReadOnlyCollection.ofSeq keys, builder, ?ordered=ordered) :> IIndex<_>

    /// Create an index from the specified data
    member builder.Create<'K when 'K : equality>(keys:ReadOnlyCollection<_>, ordered) = 
      LinearIndex<'K>(keys, builder, ?ordered=ordered) :> IIndex<_>

    /// Aggregate ordered index
    member builder.Aggregate<'K, 'TNewKey, 'R when 'K : equality and 'TNewKey : equality>
        (index:IIndex<'K>, aggregation, vector, selector:_ * _ -> 'TNewKey * OptionalValue<'R>) =
      if not index.IsOrdered then 
        invalidOp "Floating window aggregation and chunking is only supported on ordered indices. Consider sorting the series before calling the operation."
      let builder = (builder :> IIndexBuilder)
      
      // Calculate locatons (addresses) of the chunks or windows
      // For ChunkSize and WindowSize, this can be done faster, because we can just calculate
      // the locations of chunks/windows based on the number of the keys. For chunking/windowing
      // based on conditions, we actually need to iterate over all the keys...
      let locations = 
        match aggregation with 
        | ChunkSize(size, boundary) -> Seq.chunkRangesWithBounds (int64 size) boundary index.KeyCount
        | WindowSize(size, boundary) -> Seq.windowRangesWithBounds (int64 size) boundary index.KeyCount
        | ChunkWhile cond -> Seq.chunkRangesWhile cond index.Keys |> Seq.map (fun (s, e) -> DataSegmentKind.Complete, s, e)
        | WindowWhile cond -> Seq.windowRangesWhile cond index.Keys |> Seq.map (fun (s, e) -> DataSegmentKind.Complete, s, e)

      // Turn each location into vector construction using LinearRangeIndex
      let vectorConstructions =
        locations |> Seq.map (fun (kind, lo, hi) ->
          let cmd = Vectors.GetRange(vector, RangeRestriction.Fixed(Address.ofInt64 lo, Address.ofInt64 hi)) 
          let index = LinearRangeIndex(index, lo, hi)
          kind, (index :> IIndex<_>, cmd) )

      // Run the specified selector function
      let keyValuePairs = vectorConstructions |> Seq.map selector |> Array.ofSeq
      // Build & return the resulting series
      let newIndex = builder.Create(ReadOnlyCollection.ofArray (Array.map fst keyValuePairs), None)
      let vect = vectorBuilder.CreateMissing(Array.map snd keyValuePairs)
      newIndex, vect


    member builder.GroupBy<'K, 'TNewKey when 'K : equality and 'TNewKey : equality>
        (index:IIndex<'K>,  keySel:'K -> OptionalValue<'TNewKey>, vector) =
      let builder = (builder :> IIndexBuilder)
      // Build a sequence of indices & vector constructions representing the groups
      let windows = index.Keys |> Seq.groupBy keySel |> Seq.choose (fun (k, v) -> 
        if k.HasValue then Some(k.Value, v) else None)
      windows 
      |> Seq.map (fun (key, win) ->
        let len = Seq.length win |> int64
        let relocations = 
            seq { for k, newAddr in Seq.zip win (Seq.range 0L (len-1L) ) -> 
                  Address.ofInt64 newAddr, index.Locate(k) }
        let newIndex = builder.Create(win, None)
        key, (newIndex, Vectors.Relocate(vector, len, relocations)))
      |> ReadOnlyCollection.ofSeq

    /// Create chunks based on the specified key sequence
    member builder.Resample<'K, 'TNewKey, 'R when 'K : equality and 'TNewKey : equality> 
        (chunkBuilder, index:IIndex<'K>, keys:seq<'K>, dir:Direction, vector, selector:_ * _ -> 'TNewKey * OptionalValue<'R>) =

      if not index.IsOrdered then 
        invalidOp "Resampling is only supported on ordered indices"
      let builder = (builder :> IIndexBuilder)

      // Get a sequence of 'K * (Address * Address) values that represent
      // the blocks associated with individual keys 'K from the input
      let locations = 
        if dir = Direction.Forward then
          // In the "Forward" direction, the specified key should be the first key of the chunk
          // (if the key is exactly present in the index). 

          // Lookup all keys. Find nearest greater if the key is not present.
          // At the end, we get missing value (when the key is greater), so we pad it with KeyCount
          let keyLocations = keys |> Seq.map (fun k ->  
            let addr = index.Lookup(k, Lookup.ExactOrGreater, fun _ -> true)
            k, if addr.HasValue then snd addr.Value else Address.invalid )

          // To make sure we produce the last chunk, append one pair at the end
          Seq.append keyLocations [Unchecked.defaultof<_>, Address.invalid]
          |> Seq.pairwise 
          |> Seq.mapi (fun i ((k, prev), (_, next)) -> 
              // The next offset always starts *after* the end, so - 1L
              // Expand the first chunk to start at position 0L
              k, ( ( if i = 0 then index.AddressOperations.FirstElement else prev ), 
                   ( if next = Address.invalid then index.AddressOperations.LastElement 
                     else index.AddressOperations.AdjustBy(next, -1L) ) ))
        else
          // In the "Backward" direction, the specified key should be the last key of the chunk
          let keyLen = Seq.length keys

          // Lookup all keys. Find nearest smaller if the key is not present.
          // At the beginning, we get missing value (when the key is smaller), so we pad it with 0L
          let keyLocations =  keys |> Seq.map (fun k ->
            let addr = index.Lookup(k, Lookup.ExactOrSmaller, fun _ -> true)
            k, if addr.HasValue then snd addr.Value else Address.invalid ) 
          
          // To make sure we produce the first chunk, append one pair at the beginning
          Seq.append [Unchecked.defaultof<_>, Address.invalid] keyLocations
          |> Seq.pairwise
          |> Seq.mapi (fun i ((_, prev), (k, next)) ->
              // The previous offset always starts *before* the start, so + 1L
              // Expand the last chunk to start at the position KeyCount-1L
              k, ( ( if prev = Address.invalid then index.AddressOperations.FirstElement
                     else index.AddressOperations.AdjustBy(prev, +1L) ), 
                   ( if i = keyLen - 1 then index.AddressOperations.LastElement else next)))

      // Turn each location into vector construction using LinearRangeIndex
      // (NOTE: This is the same code as in the 'Aggregate' method!)
      let vectorConstructions =
        locations |> Array.ofSeq |> Array.map (fun (k, (lo, hi)) ->
          if lo = Address.invalid || hi = Address.invalid then 
            k, (LinearIndexBuilder.Instance.Create([], None) :> IIndex<_>, Vectors.Empty(0L))
          else 
            let index, cmd = chunkBuilder.GetAddressRange( (index, Vectors.Return 0), RangeRestriction.Fixed(lo, hi))
            k, (index :> IIndex<_>, cmd) )

      // Run the specified selector function
      let keyValuePairs = vectorConstructions |> Array.map selector
      // Build & return the resulting series
      let newIndex = builder.Create(Seq.map fst keyValuePairs, None)
      let vect = vectorBuilder.CreateMissing(Array.map snd keyValuePairs)
      newIndex, vect
      

    /// Order index and build vector transformation 
    member builder.OrderIndex( (index, vector) ) =
      let keys = Array.ofSeq index.Keys
      Array.sortInPlaceWith (fun a b -> index.Comparer.Compare(a, b)) keys
      let newIndex = LinearIndex(ReadOnlyCollection.ofArray keys, builder, true) :> IIndex<_>
      let relocations = 
        seq { for KeyValue(key, oldAddress) in index.Mappings ->
                let newAddress = newIndex.Lookup(key, Lookup.Exact, fun _ -> true) 
                if not newAddress.HasValue then failwith "OrderIndex: key not found in the new index"
                snd newAddress.Value, oldAddress }
      newIndex, Vectors.Relocate(vector, newIndex.KeyCount, relocations)

    /// Shift the values in the series by a specified offset, in a specified direction
    member builder.Shift( (index, vector), offset) =
      let (indexLo, indexHi), vectorRange = 
        if offset > 0 then 
          // If offset > 0 then skip first offet keys and take matching values from the start
          (int64 offset, index.KeyCount - 1L),
          RangeRestriction.Fixed(Address.ofInt64(0L), Address.ofInt64(index.KeyCount - 1L - int64 offset))
        else 
          // If offset < 0 then skip first -offset values and take matching keys from the start
          (0L, index.KeyCount - 1L + int64 offset),
          RangeRestriction.Fixed(Address.ofInt64(int64 -offset), Address.ofInt64(index.KeyCount - 1L))

      // If the shifted start/end is out of range of the index, return empty index & vector
      if indexLo > indexHi then 
        let newIndex = LinearIndex(ReadOnlyCollection.empty, indexBuilder, true) :> IIndex<_>
        newIndex, Vectors.Empty(0L)
      else
        let orderedOpt = if index.IsOrdered then Some(true) else None
        let newIndex = 
          if index.AddressingScheme = LinearAddressingScheme.Instance 
          then LinearRangeIndex(index, indexLo, indexHi) :> IIndex<_>
          else LinearIndex(index.Keys.[int indexLo .. int indexHi], LinearIndexBuilder.Instance, index.IsOrdered) :> _
        newIndex, Vectors.GetRange(vector, vectorRange)

    /// Union the index with another. For sorted indices, this needs to align the keys;
    /// for unordered, it appends new ones to the end.
    member builder.Union<'K when 'K : equality >
        ( (index1:IIndex<'K>, vector1), (index2, vector2) )= 
      let keysAndRelocs, ordered =
        if index1.IsOrdered && index2.IsOrdered then
          try Seq.alignOrdered index1.Keys index2.Keys index1.Comparer false, Some true
          with :? ComparisonFailedException ->
            Seq.alignUnordered index1.Keys index2.Keys false, None
        else
          Seq.alignUnordered index1.Keys index2.Keys false, Some false
      makeTwoSeriesConstructions keysAndRelocs vector1 vector2 ordered


    /// Intersect the index with another. This is the same as
    /// Union, but we filter & only return keys present in both sequences.
    member builder.Intersect<'K when 'K : equality >
        ( (index1:IIndex<'K>, vector1), (index2, vector2) ) = 
      let keysAndRelocs, ordered = 
        if index1.IsOrdered && index2.IsOrdered then
          try Seq.alignOrdered index1.Keys index2.Keys index1.Comparer true, Some true
          with :? ComparisonFailedException ->
            Seq.alignUnordered index1.Keys index2.Keys true, None
        else
          Seq.alignUnordered index1.Keys index2.Keys true, Some false
      makeTwoSeriesConstructions keysAndRelocs vector1 vector2 ordered


    /// Merge is similar to union, but it also combines the vectors using the specified
    /// vector transformation.
    member builder.Merge<'K when 'K : equality>(constructions:SeriesConstruction<'K> list, transform) = 
      let allOrdered = constructions |> List.forall (fun (index, _) -> index.IsOrdered)

      // Merge ordered sequences (assuming `allOrdered = true`)
      let mergeOrdered comparer =
        match constructions with
        | [li, lv; ri, rv] -> Seq.alignOrdered li.Keys ri.Keys comparer false, Some true
        | _ -> Seq.alignAllOrdered [| for i, _ in constructions -> i.Keys |] comparer, Some true
      // Merge unordered sequences (when `allOrdered = false` or when comparison fails)
      let mergeUnordered () =
        match constructions with
        | [li, lv; ri, rv] -> Seq.alignUnordered li.Keys ri.Keys false, Some false
        | _ -> Seq.alignAllUnordered [| for i, _ in constructions -> i.Keys |], Some false

      let keysAndRelocs, ordered = 
        if allOrdered then
          let comparer = (fst (constructions |> List.head)).Comparer
          try mergeOrdered comparer 
          with :? ComparisonFailedException -> mergeUnordered()
        else mergeUnordered()
      let vectors = constructions |> List.map snd
      let newIndex, vectors = makeSeriesConstructions keysAndRelocs vectors ordered
      newIndex, Vectors.Combine(lazy newIndex.KeyCount, vectors, transform)


    /// Build a new index by getting a key for each old key using the specified function
    member builder.WithIndex<'K, 'TNewKey when 'K : equality  and 'TNewKey : equality>
        (index1:IIndex<'K>, indexVector:IVector<'TNewKey>, vector) =
      let newKeys =
        [| for KeyValue(key, oldAddress) in index1.Mappings do
             let newKey = indexVector.GetValue oldAddress
             if newKey.HasValue then yield newKey.Value, oldAddress |]
      
      let newIndex = LinearIndex<'TNewKey>(Seq.map fst newKeys |> ReadOnlyCollection.ofSeq, builder)
      let len = (newIndex :> IIndex<_>).KeyCount
      let relocations = Seq.zip (Seq.range 0L (len-1L) |> Seq.map Address.ofInt64) (Seq.map snd newKeys)
      upcast newIndex, Vectors.Relocate(vector, int64 newKeys.Length, relocations)


    /// Reorder elements in the index to match with another index ordering
    member builder.Reindex(index1, index2, semantics, vector, condition) = 
      let relocations = 
        if semantics = Lookup.Exact then
            seq {  
              for KeyValue(key, newAddress) in index2.Mappings do
                let oldAddress = index1.Locate(key)
                if oldAddress <> Address.invalid && condition oldAddress then 
                  yield newAddress, oldAddress }
        else
            seq {  
              for KeyValue(key, newAddress) in index2.Mappings do
                let oldAddress = index1.Lookup(key, semantics, condition)
                if oldAddress.HasValue then 
                  yield newAddress, oldAddress.Value |> snd }
      Vectors.Relocate(vector, index2.KeyCount, relocations)


    /// Search the values of the specified vector & collect keys at the matching indices
    member builder.Search( (index, vector), searchVector, searchValue) = 
      let newKeys = ResizeArray<_>()
      let newIndices = ResizeArray<_>()
      for i in 0 .. (min index.Keys.Count (int searchVector.Length)) - 1 do
        let v = searchVector.GetValue(Address.ofInt i) 
        if v.HasValue && v.Value = searchValue then 
          newKeys.Add(index.Keys.[i])
          newIndices.Add(int64 i)
        
      let newIndex = LinearIndex<'TNewKey>(newKeys |> ReadOnlyCollection.ofSeq, builder)
      let range = newIndices |> Seq.map Address.ofInt64 |> RangeRestriction.ofSeq (int64 newIndices.Count)
      upcast newIndex, Vectors.GetRange(vector, range)


    // Hiearchical level lookup according to the specified search key
    member builder.LookupLevel( (index, vector), searchKey:ICustomLookup<'K> ) =
      let matching = 
        [| for KeyValue(key, addr) in index.Mappings do
             if searchKey.Matches(key) then yield addr, key |]
      let len = matching.Length |> int64
      let relocs = Seq.zip (Seq.range 0L (len-1L) |> Seq.map Address.ofInt64) (Seq.map fst matching)
      let newIndex = LinearIndex<_>(Seq.map snd matching |> ReadOnlyCollection.ofSeq, builder, index.IsOrdered)
      let newVector = Vectors.Relocate(vector, len, relocs)
      upcast newIndex, newVector


    /// Drop the specified item from the index
    member builder.DropItem<'K when 'K : equality >
        ( (index:IIndex<'K>, vector), key ) = 
      match index.Lookup(key, Lookup.Exact, fun _ -> true) with
      | OptionalValue.Present(addr) ->
          let newVector = Vectors.DropRange(vector, RangeRestriction.Fixed(snd addr, snd addr))
          let newKeys = index.Keys |> Seq.filter ((<>) key)
          let newIndex = LinearIndex<_>(newKeys |> ReadOnlyCollection.ofSeq, builder, index.IsOrdered)
          upcast newIndex, newVector
      | _ ->
          invalidArg "key" (sprintf "The key '%O' is not present in the index." key)

    /// Get a sub-range of the index keys based on address (rather than keys)
    member builder.GetAddressRange( (index, vector), range ) =
      let builder = (builder :> IIndexBuilder)
      match range.AsAbsolute(index.KeyCount) with
      | Choice1Of2(lo, hi) when hi < lo ->
          let newIndex = LinearIndex<_>(ReadOnlyCollection.empty, builder, ordered=true)
          upcast newIndex, Vectors.Empty(0L)
      | Choice1Of2(lo, hi) ->
          let newVector = Vectors.GetRange(vector, RangeRestriction.Fixed(lo, hi))
          let newIndex = LinearRangeIndex(index, Address.asInt64 lo, Address.asInt64 hi)
          upcast newIndex, newVector
      | Choice2Of2(indices) ->
          let newKeys = seq { for a in indices -> index.Keys.[Address.asInt a] }
          let newIndex = builder.Create(newKeys, None)
          let relocations = Seq.zip (Seq.map Address.ofInt64 (Seq.range 0L (newIndex.KeyCount-1L))) indices
          let newVector = Vectors.Relocate(vector, newIndex.KeyCount, relocations)
          newIndex, newVector

    /// Get a new index representing a sub-index of the current one
    /// (together with a transformation that should be applied to a vector)
    member builder.GetRange<'K when 'K : equality >((index:IIndex<'K>, vector), (lo, hi)) =

      // Default values are specified by the entire range
      let minAddr, maxAddr = Address.ofInt64 0L, Address.ofInt64 (index.KeyCount - 1L)

      let loBound = 
        match lo with
        | Some(key, beh) ->
            // Lookup the key or the nearest greater key that is available
            // (Use 'Greater' for exclusive and 'ExactOrGreater' for inclusive lookup)
            let sem = 
              if beh = BoundaryBehavior.Exclusive then Lookup.Greater
              else Lookup.ExactOrGreater        
            match index.Lookup(key, sem, fun _ -> true) with
            | OptionalValue.Present(_, addr) -> addr
            | OptionalValue.Missing -> Address.increment maxAddr
        | None -> minAddr

      let hiBound = 
        match hi with
        | Some(key, beh) -> 
            // Lookup the key or the nearest smaller key that is available
            // (Use 'Smaller' for exclusive and 'ExactOrSmaller' for inclusive lookup)
            let sem = 
              if beh = BoundaryBehavior.Exclusive then Lookup.Smaller
              else Lookup.ExactOrSmaller
            match index.Lookup(key, sem, fun _ -> true) with
            | OptionalValue.Present(_, addr) -> addr
            | OptionalValue.Missing -> Address.decrement minAddr
        | None -> maxAddr

      if hiBound < loBound then
        let newIndex = LinearIndex<_>(ReadOnlyCollection.ofArray [||], builder, true) :> IIndex<_>
        newIndex, Vectors.Empty(0L)
      else
        let newIndex = LinearRangeIndex(index, Address.asInt64 loBound, Address.asInt64 hiBound) :> IIndex<_>
        let newVector = Vectors.GetRange(vector, RangeRestriction.Fixed(loBound, hiBound))
        newIndex, newVector


// --------------------------------------------------------------------------------------
// Functions for creatin linear indices
// --------------------------------------------------------------------------------------

namespace Deedle 

open Deedle.Internal
open Deedle.Indices.Linear
open System.Collections.Generic
open System.Collections.ObjectModel

/// Defines non-generic `Index` type that provides functions for building indices
/// (hard-bound to `LinearIndexBuilder` type). In F#, the module is automatically opened
/// using `AutoOpen`. The methods are not designed for the use from C#.
///
/// [category:Vectors and indices]
[<AutoOpen>]
module ``F# Index extensions`` =
  open System

  /// Type that provides a simple access to creating indices represented
  /// using the built-in `LinearVector` type.
  type Index = 
    /// Create an index from a sequence of keys and check if they are sorted or not
    static member ofKeys<'T when 'T : equality>(keys:ReadOnlyCollection<'T>) =
      LinearIndexBuilder.Instance.Create<'T>(keys, None)

    /// Create an index from a sequence of keys and assume they are not sorted
    /// (the resulting index is also not sorted).
    static member ofUnorderedKeys<'T when 'T : equality>(keys:ReadOnlyCollection<'T>) = 
      LinearIndexBuilder.Instance.Create<'T>(keys, Some false) 

    /// Create an index from a sequence of keys and check if they are sorted or not
    static member ofKeys<'T when 'T : equality>(keys:'T[]) =
      Index.ofKeys(ReadOnlyCollection.ofArray keys)

    /// Create an index from a sequence of keys and assume they are not sorted
    /// (the resulting index is also not sorted).
    static member ofUnorderedKeys<'T when 'T : equality>(keys:'T[]) = 
      Index.ofUnorderedKeys(ReadOnlyCollection.ofArray keys)

    /// Create an index from a sequence of keys and check if they are sorted or not
    static member ofKeys<'T when 'T : equality>(keys:'T list) =
      Index.ofKeys(ReadOnlyCollection.ofSeq keys)

    /// Create an index from a sequence of keys and assume they are not sorted
    /// (the resulting index is also not sorted).
    static member ofUnorderedKeys<'T when 'T : equality>(keys:'T list) = 
      Index.ofUnorderedKeys(ReadOnlyCollection.ofSeq keys)

/// Type that provides access to creating indices (represented as `LinearIndex` values)
///
/// [category:Vectors and indices]
type Index =
  /// Create an index from a sequence of keys and check if they are sorted or not
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member Create<'T when 'T : equality>(keys:seq<'T>) =
    LinearIndexBuilder.Instance.Create<'T>(keys, None)

  /// Create an index from a sequence of keys and assume they are not sorted
  /// (the resulting index is also not sorted).
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member CreateUnordered<'T when 'T : equality>(keys:seq<'T>) = 
    LinearIndexBuilder.Instance.Create<'T>(keys, Some false)        
