// --------------------------------------------------------------------------------------
// A concrete implementation of an index. Represents an index where the values are sto-
// red in an array (or similar structure) with linearly ordered addresses without holes.
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame.Indices.Linear

open System.Linq
open System.Collections.Generic
open FSharp.DataFrame
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Indices


/// An index that maps 'K to offsets Address. The keys cannot be duplicated.
/// If a comparer is provided, then the index preserves the ordering of elements
/// (and it assumes that 'keys' are already sorted).
type LinearIndex<'K when 'K : equality> 
    internal (keys:seq<'K>, builder, ?ordered, ?comparer) =

  // Build a lookup table etc.
  let comparer = defaultArg comparer Comparer<'K>.Default
  let ordered = match ordered with Some ord -> ord | _ -> Seq.isSorted keys comparer

  // These are used for NearestSmaller/NearestGreater lookup and 
  // might not work for big data sets
  let keysArray = lazy Array.ofSeq keys
  let keysArrayRev = lazy (Array.ofSeq keys |> Array.rev)

  let lookup = Dictionary<'K, Address>()
  let addresses = Address.generateRange(Address.rangeOf(keys))
  let mappings = Seq.zip keys addresses
  do for k, v in mappings do 
       match lookup.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> lookup.[k] <- v  

  override index.Equals(another) = 
    match another with
    | null -> false
    | :? IIndex<'K> as another -> Seq.structuralEquals mappings another.Mappings
    | _ -> false

  override index.GetHashCode() =
    mappings |> Seq.structuralHash

  // Expose some internals for interface implementations...
  interface IIndex<'K> with
    member x.Keys = keys
    member x.Builder = builder
    member x.KeyRange = 
      if not ordered then invalidOp "KeyRange is not supported for unordered index."
      Seq.head keys, Seq.head keysArrayRev.Value

    /// Get the address for the specified key.
    /// The 'semantics' specifies fancy lookup methods.
    member x.Lookup(key, semantics, check) = 
      match lookup.TryGetValue(key), semantics, Address.int32Convertor with

      // When the value exists directly and the user requires exact match, we 
      // just return it (ignoring the fact that Vector value may be missing)
      | (true, res), Lookup.Exact, _ -> OptionalValue((key, res))
      // otherwise, only return it if there is associated value
      | (true, res), _, _ when check res -> OptionalValue((key, res))
      // if we find it, but 'check' does not like it & we're looking for exact, we return missing
      | (true, _), Lookup.Exact, _ -> OptionalValue.Missing

      // If we can convert array index to address, we can use binary search!
      // (Find the index & generate all previous/next indices so that we can 'check' them)
      | _, Lookup.NearestSmaller, Some asAddr when ordered ->
          let addrOpt = Array.binarySearchNearestSmaller key comparer keysArray.Value
          let indices = addrOpt |> Option.map (fun v -> seq { v .. -1 .. 0 })
          let indices = defaultArg indices Seq.empty
          indices 
          |> Seq.filter (asAddr >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption
          |> OptionalValue.map (fun idx -> keysArray.Value.[idx], asAddr idx)

      | _, Lookup.NearestGreater, Some asAddr when ordered ->
          let addrOpt = Array.binarySearchNearestGreater key comparer keysArray.Value
          let indices = addrOpt |> Option.map (fun v -> seq { v .. keysArray.Value.Length - 1 })
          let indices = defaultArg indices Seq.empty
          indices 
          |> Seq.filter (asAddr >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption
          |> OptionalValue.map (fun idx -> keysArray.Value.[idx], asAddr idx)

      // When we cannot convert array index to address, we have to use sequential search...
      //
      // Find the index of the first key that is greater than the one specified
      // (generate address range and find the address using 'skipWhile')
      | _, Lookup.NearestGreater, None when ordered ->
          Seq.zip keysArray.Value (Address.generateRange(Address.rangeOf(keys)))
          |> Seq.skipWhile (fun (k, _) -> comparer.Compare(k, key) < 0) 
          |> Seq.filter (snd >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption

      // Find the index of the last key before the specified one
      // (generate address range prefixed with None, find the first greater key
      // and then return the previous address from the prefixed sequence)
      | _, Lookup.NearestSmaller, None when ordered ->
          let lo, hi = Address.rangeOf(keys)
          Seq.zip keysArrayRev.Value (Address.generateRange(hi, lo))
          |> Seq.skipWhile (fun (k, _) -> comparer.Compare(k, key) > 0) 
          |> Seq.filter (snd >> check)
          |> Seq.headOrNone
          |> OptionalValue.ofOption

      // If we did not find the key (or when we're unsorted & user wants fancy semantics), fail
      | _ -> OptionalValue.Missing

    /// Returns all mappings of the index (key -> address) 
    member x.Mappings = mappings
    /// Returns the range used by the index
    member x.Range = Address.rangeOf(keys)
    /// Are the keys of the index ordered?
    member x.Ordered = ordered 
    member x.Comparer = comparer


// --------------------------------------------------------------------------------------
// Linear index builder - provides operations for indices (like unioning, 
// intersection, appending and reindexing)
// --------------------------------------------------------------------------------------

type LinearIndexBuilder(vectorBuilder:Vectors.IVectorBuilder) =

  /// Given the result of 'Seq.alignWithOrdering', create a new index
  /// and apply the transformations on two specified vector constructors
  let returnUsingAlignedSequence joined vector1 vector2 : (IIndex<_> * _ * _) = 
    // Create a new index using the sorted keys
    let newIndex = LinearIndex<_>(seq { for k, _, _ in joined -> k}, LinearIndexBuilder.Instance, true)
    let range = (newIndex :> IIndex<_>).Range

    // Create relocation transformations for both vectors
    let joinedWithIndex = Seq.zip (Address.generateRange range) joined
    let vect1Reloc = seq { for n, (_, o, _) in joinedWithIndex do if Option.isSome o then yield n, o.Value }
    let newVector1 = Vectors.Relocate(vector1, range, vect1Reloc)
    let vect2Reloc = seq { for n, (_, _, o) in joinedWithIndex do if Option.isSome o then yield n, o.Value }
    let newVector2 = Vectors.Relocate(vector2, range, vect2Reloc)

    // That's it! Return the result.
    ( upcast newIndex, newVector1, newVector2 )

  /// Instance of the index builder (specialized to Int32 addresses)
  static let indexBuilder = LinearIndexBuilder(Vectors.ArrayVector.ArrayVectorBuilder.Instance)
  /// Provides a global access to an instance of LinearIndexBuilder
  static member Instance = indexBuilder :> IIndexBuilder

  interface IIndexBuilder with
    member builder.Create<'K when 'K : equality>(keys, ordered) = 
      upcast LinearIndex<'K>(keys, builder, ?ordered=ordered)

    member builder.Aggregate<'K, 'R, 'TNewKey when 'K : equality and 'TNewKey : equality>
        (index:IIndex<'K>, aggregation, vector, valueSel:_ * _ * _ -> OptionalValue<'R>, keySel:_ * _ * _ -> 'TNewKey) =
      let builder = (builder :> IIndexBuilder)
      let ranges =
        if not index.Ordered then invalidOp "Floating window aggregation or chunking is not supported on un-ordered indices."
        let windows = 
          match aggregation with
          | WindowWhile cond -> Seq.windowedWhile cond index.Keys |> Seq.map (fun vs -> DataSegment(Complete, vs))
          | ChunkWhile cond -> Seq.chunkedWhile cond index.Keys |> Seq.map (fun vs -> DataSegment(Complete, vs))
          | WindowSize(size, bounds) -> Seq.windowedWithBounds size bounds index.Keys 
          | ChunkSize(size, bounds) -> Seq.chunkedWithBounds size bounds index.Keys
        windows |> Seq.map (fun win -> 
          let index, cmd = 
            builder.GetRange
              ( index, Some(win.Data.[0], BoundaryBehavior.Inclusive), 
                Some(win.Data.[win.Data.Length - 1], BoundaryBehavior.Inclusive), vector)
          win.Kind, index, cmd )

      let ranges = ranges |> Array.ofSeq          
      let keys = ranges |> Seq.map keySel
      let newIndex = builder.Create(keys, None)
      let vect = ranges |> Seq.map valueSel |> Array.ofSeq |> vectorBuilder.CreateMissing
      newIndex, vect

    member builder.GroupBy<'K, 'TNewKey, 'R when 'K : equality and 'TNewKey : equality>
        (index:IIndex<'K>, keySel:'K -> 'TNewKey, vector, valueSel:_ * _ * _ -> OptionalValue<'R>) =
      let builder = (builder :> IIndexBuilder)
      let ranges =
        let windows = index.Keys |> Seq.groupBy keySel
        windows |> Seq.map (fun (key, win) ->
          let relocations = 
            seq { for k, newAddr in Seq.zip win (Address.generateRange(Address.rangeOf(win))) -> 
                    newAddr, index.Lookup(k, Lookup.Exact, fun _ -> true).Value |> snd }
          let newIndex = builder.Create(win, None)
          key, newIndex, Vectors.Relocate(vector, Address.rangeOf(win), relocations))

      let ranges = ranges |> Array.ofSeq          
      let keys = ranges |> Seq.map (fun (k, idx, vec) -> k)
      let newIndex = builder.Create(keys, None)
      let vect = ranges |> Seq.map valueSel |> Array.ofSeq |> vectorBuilder.CreateMissing
      newIndex, vect

    member builder.OrderIndex(index, vector) =
      let keys = Array.ofSeq index.Keys
      Array.sortInPlaceWith (fun a b -> index.Comparer.Compare(a, b)) keys
      let newIndex = LinearIndex(keys, builder, true, index.Comparer) :> IIndex<_>
      let relocations = 
        seq { for key, oldAddress in index.Mappings ->
                let newAddress = newIndex.Lookup(key, Lookup.Exact, fun _ -> true) 
                if not newAddress.HasValue then failwith "OrderIndex: key not found in the new index"
                snd newAddress.Value, oldAddress }
      newIndex, Vectors.Relocate(vector, newIndex.Range, relocations)

    member builder.Union<'K when 'K : equality >
        (index1:IIndex<'K>, index2, vector1, vector2) = 
      let joined =
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.alignWithoutOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      returnUsingAlignedSequence joined vector1 vector2
        
    member builder.Append<'K when 'K : equality >
        (index1:IIndex<'K>, index2, vector1, vector2, transform) = 
      let joined = 
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.alignWithoutOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let newIndex, vec1Cmd, vec2Cmd = returnUsingAlignedSequence joined vector1 vector2
      newIndex, Vectors.Combine(vec1Cmd, vec2Cmd, transform)

    /// Intersect the index with another. For sorted indices, this is the same as
    /// UnionWith, but we filter & only return keys present in both sequences.
    member builder.Intersect<'K when 'K : equality >
        (index1:IIndex<'K>, index2, vector1, vector2) = 
      let joined = 
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.alignWithoutOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let joined = joined |> Seq.filter (function _, Some _, Some _ -> true | _ -> false)
      returnUsingAlignedSequence joined vector1 vector2

    member builder.WithIndex<'K, 'TNewKey when 'K : equality  and 'TNewKey : equality>
        (index1:IIndex<'K>, f:Address -> OptionalValue<'TNewKey>, vector) =
      let newKeys =
        [| for key, oldAddress in index1.Mappings do
             let newKey = f oldAddress
             if newKey.HasValue then yield newKey.Value, oldAddress |]
      
      let newIndex = LinearIndex<'TNewKey>(Seq.map fst newKeys, builder)
      let newRange = (newIndex :> IIndex<_>).Range
      let relocations = Seq.zip (Address.generateRange(newRange)) (Seq.map snd newKeys)
      upcast newIndex, Vectors.Relocate(vector, newRange, relocations)

    member builder.Reindex(index1, index2, semantics, vector) = 
      let relocations = seq {  
        for key, newAddress in index2.Mappings do
          let oldAddress = index1.Lookup(key, semantics, fun _ -> true)
          if oldAddress.HasValue then 
            yield newAddress, oldAddress.Value |> snd }
      Vectors.Relocate(vector, index2.Range, relocations)

    member builder.DropItem<'K when 'K : equality >
        (index:IIndex<'K>, key, vector) = 
      match index.Lookup(key, Lookup.Exact, fun _ -> true) with
      | OptionalValue.Present(addr) ->
          let newVector = Vectors.DropRange(vector, (snd addr, snd addr))
          let newKeys = index.Keys |> Seq.filter ((<>) key)
          let newIndex = LinearIndex<_>(newKeys, builder, index.Ordered)
          upcast newIndex, newVector
      | _ ->
          invalidArg "key" (sprintf "The key '%O' is not present in the index." key)


    /// Get a new index representing a sub-index of the current one
    /// (together with a transformation that should be applied to a vector)
    member builder.GetRange<'K when 'K : equality >
        (index:IIndex<'K>, lo, hi, vector) =
      // Default values are specified by the entire range
      let defaults = lazy Address.rangeOf(index.Keys)
      let getBound offs semantics proj = 
        let (|Lookup|_|) x = 
          match index.Lookup(x, semantics, fun _ -> true) with 
          | OptionalValue.Present(_, v) -> Some v | _ -> None
        match offs with 
        | None -> (proj defaults.Value), BoundaryBehavior.Inclusive
        | Some (Lookup i, bound) -> i, bound
        | _ -> invalidArg "lo,hi" "Keys of the range were not found in the index."

      // Create new index using the range & vector transformation
      let (lo, hi) = 
        getBound lo Lookup.NearestGreater fst, 
        getBound hi Lookup.NearestSmaller snd
      let lo = if snd lo = BoundaryBehavior.Exclusive then Address.increment (fst lo) else fst lo
      let hi = if snd hi = BoundaryBehavior.Exclusive then Address.decrement (fst hi) else fst hi
      let newKeys = Address.getRange(index.Keys, lo, hi) |> Array.ofSeq
      let newVector = Vectors.GetRange(vector, (lo, hi))
      upcast LinearIndex<_>(newKeys, builder, index.Ordered), newVector

// --------------------------------------------------------------------------------------
// ??
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame 

open System.Collections.Generic
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Indices.Linear

type Index = 
  static member Create<'T when 'T : equality>(keys:seq<'T>) =
    LinearIndexBuilder.Instance.Create<'T>(keys, None)
  static member CreateUnsorted<'T when 'T : equality>(keys:seq<'T>) = 
    LinearIndexBuilder.Instance.Create<'T>(keys, Some false)        