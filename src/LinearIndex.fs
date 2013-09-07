// --------------------------------------------------------------------------------------
// A concrete implementation of an index. Represents an index where the values are sto-
// red in an array (or similar structure) with linearly ordered addresses without holes.
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame.Indices.Linear

open System.Linq
open System.Collections.Generic
open FSharp.DataFrame
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices


/// An index that maps 'TKey to offsets Address. The keys cannot be duplicated.
/// If a comparer is provided, then the index preserves the ordering of elements
/// (and it assumes that 'keys' are already sorted).
type LinearIndex<'TKey when 'TKey : equality> 
    internal (keys:seq<'TKey>, ops:IAddressOperations, ?ordered, ?comparer) =

  // Build a lookup table etc.
  let comparer = defaultArg comparer Comparer<'TKey>.Default
  let ordered = match ordered with Some ord -> ord | _ -> Seq.isSorted keys comparer

  let lookup = Dictionary<'TKey, Address>()
  let keysArray = lazy Array.ofSeq keys
  let addresses = ops.GenerateRange(ops.RangeOf(keys))
  let mappings = Seq.zip keys addresses
  do for k, v in mappings do 
       match lookup.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> lookup.[k] <- v  

  // Expose some internals for interface implementations...
  interface IIndex<'TKey> with
    member x.Keys = keys
    /// Get the address for the specified key
    member x.Lookup(key, semantics) = 
      match lookup.TryGetValue(key), semantics, ops.Int32Convertor with
      // When the value exists directly, it is a best match in all cases
      | (true, res), _, _ -> OptionalValue(res)

      // If we can convert array index to address, we can use binary search!
      | (false, _), Lookup.NearestSmaller, Some asAddr when ordered ->
          Array.binarySearchNearestSmaller key comparer keysArray.Value
          |> OptionalValue.ofOption
          |> OptionalValue.map asAddr
      | (false, _), Lookup.NearestGreater, Some asAddr when ordered ->
          Array.binarySearchNearestGreater key comparer keysArray.Value
          |> OptionalValue.ofOption
          |> OptionalValue.map asAddr

      // When we cannot convert array index to address, we have to use sequential search...
      //
      // Find the index of the first key that is greater than the one specified
      // (generate address range and find the address using 'skipWhile')
      | (false, _), Lookup.NearestGreater, None when ordered ->
          Seq.zip keysArray.Value (ops.GenerateRange(ops.RangeOf(keys)))
          |> Seq.skipWhile (fun (k, _) -> comparer.Compare(k, key) < 0) 
          |> Seq.map snd
          |> Seq.takeAtMost 1 |> List.ofSeq
          |> OptionalValue.ofSingletonList

      // Find the index of the last key before the specified one
      // (generate address range prefixed with None, find the first greater key
      // and then return the previous address from the prefixed sequence)
      | (false, _), Lookup.NearestSmaller, None when ordered ->
          let range = ops.RangeOf(keys)
          let shifted = Seq.append [None] (Seq.map Some (ops.GenerateRange(range)))
          let indexOpt =
            Seq.zip keysArray.Value shifted
            |> Seq.skipWhile (fun (k, _) -> comparer.Compare(k, key) < 0) 
            |> Seq.map snd
            |> Seq.takeAtMost 1 |> List.ofSeq
          match indexOpt with 
          | (Some it)::_ -> OptionalValue(it) // Found index - return it
          | [] -> OptionalValue(snd range)    // Skipped everything - return the last one
          | None::_ -> OptionalValue.Missing  // Found the first one - no nearest below

      // If we did not find the key (or when we're unsorted & user wants fancy semantics), fail
      | _ -> OptionalValue.Missing

    /// Returns all mappings of the index (key -> address) 
    member x.Mappings = mappings
    /// Returns the range used by the index
    member x.Range = ops.RangeOf(keys)
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
    let ops = AddressHelpers.getAddressOperations()
    let newIndex = LinearIndex<_>(seq { for k, _, _ in joined -> k}, ops, true)
    let range = (newIndex :> IIndex<_>).Range

    // Create relocation transformations for both vectors
    let joinedWithIndex = Seq.zip (ops.GenerateRange range) joined
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
    member builder.Create<'TKey when 'TKey : equality>(keys, ordered) = 
      let ops = AddressHelpers.getAddressOperations()
      upcast LinearIndex<'TKey>(keys, ops, ?ordered=ordered)

    member builder.Aggregate(index, aggregation, vector, valueSel, keySel) =
      let builder = (builder :> IIndexBuilder)
      let ranges =
        match aggregation with
        | WindowSize _ | ChunkSize _ | WindowWhile _ | ChunkWhile _ -> 
            if not index.Ordered then invalidOp "Floating window aggregation or chunking is not supported on un-ordered indices."
            let windows = 
              match aggregation with
              | WindowSize size -> Seq.windowed size index.Keys
              | ChunkSize size -> Seq.chunked size index.Keys
              | WindowWhile cond -> Seq.windowedWhile cond index.Keys
              | ChunkWhile cond -> Seq.chunkedWhile cond index.Keys
              | _ -> failwith "Aggregate: Pattern matching failed"
            windows |> Seq.map (fun win -> 
              builder.GetRange(index, Some win.[0], Some win.[win.Length - 1], vector))
        | GroupBy keySel -> 
            let windows = index.Keys |> Seq.groupBy keySel
            windows |> Seq.map (fun (_, win) ->
              let vectorParts = 
                win |> Seq.map (fun key -> 
                  builder.GetRange(index, Some key, Some key, vector)) 
              vectorParts |> Seq.reduce (fun (index, vector) (indexPart, vectorPart) ->
                builder.Append(index, indexPart, vector, vectorPart, Vectors.VectorValueTransform.LeftOrRight)) )

      let ranges = ranges |> Array.ofSeq          
      let keys = ranges |> Seq.map keySel
      let newIndex = builder.Create(keys, None)
      let vect = ranges |> Seq.map valueSel |> Array.ofSeq |> vectorBuilder.CreateOptional
      newIndex, vect

    member builder.OrderIndex(index, vector) =
      let ops = AddressHelpers.getAddressOperations()
      let keys = Array.ofSeq index.Keys
      Array.sortInPlaceWith (fun a b -> index.Comparer.Compare(a, b)) keys
      let newIndex = LinearIndex(keys, ops, true, index.Comparer) :> IIndex<_>
      let relocations = 
        seq { for key, oldAddress in index.Mappings ->
                let newAddress = newIndex.Lookup(key, Lookup.Exact) 
                if not newAddress.HasValue then failwith "OrderIndex: key not found in the new index"
                newAddress.Value, oldAddress }
      newIndex, Vectors.Relocate(vector, newIndex.Range, relocations)

    member builder.Union<'TKey when 'TKey : equality >
        (index1:IIndex<'TKey>, index2, vector1, vector2) = 
      let joined =
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      returnUsingAlignedSequence joined vector1 vector2
        
    member builder.Append<'TKey when 'TKey : equality >
        (index1:IIndex<'TKey>, index2, vector1, vector2, transform) = 
      let joined = 
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let newIndex, vec1Cmd, vec2Cmd = returnUsingAlignedSequence joined vector1 vector2
      newIndex, Vectors.Combine(vec1Cmd, vec2Cmd, transform)

    /// Intersect the index with another. For sorted indices, this is the same as
    /// UnionWith, but we filter & only return keys present in both sequences.
    member builder.Intersect<'TKey when 'TKey : equality >
        (index1:IIndex<'TKey>, index2, vector1, vector2) = 
      let joined = 
        if index1.Ordered && index2.Ordered then
          Seq.alignWithOrdering index1.Mappings index2.Mappings index1.Comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let joined = joined |> Seq.filter (function _, Some _, Some _ -> true | _ -> false)
      returnUsingAlignedSequence joined vector1 vector2

    member builder.WithIndex<'TKey, 'TNewKey when 'TKey : equality  and 'TNewKey : equality>
        (index1:IIndex<'TKey>, f:Address -> OptionalValue<'TNewKey>, vector) =
      let newKeys =
        [| for key, oldAddress in index1.Mappings do
             let newKey = f oldAddress
             if newKey.HasValue then yield newKey.Value, oldAddress |]
      
      let ops = AddressHelpers.getAddressOperations()
      let newIndex = LinearIndex<'TNewKey>(Seq.map fst newKeys, ops)
      let newRange = (newIndex :> IIndex<_>).Range
      let relocations = Seq.zip (ops.GenerateRange(newRange)) (Seq.map snd newKeys)
      upcast newIndex, Vectors.Relocate(vector, newRange, relocations)

    member builder.Reindex(index1, index2, semantics, vector) = 
      let relocations = seq {  
        for key, newAddress in index2.Mappings do
          let oldAddress = index1.Lookup(key, semantics)
          if oldAddress.HasValue then 
            yield newAddress, oldAddress.Value }
      Vectors.Relocate(vector, index2.Range, relocations)

    member builder.DropItem<'TKey when 'TKey : equality >
        (index:IIndex<'TKey>, key, vector) = 
      match index.Lookup(key, Lookup.Exact) with
      | OptionalValue.Present(addr) ->
          let ops = AddressHelpers.getAddressOperations()
          let newVector = Vectors.DropRange(vector, (addr, addr))
          let newKeys = index.Keys |> Seq.filter ((<>) key)
          let newIndex = LinearIndex<_>(newKeys, ops, index.Ordered)
          upcast newIndex, newVector
      | _ ->
          invalidArg "key" (sprintf "The key '%O' is not present in the index." key)


    /// Get a new index representing a sub-index of the current one
    /// (together with a transformation that should be applied to a vector)
    member builder.GetRange<'TKey when 'TKey : equality >
        (index:IIndex<'TKey>, lo, hi, vector) =
      // Default values are specified by the entire range
      let ops = AddressHelpers.getAddressOperations()
      let defaults = lazy ops.RangeOf(index.Keys)
      let getBound offs semantics proj = 
        let (|Lookup|_|) x = 
          match index.Lookup(x, semantics) with 
          | OptionalValue.Present(v) -> Some v | _ -> None
        match offs with 
        | None -> proj defaults.Value 
        | Some (Lookup i) -> i
        | _ -> invalidArg "lo,hi" "Keys of the range were not found in the index."

      // Create new index using the range & vector transformation
      let (lo, hi) as range = 
        getBound lo Lookup.NearestGreater fst, 
        getBound hi Lookup.NearestSmaller snd
      let newKeys = ops.GetRange(index.Keys, lo, hi) |> Array.ofSeq
      let newVector = Vectors.GetRange(vector, range)
      upcast LinearIndex<_>(newKeys, ops, index.Ordered), newVector

// --------------------------------------------------------------------------------------
// ??
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame 

open System.Collections.Generic
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices.Linear

type Index = 
  static member Create<'T when 'T : equality>(keys:seq<'T>) =
    LinearIndexBuilder.Instance.Create<'T>(keys, None)
  static member CreateUnsorted<'T when 'T : equality>(keys:seq<'T>) = 
    LinearIndexBuilder.Instance.Create<'T>(keys, Some false)        