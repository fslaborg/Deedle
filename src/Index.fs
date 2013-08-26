namespace FSharp.DataFrame.Indices

open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Vectors

// --------------------------------------------------------------------------------------
// Indexing - index provides access to data in vector via keys. Optionally, the keys
// can be sorted. The index provides various operations for joining indices (etc.) that
// produce a new index, together with transformations to be applied on the data vector.
// Index is an interface and so you can define your own. 
// --------------------------------------------------------------------------------------

/// An interface that represents index mapping keys of type 'TKey to locations
/// of address 'TAddress.
type IIndex<'TKey, 'TAddress> = 
  abstract Lookup : 'TKey -> OptionalValue<'TAddress>  
  abstract Mappings : seq<'TKey * 'TAddress>
  abstract Range : 'TAddress * 'TAddress
  
  abstract GetRange : 
    option<'TKey> * option<'TKey> * VectorConstruction<'TAddress> ->
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> 

  abstract UnionWith<'TValue> : 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress>

  abstract IntersectWith<'TValue> :
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress>

  abstract Append<'TValue> :
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> * IVectorValueTransform -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress>

  abstract Reindex<'TValue> :
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> -> VectorConstruction<'TAddress>

  abstract DropItem : 'TKey * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> 
  

// --------------------------------------------------------------------------------------
// A concrete implementation of an index. Represents an index where the values are sto-
// red in an array (or similar structure) with linearly ordered addresses without holes.
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame.Indices.Linear

open System.Linq
open System.Collections.Generic
open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices

/// To make the LinearIndex generic in the address type, this interface
/// is used to provide basic operations on the addresses (e.g. this can be 
/// implemented to use 'long' as an address)
type IAddressOperations<'TAddress> = 
  abstract GenerateRange : 'TAddress * 'TAddress -> seq<'TAddress>
  //abstract Zero : 'TAddress 
  abstract RangeOf : seq<'T> -> 'TAddress * 'TAddress
  abstract GetRange : seq<'T> * 'TAddress * 'TAddress -> seq<'T>

module AddressHelpers = 
  let getAddressOperations<'TAddress>() : IAddressOperations<'TAddress> = 
    if typeof<'TAddress> = typeof<int> then
      { new IAddressOperations<int> with
          member x.GenerateRange(lo, hi) : seq<int> = seq { lo .. hi } 
          //member x.Zero = 0
          member x.RangeOf(seq) = 0, (Seq.length seq) - 1 
          member x.GetRange(seq, lo, hi) = 
            if hi >= lo then seq |> Seq.skip lo |> Seq.take (hi - lo + 1) 
            else Seq.empty } |> unbox
    else failwith "getAddressOperations: Address type not supported."
  

/// An index that maps 'TKey to offsets 'TAddress. The keys cannot be duplicated.
/// If a comparer is provided, then the index preserves the ordering of elements
/// (and it assumes that 'keys' are already sorted).
type LinearIndex<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> 
    internal (keys:seq<'TKey>, ops:IAddressOperations<'TAddress>, sorted) =

  // Build a lookup table etc.
  let comparer = Comparer<'TKey>.Default
  let lookup = Dictionary<'TKey, 'TAddress>()
  let addresses = ops.GenerateRange(ops.RangeOf(keys))
  let mappings = Seq.zip keys addresses
  do for k, v in mappings do 
       match lookup.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> lookup.[k] <- v  

  // Given an index (and vector), create a linear index from the index
  // (and drop any clever indexing that may have been done by used vector)
  let asLinearIndex (index:IIndex<_, _>) (vector:Vectors.VectorConstruction<_>) = 
    if index :? LinearIndex<'TKey, 'TAddress> then index :?> LinearIndex<'TKey, 'TAddress>, vector
    else 
      let sorted = Seq.isSorted (Seq.map fst index.Mappings) Comparer<'TKey>.Default
      let vector = Vectors.ReturnAsOrdinal(vector)
      LinearIndex<_, _>(Seq.map fst index.Mappings, ops, sorted), vector

  /// Given the result of 'Seq.alignWithOrdering', create a new index
  /// and apply the transformations on two specified vector constructors
  let returnUsingAlignedSequence joined vector1 vector2 : (IIndex<_, _> * _ * _) = 
    // Create a new index using the sorted keys
    let newIndex = LinearIndex<_, _>(seq { for k, _, _ in joined -> k}, ops, true)
    let range = (newIndex :> IIndex<_, _>).Range

    // Create relocation transformations for both evectors
    let joinedWithIndex = Seq.zip (ops.GenerateRange range) joined
    let vect1Reloc = seq { for n, (_, o, _) in joinedWithIndex do if Option.isSome o then yield o.Value, n }
    let newVector1 = Vectors.Relocate(vector1, range, vect1Reloc)
    let vect2Reloc = seq { for n, (_, _, o) in joinedWithIndex do if Option.isSome o then yield o.Value, n }
    let newVector2 = Vectors.Relocate(vector2, range, vect2Reloc)

    // That's it! Return the result.
    ( upcast newIndex, newVector1, newVector2 )

  // Expose some internals for interface implementations...
  member private index.Mappings = mappings
  member private index.Sorted = sorted
  member private index.Keys = keys

  interface IIndex<'TKey, 'TAddress> with

    /// Get the address for the specified key
    member x.Lookup(key) = lookup.TryGetValue(key) |> OptionalValue.ofTuple
    /// Returns all mappings of the index (key -> address) 
    member x.Mappings = mappings
    /// Returns the range used by the index
    member x.Range = ops.RangeOf(keys)

    member index1.UnionWith(index2, vector1, vector2) = 
      let index2, vector2 = asLinearIndex index2 vector2
      let joined =
        if index1.Sorted && index2.Sorted then
          Seq.alignWithOrdering index1.Mappings index2.Mappings comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      returnUsingAlignedSequence joined vector1 vector2
        
    member index1.Append(index2, vector1, vector2, transform) = 
      let index2, vector2 = asLinearIndex index2 vector2
      let joined = 
        if index1.Sorted && index2.Sorted then
          Seq.alignWithOrdering index1.Mappings index2.Mappings comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let newIndex, vec1Cmd, vec2Cmd = returnUsingAlignedSequence joined vector1 vector2
      newIndex, Vectors.Combine(vec1Cmd, vec2Cmd, transform)

    /// Intersect the index with another. For sorted indices, this is the same as
    /// UnionWith, but we filter & only return keys present in both sequences.
    member index1.IntersectWith(index2, vector1, vector2) = 
      let index2, vector2 = asLinearIndex index2 vector2
      let joined = 
        if index1.Sorted && index2.Sorted then
          Seq.alignWithOrdering index1.Mappings index2.Mappings comparer |> Array.ofSeq 
        else
          Seq.unionWithOrdering index1.Mappings index2.Mappings |> Array.ofSeq 
      let joined = joined |> Seq.filter (function _, Some _, Some _ -> true | _ -> false)
      returnUsingAlignedSequence joined vector1 vector2

    member index1.Reindex(index2, vector) = 
      let relocations = seq {  
        for key, oldAddress in index1.Mappings do
          let newAddress = index2.Lookup(key)
          if newAddress.HasValue then 
            yield oldAddress, newAddress.Value }
      Vectors.Relocate(vector, index2.Range, relocations)

    member index.DropItem(key, vector) = 
      match lookup.TryGetValue(key) with
      | true, addr ->
          let newVector = Vectors.DropRange(vector, (addr, addr))
          let newIndex = LinearIndex<_, _>(Seq.filter ((<>) key) keys, ops, sorted)
          upcast newIndex, newVector
      | _ ->
          invalidArg "key" (sprintf "The key '%O' is not present in the index." key)


    /// Get a new index representing a sub-index of the current one
    /// (together with a transformation that should be applied to a vector)
    member x.GetRange(lo, hi, vector) =
      // Default values are specified by the entire range
      let defaults = lazy ops.RangeOf(keys)
      let getBound offs proj = 
        let (|Lookup|_|) x = 
          match lookup.TryGetValue(x) with true, v -> Some v | _ -> None
        match offs with 
        | None -> proj defaults.Value 
        | Some (Lookup i) -> i
        | _ -> invalidArg "lo,hi" "Keys of the range were not found in the index."

      // Create new index using the range & vector transformation
      let (lo, hi) as range = getBound lo fst, getBound hi snd
      let newKeys = ops.GetRange(keys, lo, hi)
      let newVector = Vectors.GetRange(vector, range)
      upcast LinearIndex<_, _>(newKeys, ops, sorted), newVector

(*

    member x.DropItem(key) = 
      upcast Index<_, _>(Seq.filter ((<>) key) keys, ops) 

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
*)      

// --------------------------------------------------------------------------------------
// ??
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame 

open System.Collections.Generic
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices.Linear

type Index = 
  static member Create<'T when 'T : equality>(keys:seq<'T>) =  // TODO: This could be optimized using static constraints
    // Check if the data is sorted
    let comparer = Comparer<'T>.Default
    let sorted = Seq.isSorted keys comparer
    LinearIndex<'T, int>(keys, AddressHelpers.getAddressOperations<int>(), sorted)

  static member CreateUnsorted<'T when 'T : equality>(keys:seq<'T>) = 
    LinearIndex<'T, int>(keys, AddressHelpers.getAddressOperations<int>(), false)
        