namespace FSharp.DataFrame.Indices
open FSharp.DataFrame.Common

// --------------------------------------------------------------------------------------
// Indexing - index provides access to data in vector via keys. Optionally, the keys
// can be sorted. The index provides various operations for joining indices (etc.) that
// produce a new index, together with transformations to be applied on the data vector.
// Index is an interface and so you can define your own. 
// --------------------------------------------------------------------------------------

/// An interface that represents index mapping keys of type 'TKey to locations
/// of address 'TAddress.
type IIndex<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> = 
  abstract Lookup : 'TKey -> OptionalValue<'TAddress>  
  abstract Mappings : seq<'TKey * 'TAddress>

/// A "mini-DSL" that describes the construction of an index. An index can be constructed
/// from a sequence of keys (sorted or unsorted), from an existing index or by various
/// operations - such as union and intersection.
type IndexConstruction<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> =
  | CreateSorted of seq<'TKey>
  | CreateUnsorted of seq<'TKey>
  | Return of IIndex<'TKey, 'TAddress>
  | Union of IndexConstruction<'TKey, 'TAddress> * IndexConstruction<'TKey, 'TAddress>

  //abstract Range : 'TAddress * 'TAddress
  //abstract Append : IIndex<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>
  //abstract DropItem : 'TKey -> IIndex<'TKey, 'TAddress>
  //abstract GetRange : option<'TKey> * option<'TKey> -> IIndex<'TKey, 'TAddress>

/// Represents an object that can construct vector values by processing 
// the "mini-DSL" representation `VectorConstruction<'TAddress, 'TValue>`
type IIndexBuilder = 
  abstract Build : IndexConstruction<'TKey, 'TAddress> -> IIndex<'TKey, 'TAddress>

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
  //abstract GetRange : seq<'T> * 'TAddress * 'TAddress -> seq<'T>

module AddressHelpers = 
  let getAddressOperations<'TAddress>() : IAddressOperations<'TAddress> = 
    if typeof<'TAddress> = typeof<int> then
      { new IAddressOperations<int> with
          member x.GenerateRange(lo, hi) : seq<int> = seq { lo .. hi } 
          //member x.Zero = 0
          member x.RangeOf(seq) = 0, (Seq.length seq) - 1 
          //member x.GetRange(seq, lo, hi) = 
          //  if hi >= lo then seq |> Seq.skip lo |> Seq.take (hi - lo + 1) 
          //  else Seq.empty 
          } |> unbox
    else failwith "getAddressOperations: Address type not supported."


/// Implements a builder object (`IIndexBuilder`) for creating
/// linear indices of type `LinearIndex<'TKey, 'TAddress>`. This includes 
/// operations such as unioning, creation from arrays of keys etc.
type LinearIndexBuilder() = 
  
  /// Instance of the index builder
  static let linearIndexBuilder = LinearIndexBuilder() :> IIndexBuilder

  /// Builds an index using the specified commands.
  let rec buildIndex (commands:IndexConstruction<_, _>) : IIndex<_, _> = 
    linearIndexBuilder.Build(commands) 

  /// Provides a global access to an instance of LinearIndexBuilder
  static member Instance = linearIndexBuilder

  interface IIndexBuilder with
    member builder.Build<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality>(cmd) = 
      match cmd with
      | Return index -> index
      | CreateUnsorted keys -> 
          let addr = AddressHelpers.getAddressOperations<'TAddress>()
          upcast LinearIndex(keys, addr)

      | CreateSorted keys -> 
          // Check if the data is sorted
          let comparer = Comparer<'TKey>.Default
          let rec isSorted past (en:IEnumerator<'TKey>) =
            if not (en.MoveNext()) then true
            elif comparer.Compare(past, en.Current) > 0 then false
            else isSorted en.Current en
          let en = keys.GetEnumerator()
          let sorted =
            if not (en.MoveNext()) then true
            else isSorted en.Current en

          let addr = AddressHelpers.getAddressOperations<'TAddress>()
          if not sorted then upcast LinearIndex(keys, addr)
          else upcast LinearIndex(keys, addr, comparer)
      
      | Union(first, second) ->
          let first, second = buildIndex first, buildIndex second
          failwith "union"

/// An index that maps 'TKey to offsets 'TAddress. The keys cannot be duplicated.
/// If a comparer is provided, then the index preserves the ordering of elements
/// (and it assumes that 'keys' are already sorted).
and LinearIndex<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> 
    internal (keys:seq<'TKey>, ops:IAddressOperations<'TAddress>, ?comparer:IComparer<'TKey>) =

  // Build a lookup table
  let lookup = Dictionary<'TKey, 'TAddress>()
  let addresses = ops.GenerateRange(ops.RangeOf(keys))
  let mappings = Seq.zip keys addresses
  do for k, v in mappings do 
       match lookup.TryGetValue(k) with
       | true, list -> invalidArg "keys" "Duplicate keys are not allowed in the index."
       | _ -> lookup.[k] <- v  

  interface IIndex<'TKey, 'TAddress> with
    /// Get the address for the specified key
    member x.Lookup(key) = lookup.TryGetValue(key) |> OptionalValue.ofTuple
    /// Returns all mappings of the index (key -> address) 
    member x.Mappings = mappings


(*
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
*)      

// --------------------------------------------------------------------------------------
// ??
// --------------------------------------------------------------------------------------

//module IndexHelpers = 
(*
  let inline reindex (oldIndex:IIndex<'TKey, 'TAddress>) (newIndex:IIndex<'TKey, 'TAddress>) (data:IVector<'TAddress>) =
    let relocations = seq {  
      for key, oldAddress in oldIndex.Elements do
        let newAddress = newIndex.Lookup(key)
        if newAddress.HasValue then 
          yield oldAddress, newAddress.Value }
    data.Reorder(newIndex.Range, relocations)
*)

type LinearIndex = 
  static member Create<'T when 'T : equality>(keys:seq<'T>) =  
    // TODO: This could be optimized using static constraints
    CreateSorted(keys) |> LinearIndexBuilder.Instance.Build
