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
open Deedle.Addressing
open Deedle.Indices
open Deedle.Internal
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Vectors.Virtual
open System.Collections.Generic
open System.Collections.ObjectModel

// ------------------------------------------------------------------------------------------------
// Ranges - represents a sub-range as a list of pairs of indices
// ------------------------------------------------------------------------------------------------

/// Represents a sub-range of an ordinal index. The range can consist of 
/// multiple blocks, i.e. [ 0..9; 20..29 ]. The pairs represent indices
/// of first and last element (inclusively) and we also keep size so that
/// we do not have to recalculate it.
type Ranges = 
  { Ranges : (int64 * int64) list; Size : int64 }
  
  /// Create a range from a sequence of low-high pairs
  static member Create(ranges:seq<_>) =
    let ranges = List.ofSeq ranges 
    if ranges = [] then invalidArg "ranges" "Range cannot be empty"
    let size = ranges |> List.sumBy (fun (l, h) -> h - l + 1L)
    for l, h in ranges do if l > h then invalidArg "ranges" "Invalid range (first offset is greater than second)"
    { Ranges = List.ofSeq ranges; Size = size }

  /// Combine ranges - the arguments don't have to be sorted, but must not overlap
  static member Combine(ranges:seq<Ranges>) =
    if Seq.isEmpty ranges then invalidArg "ranges" "Range cannot be empty"
    let blocks = [ for r in ranges do yield! r.Ranges ] |> List.sortBy fst
    let rec loop (startl, endl) blocks = seq {
      match blocks with 
      | [] -> yield startl, endl
      | (s, e)::blocks when s <= endl -> invalidOp "Cannot combine overlapping ranges"
      | (s, e)::blocks when s = endl + 1L -> yield! loop (startl, e) blocks
      | (s, e)::blocks -> 
          yield startl, endl
          yield! loop (s, e) blocks }
    Ranges.Create (loop (List.head blocks) (List.tail blocks))

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Ranges = 
  /// Represents invalid address
  let invalid = -1L

  /// Returns the key at a given address. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   0->10, 1->11, 2->12, 3->30, 4->31, 5->32
  ///
  /// When the address is wrong, throws `IndexOutOfRangeException`
  let inline keyOfAddress addr { Ranges = ranges } =
    let rec loop sum idx = 
      if idx >= ranges.Length then raise <| IndexOutOfRangeException() else
      let l, h = ranges.[idx]
      if addr < sum + (h - l + 1L) then l + (addr - sum)
      else loop (sum + h - l + 1L) (idx + 1)
    loop 0L 0

  /// Returns the address of a given key. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   10->0, 11->1, 12->2, 30->3, 31->4, 32->5  
  ///
  /// When the key is wrong, returns `Address.Invalid`
  let inline addressOfKey key { Ranges = ranges } =
    let rec loop addr idx = 
      if idx >= ranges.Length then invalid else
      let l, h = ranges.[idx]
      if key < l then invalid
      elif key >= l && key <= h then addr + (key - l)
      else loop (addr + (h - l + 1L)) (idx + 1)
    loop 0L 0
      
  /// Returns the smallest & greatest overall key
  let inline keyRange { Ranges = ranges } = 
    fst ranges.[0], snd ranges.[ranges.Length-1]

  /// Returns an array containing all keys
  let inline keys { Ranges = ranges; Size = size } =
    let keys = Array.zeroCreate (int size)
    let mutable i = 0
    for l, h in ranges do
      for n in l .. h do 
        keys.[i] <- n
        i <- i + 1
    keys

  /// Implements a lookup using the specified semantics & check function.
  /// For `Exact` match, this is the same as `addressOfKey`. In other cases,
  /// we first find the place where the key *would* be and then scan in one
  /// or the other direction until 'check' returns 'true' or we find the end.
  let inline lookup key semantics check ({ Ranges = ranges; Size = size } as input) =
    if semantics = Lookup.Exact then
      // For exact lookup, we only check one address
      let addr = addressOfKey key input
      if addr <> invalid && check addr then
        OptionalValue( (key, addr) )
      else OptionalValue.Missing
    else
      // Otherwise, we scan next addresses in the required direction
      let step = 
        if semantics &&& Lookup.Greater = Lookup.Greater then (+) 1L
        elif semantics &&& Lookup.Smaller = Lookup.Smaller then (+) -1L
        else invalidArg "semantics" "Invalid lookup semantics"

      // Find start; if we do *not* want exact, then skip to the next
      let start =
        let rec loop addr idx = 
          if idx >= ranges.Length && semantics &&& Lookup.Greater = Lookup.Greater then invalid 
          elif idx >= ranges.Length (* semantics &&& Lookup.Smaller = Lookup.Greater *) then size - 1L else
          let l, h = ranges.[idx]
          if key < l && semantics &&& Lookup.Greater = Lookup.Greater then addr
          elif key < l && semantics &&& Lookup.Smaller = Lookup.Smaller then addr - 1L
          elif key >= l && key <= h then addr + (key - l)
          else loop (addr + (h - l + 1L)) (idx + 1)
        let addr = loop 0L 0
        // If we do not want exact, but we found exact match, skip to the next
        if addr <> invalid && 
          ( keyOfAddress addr input = key && 
            (semantics = Lookup.Greater || semantics = Lookup.Smaller) )
            then step addr else addr

      if start = invalid then OptionalValue.Missing else
      // Scan until 'check' returns true, or until we reach invalid address
      let rec scan step addr =
        if addr < 0L || addr >= size then OptionalValue.Missing
        elif check addr then OptionalValue( (keyOfAddress addr input, addr) )
        else scan step (step addr)
      scan step start
  
// ------------------------------------------------------------------------------------------------
// VirtualOrderedIndex - index where the keys are defined by the specified sorted source
// ------------------------------------------------------------------------------------------------

type VirtualOrderedIndex<'K when 'K : equality>(source:IVirtualVectorSource<'K>) =
  // TODO: Assert that the source is ordered
  // TODO: Another assumption here is that the source contains no NA values
  let keyAtAddr i = source.ValueAt(i).Value
  member x.Source = source

  interface IIndex<'K> with
    member x.KeyAt(addr) = keyAtAddr addr
    member x.AddressAt(offs) = source.AddressOperations.AddressOf offs
    member x.KeyCount = source.Length
    member x.IsEmpty = source.Length = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = 
      keyAtAddr source.AddressOperations.FirstElement,
      keyAtAddr source.AddressOperations.LastElement
    
    member x.Keys : ReadOnlyCollection<'K> = 
      source.AddressOperations.Range
      |> Seq.map keyAtAddr
      |> ReadOnlyCollection.ofSeq
    member x.Mappings = 
      source.AddressOperations.Range
      |> Seq.map (fun addr -> KeyValuePair(keyAtAddr addr, addr))
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

#if VIRTUAL_ORDINAL_INDEX
and VirtualOrdinalIndex(ranges:Ranges, addressOf:int64 -> Address, offsetOf:Address -> int64) =
  member x.Ranges = ranges
  member x.AddressOf = addressOf
  member x.OffsetOf = offsetOf
  interface IIndex<int64> with
    member x.KeyAt(addr) = 
      let addr = offsetOf addr
      if addr < 0L || addr >= ranges.Size then invalidArg "addr" "Out of range"
      else Ranges.keyOfAddress addr ranges
    member x.KeyCount = ranges.Size
    member x.IsEmpty = ranges.Size = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
    member x.KeyRange = Ranges.keyRange ranges
    member x.Keys = Ranges.keys ranges |> ReadOnlyCollection.ofArray
    member x.Mappings = 
      seq { for l, h in ranges.Ranges do
              for n in l .. h do 
                yield KeyValuePair(n, addressOf n) }
    member x.IsOrdered = true
    member x.Comparer = Comparer<int64>.Default
    member x.Locate(key) = addressing.AddressOf (Ranges.addressOfKey key ranges)
    member x.Lookup(key, semantics, check) = 
      Ranges.lookup key semantics (addressing.AddressOf >> check) ranges 
      |> OptionalValue.map (fun (key, addr) ->
          key, addressing.AddressOf addr)
#endif

// ------------------------------------------------------------------------------------------------
// VirtualIndexBuilder - implements operations on VirtualOrderedIndex and VirtualOrdinalIndex
// ------------------------------------------------------------------------------------------------

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
    
#if VIRTUAL_ORDINAL_INDEX
      let (|OrdinalSources|_|) : list<SeriesConstruction<_>> -> _ = 
        List.tryChooseBy (function
          | :? VirtualOrdinalIndex as idx, vec -> Some(idx.Ranges, vec)
          | _ -> None)
#endif

      match scs with
      | OrderedSources sources ->
          let newIndex = [ for s, _ in sources -> s ]
          let newIndex = (List.head newIndex).MergeWith(List.tail newIndex)
          let newVector = sources |> Seq.map snd |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          VirtualOrderedIndex(newIndex) :> _, newVector

#if VIRTUAL_ORDINAL_INDEX
      | OrdinalSources sources -> 
          let newRanges = Ranges.Combine (List.map fst sources)
          let newVector = sources |> Seq.map snd |> Seq.reduce (fun a b -> Vectors.Append(a, b))
          unbox (VirtualOrdinalIndex(newRanges, addressing)), newVector
#endif          
      | _ -> 
          for index, vector in scs do 
            match index with
            | :? VirtualOrderedIndex<'K>
#if VIRTUAL_ORDINAL_INDEX
            | :? VirtualOrdinalIndex -> failwith "TODO: Merge - ordered/ordinal index - avoid materialization!"
#endif
            | _ -> ()
          baseBuilder.Merge(scs, transform)


    member x.Search((index:IIndex<'K>, vector), searchVector:IVector<'V>, searchValue) = 
      let searchVector = 
        match searchVector with
        | :? IWrappedVector<'V> as wrapped -> wrapped.UnwrapVector()
        | _ -> searchVector

      match index, searchVector with
#if VIRTUAL_ORDINAL_INDEX
      | (:? VirtualOrdinalIndex as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(searchValue)
          let count =
            match mapping with
            | VectorRange.Custom(iv) -> iv.Count
            | VectorRange.Range(lo, hi) -> failwith "TODO: (Address.asInt64 hi) - (Address.asInt64 lo) + 1L"
          let newIndex = VirtualOrdinalIndex(Ranges.Create [ 0L, count-1L ])
          unbox<IIndex<'K>> newIndex, GetRange(vector, mapping)
#endif
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
#if VIRTUAL_ORDINAL_INDEX
      | (:? VirtualOrdinalIndex as index1), (:? VirtualOrdinalIndex as index2) when index1.Ranges = index2.Ranges -> 
          vector           
#endif
      | :? VirtualOrderedIndex<'K>, _ 
      | _, :? VirtualOrderedIndex<'K>
#if VIRTUAL_ORDINAL_INDEX
      | :? VirtualOrdinalIndex, _ 
      | _, :? VirtualOrdinalIndex 
#endif      
          ->
          failwith "TODO: Reindex - ordered/ordinal index - avoid materialization!"
      | _ -> baseBuilder.Reindex(index1, index2, semantics, vector, cond)

    member x.DropItem((index:IIndex<'K>, vector), key) = 
      match index with
      | :? VirtualOrderedIndex<'K>
#if VIRTUAL_ORDINAL_INDEX
      | :? VirtualOrdinalIndex -> failwith "TODO: DropItem - ordered/ordinal index - avoid materialization!"
#endif
      | _ -> baseBuilder.DropItem((index, vector), key)

    member x.Resample(index, keys, close, vect, selector) = failwith "Resample"

    member x.GetAddressRange<'K when 'K : equality>((index, vector), (lo, hi)) = 
      match index with
      | :? VirtualOrderedIndex<'K> as index ->
          // TODO: Range checks
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(Range(lo, hi)))
          let newVector = Vectors.GetRange(vector, Vectors.Range(lo, hi))
          newIndex :> IIndex<'K>, newVector

#if VIRTUAL_ORDINAL_INDEX
      | :? VirtualOrdinalIndex when hi < lo -> emptyConstruction()
      | :? VirtualOrdinalIndex as index ->
          // TODO: range checks
          let range = Vectors.Range(lo, hi)
          let newVector = Vectors.GetRange(vector, range)
          let keyLo, keyHi = (index :> IIndex<_>).KeyRange
          let newIndex = VirtualOrdinalIndex(Ranges.Create [ keyLo + (Address.asInt64 lo), keyLo + (Address.asInt64 hi) ]) 
          unbox<IIndex<'K>> newIndex, newVector
#endif
      | _ -> 
          baseBuilder.GetAddressRange((index, vector), (lo, hi))

    member x.Project(index:IIndex<'K>) = index

    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) = 
      match index with
#if VIRTUAL_ORDINAL_INDEX
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
#endif
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

          let loIdx = getRangeKey index.Source.AddressOperations.FirstElement Lookup.Greater optLo
          let hiIdx = getRangeKey index.Source.AddressOperations.LastElement Lookup.Smaller optHi

          // TODO: probably range checks
          let newVector = Vectors.GetRange(vector, Vectors.Range(loIdx, hiIdx))
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(Range(loIdx, hiIdx)))
          unbox<IIndex<'K>> newIndex, newVector

#if VIRTUAL_ORDINAL_INDEX
      | :? VirtualOrdinalIndex as ordIndex & (:? IIndex<int64> as index) -> 
          let getRangeKey proj next = function
            | None -> proj index.KeyRange 
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) -> next (unbox<int64> k)
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loIdx, hiIdx = loKey - (fst index.KeyRange), hiKey - (fst index.KeyRange)

          // TODO: range checks
          let range = Vectors.Range(Address.ofInt64 loIdx, Address.ofInt64 hiIdx)
          let newVector = Vectors.GetRange(vector, range)
          let newIndex = VirtualOrdinalIndex(Ranges.Create [ loKey, hiKey ]) 
          unbox<IIndex<'K>> newIndex, newVector
#endif
      | _ -> 
          baseBuilder.GetRange((index, vector), (optLo, optHi))
