namespace Deedle.Virtual

module IndexUtilsModule = 
  open Deedle
  open System

  /// Binary seachr in range [ 0L .. count ]. The function is generic in ^T and 
  /// is 'inline' so that the comparison on ^T is optimized.
  ///
  ///  - `count` specifies the upper bound for the binary search
  ///  - `valueAt` is a function that returns value ^T at the specified location
  ///  - `value` is the ^T value that we are looking for
  ///  - `lookup` is the lookup semantics as used in Deedle
  ///  - `check` is a function that tests whether we want a given location
  ///    (if no, we scan - this can be used to find the first available value in a series)
  ///
  let inline binarySearch count (valueAt:Func<int64, ^T>) value (lookup:Lookup) (check:Func<_, _>) = 
  
    /// Binary search the 'asOfTicks' series, looking for the 
    /// specified 'asOf' (the invariant is that: lo <= res < hi)
    /// The result is index 'idx' such that: 'asOfAt idx <= asOf && asOf (idx+1) > asOf'
    let rec binarySearch lo hi = 
      let mid = (lo + hi) / 2L
      if lo + 1L = hi then lo
      else
        if valueAt.Invoke mid > value then binarySearch lo mid
        else binarySearch mid hi

    /// Scan the series, looking for first value that passes 'check'
    let rec scan next idx = 
      if idx < 0L || idx >= count then OptionalValue.Missing
      elif check.Invoke idx then OptionalValue(idx)
      else scan next (next idx)

    if count = 0L then OptionalValue.Missing
    else
      let found = binarySearch 0L count
      match lookup with
      | Lookup.Exact -> 
          // We're looking for an exact value, if it's not the one at 'idx' then Nothing
          if valueAt.Invoke found = value && check.Invoke found then OptionalValue(found)
          else OptionalValue.Missing
      | Lookup.ExactOrGreater | Lookup.ExactOrSmaller when valueAt.Invoke found = value && check.Invoke found ->
          // We found an exact match and we the lookup behaviour permits that
          OptionalValue(found)
      | Lookup.Greater | Lookup.ExactOrGreater ->
          // Otherwise we need to scan (because the found value does not work or is not allowed)
          scan ((+) 1L) (if valueAt.Invoke found <= value then found + 1L else found)
      | Lookup.Smaller | Lookup.ExactOrSmaller ->
          scan ((-) 1L) (if valueAt.Invoke found >= value then found - 1L else found)
      | _ -> invalidArg "lookup" "Unexpected Lookup behaviour"

type IndexUtils =
  ///
  static member BinarySearch(count, valueAt, (value:int64), lookup, check) = 
    IndexUtilsModule.binarySearch count valueAt value lookup check

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

open System
open Deedle
open Deedle.Addressing
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Internal

type ISubVectorRange = interface end

type SubVectorRange =
  | Range of int64 * int64
  | Custom of ISubVectorRange

type IVirtualVectorSource =
  abstract ElementType : System.Type
  abstract Length : int64

type IVirtualVectorSource<'V> = 
  inherit IVirtualVectorSource
  abstract ValueAt : int64 -> OptionalValue<'V>
  abstract GetSubVector : SubVectorRange -> IVirtualVectorSource<'V>
  abstract LookupRange : 'V -> SubVectorRange
  abstract LookupValue : 'V * Lookup * Func<Addressing.Address, bool> -> OptionalValue<'V * Addressing.Address> 

type VirtualVector<'V>(source:IVirtualVectorSource<'V>) = 
  member vector.Source = source
  interface IVector with
    member val ElementType = typeof<'V>
    member vector.SuppressPrinting = false
    member vector.GetObject(index) = source.ValueAt(index) |> OptionalValue.map box
    member vector.ObjectSequence = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) |> OptionalValue.map box }
    member vector.Invoke(site) = site.Invoke<'V>(vector)
  interface IVector<'V> with
    member vector.GetValue(index) = source.ValueAt(index)
    member vector.Data = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) } |> VectorData.Sequence
    member vector.SelectMissing<'TNew>(rev, f:Address -> OptionalValue<'V> -> OptionalValue<'TNew>) = 
      
      let withReverseLookup op = 
        match rev with 
        | None -> failwith "Cannot lookup on virtual vector without reverse lookup"
        | Some g -> op g

      let rec mapSource (source:IVirtualVectorSource<'V>) = 
        { new IVirtualVectorSource<'TNew> with
            member x.ValueAt(idx) = f (Address.ofInt64 idx) (source.ValueAt(idx)) // TODO: Are we calculating the address correctly here??
            member x.LookupRange(v) = 
              // TODO: Not tested
              withReverseLookup (fun g -> source.LookupRange(g v))
            member x.LookupValue(v, l, c) = 
              withReverseLookup (fun g ->
                  source.LookupValue(g v, l, c)
                  |> OptionalValue.bind (fun (v, a) ->  // TODO: Make me nice and readable!
                      f a (OptionalValue v)
                      |> OptionalValue.map (fun v -> v, a))  )

            member x.GetSubVector(range) = mapSource (source.GetSubVector(range))
          interface IVirtualVectorSource with
            member x.ElementType = typeof<'TNew>
            member x.Length = source.Length }
      VirtualVector(mapSource source) :> _

    member vector.Select(f) = 
      (vector :> IVector<_>).SelectMissing(None, fun _ -> OptionalValue.map f)

type VirtualVectorBuilder() =
  let baseBuilder = VectorBuilder.Instance
  static let vectorBuilder = VirtualVectorBuilder() :> IVectorBuilder
  let build cmd args = vectorBuilder.Build(cmd, args)

  static member Instance = vectorBuilder

  interface IVectorBuilder with
    member builder.Create(values) = baseBuilder.Create(values)
    member builder.CreateMissing(optValues) = baseBuilder.CreateMissing(optValues)
    member builder.InitMissing<'T>(size, f) = 
      let isNa = MissingValues.isNA<'T> ()
      let rec createSource lo hi =
        { new IVirtualVectorSource<'T> with
            member x.ValueAt(idx) = 
              let v = f (lo + idx)
              if v.HasValue && not (isNa v.Value) then v
              else OptionalValue.Missing 
            member x.LookupValue(v, l, c) = failwith "cannot lookup on vector created by init"
            member x.LookupRange(_) = failwith "cannot lookup on vector created by init"
            member x.GetSubVector(sub) =
              match sub with 
              | Range(nlo, nhi) -> createSource (lo+nlo) (lo+nhi)
              | _ -> failwith "cannot get custom subrange of a vector created by init" // TODO: ?
          interface IVirtualVectorSource with
            member x.ElementType = typeof<'T>
            member x.Length = (hi-lo+1L) }
      VirtualVector(createSource 0L (size-1L)) :> _

    member builder.AsyncBuild<'T>(cmd, args) = baseBuilder.AsyncBuild<'T>(cmd, args)
    member builder.Build<'T>(cmd, args) = 
      match cmd with 
      | GetRange(source, (loRange, hiRange)) ->
          let restrictRange (vector:IVector<'T2>) =
            match vector with
            | :? VirtualVector<'T2> as source ->
                let subSource = source.Source.GetSubVector(Range(loRange, hiRange))
                VirtualVector<'T2>(subSource) :> IVector<'T2>
            | source -> 
                let cmd = GetRange(Return 0, (loRange, hiRange))
                baseBuilder.Build(cmd, [| source |])

          match build source args with
          | :? IBoxedVector as boxed -> // 'T = obj
              { new VectorCallSite<IVector<'T>> with
                  member x.Invoke(underlying:IVector<'U>) = 
                    boxVector (restrictRange underlying) |> unbox<IVector<'T>> }
               |> boxed.UnboxedVector.Invoke
          | vector -> restrictRange vector 
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

and VirtualOrdinalIndex(range) =
  let lo, hi = range
  let size = hi - lo + 1L
  do if size < 0L then invalidArg "range" "Invalid range"
  member x.Range = range
  interface IIndex<int64> with
    member x.KeyAt(addr) = 
      if addr < 0L || addr >= size then invalidArg "addr" "Out of range"
      else keyOfAddr lo addr
    member x.KeyCount = size
    member x.IsEmpty = size = 0L
    member x.Builder = VirtualIndexBuilder.Instance :> _
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


and VirtualIndexBuilder() = 
  let baseBuilder = IndexBuilder.Instance

  let emptyConstruction () =
    let newIndex = VirtualOrdinalIndex(0L, -1L)
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
      for index, vector in scs do 
        match index with
        | :? VirtualOrderedIndex<'K>
        | :? VirtualOrdinalIndex -> failwith "TODO: Reindex - ordered/ordinal index - avoid materialization!"
        | _ -> ()
      baseBuilder.Merge(scs, transform)

    member x.Search((index:IIndex<'K>, vector), searchVector:IVector<'V>, searchValue) = 
      match index, searchVector with
      | (:? VirtualOrderedIndex<'K> as index), (:? VirtualVector<'V> as searchVector) ->
          let mapping = searchVector.Source.LookupRange(searchValue)
          
          // TODO: Work in progres...
          //index.Source
          //searchVector.Source.Length
          failwith "!"
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
          let newVector = Vectors.GetRange(vector, (lo, hi))
          newIndex :> IIndex<'K>, newVector

      | :? VirtualOrdinalIndex when hi < lo -> emptyConstruction()
      | :? VirtualOrdinalIndex & (:? IIndex<int64> as index) -> 
          // TODO: range checks
          let newVector = Vectors.GetRange(vector, (lo, hi))
          let keyLo, keyHi = index.KeyRange
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
          let newVector = Vectors.GetRange(vector, (loIdx, hiIdx))
          let newIndex = VirtualOrderedIndex(index.Source.GetSubVector(Range(loIdx, hiIdx)))
          unbox<IIndex<'K>> newIndex, newVector

      | :? VirtualOrdinalIndex & (:? IIndex<int64> as index) -> 
          let getRangeKey proj next = function
            | None -> proj index.KeyRange 
            | Some(k, BoundaryBehavior.Inclusive) -> unbox<int64> k
            | Some(k, BoundaryBehavior.Exclusive) -> next (unbox<int64> k)
          let loKey, hiKey = getRangeKey fst ((+) 1L) optLo, getRangeKey snd ((-) 1L) optHi
          let loIdx, hiIdx = loKey - (fst index.KeyRange), hiKey - (fst index.KeyRange)

          // TODO: range checks
          let newVector = Vectors.GetRange(vector, (loIdx, hiIdx))
          let newIndex = VirtualOrdinalIndex(loKey, hiKey)
          unbox<IIndex<'K>> newIndex, newVector
      | _ -> 
          baseBuilder.GetRange((index, vector), (optLo, optHi))

// ------------------------------------------------------------------------------------------------

type VirtualVectorHelper =
  static member Create<'T>(source:IVirtualVectorSource<'T>) = 
    VirtualVector<'T>(source)

type Virtual() =
  static let createMi = typeof<VirtualVectorHelper>.GetMethod("Create")

  static let createFrame rowIndex columnIndex (sources:seq<IVirtualVectorSource>) = 
    let data = 
      sources 
      |> Seq.map (fun source -> 
          createMi.MakeGenericMethod(source.ElementType).Invoke(null, [| source |]) :?> IVector)
      |> Vector.ofValues
    Frame<_, _>(rowIndex, columnIndex, data, VirtualIndexBuilder.Instance, VirtualVectorBuilder.Instance)

  static member CreateOrdinalSeries(source) =
    let vector = VirtualVector(source)
    let index = VirtualOrdinalIndex(0L, source.Length-1L)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)

  static member CreateSeries(indexSource:IVirtualVectorSource<_>, valueSource:IVirtualVectorSource<_>) =
    if valueSource.Length <> indexSource.Length then
      invalidOp "CreateSeries: Index and value source should have the same length"

    let vector = VirtualVector(valueSource)
    let index = VirtualOrderedIndex(indexSource)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)

  static member CreateOrdinalFrame(keys, sources:seq<IVirtualVectorSource>) = 
    let count = sources |> Seq.fold (fun st src ->
      match st with 
      | None -> Some(src.Length) 
      | Some n when n = src.Length -> Some(n)
      | _ -> invalidArg "sources" "Sources should have the same length!" ) None
    if count = None then invalidArg "sources" "At least one column is required"
    let count = count.Value
    createFrame (VirtualOrdinalIndex(0L, count-1L)) (Index.ofKeys keys) sources

  static member CreateFrame(indexSource:IVirtualVectorSource<_>, keys, sources:seq<IVirtualVectorSource>) = 
    for sc in sources do 
      if sc.Length <> indexSource.Length then
        invalidArg "sources" "Sources should have the same length as index!"
    createFrame (VirtualOrderedIndex indexSource) (Index.ofKeys keys) sources
    


  // TODO: Multiple values
  // TODO: Assumptions - GetRange only works on sorted
  // TODO: Filter (Column = <value>)
  // TODO: Filter (arbitrary condition -> collection of ranges)
