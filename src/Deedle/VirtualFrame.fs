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
open Deedle
open Deedle.Internal
open Deedle.Vectors.Virtual
open Deedle.Indices.Virtual

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

#if VIRTUAL_ORDINAL_INDEX
  static member CreateOrdinalSeries(source) =
    let vector = VirtualVector(source)
    let index = VirtualOrdinalIndex(Ranges.Create [ 0L, source.Length-1L ])
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)
#endif

  static member CreateSeries(indexSource:IVirtualVectorSource<_>, valueSource:IVirtualVectorSource<_>) =
    // TODO: Check this in some better way?
    // if valueSource.Length <> indexSource.Length then
    //  invalidOp "CreateSeries: Index and value source should have the same length"

    let vector = VirtualVector(valueSource)
    let index = VirtualOrderedIndex(indexSource)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)

#if VIRTUAL_ORDINAL_INDEX
  static member CreateOrdinalFrame(keys:seq<_>, sources:seq<IVirtualVectorSource>) = 
    let count = sources |> Seq.fold (fun st src ->
      match st with 
      | None -> Some(src.Length) 
      | Some n when n = src.Length -> Some(n)
      | _ -> invalidArg "sources" "Sources should have the same length!" ) None
    if count = None then invalidArg "sources" "At least one column is required"
    let count = count.Value
    createFrame (VirtualOrdinalIndex(Ranges.Create [0L, count-1L])) (Index.ofKeys (ReadOnlyCollection.ofSeq keys)) sources
#endif

  static member CreateFrame(indexSource:IVirtualVectorSource<_>, keys, sources:seq<IVirtualVectorSource>) = 
    for sc in sources do 
      if sc.Length <> indexSource.Length then
        invalidArg "sources" "Sources should have the same length as index!"
    createFrame (VirtualOrderedIndex indexSource) (Index.ofKeys (ReadOnlyCollection.ofSeq keys)) sources
    


  // TODO: Multiple values
  // TODO: Assumptions - GetRange only works on sorted
  // TODO: Filter (Column = <value>)
  // TODO: Filter (arbitrary condition -> collection of ranges)
