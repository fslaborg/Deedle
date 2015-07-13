namespace Deedle

open System
open System.ComponentModel
open System.Collections.Generic

open Deedle.Addressing
open Deedle.Internal
open Deedle.JoinHelpers
open Deedle.Indices
open Deedle.Keys
open Deedle.Vectors
open Deedle.VectorHelpers

/// This enumeration specifies the behavior of `Union` operation on series when there are
/// overlapping keys in two series that are being unioned. The options include preferring values
/// from the left/right series or throwing an exception when both values are available.
///
/// [category:Parameters and results of various operations]
type UnionBehavior =
  /// When there are values available in both series that are being unioned, prefer the left value.
  | PreferLeft = 0
  /// When there are values available in both series that are being unioned, prefer the right value.
  | PreferRight = 1
  /// When there are values available in both series that are being unioned, raise an exception.
  | Exclusive = 2

// ------------------------------------------------------------------------------------------------
// Series
// ------------------------------------------------------------------------------------------------

/// Represents an untyped series with keys of type `K` and values of some unknown type
/// (This type should not generally be used directly, but it can be used when you need
/// to write code that works on a sequence of series of heterogeneous types).
///
/// [category:Core frame and series types]
type ISeries<'K when 'K : equality> =
  /// Returns the vector containing data of the series (as an untyped vector)
  abstract Vector : Deedle.IVector
  /// Returns the index containing keys of the series 
  abstract Index : IIndex<'K>
  /// Attempts to get the value at a specified key and return it as `obj`
  abstract TryGetObject : 'K -> OptionalValue<obj>
  /// Returns the vector builder associated with this series
  abstract VectorBuilder : IVectorBuilder

/// The type `Series<K, V>` represents a data series consisting of values `V` indexed by
/// keys `K`. The keys of a series may or may not be ordered 
///
/// [category:Core frame and series types]
and
  Series<'K, 'V when 'K : equality>
    ( index:IIndex<'K>, vector:IVector<'V>,
      vectorBuilder : IVectorBuilder, indexBuilder : IIndexBuilder ) =
  
  // Check that the addressing schemes match
  do if index.AddressingScheme <> vector.AddressingScheme then
       invalidOp "Index and vector of a series should share addressing scheme!"

  /// Value to hold the number of elements (so that we do not recalculate this all the time)
  /// (This is calculated on first access; we do not use Lazy<T> to avoid allocations)
  let mutable valueCount = -1 

  /// Returns the vector builder associated with this series
  member internal x.VectorBuilder = vectorBuilder
  /// Returns the index builder associated with this series
  member internal x.IndexBuilder = indexBuilder

  // ----------------------------------------------------------------------------------------------
  // Series data
  // ----------------------------------------------------------------------------------------------

  /// Returns the index associated with this series. This member should not generally
  /// be accessed directly, because all functionality is exposed through series operations.
  ///
  /// [category:Series data]
  member x.Index = index

  /// Returns the vector associated with this series. This member should not generally
  /// be accessed directly, because all functionality is exposed through series operations.
  ///
  /// [category:Series data]
  member x.Vector = vector

  /// Returns a collection of keys that are defined by the index of this series.
  /// Note that the length of this sequence does not match the `Values` sequence
  /// if there are missing values. To get matching sequence, use the `Observations`
  /// property or `Series.observation`.
  ///
  /// [category:Series data]
  member x.Keys = seq { for kvp in index.Mappings -> kvp.Key }

  /// Returns a collection of values that are available in the series data.
  /// Note that the length of this sequence does not match the `Keys` sequence
  /// if there are missing values. To get matching sequence, use the `Observations`
  /// property or `Series.observation`.
  ///
  /// [category:Series data]
  member x.Values = 
    index.Mappings
    |> Seq.choosel (fun idx kvp ->
        vector.GetValueAtLocation(KnownLocation(kvp.Value, idx)) 
        |> OptionalValue.asOption )

  /// Returns a collection of values, including possibly missing values. Note that 
  /// the length of this sequence matches the `Keys` sequence.
  ///
  /// [category:Series data]
  member x.ValuesAll = 
    index.Mappings
    |> Seq.mapl (fun idx kvp ->
        vector.GetValueAtLocation(KnownLocation(kvp.Value, idx)).Value)

  /// Returns a collection of observations that form this series. Note that this property
  /// skips over all missing (or NaN) values. Observations are returned as `KeyValuePair<K, V>` 
  /// objects. For an F# alternative that uses tuples, see `Series.observations`.
  ///
  /// [category:Series data]
  member x.Observations = 
    index.Mappings
    |> Seq.choosel (fun idx kvp ->
        vector.GetValueAtLocation(KnownLocation(kvp.Value, idx)) 
        |> OptionalValue.map (fun v -> KeyValuePair(kvp.Key, v))
        |> OptionalValue.asOption )

  /// Returns a collection of observations that form this series. Note that this property
  /// includes all missing (or NaN) values. Observations are returned as 
  /// `KeyValuePair<K, OptionalValue<V>>` objects. For an F# alternative that uses tuples, 
  /// see `Series.observationsAll`.
  ///
  /// [category:Series data]
  member x.ObservationsAll =
    index.Mappings
    |> Seq.mapl (fun idx kvp ->
        let v = vector.GetValueAtLocation(KnownLocation(kvp.Value, idx)) 
        KeyValuePair(kvp.Key, v))

  /// 
  ///
  /// [category:Series data]
  member x.IsEmpty = Seq.isEmpty index.Mappings

  /// [category:Series data]
  member x.IsOrdered = index.IsOrdered

  /// [category:Series data]
  member x.KeyRange = index.KeyRange

  /// Returns the total number of keys in the specified series. This returns
  /// the total length of the series, including keys for which there is no 
  /// value available.
  ///
  /// [category:Series data]
  member x.KeyCount = int index.KeyCount

  /// Returns the total number of values in the specified series. This excludes
  /// missing values or not available values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  ///
  /// [category:Series data]
  member x.ValueCount = 
    if valueCount = -1 then
      // In concurrent access, we may run this multiple times, 
      // but that's not a big deal as there are no race conditions
      let mutable count = 0
      for kvp in index.Mappings do 
        if vector.GetValue(kvp.Value).HasValue then count <- count + 1
      valueCount <- count
    valueCount

  // ----------------------------------------------------------------------------------------------
  // Accessors and slicing
  // ----------------------------------------------------------------------------------------------

  /// Internal helper used by `skip`, `take`, etc.
  member x.GetAddressRange(range) = 
    let newIndex, cmd = indexBuilder.GetAddressRange((index, Vectors.Return 0), range)
    let vec = vectorBuilder.Build(newIndex.AddressingScheme, cmd, [| vector |])
    Series(newIndex, vec, vectorBuilder, indexBuilder)

  /// [category:Accessors and slicing]
  member x.GetSubrange(lo, hi) =
    let newIndex, newVector = indexBuilder.GetRange((index, Vectors.Return 0), (lo, hi))
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, newVector, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  /// [category:Accessors and slicing]
  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetSlice(lo, hi) =
    let inclusive v = v |> Option.map (fun v -> v, BoundaryBehavior.Inclusive)
    x.GetSubrange(inclusive lo, inclusive hi)

  member series.Between(lowerInclusive, upperInclusive) = 
    series.GetSubrange
      ( Some(lowerInclusive, BoundaryBehavior.Inclusive),
        Some(upperInclusive, BoundaryBehavior.Inclusive) )

  member series.After(lowerExclusive) = 
    series.GetSubrange( Some(lowerExclusive, BoundaryBehavior.Exclusive), None )

  member series.Before(upperExclusive) = 
    series.GetSubrange( None, Some(upperExclusive, BoundaryBehavior.Exclusive) )

  member series.StartAt(lowerInclusive) = 
    series.GetSubrange( Some(lowerInclusive, BoundaryBehavior.Inclusive), None )

  member series.EndAt(upperInclusive) = 
    series.GetSubrange( None, Some(upperInclusive, BoundaryBehavior.Inclusive) )

  /// Returns a new series with an index containing the specified keys.
  /// When the key is not found in the current series, the newly returned
  /// series will contain a missing value. When the second parameter is not
  /// specified, the keys have to exactly match the keys in the current series
  /// (`Lookup.Exact`).
  ///
  /// ## Parameters
  ///
  ///  * `keys` - A collection of keys in the current series.
  ///
  /// [category:Accessors and slicing]
  member x.GetItems(keys) = x.GetItems(keys, Lookup.Exact)

  /// Returns a new series with an index containing the specified keys.
  /// When the key is not found in the current series, the newly returned
  /// series will contain a missing value. When the second parameter is not
  /// specified, the keys have to exactly match the keys in the current series
  /// (`Lookup.Exact`).
  ///
  /// ## Parameters
  ///
  ///  * `keys` - A collection of keys in the current series.
  ///  * `lookup` - Specifies the lookup behavior when searching for keys in 
  ///    the current series. `Lookup.NearestGreater` and `Lookup.NearestSmaller`
  ///    can be used when the current series is ordered.
  ///
  /// [category:Accessors and slicing]
  member series.GetItems(keys, lookup) =    
    let newIndex = indexBuilder.Create<_>((keys:seq<_>), None)
    let cmd = indexBuilder.Reindex(index, newIndex, lookup, Vectors.Return 0, fun addr -> series.Vector.GetValue(addr).HasValue)
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, cmd, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  ///
  /// [category:Accessors and slicing]
  member x.TryGetObservation(key, lookup) =
    let address = index.Lookup(key, lookup, fun addr -> vector.GetValue(addr).HasValue) 
    match address with
    | OptionalValue.Missing -> OptionalValue.Missing
    | OptionalValue.Present(key, addr) -> vector.GetValue(addr) |> OptionalValue.map (fun v -> KeyValuePair(key, v))

  ///
  /// [category:Accessors and slicing]
  member x.GetObservation(key, lookup) =
    let mapping = index.Lookup(key, lookup, fun addr -> vector.GetValue(addr).HasValue) 
    if not mapping.HasValue then keyNotFound key
    let value = vector.GetValue(snd mapping.Value) 
    if not value.HasValue then missingVal key
    KeyValuePair(fst mapping.Value, value.Value)

  ///
  /// [category:Accessors and slicing]
  member x.TryGet(key, lookup) =
    x.TryGetObservation(key, lookup) |> OptionalValue.map (fun (KeyValue(_, v)) -> v)

  ///
  /// [category:Accessors and slicing]
  member x.Get(key, lookup) =
    x.GetObservation(key, lookup).Value

  ///
  /// [category:Accessors and slicing]
  member x.GetByLevel(key:ICustomLookup<'K>) =
    let newIndex, levelCmd = indexBuilder.LookupLevel((index, Vectors.Return 0), key)
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, levelCmd, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)
    

  /// Attempts to get a value at the specified 'key'
  ///
  /// [category:Accessors and slicing]
  member x.TryGetObservation(key) = 
    let addr = index.Locate(key) 
    if addr = Address.invalid then OptionalValue.Missing 
    else
      let value = vector.GetValue(addr) 
      OptionalValue(KeyValuePair(key, value))

  ///
  /// [category:Accessors and slicing]
  member x.GetObservation(key) = 
    let addr = index.Locate(key) 
    if addr = Address.invalid then keyNotFound key
    let value = vector.GetValue(addr) 
    if not value.HasValue then missingVal key
    KeyValuePair(key, value.Value)

  ///
  /// [category:Accessors and slicing]
  member x.TryGet(key) = 
    let addr = x.Index.Locate(key) 
    if addr = Address.invalid then OptionalValue.Missing 
    else x.Vector.GetValue(addr)

  ///
  /// [category:Accessors and slicing]
  member x.Get(key) = 
    let addr = x.Index.Locate(key) 
    if addr = Address.invalid then keyNotFound key
    else 
      match x.Vector.GetValue(addr) with
      | OptionalValue.Missing   -> missingVal key
      | OptionalValue.Present v -> v

  ///
  /// [category:Accessors and slicing]
  member x.TryGetAt(index : int) = 
    x.Vector.GetValue(x.Index.AddressAt(int64 index))

  /// [category:Accessors and slicing]
  member x.GetKeyAt(index : int) = 
    x.Index.KeyAt(x.Index.AddressAt(int64 index))

  /// [category:Accessors and slicing]
  member x.GetAt(index : int) = 
    x.TryGetAt(index).Value

  ///
  /// [category:Accessors and slicing]
  member x.Item with get(a) = x.Get(a)
  ///
  /// [category:Accessors and slicing]
  member x.Item with get(items) = x.GetItems items
  ///
  /// [category:Accessors and slicing]
  member x.Item with get(a) = x.GetByLevel(a)

  ///
  /// [category:Accessors and slicing]
  static member (?) (series:Series<_, _>, name:string) = series.Get(name, Lookup.Exact)

  // ----------------------------------------------------------------------------------------------
  // Projection and filtering
  // ----------------------------------------------------------------------------------------------

  /// [category:Projection and filtering]
  member x.Where(f:System.Func<KeyValuePair<'K, 'V>, int, bool>) = 
    let keys, optValues =
      index.Mappings 
      |> Array.ofSeq |> Array.choosei (fun i (KeyValue(key, addr)) ->
          let opt = vector.GetValue(addr)
          // If a required value is missing, then skip over this
          if opt.HasValue && f.Invoke (KeyValuePair(key, opt.Value), i)
            then Some(key, opt) else None)
      |> Array.unzip
    let newIndex = indexBuilder.Create<_>(keys, None)
    let newVector = vectorBuilder.CreateMissing(optValues)
    Series(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Where(f:System.Func<KeyValuePair<'K, 'V>, bool>) = 
    x.Where(fun kvp _ -> f.Invoke kvp)

  /// [category:Projection and filtering]
  member x.WhereOptional(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, bool>) = 
    let keys, optValues =
      [| for KeyValue(key, addr) in index.Mappings do
          let opt = vector.GetValue(addr)
          if f.Invoke (KeyValuePair(key, opt)) then yield key, opt |]
      |> Array.unzip
    let newIndex = indexBuilder.Create<_>(keys, None)
    let newVector = vectorBuilder.CreateMissing(optValues)
    Series(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, int, 'R>) = 
    let newVector = vector.Select(fun loc value ->
      value |> OptionalValue.bind (fun v -> 
        let key = index.KeyAt(loc.Address)
        try OptionalValue(f.Invoke(KeyValuePair(key, v), int loc.Offset))
        with :? MissingValueException -> OptionalValue.Missing ))  
    let newIndex = indexBuilder.Project(index)
    Series<'K, 'R>(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Convert<'R>(forward:System.Func<'V, 'R>, backward:System.Func<'R, 'V>) = 
    let newVector = vector.Convert(forward.Invoke, backward.Invoke)
    let newIndex = indexBuilder.Project(index)
    Series<'K, 'R>(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, 'R>) = 
    x.SelectOptional(fun kvp ->
      kvp.Value |> OptionalValue.bind (fun v -> 
        try OptionalValue(f.Invoke(KeyValuePair(kvp.Key, v)))
        with :? MissingValueException -> OptionalValue.Missing ))  
    
  /// [category:Projection and filtering]
  member x.SelectKeys<'R when 'R : equality>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, 'R>) = 
    let newKeys =
      [| for KeyValue(key, addr) in index.Mappings -> 
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))) |]
    let newIndex = indexBuilder.Create(newKeys, None)
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, Vectors.Return 0, [| vector |])
    Series<'R, _>(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.SelectOptional<'R>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, OptionalValue<'R>>) = 
    let newVector = vector.Select(fun loc value ->
      let key = index.KeyAt(loc.Address)
      f.Invoke(KeyValuePair(key, value)))
    let newIndex = indexBuilder.Project(index)
    Series<'K, 'R>(newIndex, newVector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.SelectValues<'T>(f:System.Func<'V, 'T>) = 
    x.Select(fun kvp -> f.Invoke kvp.Value)
  
  /// Custom operator that can be used for applying a function to all elements of 
  /// a series. This provides a nicer syntactic sugar for the `Series.mapValues` 
  /// function. For example:
  ///
  ///     // Given a float series and a function on floats
  ///     let s1 = Series.ofValues [ 1.0 .. 10.0 ]
  ///     let adjust v = max 10.0 v
  ///
  ///     // Apply "adjust (v + v)" to all elements
  ///     adjust $ (s1 + s1)
  ///
  static member ($) (f, series: Series<'K,'V>) = 
    series.SelectValues(Func<_,_>(f))

  /// [category:Projection and filtering]
  member x.Reversed =
    let newIndex = index.Keys |> Array.ofSeq |> Array.rev
    let newVector = vector.DataSequence |> Array.ofSeq |> Array.rev
    Series(Index.ofKeys newIndex, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  /// [category:Projection and filtering]
  member x.ScanValues(foldFunc:System.Func<'S,'V,'S>, init) =   
    let newVector = [| 
      let accum = ref init
      for v in vector.DataSequence ->
        if v.HasValue then
          accum := foldFunc.Invoke(!accum, v.Value)
          OptionalValue(!accum)
        else
          OptionalValue.Missing |] 

    Series(index, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  /// [category:Projection and filtering]
  member x.ScanAllValues(foldFunc:System.Func<OptionalValue<'S>,OptionalValue<'V>,OptionalValue<'S>>, init) =   
    let newVector = vector.DataSequence |> Seq.scan (fun x y -> foldFunc.Invoke(x, y)) init |> Seq.skip 1 |> Seq.toArray
    Series(index, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  /// Returns the current series with the same index but with values missing wherever the 
  /// corresponding key exists in the other series index with an associated missing value.
  ///
  /// [category:Projection and filtering]
  member x.WithMissingFrom(otherSeries: Series<'K, _>) =
    let newVec = x.ObservationsAll |> Seq.map (fun obs -> 
      match otherSeries.TryGetObservation(obs.Key) with
      | OptionalValue.Present kvp -> if kvp.Value.HasValue then obs.Value else OptionalValue.Missing
      | _                         -> obs.Value)
    Series(x.Index, vectorBuilder.CreateMissing(Array.ofSeq newVec), vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Merging, joining etc
  // ----------------------------------------------------------------------------------------------

  /// [category:Merging, joining and zipping]
  member series.Merge(otherSeries:Series<'K, 'V>) =
    // Append the row indices and get transformation that combines two column vectors
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newIndex, cmd = 
      indexBuilder.Merge( [(index, Vectors.Return 0); (otherSeries.Index, Vectors.Return 1)], 
                           BinaryTransform.AtMostOne )
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, cmd, [| series.Vector; otherSeries.Vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  /// [category:Merging, joining and zipping]
  member series.Merge(otherSeries:seq<Series<'K, 'V>>) =
    series.Merge(Array.ofSeq otherSeries)

  /// [category:Merging, joining and zipping]
  member series.Merge([<ParamArray>] otherSeries:Series<'K, 'V>[]) =
    // Append the row indices and get transformation that combines two column vectors
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let constrs = otherSeries |> Array.mapi (fun i s -> s.Index, Vectors.Return(i + 1)) |> List.ofSeq
    let vectors = otherSeries |> Array.map (fun s -> s.Vector)

    let newIndex, cmd = 
      indexBuilder.Merge( (index, Vectors.Return 0)::constrs, BinaryTransform.AtMostOne )
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, cmd, [| yield series.Vector; yield! vectors |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  /// [category:Merging, joining and zipping]
  member series.Merge(another:Series<'K, 'V>, behavior) = 
    let newIndex, vec1, vec2 = indexBuilder.Union( (series.Index, Vectors.Return 0), (another.Index, Vectors.Return 1) )
    let transform = 
      match behavior with
      | UnionBehavior.PreferRight -> BinaryTransform.RightIfAvailable
      | UnionBehavior.Exclusive -> BinaryTransform.AtMostOne
      | _ -> BinaryTransform.LeftIfAvailable
    let vecCmd = Vectors.Combine(lazy newIndex.KeyCount, [vec1; vec2], transform)
    let newVec = vectorBuilder.Build(newIndex.AddressingScheme, vecCmd, [| series.Vector; another.Vector |])
    Series(newIndex, newVec, vectorBuilder, indexBuilder)

  /// [category:Merging, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>) =
    series.Zip(otherSeries, JoinKind.Outer, Lookup.Exact)

  /// [category:Merging, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>, kind) =
    series.Zip(otherSeries, kind, Lookup.Exact)

  member private series.ZipHelper(otherSeries:Series<'K, 'V2>, kind, lookup) =
    // Union row indices and get transformations to apply to left/right vectors
    let newIndex, thisRowCmd, otherRowCmd = 
      createJoinTransformation indexBuilder otherSeries.IndexBuilder 
        kind lookup index otherSeries.Index (Vectors.Return 0) (Vectors.Return 0)

    let lVec = vectorBuilder.Build(newIndex.AddressingScheme, thisRowCmd, [| series.Vector |])
    let rVec = vectorBuilder.Build(newIndex.AddressingScheme, otherRowCmd, [| otherSeries.Vector |])
    newIndex, lVec, rVec

  /// [category:Merging, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>, kind, lookup) : Series<'K, 'V opt * 'V2 opt> =
    let newIndex, lVec, rVec = series.ZipHelper(otherSeries, kind, lookup)
    let vecRes = 
      Vectors.Combine(lazy newIndex.KeyCount, [Vectors.Return 0; Vectors.Return 1], 
        BinaryTransform.CreateLifted<'V opt * 'V2 opt>(fun (l, _) (_, r) -> l, r))

    let args =
      [| lVec.Select(fun _ v -> OptionalValue((v, OptionalValue.Missing)))
         rVec.Select(fun _ v -> OptionalValue((OptionalValue.Missing, v))) |]
    let zipV = vectorBuilder.Build<'V opt * 'V2 opt>(newIndex.AddressingScheme, vecRes, args)
    Series(newIndex, zipV, vectorBuilder, indexBuilder)

  /// [category:Merging, joining and zipping]
  member series.ZipInner<'V2>(otherSeries:Series<'K, 'V2>) : Series<'K, 'V * 'V2> =
    let newIndex, lVec, rVec = series.ZipHelper(otherSeries, JoinKind.Inner, Lookup.Exact)
    
    let vecRes = 
      Vectors.Combine(lazy newIndex.KeyCount, [Vectors.Return 0; Vectors.Return 1], 
        BinaryTransform.CreateLifted<Choice<'V, 'V2, 'V * 'V2>>(fun l r ->
          match l, r with
          | Choice1Of3 l, Choice2Of3 r -> Choice3Of3(l, r)
          | _ -> failwith "logic error"))

    let zipV = 
      vectorBuilder.Build<Choice<'V, 'V2, 'V * 'V2>>
        (newIndex.AddressingScheme, vecRes, [| lVec.Select(Choice1Of3); rVec.Select(Choice2Of3) |])
    let zipV = zipV.Select(function Choice3Of3 v -> v | _ -> failwith "logic error")
    Series(newIndex, zipV, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Resampling
  // ----------------------------------------------------------------------------------------------


  /// Resample the series based on a provided collection of keys. The values of the series
  /// are aggregated into chunks based on the specified keys. Depending on `direction`, the 
  /// specified key is either used as the smallest or as the greatest key of the chunk (with
  /// the exception of boundaries that are added to the first/last chunk).
  ///
  /// Such chunks are then aggregated using the provided `valueSelector` and `keySelector`
  /// (an overload that does not take `keySelector` just selects the explicitly provided key).
  ///
  /// ## Parameters
  ///  - `keys` - A collection of keys to be used for resampling of the series
  ///  - `direction` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `valueSelector` - A function that is used to collapse a generated chunk into a 
  ///    single value. Note that this function may be called with empty series.
  ///  - `keySelector` - A function that is used to generate a new key for each chunk.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  ///
  /// [category:Resampling]
  member x.Resample<'TNewKey, 'R when 'TNewKey : equality>(keys, direction, valueSelector:Func<_, _, _>, keySelector:Func<_, _, _>) =
    let newIndex, newVector = 
      indexBuilder.Resample
        ( indexBuilder, x.Index, keys, direction, Vectors.Return 0, 
          (fun (key, (index, cmd)) -> 
              let vector = vectorBuilder.Build(index.AddressingScheme, cmd, [| vector |])
              let window = Series<_, _>(index, vector, vectorBuilder, indexBuilder)
              let newKey = keySelector.Invoke(key, window)
              newKey, OptionalValue(valueSelector.Invoke(newKey, window))) )
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Resample the series based on a provided collection of keys. The values of the series
  /// are aggregated into chunks based on the specified keys. Depending on `direction`, the 
  /// specified key is either used as the smallest or as the greatest key of the chunk (with
  /// the exception of boundaries that are added to the first/last chunk).
  ///
  /// Such chunks are then aggregated using the provided `valueSelector` and `keySelector`
  /// (an overload that does not take `keySelector` just selects the explicitly provided key).
  ///
  /// ## Parameters
  ///  - `keys` - A collection of keys to be used for resampling of the series
  ///  - `direction` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `valueSelector` - A function that is used to collapse a generated chunk into a 
  ///    single value. Note that this function may be called with empty series.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  ///
  /// [category:Resampling]
  member x.Resample(keys, direction, valueSelector) =
    x.Resample(keys, direction, valueSelector, fun nk _ -> nk)

  /// Resample the series based on a provided collection of keys. The values of the series
  /// are aggregated into chunks based on the specified keys. Depending on `direction`, the 
  /// specified key is either used as the smallest or as the greatest key of the chunk (with
  /// the exception of boundaries that are added to the first/last chunk). The chunks
  /// are then returned as a nested series. 
  ///
  /// ## Parameters
  ///  - `keys` - A collection of keys to be used for resampling of the series
  ///  - `direction` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  ///
  /// [category:Resampling]
  member x.Resample(keys, direction) =
    x.Resample(keys, direction, (fun k v -> v), fun nk _ -> nk)

  // ----------------------------------------------------------------------------------------------
  // Aggregation
  // ----------------------------------------------------------------------------------------------

  /// Returns a series containing an element and its neighbor for each input.
  /// The returned series is one key shorter (it does not contain a 
  /// value for the first or last key depending on `boundary`). If `boundary` is 
  /// other than `Boundary.Skip`, then the key is included in the returned series, 
  /// but its value is missing.
  ///
  /// ## Parameters
  ///  - `series` - The input series to be aggregated.
  ///  - `boundary` - Specifies the direction in which the series is aggregated and 
  ///    how the corner case is handled. If the value is `Boundary.AtEnding`, then the
  ///    function returns value and its successor, otherwise it returns value and its
  ///    predecessor.
  ///
  /// ## Example
  ///
  ///     let input = series [ 1 => 'a'; 2 => 'b'; 3 => 'c']
  ///     let res = input.Pairwise()
  ///     res = series [2 => ('a', 'b'); 3 => ('b', 'c') ]
  ///
  /// [category:Windowing, chunking and grouping]
  member x.Pairwise(boundary) =
    let dir = if boundary = Boundary.AtEnding then Direction.Forward else Direction.Backward
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, WindowSize(2, boundary), Vectors.Return 0, 
          (fun (kind, (index, cmd)) -> 
              // Calculate new key
              let newKey = 
                if dir = Direction.Backward then index.Keys |> Seq.last
                else index.Keys |> Seq.head
              // Calculate value for the chunk
              let newValue = 
                let actualVector = vectorBuilder.Build(index.AddressingScheme, cmd, [| vector |])
                let obs = [ for KeyValue(k, addr) in index.Mappings -> actualVector.GetValue(addr) ]
                match obs with
                | [ OptionalValue.Present v1; OptionalValue.Present v2 ] -> 
                    OptionalValue( DataSegment(kind, (v1, v2)) )
                | [ _; _ ] -> OptionalValue.Missing
                | _ -> failwith "Pairwise: failed - expected two values"
              // Return key value for the result
              newKey, newValue )) 
    Series<'K, DataSegment<'V * 'V>>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Returns a series containing the predecessor and an element for each input, except
  /// for the first one. The returned series is one key shorter (it does not contain a 
  /// value for the first key).
  ///
  /// ## Parameters
  ///  - `series` - The input series to be aggregated.
  ///
  /// ## Example
  ///
  ///     let input = series [ 1 => 'a'; 2 => 'b'; 3 => 'c']
  ///     let res = input.Pairwise()
  ///     res = series [2 => ('a', 'b'); 3 => ('b', 'c') ]
  ///
  /// [category:Windowing, chunking and grouping]
  member x.Pairwise() =
    x.Pairwise(Boundary.Skip).Select(fun (kvp:KeyValuePair<_, DataSegment<_>>) -> kvp.Value.Data)

  /// Aggregates an ordered series using the method specified by `Aggregation<K>` and then
  /// applies the provided `valueSelector` on each window or chunk to produce the result
  /// which is returned as a new series. A key for each window or chunk is
  /// selected using the specified `keySelector`.
  ///
  /// ## Parameters
  ///  - `aggregation` - Specifies the aggregation method using `Aggregation<K>`. This is
  ///    a discriminated union listing various chunking and windowing conditions.
  ///  - `keySelector` - A function that is called on each chunk to obtain a key.
  ///  - `valueSelector` - A value selector function that is called to aggregate each chunk or window.
  ///
  /// [category:Windowing, chunking and grouping]
  member x.Aggregate<'TNewKey, 'R when 'TNewKey : equality>
        (aggregation, keySelector:Func<_, _>, valueSelector:Func<_, _>) =
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, aggregation, Vectors.Return 0, 
          (fun (kind, (index, cmd)) -> 
              // Create series for the chunk/window
              let series = Series<_, _>(index, vectorBuilder.Build(index.AddressingScheme, cmd, [| vector |]), vectorBuilder, indexBuilder)
              let segment = DataSegment(kind, series)
              // Call key & value selectors to produce the result
              let newKey = keySelector.Invoke segment
              let newValue = valueSelector.Invoke segment
              newKey, newValue ))
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Aggregates an ordered series using the method specified by `Aggregation<K>` and then
  /// applies the provided `observationSelector` on each window or chunk to produce the result
  /// which is returned as a new series. The selector returns both the key and the value.
  ///
  /// ## Parameters
  ///  - `aggregation` - Specifies the aggregation method using `Aggregation<K>`. This is
  ///    a discriminated union listing various chunking and windowing conditions.
  ///  - `observationSelector` - A function that is called on each chunk to obtain a key and a value.
  ///
  /// [category:Windowing, chunking and grouping]
  member x.Aggregate<'TNewKey, 'R when 'TNewKey : equality>
        (aggregation, observationSelector:Func<_, KeyValuePair<_, _>>) =
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, aggregation, Vectors.Return 0, 
          (fun (kind, (index, cmd)) -> 
              // Create series for the chunk/window
              let series = Series<_, _>(index, vectorBuilder.Build(index.AddressingScheme, cmd, [| vector |]), vectorBuilder, indexBuilder)
              let segment = DataSegment(kind, series)
              // Call key & value selectors to produce the result
              let (KeyValue(newKey, newValue)) = observationSelector.Invoke(segment)
              newKey, newValue ))
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Groups a series (ordered or unordered) using the specified key selector (`keySelector`) 
  ///
  /// ## Parameters
  ///  - `keySelector` - Generates a new key that is used for aggregation, based on the original 
  ///    key and value. The new key must support equality testing.
  ///
  /// [category:Windowing, chunking and grouping]
  member x.GroupBy(keySelector:Func<_, _>) =
    let index = x.Index
    let ks key =  x.TryGet(key) |> OptionalValue.map (fun v -> keySelector.Invoke(KeyValuePair(key, v)))
    let cmd = indexBuilder.GroupBy(index, ks, VectorConstruction.Return 0) 
    let newIndex  = Index.ofKeys (cmd |> ReadOnlyCollection.map fst)
    let newGroups = cmd |> Seq.map snd |> Seq.map (fun sc -> 
        Series(fst sc, vectorBuilder.Build(newIndex.AddressingScheme, snd sc, [| x.Vector |]), vectorBuilder, indexBuilder))
    Series<'TNewKey, _>(newIndex, Vector.ofValues newGroups, vectorBuilder, indexBuilder)

  /// Interpolates an ordered series given a new sequence of keys. The function iterates through
  /// each new key, and invokes a function on the current key, the nearest smaller and larger valid 
  /// observations from the series argument. The function must return a new valid float. 
  ///
  /// ## Parameters
  ///  - `keys` - Sequence of new keys that forms the index of interpolated results
  ///  - `f` - Function to do the interpolating
  ///
  /// [category:Windowing, chunking and grouping]
  member x.Interpolate(keys:'K seq, f:Func<'K, OptionalValue<KeyValuePair<'K,'V>>, OptionalValue<KeyValuePair<'K,'V>>, 'V>) =
    let newObs = 
      seq {
        for k in keys do
          let smaller = x.TryGetObservation(k, Lookup.ExactOrSmaller)
          let bigger  = x.TryGetObservation(k, Lookup.ExactOrGreater)
          yield f.Invoke(k, smaller, bigger) }
      |> Seq.toArray

    let newIndex = Index.ofKeys (ReadOnlyCollection.ofSeq keys)
    let newValues = newObs
    Series(newIndex, Vector.ofValues newValues, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Indexing
  // ----------------------------------------------------------------------------------------------

  /// [category:Indexing]
  member x.Realign(newKeys) = 
    let findAll getter = seq {
      for k in newKeys -> 
        let addr = index.Locate(k) 
        if addr = Address.invalid then OptionalValue.Missing
        else getter addr }
    let newIndex = Index.ofKeys (ReadOnlyCollection.ofSeq newKeys)
    let newVector = findAll x.Vector.GetValue |> Vector.ofOptionalValues
    Series<_,_>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Replace the index of the series with ordinally generated integers starting from zero.
  /// The elements of the series are assigned index according to the current order, or in a
  /// non-deterministic way, if the current index is not ordered.
  ///
  /// [category:Indexing]
  member x.IndexOrdinally() = 
    let newIndex = indexBuilder.Create(x.Index.Keys |> Seq.mapi (fun i _ -> i), Some true)
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, Vectors.Return 0, [| vector |])
    Series<int, _>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// [category:Indexing]
  member x.IndexWith(keys:seq<_>) = 
    let newIndex = indexBuilder.Create(keys, None)
    let vectorCmd = 
      if newIndex.KeyCount = int64 x.KeyCount then
        // Just return the vector, because it has the same length
        Vectors.Return 0
      elif newIndex.KeyCount > int64 x.KeyCount then
        // Pad vector with missing values
        Vectors.Append(Vectors.Return 0, Vectors.Empty(newIndex.KeyCount - int64 x.KeyCount))
      else 
        // Get sub-range of the source vector
        Vectors.GetRange(Vectors.Return 0, RangeRestriction.Fixed(x.Index.AddressAt(0L), x.Index.AddressAt(newIndex.KeyCount - 1L)))

    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, vectorCmd, [| vector |])
    Series<'TNewKey, _>(newIndex, newVector, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Asynchronous support 
  // ----------------------------------------------------------------------------------------------
    
  member x.AsyncMaterialize() = async {
    let newIndexAsync, cmd = indexBuilder.AsyncMaterialize(index, Vectors.Return 0)
    let! newIndex = newIndexAsync
    let! newVector = vectorBuilder.AsyncBuild(newIndex.AddressingScheme, cmd, [| vector |])
    return Series<_, _>(newIndex, newVector, vectorBuilder, indexBuilder) }

  member x.MaterializeAsync() = 
    x.AsyncMaterialize() |> Async.StartAsTask

  member x.Materialize() = 
    let newIndex, cmd = indexBuilder.AsyncMaterialize(index, Vectors.Return 0)
    let newIndex = newIndex |> Async.RunSynchronously
    let newVector = vectorBuilder.Build(newIndex.AddressingScheme, cmd, [| vector |])
    Series<_, _>(newIndex, newVector, vectorBuilder, newIndex.Builder)
    
  // ----------------------------------------------------------------------------------------------
  // Operators and F# functions
  // ----------------------------------------------------------------------------------------------

  static member inline internal UnaryGenericOperation<'K, 'T1, 'T2>(series:Series<'K, 'T1>, op : 'T1 -> 'T2) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal UnaryOperation<'T>(series:Series<'K, 'T>, op : 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal ScalarOperationL<'T>(series:Series<'K, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline internal ScalarOperationR<'T>(scalar, series:Series<'K, 'T>, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op scalar v)

  static member inline internal VectorOperation<'T>(series1:Series<'K,'T>, series2:Series<'K,'T>, op): Series<_, 'T> =
    let newIndex, lcmd, rcmd = 
      createJoinTransformation series1.IndexBuilder series2.IndexBuilder 
        JoinKind.Outer Lookup.Exact series1.Index series2.Index (Vectors.Return 0) (Vectors.Return 1)
    let vecRes = Vectors.Combine(lazy newIndex.KeyCount, [lcmd; rcmd], BinaryTransform.CreateLifted<'T>(op))
    let vector = series1.VectorBuilder.Build<'T>(newIndex.AddressingScheme, vecRes, [| series1.Vector; series2.Vector |])
    Series(newIndex, vector, series1.VectorBuilder, series1.IndexBuilder)

  /// [category:Operators]
  static member (~-)(series) = Series<'K, _>.UnaryOperation<float>(series, (~-))
  /// [category:Operators]
  static member (~-)(series) = Series<'K, _>.UnaryOperation<int>(series, (~-))

  /// [category:Operators]
  static member (+) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (+))
  /// [category:Operators]
  static member (+) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (+))
  /// [category:Operators]
  static member (-) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (-))
  /// [category:Operators]
  static member (-) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (-))
  /// [category:Operators]
  static member (*) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (*))
  /// [category:Operators]
  static member (*) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (*))
  /// [category:Operators]
  static member (/) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (/))
  /// [category:Operators]
  static member (/) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (/))

  /// [category:Operators]
  static member (+) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (+))
  /// [category:Operators]
  static member (+) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (+))
  /// [category:Operators]
  static member (-) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (-))
  /// [category:Operators]
  static member (-) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (-))
  /// [category:Operators]
  static member (*) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (*))
  /// [category:Operators]
  static member (*) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (*))
  /// [category:Operators]
  static member (/) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (/))
  /// [category:Operators]
  static member (/) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (/))
  /// [category:Operators]
  static member Pow (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, ( ** ))
  /// [category:Operators]
  static member Pow (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, ( ** ))

  /// [category:Operators]
  static member (+) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (+))
  /// [category:Operators]
  static member (-) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (-))
  /// [category:Operators]
  static member (*) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (*))
  /// [category:Operators]
  static member (/) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (/))

  /// [category:Operators]
  static member (+) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (+))
  /// [category:Operators]
  static member (-) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (-))
  /// [category:Operators]
  static member (*) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (*))
  /// [category:Operators]
  static member (/) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (/))
  /// [category:Operators]
  static member Pow(s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, ( ** ))

  // Trigonometric
  
  /// [category:Operators]
  static member Acos(series) = Series<'K, _>.UnaryOperation<float>(series, acos)
  /// [category:Operators]
  static member Asin(series) = Series<'K, _>.UnaryOperation<float>(series, asin)
  /// [category:Operators]
  static member Atan(series) = Series<'K, _>.UnaryOperation<float>(series, atan)
  /// [category:Operators]
  static member Sin(series) = Series<'K, _>.UnaryOperation<float>(series, sin)
  /// [category:Operators]
  static member Sinh(series) = Series<'K, _>.UnaryOperation<float>(series, sinh)
  /// [category:Operators]
  static member Cos(series) = Series<'K, _>.UnaryOperation<float>(series, cos)
  /// [category:Operators]
  static member Cosh(series) = Series<'K, _>.UnaryOperation<float>(series, cosh)
  /// [category:Operators]
  static member Tan(series) = Series<'K, _>.UnaryOperation<float>(series, tan)
  /// [category:Operators]
  static member Tanh(series) = Series<'K, _>.UnaryOperation<float>(series, tanh)

  // Actually useful

  /// [category:Operators]
  static member Abs(series) = Series<'K, _>.UnaryOperation<float>(series, abs)
  /// [category:Operators]
  static member Abs(series) = Series<'K, _>.UnaryOperation<int>(series, abs)
  /// [category:Operators]
  static member Ceiling(series) = Series<'K, _>.UnaryOperation<float>(series, ceil)
  /// [category:Operators]
  static member Exp(series) = Series<'K, _>.UnaryOperation<float>(series, exp)
  /// [category:Operators]
  static member Floor(series) = Series<'K, _>.UnaryOperation<float>(series, floor)
  /// [category:Operators]
  static member Truncate(series) = Series<'K, _>.UnaryOperation<float>(series, truncate)
  /// [category:Operators]
  static member Log(series) = Series<'K, _>.UnaryOperation<float>(series, log)
  /// [category:Operators]
  static member Log10(series) = Series<'K, _>.UnaryOperation<float>(series, log10)
  /// [category:Operators]
  static member Round(series) = Series<'K, _>.UnaryOperation<float>(series, round)
  /// [category:Operators]
  static member Sign(series) = Series<'K, _>.UnaryGenericOperation<_, float, _>(series, sign)
  /// [category:Operators]
  static member Sqrt(series) = Series<'K, _>.UnaryGenericOperation<_, float, _>(series, sqrt)

  // ----------------------------------------------------------------------------------------------
  // Overrides & interfaces
  // ----------------------------------------------------------------------------------------------

  override series.Equals(another) = 
    match another with
    | null -> false
    | :? Series<'K, 'V> as another -> 
        series.Index.Equals(another.Index) && series.Vector.Equals(another.Vector)
    | _ -> false

  override series.GetHashCode() =
    let combine h1 h2 = ((h1 <<< 5) + h1) ^^^ h2
    combine (series.Index.GetHashCode()) (series.Vector.GetHashCode())

  interface ISeries<'K> with
    member x.TryGetObject(k) = x.TryGet(k) |> OptionalValue.map box
    member x.Vector = vector :> IVector
    member x.Index = index
    member x.VectorBuilder = vectorBuilder

  member private series.GetPrintedObservations(startCount, endCount) = 
    // NOTE: Do not check the length, because that would evaluate the whole 
    // series. Instead, we just check if it has more than startCount+endCount
    let smaller = series.Index.Mappings |> Seq.skipAtMost (startCount+endCount) |> Seq.isEmpty
    if smaller then
      seq { for obs in series.ObservationsAll -> Choice1Of3(obs.Key, obs.Value) } 
    else
      let starts = series.GetAddressRange(RangeRestriction.Start(int64 startCount))
      let ends = series.GetAddressRange(RangeRestriction.End(int64 endCount))
      seq { for obs in starts.ObservationsAll do yield Choice1Of3(obs.Key, obs.Value)
            yield Choice2Of3()
            for obs in ends.ObservationsAll do yield Choice1Of3(obs.Key, obs.Value) }

  override series.ToString() =
    if vector.SuppressPrinting then "(Suppressed)" else
      series.GetPrintedObservations(Formatting.StartInlineItemCount, Formatting.EndInlineItemCount) 
      |> Seq.map (function
          | Choice2Of3() -> " ... "
          | Choice1Of3(k, v) | Choice3Of3(k, v) -> sprintf "%O => %O" k v )
      |> String.concat "; "
      |> sprintf "series [ %s]" 

  /// Shows the series content in a human-readable format. The resulting string
  /// shows a limited number of values from the series.
  member series.Format() =
    series.Format(Formatting.StartItemCount, Formatting.EndItemCount)

  /// Shows the series content in a human-readable format. The resulting string
  /// shows a limited number of values from the series.
  ///
  /// ## Parameters
  ///  - `itemCount` - The total number of items to show. The result will show
  ///    at most `itemCount/2` items at the beginning and ending of the series.
  member series.Format(itemCount) =
    let half = itemCount / 2
    series.Format(half, half)

  /// Shows the series content in a human-readable format. The resulting string
  /// shows a limited number of values from the series.
  ///
  /// ## Parameters
  ///  - `startCount` - The number of elements to show at the beginning of the series
  ///  - `endCount` - The number of elements to show at the end of the series
  member series.Format(startCount, endCount) = 
    let getLevel ordered previous reset maxLevel level (key:'K) = 
      let levelKey = 
        if level = 0 && maxLevel = 0 then box key
        else CustomKey.Get(key).GetLevel(level)
      if ordered && (Some levelKey = !previous) then "" 
      else previous := Some levelKey; reset(); levelKey.ToString()

    if vector.SuppressPrinting then "(Suppressed)" else
      if series.IsEmpty then 
        "(Empty)"
      else
        let firstKey = series.GetKeyAt(0)
        let levels = CustomKey.Get(firstKey).Levels
        let previous = Array.init levels (fun _ -> ref None)
        let reset i () = for j in i + 1 .. levels - 1 do previous.[j] := None

        series.GetPrintedObservations(startCount, endCount)
        |> Seq.map (function
            | Choice1Of3(k, v) | Choice3Of3(k, v) -> 
                [ // Yield all row keys
                  for level in 0 .. levels - 1 do 
                    yield getLevel series.Index.IsOrdered previous.[level] (reset level) levels level k
                  yield "->"
                  yield v.ToString() ]
            | Choice2Of3() -> 
                [ yield "..."
                  for level in 1 .. levels - 1 do yield ""
                  yield "->"
                  yield "..." ] )
        |> array2D
        |> Formatting.formatTable

  interface IFsiFormattable with
    member x.Format() = (x :> Series<_, _>).Format()



  // ----------------------------------------------------------------------------------------------
  // Nicer constructor
  // ----------------------------------------------------------------------------------------------

  new(pairs:seq<KeyValuePair<'K, 'V>>) =
    Series(pairs |> Seq.map (fun kvp -> kvp.Key), pairs |> Seq.map (fun kvp -> kvp.Value))

  new(keys:seq<_>, values:seq<_>) = 
    let vectorBuilder = VectorBuilder.Instance
    let indexBuilder = IndexBuilder.Instance
    Series( Index.ofKeys (ReadOnlyCollection.ofSeq keys), vectorBuilder.Create (Array.ofSeq values),
            vectorBuilder, indexBuilder )

  new(keys:_[], values:_[]) = 
    let vectorBuilder = VectorBuilder.Instance
    let indexBuilder = IndexBuilder.Instance
    Series(Index.ofKeys (ReadOnlyCollection.ofArray keys), vectorBuilder.Create values, vectorBuilder, indexBuilder )

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

/// Represents a series containing boxed values. This type is inherited from `Series<'K, obj>` 
/// and it adds additional operations for accessing values with unboxing. This includes operations
/// such as `os.GetAs<'T>`, `os.TryGetAs<'T>` and `os.TryAs<'T>` which (attempt to) convert
/// values to the specified type `'T`.
///
/// [category:Specialized frame and series types]
type ObjectSeries<'K when 'K : equality> internal(index:IIndex<_>, vector, vectorBuilder, indexBuilder) = 
  inherit Series<'K, obj>(index, vector, vectorBuilder, indexBuilder)

  new(series:Series<'K, obj>) = 
    ObjectSeries<_>(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder)

  member x.GetValues<'R>(conversionKind) = 
    x.Values |> Seq.choose (fun v ->
      try Some(Convert.convertType<'R> conversionKind v) 
      with _ -> None)

  member x.GetValues<'R>() = x.GetValues<'R>(ConversionKind.Safe)

  member x.GetAs<'R>(column) : 'R = 
    Convert.convertType<'R> ConversionKind.Flexible (x.Get(column))

  member x.GetAs<'R>(column, fallback) : 'R =
    let address = index.Lookup(column, Lookup.Exact, fun _ -> true) 
    match address with
    | OptionalValue.Present a -> 
        match (vector.GetValue(snd a)) with
        | OptionalValue.Present v -> Convert.convertType<'R> ConversionKind.Flexible v
        | OptionalValue.Missing   -> fallback
    | OptionalValue.Missing -> keyNotFound column

  member x.GetAtAs<'R>(index) : 'R = 
    Convert.convertType<'R> ConversionKind.Flexible (x.GetAt(index))

  member x.GetAtAs<'R>(index, conversionKind) : 'R = 
    Convert.convertType<'R> conversionKind (x.GetAt(index))

  member x.TryGetAs<'R>(column) : OptionalValue<'R> = 
    x.TryGet(column) |> OptionalValue.map (fun v -> Convert.convertType<'R> ConversionKind.Flexible v)

  member x.TryGetAs<'R>(column, conversionKind) : OptionalValue<'R> = 
    x.TryGet(column) |> OptionalValue.map (fun v -> Convert.convertType<'R> conversionKind v)

  static member (?) (series:ObjectSeries<_>, name:string) = 
    series.GetAs<float>(name, nan)

  member x.TryAs<'R>(conversionKind) : OptionalValue<Series<_, 'R>> =
    VectorHelpers.tryConvertType conversionKind vector
    |> OptionalValue.map (fun vec -> 
      let newIndex = indexBuilder.Project(index)
      Series(newIndex, vec, vectorBuilder, indexBuilder))

  member x.TryAs<'R>() = x.TryAs<'R>(ConversionKind.Safe)

  member x.As<'R>() =
    let newIndex = indexBuilder.Project(index)
    Series(newIndex, VectorHelpers.convertType<'R> ConversionKind.Flexible vector, vectorBuilder, indexBuilder)


  [<Obsolete("GetValues(bool) is obsolete. Use GetValues(ConversionKind) instead.")>]
  member x.GetValues<'R>(strict) = x.GetValues(if strict then ConversionKind.Exact else ConversionKind.Flexible)
  [<Obsolete("TryAs(bool) is obsolete. Use TryAs(ConversionKind) instead.")>]
  member x.TryAs<'R>(strict) = x.TryAs<'R>(if strict then ConversionKind.Exact else ConversionKind.Flexible)
