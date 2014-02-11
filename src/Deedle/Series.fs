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

/// This enumeration specifeis the behavior of `Union` operation on series when there are
/// overlapping keys in two series that are being unioned. The options include prefering values
/// from the left/right series or throwing an exception when both values are available.
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
type ISeries<'K when 'K : equality> =
  /// Returns the vector containing data of the series (as an untyped vector)
  abstract Vector : Deedle.IVector
  /// Returns the index containing keys of the series 
  abstract Index : IIndex<'K>
  /// Attempts to get the value at a specified key and return it as `obj`
  abstract TryGetObject : 'K -> OptionalValue<obj>


/// The type `Series<K, V>` represents a data series consisting of values `V` indexed by
/// keys `K`. The keys of a series may or may not be ordered 
and
  Series<'K, 'V when 'K : equality>
    ( index:IIndex<'K>, vector:IVector<'V>,
      vectorBuilder : IVectorBuilder, indexBuilder : IIndexBuilder ) as this =
  
  /// Lazy value to hold the number of elements (so that we do not recalculate this all the time)
  let valueCount = Lazy.Create (fun () -> 
    let mutable count = 0
    for _, a in index.Mappings do if vector.GetValue(a).HasValue then count <- count + 1
    count )

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
  member x.Keys = seq { for key, _ in index.Mappings -> key }

  /// Returns a collection of values that are available in the series data.
  /// Note that the length of this sequence does not match the `Keys` sequence
  /// if there are missing values. To get matching sequence, use the `Observations`
  /// property or `Series.observation`.
  ///
  /// [category:Series data]
  member x.Values = seq { 
    for _, a in index.Mappings do 
      let v = vector.GetValue(a) 
      if v.HasValue then yield v.Value }

  /// Returns a collection of observations that form this series. Note that this property
  /// skips over all missing (or NaN) values. Observations are returned as `KeyValuePair<K, V>` 
  /// objects. For an F# alternative that uses tuples, see `Series.observations`.
  ///
  /// [category:Series data]
  member x.Observations = seq {
    for k, a in index.Mappings do
      let v = vector.GetValue(a)
      if v.HasValue then yield KeyValuePair(k, v.Value) }

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
  member x.ValueCount = valueCount.Value

  // ----------------------------------------------------------------------------------------------
  // Accessors and slicing
  // ----------------------------------------------------------------------------------------------

  /// [category:Accessors and slicing]
  member x.GetSubrange(lo, hi) =
    let newIndex, newVector = indexBuilder.GetRange(index, lo, hi, Vectors.Return 0)
    let newVector = vectorBuilder.Build(newVector, [| vector |])
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
    let newIndex = indexBuilder.Create<_>(keys, None)
    let newVector = vectorBuilder.Build(indexBuilder.Reindex(index, newIndex, lookup, Vectors.Return 0, fun addr -> series.Vector.GetValue(addr).HasValue), [| vector |])
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
    let newVector = vectorBuilder.Build(levelCmd, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)
    

  /// Attempts to get a value at the specified 'key'
  ///
  /// [category:Accessors and slicing]
  member x.TryGetObservation(key) = x.TryGetObservation(key, Lookup.Exact)
  ///
  /// [category:Accessors and slicing]
  member x.GetObservation(key) = x.GetObservation(key, Lookup.Exact)
  ///
  /// [category:Accessors and slicing]
  member x.TryGet(key) = x.TryGet(key, Lookup.Exact)
  ///
  /// [category:Accessors and slicing]
  member x.Get(key) = x.Get(key, Lookup.Exact)
  ///
  /// [category:Accessors and slicing]
  member x.TryGetAt(index) = 
    x.Vector.GetValue(Address.ofInt index)
  /// [category:Accessors and slicing]
  member x.GetKeyAt(index) = 
    x.Index.KeyAt(Address.ofInt index)
  /// [category:Accessors and slicing]
  member x.GetAt(index) = 
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
      |> Array.ofSeq |> Array.choosei (fun i (key, addr) ->
          let opt = vector.GetValue(addr)
          // If a required value is missing, then skip over this
          if opt.HasValue && f.Invoke (KeyValuePair(key, opt.Value), i)
            then Some(key, opt) else None)
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateMissing(optValues),
        vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Where(f:System.Func<KeyValuePair<'K, 'V>, bool>) = 
    x.Where(fun kvp _ -> f.Invoke kvp)

  /// [category:Projection and filtering]
  member x.WhereOptional(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          if f.Invoke (KeyValuePair(key, opt)) then yield key, opt |]
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateMissing(optValues),
        vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, int, 'R>) = 
    let newVector =
      index.Mappings 
      |> Seq.mapi (fun i (key, addr) ->
           vector.GetValue(addr) |> OptionalValue.bind (fun v -> 
             // If a required value is missing, then skip over this
             try OptionalValue(f.Invoke(KeyValuePair(key, v), i))
             with :? MissingValueException -> OptionalValue.Missing )) 
      |> Array.ofSeq
    let newIndex = indexBuilder.Project(index)
    Series<'K, 'R>(newIndex, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, 'R>) = 
    x.Select(fun kvp _ -> f.Invoke kvp)

  /// [category:Projection and filtering]
  member x.SelectKeys<'R when 'R : equality>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, 'R>) = 
    let newKeys =
      [| for key, addr in index.Mappings -> 
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))) |]
    let newIndex = indexBuilder.Create(newKeys, None)
    Series<'R, _>(newIndex, vector, vectorBuilder, indexBuilder )

  /// [category:Projection and filtering]
  member x.SelectOptional<'R>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, OptionalValue<'R>>) = 
    let newVector =
      index.Mappings |> Array.ofSeq |> Array.map (fun (key, addr) ->
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))))
    let newIndex = indexBuilder.Project(index)
    Series<'K, 'R>(newIndex, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  /// [category:Projection and filtering]
  member x.Reversed =
    let newIndex = index.Keys |> Array.ofSeq |> Array.rev
    let newVector = vector.DataSequence |> Array.ofSeq |> Array.rev
    Series(Index.ofKeys newIndex, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Appending, joining etc
  // ----------------------------------------------------------------------------------------------

  /// [category:Appending, joining and zipping]
  member series.Append(otherSeries:Series<'K, 'V>) =
    // Append the row indices and get transformation that combines two column vectors
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newIndex, cmd = 
      indexBuilder.Append( (index, Vectors.Return 0), (otherSeries.Index, Vectors.Return 1), 
                           VectorValueTransform.LeftOrRight )
    let newVector = vectorBuilder.Build(cmd, [| series.Vector; otherSeries.Vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  /// [category:Appending, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>) =
    series.Zip(otherSeries, JoinKind.Outer, Lookup.Exact)

  /// [category:Appending, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>, kind) =
    series.Zip(otherSeries, kind, Lookup.Exact)

  member private series.ZipHelper(otherSeries:Series<'K, 'V2>, kind, lookup) =
    // Union row indices and get transformations to apply to left/right vectors
    let newIndex, thisRowCmd, otherRowCmd = 
      createJoinTransformation indexBuilder kind lookup index otherSeries.Index (Vectors.Return 0) (Vectors.Return 0)

    let lVec = vectorBuilder.Build(thisRowCmd, [| series.Vector |])
    let rVec = vectorBuilder.Build(otherRowCmd, [| otherSeries.Vector |])

    newIndex, lVec, rVec

  /// [category:Appending, joining and zipping]
  member series.Zip<'V2>(otherSeries:Series<'K, 'V2>, kind, lookup) : Series<'K, 'V opt * 'V2 opt> =
    let newIndex, lVec, rVec = series.ZipHelper(otherSeries, kind, lookup)
    let zipv = rVec.DataSequence |> Seq.zip lVec.DataSequence |> Vector.ofValues
    Series(newIndex, zipv, vectorBuilder, indexBuilder)

  /// [category:Appending, joining and zipping]
  member series.ZipInner<'V2>(otherSeries:Series<'K, 'V2>) : Series<'K, 'V * 'V2> =
    let newIndex, lVec, rVec = series.ZipHelper(otherSeries, JoinKind.Inner, Lookup.Exact)
    let zipv = rVec.Data.Values |> Seq.zip lVec.Data.Values |> Vector.ofValues
    Series(newIndex, zipv, vectorBuilder, indexBuilder)

  /// [category:Appending, joining and zipping]
  member series.Union(another:Series<'K, 'V>) = 
    series.Union(another, UnionBehavior.PreferLeft)
  
  /// [category:Appending, joining and zipping]
  member series.Union(another:Series<'K, 'V>, behavior) = 
    let newIndex, vec1, vec2 = indexBuilder.Union( (series.Index, Vectors.Return 0), (another.Index, Vectors.Return 1) )
    let transform = 
      match behavior with
      | UnionBehavior.PreferRight -> VectorHelpers.VectorValueTransform.RightIfAvailable
      | UnionBehavior.Exclusive -> VectorHelpers.VectorValueTransform.LeftOrRight
      | _ -> VectorHelpers.VectorValueTransform.LeftIfAvailable
    let vecCmd = Vectors.Combine(vec1, vec2, transform)
    let newVec = vectorBuilder.Build(vecCmd, [| series.Vector; another.Vector |])
    Series(newIndex, newVec, vectorBuilder, indexBuilder)

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
        ( x.Index, keys, direction, Vectors.Return 0, 
          (fun (key, (index, cmd)) -> 
              let window = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              OptionalValue(valueSelector.Invoke(key, window))),
          (fun (key, (index, cmd)) -> 
              keySelector.Invoke(key, Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder))) )
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
                let actualVector = vectorBuilder.Build(cmd, [| vector |])
                let obs = [ for k, addr in index.Mappings -> actualVector.GetValue(addr) ]
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
              let series = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
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
              let series = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              let segment = DataSegment(kind, series)
              // Call key & value selectors to produce the result
              let (KeyValue(newKey, newValue)) = observationSelector.Invoke(segment)
              newKey, newValue ))
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Groups a series (ordered or unordered) using the specified key selector (`keySelector`) 
  /// and then aggregates each group into a single value, returned in the resulting series,
  /// using the provided `valueSelector` function.
  ///
  /// ## Parameters
  ///  - `keySelector` - Generates a new key that is used for aggregation, based on the original 
  ///    key and value. The new key must support equality testing.
  ///  - `valueSelector` - A value selector function that is called to aggregate 
  ///    each group of collected elements.
  ///
  /// [category:Windowing, chunking and grouping]
  member x.GroupBy(keySelector:Func<_, _>, valueSelector:Func<_, _>) =
    let newIndex, newVector = 
      indexBuilder.GroupBy
        ( x.Index, 
          (fun key -> 
              x.TryGet(key) |> OptionalValue.map (fun v -> 
                let kvp = KeyValuePair(key, v)
                keySelector.Invoke(kvp))), Vectors.Return 0, 
          (fun (newKey, (index, cmd)) -> 
              let group = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              let kvp = KeyValuePair(newKey, group)
              valueSelector.Invoke(kvp) ) )
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Indexing
  // ----------------------------------------------------------------------------------------------

  /// [category:Indexing]
  member x.Realign(newKeys) = 
    let findAll getter = seq {
      for k in newKeys -> 
        match index.Locate(k) with
        | addr when addr >= 0L -> getter addr
        | _                    -> OptionalValue.Missing }
    let newIndex = Index.ofKeys newKeys
    let newVector = findAll x.Vector.GetValue |> Vector.ofOptionalValues
    Series<_,_>(newIndex, newVector, vectorBuilder, indexBuilder)

  /// Replace the index of the series with ordinarilly generated integers starting from zero.
  /// The elements of the series are assigned index according to the current order, or in a
  /// non-deterministic way, if the current index is not ordered.
  ///
  /// [category:Indexing]
  member x.IndexOrdinally() = 
    let newIndex = indexBuilder.Create(x.Index.Keys |> Seq.mapi (fun i _ -> i), Some true)
    Series<int, _>(newIndex, vector, vectorBuilder, indexBuilder)

  /// [category:Indexing]
  member x.IndexWith(keys) = 
    let newIndex = indexBuilder.Create(keys, None)
    Series<'TNewKey, _>(newIndex, vector, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Asynchronous support 
  // ----------------------------------------------------------------------------------------------
    
  member x.AsyncMaterialize() = async {
    let newIndexAsync, cmd = indexBuilder.AsyncMaterialize(index, Vectors.Return 0)
    let! newIndex = newIndexAsync
    let! newVector = vectorBuilder.AsyncBuild(cmd, [| vector |])
    return Series<_, _>(newIndex, newVector, vectorBuilder, indexBuilder) }

  member x.MaterializeAsync() = 
    x.AsyncMaterialize() |> Async.StartAsTask

  member x.Materialize() = 
    let newIndex = indexBuilder.Project(index)
    let newVector = vector.Select id
    Series<_, _>(newIndex, newVector, vectorBuilder, indexBuilder)
    
  // ----------------------------------------------------------------------------------------------
  // Operators and F# functions
  // ----------------------------------------------------------------------------------------------

  static member inline internal NullaryGenericOperation<'K, 'T1, 'T2>(series:Series<'K, 'T1>, op : 'T1 -> 'T2) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal NullaryOperation<'T>(series:Series<'K, 'T>, op : 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal ScalarOperationL<'T>(series:Series<'K, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline internal ScalarOperationR<'T>(scalar, series:Series<'K, 'T>, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op scalar v)

  static member inline internal VectorOperation<'T>(series1:Series<'K,'T>, series2:Series<'K,'T>, op): Series<_, 'T> =
    let newIndex, lVec, rVec = series1.ZipHelper(series2, JoinKind.Outer, Lookup.Exact)
    
    let vector = 
      Seq.zip lVec.DataSequence rVec.DataSequence 
      |> Seq.map (function 
        | OptionalValue.Present a, OptionalValue.Present b -> OptionalValue(op a b)
        | _ -> OptionalValue.Missing)
      |> Vector.ofOptionalValues

    Series(newIndex, vector, series1.VectorBuilder, series1.IndexBuilder)

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
  static member Acos(series) = Series<'K, _>.NullaryOperation<float>(series, acos)
  /// [category:Operators]
  static member Asin(series) = Series<'K, _>.NullaryOperation<float>(series, asin)
  /// [category:Operators]
  static member Atan(series) = Series<'K, _>.NullaryOperation<float>(series, atan)
  /// [category:Operators]
  static member Sin(series) = Series<'K, _>.NullaryOperation<float>(series, sin)
  /// [category:Operators]
  static member Sinh(series) = Series<'K, _>.NullaryOperation<float>(series, sinh)
  /// [category:Operators]
  static member Cos(series) = Series<'K, _>.NullaryOperation<float>(series, cos)
  /// [category:Operators]
  static member Cosh(series) = Series<'K, _>.NullaryOperation<float>(series, cosh)
  /// [category:Operators]
  static member Tan(series) = Series<'K, _>.NullaryOperation<float>(series, tan)
  /// [category:Operators]
  static member Tanh(series) = Series<'K, _>.NullaryOperation<float>(series, tanh)

  // Actually useful

  /// [category:Operators]
  static member Abs(series) = Series<'K, _>.NullaryOperation<float>(series, abs)
  /// [category:Operators]
  static member Abs(series) = Series<'K, _>.NullaryOperation<int>(series, abs)
  /// [category:Operators]
  static member Ceiling(series) = Series<'K, _>.NullaryOperation<float>(series, ceil)
  /// [category:Operators]
  static member Exp(series) = Series<'K, _>.NullaryOperation<float>(series, exp)
  /// [category:Operators]
  static member Floor(series) = Series<'K, _>.NullaryOperation<float>(series, floor)
  /// [category:Operators]
  static member Truncate(series) = Series<'K, _>.NullaryOperation<float>(series, truncate)
  /// [category:Operators]
  static member Log(series) = Series<'K, _>.NullaryOperation<float>(series, log)
  /// [category:Operators]
  static member Log10(series) = Series<'K, _>.NullaryOperation<float>(series, log10)
  /// [category:Operators]
  static member Round(series) = Series<'K, _>.NullaryOperation<float>(series, round)
  /// [category:Operators]
  static member Sign(series) = Series<'K, _>.NullaryGenericOperation<_, float, _>(series, sign)
  /// [category:Operators]
  static member Sqrt(series) = Series<'K, _>.NullaryGenericOperation<_, float, _>(series, sqrt)

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
    member x.TryGetObject(k) = this.TryGet(k) |> OptionalValue.map box
    member x.Vector = vector :> IVector
    member x.Index = index

  override series.ToString() =
    if vector.SuppressPrinting then "(Suppressed)" else
      seq { for item in series.Observations |> Seq.startAndEnd Formatting.StartInlineItemCount Formatting.EndInlineItemCount ->
              match item with 
              | Choice2Of3() -> " ... "
              | Choice1Of3(KeyValue(k, v)) | Choice3Of3(KeyValue(k, v)) -> sprintf "%O => %O" k v }
      |> String.concat "; "
      |> sprintf "series [ %s]" 

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows. The property is used 
  /// automatically by F# Interactive.
  member series.Format() = 
    let getLevel ordered previous reset maxLevel level (key:'K) = 
      let levelKey = 
        if level = 0 && maxLevel = 0 then box key
        else CustomKey.Get(key).GetLevel(level)
      if ordered && (Some levelKey = !previous) then "" 
      else previous := Some levelKey; reset(); levelKey.ToString()

    if vector.SuppressPrinting then "(Suppressed)" else
      let key = series.Index.Keys |> Seq.headOrNone
      match key with 
      | None -> "(Empty)"
      | Some key ->
          let levels = CustomKey.Get(key).Levels
          let previous = Array.init levels (fun _ -> ref None)
          let reset i () = for j in i + 1 .. levels - 1 do previous.[j] := None
          seq { for item in index.Mappings |> Seq.startAndEnd Formatting.StartItemCount Formatting.EndItemCount  do
                  match item with 
                  | Choice1Of3(k, a) | Choice3Of3(k, a) -> 
                      let v = vector.GetValue(a)
                      yield [ 
                        // Yield all row keys
                        for level in 0 .. levels - 1 do 
                          yield getLevel series.Index.IsOrdered previous.[level] (reset level) levels level k
                        yield "->"
                        yield v.ToString() ]
                  | Choice2Of3() -> 
                      yield [ 
                        yield "..."
                        for level in 1 .. levels - 1 do yield ""
                        yield "->"
                        yield "..." ] }
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
    Series( Index.ofKeys keys, vectorBuilder.Create (Array.ofSeq values),
            vectorBuilder, indexBuilder )

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

type ObjectSeries<'K when 'K : equality> internal(index:IIndex<_>, vector, vectorBuilder, indexBuilder) = 
  inherit Series<'K, obj>(index, vector, vectorBuilder, indexBuilder)

  new(series:Series<'K, obj>) = 
    ObjectSeries<_>(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder)

  member x.GetValues<'R>(strict) = 
    if strict then System.Linq.Enumerable.OfType<'R>(x.Values)
    else x.Values |> Seq.choose (fun v ->
      try Some(Convert.changeType<'R> v) 
      with _ -> None)

  member x.GetValues<'R>() = x.GetValues<'R>(true)

  member x.GetAs<'R>(column) : 'R = 
    Convert.changeType<'R> (x.Get(column))

  member x.GetAs<'R>(column, fallback) : 'R =
    let address = index.Lookup(column, Lookup.Exact, fun _ -> true) 
    match address with
    | OptionalValue.Present a -> 
        match (vector.GetValue(snd a)) with
        | OptionalValue.Present v -> Convert.changeType<'R> v
        | OptionalValue.Missing   -> fallback
    | OptionalValue.Missing -> keyNotFound column

  member x.GetAtAs<'R>(index) : 'R = 
    Convert.changeType<'R> (x.GetAt(index))

  member x.TryGetAs<'R>(column) : OptionalValue<'R> = 
    x.TryGet(column) |> OptionalValue.map (fun v -> Convert.changeType<'R> v)

  static member (?) (series:ObjectSeries<_>, name:string) = 
    series.GetAs<float>(name, nan)

  member x.TryAs<'R>(strict) =
    match box vector with
    | :? IVector<'R> as vec -> 
        let newIndex = indexBuilder.Project(index)
        OptionalValue(Series(newIndex, vec, vectorBuilder, indexBuilder))
    | _ -> 
        ( if strict then VectorHelpers.tryCastType vector
          else               
            let attempt = VectorHelpers.tryChangeType vector
            if not attempt.HasValue then 
              try VectorHelpers.tryCastType vector
              with :? InvalidCastException -> OptionalValue.Missing
            else attempt
        )
        |> OptionalValue.map (fun vec -> 
          let newIndex = indexBuilder.Project(index)
          Series(newIndex, vec, vectorBuilder, indexBuilder))

  member x.TryAs<'R>() =
    x.TryAs<'R>(false)

  member x.As<'R>() =
    let newIndex = indexBuilder.Project(index)
    match box vector with
    | :? IVector<'R> as vec -> Series(newIndex, vec, vectorBuilder, indexBuilder)
    | _ -> Series(newIndex, VectorHelpers.changeType vector, vectorBuilder, indexBuilder)
