#nowarn "77" // Static constraint in Series.sample requires special + operator

namespace FSharp.DataFrame
open FSharp.DataFrame.Keys

/// Series module comment..
/// 
/// ## Example
/// Not really
///
/// ## Lookup, resampling and scaling
/// More stuff here
///
/// ## Windowing, chunking and grouping
/// The functions with name starting with `windowed` take a series and generate floating 
/// (overlapping) windows. The `chunk` functions 
///
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series = 
  open System
  open System.Linq
  open FSharp.DataFrame.Internal
  open FSharp.DataFrame.Vectors
  open MathNet.Numerics.Statistics

  // Non-public helper
  let inline private streamingAggregation f (series:Series<_, _>) =
    match series.Vector.Data with
    | VectorData.DenseList list -> f (list :> seq<_>)
    | VectorData.SparseList list -> f (Seq.choose OptionalValue.asOption list)
    | VectorData.Sequence seq -> f (Seq.choose OptionalValue.asOption seq)

  let inline private fastAggregation flist foptlist fseq (series:Series<_, _>) =
    match series.Vector.Data with
    | VectorData.DenseList list -> flist list
    | VectorData.SparseList list -> foptlist list
    | VectorData.Sequence seq -> fseq (Seq.choose OptionalValue.asOption seq)

  /// [omit]
  /// Does stuff
  let inline fastStatBy keySelector flist foptlist fseq (series:Series<_, _>) : Series<_, _> = 
    series.GroupBy
      ( (fun key ser -> keySelector key),
        (fun key ser -> OptionalValue(fastAggregation flist foptlist fseq ser)))

  [<CompiledName("Statistic")>]
  let inline stat op (series:Series<'K, _>) = 
    series |> streamingAggregation op

  [<CompiledName("Sum")>]
  let inline sum (series:Series<_, _>) = 
    series |> fastAggregation IReadOnlyList.sum IReadOnlyList.sumOptional Seq.sum

  [<CompiledName("Mean")>]
  let inline mean (series:Series<_, _>) = 
    series |> fastAggregation IReadOnlyList.average IReadOnlyList.averageOptional Seq.average

  [<CompiledName("StandardDeviation")>]
  let inline sdv (series:Series<'K, float>) = series |> stat Statistics.StandardDeviation 

  [<CompiledName("Median")>]
  let inline median (series:Series<'K, float>) = series |> stat Statistics.Median


  [<CompiledName("StatisticBy")>]
  let inline statBy keySelector op (series:Series<_, _>) : Series<_, _> = 
    series.GroupBy
      ( (fun key ser -> keySelector key),
        (fun key ser -> OptionalValue(stat op ser)))

  [<CompiledName("SumBy")>]
  let inline sumBy keySelector (series:Series<_, _>) = 
    series |> fastStatBy keySelector IReadOnlyList.sum IReadOnlyList.sumOptional Seq.sum

  [<CompiledName("CountBy")>]
  let inline countBy keySelector (series:Series<_, _>) = 
    series |> fastStatBy keySelector IReadOnlyList.length IReadOnlyList.lengthOptional Seq.length

  [<CompiledName("MeanBy")>]
  let inline meanBy keySelector (series:Series<_, _>) = 
    series |> fastStatBy keySelector IReadOnlyList.average IReadOnlyList.averageOptional Seq.average

  [<CompiledName("StandardDeviationBy")>]
  let inline sdvBy keySelector (series:Series<_, float>) = 
    series |> statBy keySelector Statistics.StandardDeviation 

  [<CompiledName("MedianBy")>]
  let inline medianBy keySelector (series:Series<_, float>) = 
    series |> statBy keySelector Statistics.Median


  let foldBy keySelector op (series:Series<_, _>) = 
    series.GroupBy
      ( (fun key ser -> keySelector key),
        (fun key ser -> OptionalValue(op ser)))
    

  /// Return observations with available values. The operation skips over 
  /// all keys with missing values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  let observations (series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings do
      let v = series.Vector.GetValue(address)
      if v.HasValue then yield key, v.Value }
  
  /// Returns all keys from the sequence, together with the associated (optional) values. 
  let observationsAll (series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings ->
      key, OptionalValue.asOption (series.Vector.GetValue(address)) }

  /// Create a new series that contains values for all provided keys.
  /// Use the specified lookup semantics - for exact matching, use `getAll`
  let lookupAll keys lookup (series:Series<'K, 'T>) = series.GetItems(keys, lookup)

  /// Create a new series that contains values for all provided keys.
  /// Uses exact lookup semantics for key lookup - use `lookupAll` for more options
  let getAll keys (series:Series<'K, 'T>) = series.GetItems(keys)

  /// Get the value for the specified key.
  /// Use the specified lookup semantics - for exact matching, use `get`
  let lookup key lookup (series:Series<'K, 'T>) = series.Get(key, lookup)

  /// Get the value for the specified key.
  /// Uses exact lookup semantics for key lookup - use `lookupAll` for more options
  let get key (series:Series<'K, 'T>) = series.Get(key)

  let tryLookup key lookup (series:Series<'K, 'T>) = series.TryGet(key, lookup) |> OptionalValue.asOption

  let tryGet key (series:Series<'K, 'T>) = series.TryGet(key) |> OptionalValue.asOption

  let tryGetAt index (series:Series<'K, 'T>) = series.TryGetAt(index) |> OptionalValue.asOption
  let getAt index (series:Series<'K, 'T>) = series.GetAt(index)

  let realign keys (series:Series<'K, 'T>) = 
    series.Realign(keys)

  let indexOrdinal (series:Series<'K, 'T>) = 
    series.IndexWithOrdinals()

  let indexKeys (keys:seq<'K2>) (series:Series<'K1, 'T>) = 
    series.IndexWithKeys(keys)

  let filter f (series:Series<'K, 'T>) = 
    series.Where(fun kvp -> f kvp.Key kvp.Value)

  let filterValues f (series:Series<'K, 'T>) = 
    series.Where(fun kvp -> f kvp.Value)

  let map (f:'K -> 'T -> 'R) (series:Series<'K, 'T>) = 
    series.Select(fun kvp -> f kvp.Key kvp.Value)

  let mapValues (f:'T -> 'R) (series:Series<'K, 'T>) = 
    series.Select(fun kvp -> f kvp.Value)

  let mapKeys (f:'K -> 'R) (series:Series<'K, 'T>) = 
    series.SelectKeys(fun kvp -> f kvp.Key)

  let filterAll f (series:Series<'K, 'T>) = 
    series.WhereOptional(fun kvp -> f kvp.Key (OptionalValue.asOption kvp.Value))

  let mapAll (f:_ -> _ -> option<'R>) (series:Series<'K, 'T>) = 
    series.SelectOptional(fun kvp -> 
      f kvp.Key (OptionalValue.asOption kvp.Value) |> OptionalValue.ofOption)

  let pairwise (series:Series<'K, 'T>) = 
    series.Pairwise() |> map (fun k v -> v.Data)
  
  let pairwiseWith f (series:Series<'K, 'T>) = 
    series.Pairwise() |> map (fun k v -> f k v.Data)

  /// `result[k] = series[k] - series[k - offset]`
  let inline diff offset (series:Series<'K, 'T>) = 
    series.Aggregate
      ( WindowSize((abs offset) + 1, Boundary.Skip), 
        (fun ks -> if offset < 0 then ks.Data.Keys.First() else ks.Data.Keys.Last() ),
        (fun ds ->  
          let h, t = ds.Data.Values.First(), ds.Data.Values.Last() in 
          if offset < 0 then h - t else t - h) )

  /// Stuff
  /// [category:Windowing, chunking and grouping]
  let aggregate aggregation keySelector (series:Series<'K, 'T>) : Series<'TNewKey, _> =
    series.Aggregate
      ( aggregation, System.Func<_, _>(keySelector), System.Func<_, _>(id))

  let aggregateInto aggregation keySelector valueSelector (series:Series<'K, 'T>) : Series<'TNewKey, 'R> =
    series.Aggregate
      ( aggregation, System.Func<_, _>(keySelector), System.Func<_, _>(valueSelector))

  // Window based on size

  let windowSizeInto bounds f (series:Series<'K, 'T>) : Series<'K, 'R> =
    let dir = if snd bounds = Boundary.AtEnding then Direction.Forward else Direction.Backward
    let keySel = System.Func<DataSegment<Series<_, _>>, _>(fun data -> 
      if dir = Direction.Backward then data.Data.Index.Keys |> Seq.last
      else data.Data.Index.Keys |> Seq.head )
    series.Aggregate(WindowSize(bounds), keySel, (fun ds -> f ds))

  let inline windowSize bounds (series:Series<'K, 'T>) = 
    windowSizeInto bounds DataSegment.data series 

  // Based on distance

  let inline windowDistInto distance f (series:Series<'K, 'T>) =
    series.Aggregate(WindowWhile(fun skey ekey -> (ekey - skey) < distance), (fun d -> d.Data.Keys |> Seq.head), fun ds -> f ds.Data)
  let inline windowDist distance (series:Series<'K, 'T>) = 
    windowDistInto distance id series 

  // Window using while

  let inline windowWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(WindowWhile(cond), (fun d -> d.Data.Keys |> Seq.head), fun ds -> f ds.Data)
  let inline windowWhile cond (series:Series<'K, 'T>) = 
    windowWhileInto cond id series 

  // Chunk based on size

  let inline chunkSizeInto bounds f (series:Series<'K, 'T>) : Series<'K, 'R> =
    series.Aggregate(ChunkSize(bounds), (fun d -> d.Data.Keys |> Seq.head), fun ds -> f ds)
  let inline chunkSize bounds (series:Series<'K, 'T>) = 
    chunkSizeInto bounds DataSegment.data series 

  // Chunk based on distance

  let inline chunkDistInto (distance:^D) f (series:Series<'K, 'T>) : Series<'K, 'R> =
    series.Aggregate(ChunkWhile(fun skey ekey -> (ekey - skey) < distance), (fun d -> d.Data.Keys |> Seq.head), fun ds -> f ds.Data)
  let inline chunkDist (distance:^D) (series:Series<'K, 'T>) = 
    chunkDistInto distance id series 

  // Chunk while

  let inline chunkWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(ChunkWhile(cond), (fun d -> d.Data.Keys |> Seq.head), fun ds -> f ds.Data)
  let inline chunkWhile cond (series:Series<'K, 'T>) = 
    chunkWhileInto cond id series 

  // Skipping most-common case functions

  let inline windowInto size f (series:Series<'K, 'T>) : Series<'K, 'R> =
    windowSizeInto (size, Boundary.Skip) (DataSegment.data >> f) series
  let inline window size (series:Series<'K, 'T>) =
    windowSize (size, Boundary.Skip) series

  let inline chunkInto size f (series:Series<'K, 'T>) : Series<'K, 'R> =
    chunkSizeInto (size, Boundary.Skip) (DataSegment.data >> f) series
  let inline chunk size (series:Series<'K, 'T>) =
    chunkSize (size, Boundary.Skip) series

  // Grouping

  let groupInto (keySelector:'K -> 'T -> 'TNewKey) f (series:Series<'K, 'T>) : Series<'TNewKey, 'TNewValue> =
    series.GroupBy(keySelector, fun k s -> OptionalValue(f k s))

  let groupBy (keySelector:'K -> 'T -> 'TNewKey) (series:Series<'K, 'T>) =
    groupInto keySelector (fun k s -> s) series

  // Unioning

  let union (series1:Series<'K, 'V>) (series2:Series<'K, 'V>) = 
    series1.Union(series2)

  let unionUsing behavior (series1:Series<'K, 'V>) (series2:Series<'K, 'V>) = 
    series1.Union(series2, behavior)

  // ----------------------------------------------------------------------------------------------
  // Handling of missing values
  // ----------------------------------------------------------------------------------------------

  let dropMissing (series:Series<'K, 'T>) = 
    series.WhereOptional(fun (KeyValue(k, v)) -> v.HasValue)

  /// Fill missing values in the series using the specified function.
  /// The specified function is called with all keys for which the series
  /// does not contain value and the result of the call is used in place 
  /// of the missing value. 
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `f` - A function that takes key `K` and generates a value to be
  ///    used in a place where the original series contains a missing value.
  ///
  /// ## Remarks
  /// This function can be used to implement more complex interpolation.
  /// For example see [handling missing values in the tutorial](../features.html#missing)
  let fillMissingUsing f (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> Some(f k)
      | value -> value)

  /// Fill missing values in the series with a constant value.
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `value` - A constant value that is used to fill all missing values
  let fillMissingWith value (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> Some(value)
      | value -> value)

  /// Fill missing values in the series with the nearest available value
  /// (using the specified direction). Note that the series may still contain
  /// missing values after call to this function. This operation can only be
  /// used on ordered series.
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  ///
  /// ## Example
  ///
  ///     let sample = Series.ofValues [ Double.NaN; 1.0; Double.NaN; 3.0 ]
  ///
  ///     // Returns a series consisting of [1; 1; 3; 3]
  ///     sample |> Series.fillMissing Direction.Backward
  ///
  ///     // Returns a series consisting of [<missing>; 1; 1; 3]
  ///     sample |> Series.fillMissing Direction.Forward 
  ///
  let fillMissing direction (series:Series<'K, 'T>) = 
    let lookup = if direction = Direction.Forward then Lookup.NearestSmaller else Lookup.NearestGreater
    series |> mapAll (fun k -> function 
      | None -> series.TryGet(k, lookup) |> OptionalValue.asOption
      | value -> value)

  // ----------------------------------------------------------------------------------------------
  // Resampling and similar stuff
  // ----------------------------------------------------------------------------------------------

  /// Resample the series based on a provided collection of keys. The values of the series
  /// are aggregated into chunks based on the specified keys. Depending on `direction`, the 
  /// specified key is either used as the smallest or as the greatest key of the chunk (with
  /// the exception of boundaries that are added to the first/last chunk).
  /// Such chunks are then aggregated using the provided function `f`.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `keys` - A collection of keys to be used for resampling of the series
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `f` - A function that is used to collapse a generated chunk into a 
  ///    single value. Note that this function may be called with empty series.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  ///
  /// [category:Lookup, resampling and scaling]
  let resampleInto keys dir f (series:Series<'K, 'V>) =
    series.Resample(keys, dir, (fun k s -> f k s))

  /// Resample the series based on a provided collection of keys. The values of the series
  /// are aggregated into chunks based on the specified keys. Depending on `direction`, the 
  /// specified key is either used as the smallest or as the greatest key of the chunk (with
  /// the exception of boundaries that are added to the first/last chunk).
  /// Such chunks are then returned as nested series.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `keys` - A collection of keys to be used for resampling of the series
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  ///
  /// [category:Lookup, resampling and scaling]
  let resample keys dir (series:Series<'K, 'V>) =
    resampleInto keys dir (fun k s -> s) series

  /// Resample the series based on equivalence class on the keys. A specified function
  /// `keyProj` is used to project keys to another space and the observations for which the 
  /// projected keys are equivalent are grouped into chunks. The chunks are then transformed
  /// to values using the provided function `f`.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///  - `f` - A function that is used to collapse a generated chunk into a 
  ///    single value. 
  ///
  /// ## Remarks
  /// This function is similar to `Series.chunkBy`, with the exception that it transforms
  /// keys to a new space.
  ///
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. For unordered
  /// series, similar functionality can be implemented using `Series.groupBy`.
  ///
  /// [category:Lookup, resampling and scaling]
  let inline resampleEquivInto (keyProj:'K1 -> 'K2) (f:_ -> 'V2) (series:Series<'K1, 'V1>) =
    series 
    |> chunkWhile (fun k1 k2 -> keyProj k1 = keyProj k2)
    |> mapKeys keyProj
    |> mapValues f

  /// Resample the series based on equivalence class on the keys. A specified function
  /// `keyProj` is used to project keys to another space and the observations for which the 
  /// projected keys are equivalent are grouped into chunks. The chunks are then returned
  /// as nested series.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///
  /// ## Remarks
  /// This function is similar to `Series.chunkBy`, with the exception that it transforms
  /// keys to a new space.
  ///
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. For unordered
  /// series, similar functionality can be implemented using `Series.groupBy`.
  ///
  /// [category:Lookup, resampling and scaling]
  let inline resampleEquiv (keyProj:'K1 -> 'K2) (series:Series<'K1, 'V1>) =
    resampleEquivInto keyProj id series

  /// Resample the series based on equivalence class on the keys and also generate values 
  /// for all keys of the target space that are between the minimal and maximal key of the
  /// specified series (e.g. generate value for all days in the range covered by the series).
  /// A specified function `keyProj` is used to project keys to another space and `nextKey`
  /// is used to generate all keys in the range. The chunk is then aggregated using `f`.
  ///
  /// When there are no values for a (generated) key, then the function behaves according to
  /// `fillMode`. It can look at the greatest value of previous chunk or smallest value of the
  /// next chunk, or it produces an empty series.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `fillMode` - When set to `Lookup.NearestSmaller` or `Lookup.NearestGreater`, 
  ///     the function searches for a nearest available observation in an neighboring chunk.
  ///     Otherwise, the function `f` is called with an empty series as an argument.
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///  - `nextKey` - A function that gets the next key in the transformed space
  ///  - `f` - A function that is used to collapse a generated chunk into a 
  ///    single value. The function may be called on empty series when `fillMode` is
  ///    `Lookup.Exact`.
  ///    
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  ///
  /// [category:Lookup, resampling and scaling]
  let resampleUniformInto (fillMode:Lookup) (keyProj:'K1 -> 'K2) (nextKey:'K2 -> 'K2) f (series:Series<'K1, 'V>) =
    let min, max = series.KeyRange
    
    // Generate keys of the new space that fit in the range of the series
    let min, max = keyProj min, keyProj max
    let keys = min |> Seq.unfold (fun dt -> 
      if dt <= max then Some(dt, nextKey dt) else None) 
    
    // Chunk existing values and align them to the new keys
    let reindexed = 
      series 
      |> chunkWhile (fun k1 k2 -> keyProj k1 = keyProj k2)
      |> mapKeys keyProj
      |> realign keys

    // For keys that have no values, use empty series or singleton series from above/below
    reindexed
    |> fillMissingUsing (fun k ->
        match fillMode with
        | Lookup.NearestSmaller ->
            let res = reindexed.Get(k, fillMode)
            Series([res.KeyRange |> snd], [res.[res.KeyRange |> snd]])
        | Lookup.NearestGreater ->
            let res = reindexed.Get(k, fillMode)
            Series([res.KeyRange |> fst], [res.[res.KeyRange |> fst]]) 
        | _ -> Series([], [])  )
    |> mapValues f

  /// Resample the series based on equivalence class on the keys and also generate values 
  /// for all keys of the target space that are between the minimal and maximal key of the
  /// specified series (e.g. generate value for all days in the range covered by the series).
  /// A specified function `keyProj` is used to project keys to another space and `nextKey`
  /// is used to generate all keys in the range. Then return the chunks as nested series.
  ///
  /// When there are no values for a (generated) key, then the function behaves according to
  /// `fillMode`. It can look at the greatest value of previous chunk or smallest value of the
  /// next chunk, or it produces an empty series.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `fillMode` - When set to `Lookup.NearestSmaller` or `Lookup.NearestGreater`, 
  ///     the function searches for a nearest available observation in an neighboring chunk.
  ///     Otherwise, the function `f` is called with an empty series as an argument.
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///  - `nextKey` - A function that gets the next key in the transformed space
  ///    
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  ///
  /// [category:Lookup, resampling and scaling]
  let resampleUniform fillMode (keyProj:'K1 -> 'K2) (nextKey:'K2 -> 'K2) (series:Series<'K1, 'V>) =
    resampleUniformInto fillMode keyProj nextKey id series

  /// [omit]
  /// Module that contains an implementation of sampling for `sampleTime` and 
  /// `sampleTimeInto`. For technical reasons (`inline`) this is public..
  module Implementation = 

    /// Given a specified starting time and time span, generates all keys that fit in the
    /// range of the series (and one additional, if `dir = Backward`) and then performs
    /// sampling using `resampleInto`.
    let sampleTimeIntoInternal add startOpt (interval:'T) dir f (series:Series<'K , 'V>) =
      let comparer = System.Collections.Generic.Comparer<'K>.Default
      let smallest, largest = series.KeyRange
      let keys =
        let rec genKeys current = seq {
          // For Backward, we need one more key after the end
          if dir = Direction.Backward then yield current
          if comparer.Compare(current, largest) <= 0 then
            // For Forward, we only want keys in the range
            if dir = Direction.Forward then yield current
            yield! genKeys (add current interval) }
        genKeys (defaultArg startOpt smallest)
      resampleInto keys dir f series

  /// TODO
  /// [category:Lookup, resampling and scaling]
  let inline sampleTimeInto interval dir f (series:Series< ^K , ^V >) = 
    let add dt ts = (^K: (static member (+) : ^K * TimeSpan -> ^K) (dt, ts))
    Implementation.sampleTimeIntoInternal add None interval dir (fun _ -> f) series

  /// TODO
  /// [category:Lookup, resampling and scaling]
  let inline sampleTime interval dir series = sampleTimeInto interval dir id series
  //*)

  let lastKey (series:Series< 'K , 'V >) = series.KeyRange |> snd
  let firstKey (series:Series< 'K , 'V >) = series.KeyRange |> fst
  let lastValue (series:Series< 'K , 'V >) = series |> get (series.KeyRange |> snd)
  let firstValue (series:Series< 'K , 'V >) = series |> get (series.KeyRange |> fst)

  // ----------------------------------------------------------------------------------------------
  // Counting & checking if values are present
  // ----------------------------------------------------------------------------------------------

  /// Returns the total number of values in the specified series. This excludes
  /// missing values or not available values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  let countValues (series:Series<'K, 'T>) = series.Values |> Seq.length

  /// Returns the total number of keys in the specified series. This returns
  /// the total length of the series, including keys for which there is no 
  /// value available.
  let countKeys (series:Series<'K, 'T>) = series.Keys |> Seq.length

  let hasAll keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).HasValue)
  let hasSome keys (series:Series<'K, 'T>) = 
    keys |> Seq.exists (fun k -> series.TryGet(k).HasValue)
  let hasNone keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).HasValue |> not)
  let has key (series:Series<'K, 'T>) = series.TryGet(key).HasValue
  let hasNot key (series:Series<'K, 'T>) = series.TryGet(key).HasValue



  let values (series:Series<'K, 'T>) = series.Values
  let keys (series:Series<'K, 'T>) = series.Keys


  // TODO: This can be simplified using fancier aggregate?

  let shift offset (series:Series<'K, 'T>) = 
    let shifted = 
      if offset < 0 then
        let offset = -offset
        series |> aggregateInto (WindowSize(offset + 1, Boundary.Skip)) 
          (fun s -> s.Data.Keys.First())
          (fun s -> s.Data.Values |> Seq.nth offset)          
      else
        series |> aggregateInto (WindowSize(offset + 1, Boundary.Skip)) 
          (fun s -> s.Data.Keys.Last())
          (fun s -> s.Data.Values |> Seq.head)           
    shifted.GetItems(series.Keys)

  let takeLast count (series:Series<'K, 'T>) = 
    let keys = series.Keys |> Seq.lastFew count 
    Series(keys, seq { for k in keys -> series.[k] })

  let inline maxBy f (series:Series<'K, 'T>) = 
    series |> observations |> Seq.maxBy (snd >> f)

  let inline minBy f (series:Series<'K, 'T>) = 
    series |> observations |> Seq.maxBy (snd >> f)

