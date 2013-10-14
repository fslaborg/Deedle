namespace FSharp.DataFrame

open System
open System.Linq
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open Microsoft.FSharp.Quotations

open FSharp.DataFrame.Keys
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Internal

[<AutoOpen>]
module FSharpSeriesExtensions =
  open System

  type Series = 
    static member ofObservations(observations) = 
      Series(Seq.map fst observations, Seq.map snd observations)
    static member ofValues(values) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values)
    static member ofNullables(values:seq<Nullable<_>>) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values).Select(fun (KeyValue(_, v:Nullable<_>)) -> v.Value)

  let series observations = Series.ofObservations observations

[<Extension>]
type EnumerableExtensions =
  [<Extension>]
  static member ToSeries(observations:seq<KeyValuePair<'K, 'V>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofObservations
  [<Extension>]
  static member ToOrdinalSeries(observations:seq<'V>) = 
    observations |> Series.ofValues

type internal Series =
  /// Vector & index builders
  static member internal vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
  static member internal indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

  static member internal Create(data:seq<'V>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'V>(Index.ofKeys(lookup), Vector.ofValues(data), Series.vectorBuilder, Series.indexBuilder)
  static member internal Create(index:seq<'K>, data:seq<'V>) =
    Series<'K, 'V>(Index.ofKeys(index), Vector.ofValues(data), Series.vectorBuilder, Series.indexBuilder)
  static member internal Create(index:IIndex<'K>, data:IVector<'V>) = 
    Series<'K, 'V>(index, data, Series.vectorBuilder, Series.indexBuilder)
  static member internal CreateUntyped(index:IIndex<'K>, data:IVector<obj>) = 
    ObjectSeries<'K>(index, data, Series.vectorBuilder, Series.indexBuilder)


type SeriesBuilder<'K, 'V when 'K : equality>() = 
  let mutable keys = []
  let mutable values = []

  member x.Add(key:'K, value:'V) =
    keys <- key::keys
    values <- value::values
  
  member x.Series =
    Series.Create(Index.ofKeys (List.rev keys), Vector.ofValues(List.rev values))

  static member (?<-) (builder:SeriesBuilder<string, 'V>, name:string, value:'V) =
    builder.Add(name, value)
  
  interface System.Collections.IEnumerable with
    member builder.GetEnumerator() = (builder :> seq<_>).GetEnumerator() :> Collections.IEnumerator
  interface seq<KeyValuePair<'K, 'V>> with
    member builder.GetEnumerator() = 
      (Seq.zip keys values |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()

  interface System.Dynamic.IDynamicMetaObjectProvider with 
    member builder.GetMetaObject(expr) = 
      DynamicExtensions.createSetterFromFunc expr builder (fun builder name value -> 
        let converted = 
          if value :? 'V then unbox<'V> value
          else System.Convert.ChangeType(value, typeof<'V>) |> unbox<'V>
        builder.Add(unbox<'K> name, converted))

type SeriesBuilder<'K when 'K : equality>() =
  inherit SeriesBuilder<'K, obj>()

[<Extension>]
type SeriesExtensions =
  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:Series<'K1 * 'K2, 'V>, lo1:option<'K1>, hi1:option<'K1>, lo2:option<'K2>, hi2:option<'K2>) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel(SimpleLookup [|Option.map box lo1; Option.map box lo2|])

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:Series<'K1 * 'K2, 'V>, lo1:option<'K1>, hi1:option<'K1>, k2:'K2) =
    if lo1 <> None || hi1 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel(SimpleLookup [|Option.map box lo1; Some (box k2) |])

  [<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
  static member GetSlice(series:Series<'K1 * 'K2, 'V>, k1:'K1, lo2:option<'K2>, hi2:option<'K2>) =
    if lo2 <> None || hi2 <> None then invalidOp "Slicing on level of a hierarchical indices is not supported"
    series.GetByLevel(SimpleLookup [|Some (box k1); Option.map box lo2|])

  [<Extension>]
  static member Between(series:Series<'K, 'V>, lowerInclusive, upperInclusive) = 
    series.GetSubrange
      ( Some(lowerInclusive, BoundaryBehavior.Inclusive),
        Some(upperInclusive, BoundaryBehavior.Inclusive) )

  [<Extension>]
  static member After(series:Series<'K, 'V>, lowerExclusive) = 
    series.GetSubrange( Some(lowerExclusive, BoundaryBehavior.Exclusive), None )

  [<Extension>]
  static member Before(series:Series<'K, 'V>, upperExclusive) = 
    series.GetSubrange( None, Some(upperExclusive, BoundaryBehavior.Exclusive) )

  [<Extension>]
  static member StartAt(series:Series<'K, 'V>, lowerInclusive) = 
    series.GetSubrange( Some(lowerInclusive, BoundaryBehavior.Inclusive), None )

  [<Extension>]
  static member EndAt(series:Series<'K, 'V>, upperInclusive) = 
    series.GetSubrange( None, Some(upperInclusive, BoundaryBehavior.Inclusive) )

  [<Extension>]
  static member Log(series:Series<'K, float>) = log series

  [<Extension>]
  static member Shift(series:Series<'K, 'V>, offset) = Series.shift offset series

  [<Extension>]
  static member Sum(series:Series<'K, float>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, int>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, float32>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, decimal>) = Series.sum series

  [<Extension>]
  static member ContainsKey(series:Series<'K, 'T>, key:'K) = 
    series.Keys.Contains(key)

  /// Returns all keys from the sequence, together with the associated (optional)
  /// values. The values are returned using the `OptionalValue<T>` struct which
  /// provides `HasValue` for testing if the value is available.
  [<Extension>]
  static member GetAllObservations(series:Series<'K, 'T>) = seq {
    for key, address in series.Index.Mappings ->
      KeyValuePair(key, series.Vector.GetValue(address)) }

  /// Return observations with available values. The operation skips over 
  /// all keys with missing values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  [<Extension>]
  static member GetObservations(series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings do
      let v = series.Vector.GetValue(address)
      if v.HasValue then yield KeyValuePair(key, v.Value) }

  /// Returns the total number of values in the specified series. This excludes
  /// missing values or not available values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  [<Extension>]
  static member CountValues(series:Series<'K, 'T>) = Series.countValues series

  /// Returns the total number of keys in the specified series. This returns
  /// the total length of the series, including keys for which there is no 
  /// value available.
  [<Extension>]
  static member CountKeys(series:Series<'K, 'T>) = Series.countKeys series


  // static member Where(series:Series<'K, 'T>, f:System.Func<KeyValuePair<'K, 'V>, bool>) = 


  [<Extension>]
  static member Diff(series:Series<'K, float>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, float32>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, decimal>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, int>, offset) = series |> Series.diff offset

  [<Extension>]
  static member WindowInto(series:Series<'K, 'V>, size:int, reduce:Func<Series<'K, 'V>,'U>): Series<'K, 'U> = 
    Series.windowInto size reduce.Invoke series

  [<Extension>]
  static member Window(series:Series<'K, 'V>, size:int): Series<'K, Series<'K, 'V>> = 
    Series.window size series


  // --- end

  [<Extension>]
  static member FirstKey(series:Series<'K, 'V>) = series.KeyRange |> fst

  [<Extension>]
  static member LastKey(series:Series<'K, 'V>) = series.KeyRange |> snd

  // ----------------------------------------------------------------------------------------------
  // Missing values
  // ----------------------------------------------------------------------------------------------

  /// Drop missing values from the specified series. The returned series contains 
  /// only those keys for which there is a value available in the original one.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be filtered
  ///
  /// ## Example
  ///
  ///     let s = series [ 1 => 1.0; 2 => Double.NaN ]
  ///     s.DropMissing()
  ///     [fsi:val it : Series<int,float> = series [ 1 => 1]
  ///
  /// [category:Missing values]
  [<Extension>]
  static member DropMissing(series:Series<'K, 'V>) =
    Series.dropMissing series

  /// Fill missing values in the series with a constant value.
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `value` - A constant value that is used to fill all missing values
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, value:'T) = 
    Series.fillMissingWith value series

  /// Fill missing values in the series with the nearest available value
  /// (using the specified direction). The default direction is `Direction.Backward`.
  /// Note that the series may still contain missing values after call to this 
  /// function. This operation can only be used on ordered series. 
  ///
  /// ## Example
  ///
  ///     let sample = Series.ofValues [ Double.NaN; 1.0; Double.NaN; 3.0 ]
  ///
  ///     // Returns a series consisting of [1; 1; 3; 3]
  ///     sample.FillMissing(Direction.Backward)
  ///
  ///     // Returns a series consisting of [<missing>; 1; 1; 3]
  ///     sample.FillMissing(Direction.Forward)
  ///
  /// ## Parameters
  ///  - `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, [<Optional>] direction) = 
    Series.fillMissing direction series

  /// Fill missing values in the series using the specified function.
  /// The specified function is called with all keys for which the series
  /// does not contain value and the result of the call is used in place 
  /// of the missing value. 
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `filler` - A function that takes key `K` and generates a value to be
  ///    used in a place where the original series contains a missing value.
  ///
  /// ## Remarks
  /// This function can be used to implement more complex interpolation.
  /// For example see [handling missing values in the tutorial](../features.html#missing)
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, filler:Func<_, _>) = 
    Series.fillMissingUsing filler.Invoke series

  // ----------------------------------------------------------------------------------------------
  // Lookup, resampling and scaling
  // ----------------------------------------------------------------------------------------------

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
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. For unordered
  /// series, similar functionality can be implemented using `GroupBy`.
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member ResampleEquivalence(series:Series<'K, 'V>, keyProj:Func<_, _>) =
    Series.resampleEquiv keyProj.Invoke series

  /// Resample the series based on equivalence class on the keys. A specified function
  /// `keyProj` is used to project keys to another space and the observations for which the 
  /// projected keys are equivalent are grouped into chunks. The chunks are then transformed
  /// to values using the provided function `f`.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///  - `aggregate` - A function that is used to collapse a generated chunk into a 
  ///    single value. 
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. For unordered
  /// series, similar functionality can be implemented using `GroupBy`.
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member ResampleEquivalence(series:Series<'K, 'V>, keyProj:Func<_, _>, aggregate:Func<_, _>) =
    Series.resampleEquivInto keyProj.Invoke aggregate.Invoke series

  /// Resample the series based on equivalence class on the keys and also generate values 
  /// for all keys of the target space that are between the minimal and maximal key of the
  /// specified series (e.g. generate value for all days in the range covered by the series).
  /// For each equivalence class (e.g. date), select the latest value (with greatest key).
  /// A specified function `keyProj` is used to project keys to another space and `nextKey`
  /// is used to generate all keys in the range. 
  ///
  /// When there are no values for a (generated) key, then the function attempts to get the
  /// greatest value from the previous smaller chunk (i.e. value for the previous date time).
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
  [<Extension>]
  static member ResampleUniform(series:Series<'K, 'V>, keyProj:Func<_, _>, nextKey:Func<_, _>) =
    Series.resampleUniformInto Lookup.NearestSmaller keyProj.Invoke nextKey.Invoke Series.lastValue series

  /// Resample the series based on equivalence class on the keys and also generate values 
  /// for all keys of the target space that are between the minimal and maximal key of the
  /// specified series (e.g. generate value for all days in the range covered by the series).
  /// A specified function `keyProj` is used to project keys to another space and `nextKey`
  /// is used to generate all keys in the range. The chunk is then aggregated using `aggregate`.
  ///
  /// When there are no values for a (generated) key, then the function attempts to get the
  /// greatest value from the previous smaller chunk (i.e. value for the previous date time).
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `fillMode` - When set to `Lookup.NearestSmaller` or `Lookup.NearestGreater`, 
  ///     the function searches for a nearest available observation in an neighboring chunk.
  ///     Otherwise, the function `f` is called with an empty series as an argument.
  ///  - `keyProj` - A function that transforms keys from original space to a new 
  ///    space (which is then used for grouping based on equivalence)
  ///  - `nextKey` - A function that gets the next key in the transformed space
  ///  - `aggregate` - A function that is used to collapse a generated chunk into a 
  ///    single value. The function may be called on empty series when `fillMode` is
  ///    `Lookup.Exact`.
  ///    
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member ResampleUniform(series:Series<'K1, 'V>, keyProj:Func<'K1, 'K2>, nextKey:Func<'K2, 'K2>, fillMode:Lookup, aggregate) =
    Series.resampleUniformInto fillMode keyProj.Invoke nextKey.Invoke aggregate series

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting at the specified `start` time, using the specified `interval`
  /// and then finds nearest smaller values close to such keys according to `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `start` - The initial time to be used for sampling
  ///  - `interval` - The interval between the individual samples 
  ///  - `lookup` - Specifies how the lookup based on keys is performed. `Exact` means that the
  ///    values at exact keys will be returned; `NearestGreater` returns the nearest greater key value
  ///    (starting at the first key) and `NearestSmaller` returns the nearest smaller key value
  ///    (starting at most `interval` after the end of the series)
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTime, 'V>, start:DateTime, interval:TimeSpan, dir) =
    series |> Series.Implementation.lookupTimeInternal (+) (Some start) interval dir Lookup.NearestSmaller

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting at the specified `start` time, using the specified `interval`
  /// and then finds nearest smaller values close to such keys according to `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `start` - The initial time to be used for sampling
  ///  - `interval` - The interval between the individual samples 
  ///  - `lookup` - Specifies how the lookup based on keys is performed. `Exact` means that the
  ///    values at exact keys will be returned; `NearestGreater` returns the nearest greater key value
  ///    (starting at the first key) and `NearestSmaller` returns the nearest smaller key value
  ///    (starting at most `interval` after the end of the series)
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTimeOffset, 'V>, start:DateTimeOffset, interval:TimeSpan, dir) =
    series |> Series.Implementation.lookupTimeInternal (+) (Some start) interval dir Lookup.NearestSmaller

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting from the smallest key of the original series, using the specified `interval`
  /// and then finds nearest smaller values close to such keys according to `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `lookup` - Specifies how the lookup based on keys is performed. `Exact` means that the
  ///    values at exact keys will be returned; `NearestGreater` returns the nearest greater key value
  ///    (starting at the first key) and `NearestSmaller` returns the nearest smaller key value
  ///    (starting at most `interval` after the end of the series)
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTime, 'V>, interval:TimeSpan, dir) =
    series |> Series.Implementation.lookupTimeInternal (+) None interval dir Lookup.NearestSmaller

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting from the smallest key of the original series, using the specified `interval`
  /// and then finds nearest smaller values close to such keys according to `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - Specifies how the keys should be generated. `Direction.Forward` means that the 
  ///    key is the smallest value of each chunk (and so first key of the series is returned and 
  ///    the last is not, unless it matches exactly _start + k*interval_); `Direction.Backward`
  ///    means that the first key is skipped and sample is generated at, or just before the end 
  ///    of interval and at the end of the series.
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTimeOffset, 'V>, interval:TimeSpan, dir) =
    series |> Series.Implementation.lookupTimeInternal (+) None interval dir Lookup.NearestSmaller

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting from the smallest key of the original series, using the specified `interval`
  /// and then finds nearest smaller values close to such keys. The function generates samples
  /// at, or just before the end of an interval and at, or after, the end of the series.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTime, 'V>, interval:TimeSpan) =
    SeriesExtensions.Sample(series, interval, Direction.Backward)

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting from the smallest key of the original series, using the specified `interval`
  /// and then finds nearest smaller values close to such keys. The function generates samples
  /// at, or just before the end of an interval and at, or after, the end of the series.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'V>(series:Series<DateTimeOffset, 'V>, interval:TimeSpan) =
    SeriesExtensions.Sample(series, interval, Direction.Backward)

  /// Performs sampling by time and aggregates chunks obtained by time-sampling into a single
  /// value using a specified function. The operation generates keys starting at the first
  /// key in the source series, using the specified `interval` and then obtains chunks based on 
  /// these keys in a fashion similar to the `Series.resample` function.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `aggregate` - A function that is called to aggregate each chunk into a single value.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member SampleInto<'V>(series:Series<DateTime, 'V>, interval:TimeSpan, dir, aggregate:Func<_, _>) =
    series |> Series.Implementation.sampleTimeIntoInternal (+) None interval dir aggregate.Invoke

  /// Performs sampling by time and aggregates chunks obtained by time-sampling into a single
  /// value using a specified function. The operation generates keys starting at the first
  /// key in the source series, using the specified `interval` and then obtains chunks based on 
  /// these keys in a fashion similar to the `Series.resample` function.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `aggregate` - A function that is called to aggregate each chunk into a single value.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member SampleInto<'V>(series:Series<DateTimeOffset, 'V>, interval:TimeSpan, dir, aggregate:Func<_, _>) =
    series |> Series.Implementation.sampleTimeIntoInternal (+) None interval dir aggregate.Invoke
