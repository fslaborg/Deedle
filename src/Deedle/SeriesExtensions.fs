namespace Deedle

open System
open System.Linq
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open Microsoft.FSharp.Quotations

open Deedle.Keys
open Deedle.Indices
open Deedle.Internal

// --------------------------------------------------------------------------------------
// Functions and methods for creating series
// --------------------------------------------------------------------------------------

/// Contains extensions for creating values of type `Series<'K, 'V>` including
/// a type with functions such as `Series.ofValues` and the `series` function.
/// The module is automatically opened for all F# code that references `Deedle`.
///
/// [category:Core frame and series types]
[<AutoOpen>]
module ``F# Series extensions`` =
  open System

  type Series = 
    /// Create a series from a sequence of key-value pairs that represent
    /// the observations of the series. Consider using a shorthand 
    /// `series` function instead.
    static member ofObservations(observations) = 
      let observations = Array.ofSeq observations
      Series(Array.map fst observations, Array.map snd observations)

    /// Create a series from the specified sequence of values. The keys
    /// of the resulting series are generated ordinarilly, starting from 0.
    static member ofValues(values) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values)

    /// Create a series from a sequence of nullable values. The keys
    /// of the resulting series are generated ordinarilly, starting from 0.
    /// The resulting series will contain keys associated with the `null`
    /// values, but the values are treated as missing.
    static member ofNullables(values:seq<Nullable<_>>) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values).Select(fun (KeyValue(_, v:Nullable<_>)) -> v.Value)
    
    /// Create a series from a sequence of observations where the value is of
    /// type `option<'T>`. When the value is `None`, the key remains in the
    /// series, but the value is treated as missing.
    static member ofOptionalObservations(observations:seq<'K * option<_>>) = 
      Series(Seq.map fst observations, Seq.map snd observations)
        .SelectOptional(fun kvp -> OptionalValue.bind OptionalValue.ofOption kvp.Value)

  /// Create a series from a sequence of key-value pairs that represent
  /// the observations of the series. This function can be used together
  /// with the `=>` operator to create key-value pairs.
  ///
  /// ## Example
  ///
  ///     // Creates a series with squares of numbers
  ///     let sqs = series [ 1 => 1.0; 2 => 4.0; 3 => 9.0 ]
  ///
  let series observations = Series.ofObservations observations


/// Contains C#-friendly extension methods for various instances of `IEnumerable` 
/// that can be used for creating `Series<'K, 'V>` from the `IEnumerable` value.
/// You can create an ordinal series from `IEnumerable<'T>` or an indexed series from
/// `IEnumerable<KeyValuePair<'K, 'V>>` or from `IEnumerable<KeyValuePair<'K, OptionalValue<'V>>>`.
///
/// [category:Frame and series operations]
[<Extension>]
type EnumerableExtensions =
  /// Convert the `IEnumerable` to a `Series`, using the keys and values of
  /// the `KeyValuePair` as keys and values of the resulting series.
  [<Extension>]
  static member ToSeries(observations:seq<KeyValuePair<'K, 'V>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofObservations

  /// Convert the `IEnumerable` to a  `Series`, using the keys and values of
  /// the `KeyValuePair` as keys and values of the resulting series.
  /// `OptionalValue.Missing` can be used to denote missing values.
  [<Extension>]
  static member ToSparseSeries(observations:seq<KeyValuePair<'K, OptionalValue<'V>>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, OptionalValue.asOption kvp.Value) |> Series.ofOptionalObservations

  /// Convert the `IEnumerable` to a `Series`, using the seuqence as the values
  /// of the resulting series. The keys are generated ordinarilly, starting from 0.
  [<Extension>]
  static member ToOrdinalSeries(observations:seq<'V>) = 
    observations |> Series.ofValues

// --------------------------------------------------------------------------------------
// Series builder
// --------------------------------------------------------------------------------------

/// The type can be used for creating series using mutation. You can add 
/// items using `Add` and get the resulting series using the `Series` property.
///
/// ## Using from C#
///
/// The type supports the C# collection builder pattern:
///
///    	var s = new SeriesBuilder<string, double>
///       { { "A", 1.0 }, { "B", 2.0 }, { "C", 3.0 } }.Series;
///
/// The type also supports the `dynamic` operator:
///
///     dynamic sb = new SeriesBuilder<string, obj>();
///     sb.ID = 1;
///     sb.Value = 3.4;
///
/// [category:Specialized frame and series types]
type SeriesBuilder<'K, 'V when 'K : equality and 'V : equality>() = 
  let mutable keys = []
  let mutable values = []

  /// Add specified key and value to the series being build
  member x.Add(key:'K, value:'V) =
    keys <- key::keys
    values <- value::values
  
  /// Returns the constructed series. The series is an immutable
  /// copy of the current values and so further additions will not
  /// change the returned series.
  member x.Series =
    Series
      ( Index.ofKeys (List.rev keys), Vector.ofValues(List.rev values), 
        VectorBuilder.Instance, IndexBuilder.Instance )

  static member (?<-) (builder:SeriesBuilder<string, 'V>, name:string, value:'V) =
    builder.Add(name, value)
  
  interface System.Collections.IEnumerable with
    member builder.GetEnumerator() = (builder :> seq<_>).GetEnumerator() :> Collections.IEnumerator

  interface seq<KeyValuePair<'K, 'V>> with
    member builder.GetEnumerator() = 
      (List.zip keys values |> List.rev |> Seq.map (fun (k, v) -> KeyValuePair(k, v))).GetEnumerator()

  interface IDictionary<'K, 'V> with
    member x.Keys = upcast ReadOnlyCollection.ofSeq keys
    member x.Values = upcast ReadOnlyCollection.ofSeq values
    member x.Clear() = keys <- []; values <- []
    member x.Item 
      with get key = failwith "!"
      and set key value = failwith "!"
    member x.Add(k, v) = x.Add(k, v)
    member x.Add(kvp:KeyValuePair<_, _>) = x.Add(kvp.Key, kvp.Value)
    member x.ContainsKey(k) = Seq.exists ((=) k) keys
    member x.Contains(kvp) = (x :> seq<_>).Contains(kvp)
    member x.Remove(kvp:KeyValuePair<_, _>) = 
      let newPairs = List.zip keys values |> List.filter (fun (k, v) -> k <> kvp.Key || v <> kvp.Value) 
      let res = newPairs.Length < keys.Length
      keys <- List.map fst newPairs
      values <- List.map snd newPairs
      res
    member x.Remove(key) = 
      let newPairs = List.zip keys values |> List.filter (fun (k, v) -> k <> key) 
      let res = newPairs.Length < keys.Length
      keys <- List.map fst newPairs;
      values <- List.map snd newPairs
      res
    member x.CopyTo(array, offset) = x |> Seq.iteri (fun i v -> array.[i + offset] <- v)
    member x.Count = List.length keys
    member x.IsReadOnly = false
    member x.TryGetValue(key, value) = 
      match Seq.zip keys values |> Seq.tryFind (fun (k, v) -> k = key) with
      | Some (_, v) -> value <- v; true | _ -> false
    
  interface System.Dynamic.IDynamicMetaObjectProvider with 
    member builder.GetMetaObject(expr) = 
      DynamicExtensions.createGetterAndSetterFromFunc expr builder
        (fun builder name ->
            let dict = builder :> IDictionary<'K, 'V>
            match dict.TryGetValue(unbox<'K> name) with
            | (true, v) -> v :> obj
            | (false, _) -> failwithf "%s does not exist" name)
        (fun builder name value ->
            let converted = Convert.convertType<'V> ConversionKind.Flexible value
            builder.Add(unbox<'K> name, converted))

/// A simple class that inherits from `SeriesBuilder<'K, obj>` and can be
/// used instead of writing `SeriesBuilder<'K, obj>` with two type arguments.
///
/// [category:Specialized frame and series types]
type SeriesBuilder<'K when 'K : equality>() =
  inherit SeriesBuilder<'K, obj>()

// --------------------------------------------------------------------------------------
// Extensions providing nice C#-friendly API
// --------------------------------------------------------------------------------------

/// The type implements C# and F# extension methods for the `Series<'K, 'V>` type.
/// The members are automatically available when you import the `Deedle` namespace.
/// The type contains object-oriented counterparts to most of the functionality 
/// from the `Series` module.
///
/// [category:Frame and series operations]
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
  static member Print(series:Series<'K, 'V>) = Console.WriteLine(series.Format())

  [<Extension>]
  static member Log(series:Series<'K, float>) = log series

  /// Returns a series with values shifted by the specified offset. When the offset is 
  /// positive, the values are shifted forward and first `offset` keys are dropped. When the
  /// offset is negative, the values are shifted backwards and the last `offset` keys are dropped.
  /// Expressed in pseudo-code:
  ///
  ///     result[k] = series[k - offset]
  ///
  /// ## Parameters
  ///  - `offset` - Can be both positive and negative number.
  ///  - `series` - The input series to be shifted.
  ///
  /// ## Remarks
  /// If you want to calculate the difference, e.g. `s - (Series.shift 1 s)`, you can
  /// use `Series.diff` which will be a little bit faster.
  [<Extension>]
  static member Shift(series:Series<'K, 'V>, offset) = Series.shift offset series

  [<Extension>]
  static member Sort(series:Series<'K, 'V>) = Series.sort series

  [<Extension>]
  static member SortBy(series:Series<'K, 'V>, f: Func<'V,'V2>) = Series.sortBy f.Invoke series

  [<Extension>]
  static member SortWith(series:Series<'K, 'V>, cmp: Comparer<'V>) = Series.sortWith (fun x y -> cmp.Compare(x, y)) series

  [<Extension>]
  static member ContainsKey(series:Series<'K, 'T>, key:'K) = 
    series.TryGet(key).HasValue

  /// Collapses a series of OptionalValue<'T> values to just 'T values
  [<Extension>]
  static member Flatten(series:Series<'K,'T opt>) =
    series |> Series.mapValues OptionalValue.asOption |> Series.flatten

  /// Returns all keys from the sequence, together with the associated (optional)
  /// values. The values are returned using the `OptionalValue<T>` struct which
  /// provides `HasValue` for testing if the value is available.
  [<Extension>]
  static member GetAllObservations(series:Series<'K, 'T>) = seq {
    for KeyValue(key, address) in series.Index.Mappings ->
      KeyValuePair(key, series.Vector.GetValue(address)) }

  /// Returns all (optional) values. The values are returned using the 
  /// `OptionalValue<T>` struct which provides `HasValue` for testing 
  /// if the value is available.
  [<Extension>]
  static member GetAllValues(series:Series<'K, 'T>) = seq {
    for KeyValue(key, address) in series.Index.Mappings -> 
      series.Vector.GetValue(address) }

  /// Return observations with available values. The operation skips over 
  /// all keys with missing values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  [<Extension>]
  static member GetObservations(series:Series<'K, 'T>) = seq { 
    for KeyValue(key, address) in series.Index.Mappings do
      let v = series.Vector.GetValue(address)
      if v.HasValue then yield KeyValuePair(key, v.Value) }

  // static member Where(series:Series<'K, 'T>, f:System.Func<KeyValuePair<'K, 'V>, bool>) = 


  /// Returns a series containing difference between a value in the original series and 
  /// a value at the specified offset. For example, calling `Series.diff 1 s` returns a 
  /// series where previous value is subtracted from the current one. 
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `series` - The input series.
  ///
  [<Extension>]
  static member Diff(series:Series<'K, float>, offset) = series |> Series.diff offset

  /// Returns a series containing difference between a value in the original series and 
  /// a value at the specified offset. For example, calling `Series.diff 1 s` returns a 
  /// series where previous value is subtracted from the current one. 
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `series` - The input series.
  ///
  [<Extension>]
  static member Diff(series:Series<'K, float32>, offset) = series |> Series.diff offset

  /// Returns a series containing difference between a value in the original series and 
  /// a value at the specified offset. For example, calling `Series.diff 1 s` returns a 
  /// series where previous value is subtracted from the current one. 
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `series` - The input series.
  ///
  [<Extension>]
  static member Diff(series:Series<'K, decimal>, offset) = series |> Series.diff offset

  /// Returns a series containing difference between a value in the original series and 
  /// a value at the specified offset. For example, calling `Series.diff 1 s` returns a 
  /// series where previous value is subtracted from the current one. 
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `series` - The input series.
  ///
  [<Extension>]
  static member Diff(series:Series<'K, int>, offset) = series |> Series.diff offset

  [<Extension>]
  static member WindowInto(series:Series<'K1, 'V>, size:int, selector:Func<Series<'K1, 'V>, KeyValuePair<'K2, 'U>>): Series<'K2, 'U> = 
    series.Aggregate(WindowSize(size, Boundary.Skip), (fun ds ->
      let res = selector.Invoke ds.Data
      KeyValuePair(res.Key, OptionalValue(res.Value)) ))

  [<Extension>]
  static member WindowInto(series:Series<'K, 'V>, size:int, reduce:Func<Series<'K, 'V>,'U>): Series<'K, 'U> = 
    Series.windowInto size reduce.Invoke series

  [<Extension>]
  static member Window(series:Series<'K, 'V>, size:int): Series<'K, Series<'K, 'V>> = 
    Series.window size series

  [<Extension>]
  static member ChunkInto(series:Series<'K1, 'V>, size:int, selector:Func<Series<'K1, 'V>, KeyValuePair<'K2, 'U>>): Series<'K2, 'U> = 
    series.Aggregate(ChunkSize(size, Boundary.Skip), (fun ds ->
      let res = selector.Invoke ds.Data
      KeyValuePair(res.Key, OptionalValue(res.Value)) ))

  [<Extension>]
  static member ChunkInto(series:Series<'K, 'V>, size:int, reduce:Func<Series<'K, 'V>,'U>): Series<'K, 'U> = 
    Series.chunkInto size reduce.Invoke series

  [<Extension>]
  static member Chunk(series:Series<'K, 'V>, size:int): Series<'K, Series<'K, 'V>> = 
    Series.chunk size series

  // --- end

  [<Extension>]
  static member FirstKey(series:Series<'K, 'V>) = series |> Series.firstKey

  [<Extension>]
  static member LastKey(series:Series<'K, 'V>) = series |> Series.lastKey

  [<Extension>]
  static member FirstValue(series:Series<'K, 'V>) = series |> Series.firstValue

  [<Extension>]
  static member LastValue(series:Series<'K, 'V>) = series |> Series.lastValue

  [<Extension>]
  static member TryFirstValue(series:Series<'K, 'V>) = series |> Series.tryFirstValue

  [<Extension>]
  static member TryLastValue(series:Series<'K, 'V>) = series |> Series.tryLastValue

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

  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, startKey, endKey, [<Optional>] direction) = 
    Series.fillMissingBetween (startKey, endKey) direction series

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
  /// For example see [handling missing values in the tutorial](../frame.html#missing)
  ///
  /// [category:Missing values]
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, filler:Func<_, _>) = 
    Series.fillMissingUsing filler.Invoke series

  // ----------------------------------------------------------------------------------------------
  // Sorting
  // ----------------------------------------------------------------------------------------------

  /// Returns a new series whose entries are reordered according to index order
  ///
  /// ## Parameters
  ///  - `series` - An input series to be used
  ///
  /// [category:Data structure manipulation]
  [<Extension>]
  static member SortByKey(series:Series<'K, 'T>) = 
    Series.sortByKey series

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
    Series.resampleUniformInto Lookup.ExactOrSmaller keyProj.Invoke nextKey.Invoke Series.tryLastValue series |> Series.flatten

  /// Resample the series based on equivalence class on the keys and also generate values 
  /// for all keys of the target space that are between the minimal and maximal key of the
  /// specified series (e.g. generate value for all days in the range covered by the series).
  /// A specified function `keyProj` is used to project keys to another space and `nextKey`
  /// is used to generate all keys in the range. The last value of each chunk is returned.
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
  static member ResampleUniform(series:Series<'K1, 'V>, keyProj:Func<'K1, 'K2>, nextKey:Func<'K2, 'K2>, fillMode:Lookup) =
    Series.resampleUniformInto fillMode keyProj.Invoke nextKey.Invoke Series.tryLastValue series |> Series.flatten

  /// Sample an (ordered) series by finding the value at the exact or closest prior key 
  /// for some new sequence of keys. 
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be sampled
  ///  - `keys`   - The keys at which to sample
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered.
  /// 
  /// [category:Lookup, resampling and scaling]
  [<Extension>]
  static member Sample<'K, 'V when 'K : equality>(series:Series<'K, 'V>, keys) =
    series |> Series.sample keys

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
    series |> Series.Implementation.lookupTimeInternal (+) (Some start) interval dir Lookup.ExactOrSmaller

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
    series |> Series.Implementation.lookupTimeInternal (+) (Some start) interval dir Lookup.ExactOrSmaller

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
    series |> Series.Implementation.lookupTimeInternal (+) None interval dir Lookup.ExactOrSmaller

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
    series |> Series.Implementation.lookupTimeInternal (+) None interval dir Lookup.ExactOrSmaller

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


  // ----------------------------------------------------------------------------------------------
  // Obsolete - kept for temporary compatibility
  // ----------------------------------------------------------------------------------------------

  [<Extension; Obsolete("Use SortByKeys instead. This function will be removed in futrue versions.")>]
  static member OrderByKey(series:Series<'K, 'T>) = Series.sortByKey series

