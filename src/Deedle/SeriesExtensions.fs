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
    static member ofOptionalObservations(observations:seq<'K * OptionalValue<_>>) = 
      Series(Seq.map fst observations, Seq.map snd observations)
        .SelectOptional(fun kvp -> OptionalValue.bind id kvp.Value)

  let series observations = Series.ofObservations observations

[<Extension>]
type EnumerableExtensions =
  [<Extension>]
  static member ToSeries(observations:seq<KeyValuePair<'K, 'V>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofObservations
  [<Extension>]
  static member ToSparseSeries(observations:seq<KeyValuePair<'K, OptionalValue<'V>>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofOptionalObservations
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


type SeriesBuilder<'K, 'V when 'K : equality and 'V : equality>() = 
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
    

(*
ICollection<TKey> Keys { get; }
ICollection<TValue> Values { get; }

TValue this[TKey key] { get; set; }

void Add(TKey key, TValue value);
bool ContainsKey(TKey key);
bool Remove(TKey key);
bool TryGetValue(TKey key, out TValue value);
*)
  interface System.Dynamic.IDynamicMetaObjectProvider with 
    member builder.GetMetaObject(expr) = 
      DynamicExtensions.createSetterFromFunc expr builder (fun builder name value -> 
        let converted = Convert.changeType<'V> value
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
  static member Print(series:Series<'K, 'V>) = Console.WriteLine(series.Format())

  [<Extension>]
  static member Log(series:Series<'K, float>) = log series

  [<Extension>]
  static member Shift(series:Series<'K, 'V>, offset) = Series.shift offset series

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
  static member FirstKey(series:Series<'K, 'V>) = series.KeyRange |> fst

  [<Extension>]
  static member LastKey(series:Series<'K, 'V>) = series.KeyRange |> snd

  // ----------------------------------------------------------------------------------------------
  // Statistics
  // ----------------------------------------------------------------------------------------------

  /// Returns the mean of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Mean(series:Series<'K, float>) = Series.mean series

  /// Returns the mean of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Mean(series:Series<'K, float32>) = Series.mean series

  /// Returns the mean of the elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Mean(series:Series<'K, decimal>) = Series.mean series

  /// Returns the standard deviation of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member StandardDeviation(series:Series<'K, float>) = Series.sdv series

  /// Returns the median of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Median(series:Series<'K, float>) = Series.median series

  /// Returns the sum of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Sum(series:Series<'K, float>) = Series.sum series

  /// Returns the sum of the elements of the series. The operation skips over
  /// missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Sum(series:Series<'K, float32>) = Series.sum series

  /// Returns the sum of the elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Sum(series:Series<'K, int>) = Series.sum series

  /// Returns the sum of the elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Sum(series:Series<'K, decimal>) = Series.sum series

  /// Returns the smallest of all elements of the series. The operation 
  /// skips over missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Min(series:Series<'K, float>) = Series.min series

  /// Returns the smallest of all elements of the series. The operation 
  /// skips over missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Min(series:Series<'K, float32>) = Series.min series

  /// Returns the smallest of all elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Min(series:Series<'K, int>) = Series.min series

  /// Returns the smallest of all elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Min(series:Series<'K, decimal>) = Series.min series

  /// Returns the greatest of all elements of the series. The operation 
  /// skips over missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Max(series:Series<'K, float>) = Series.max series

  /// Returns the greatest of all elements of the series. The operation 
  /// skips over missing values and so the result will never be `NaN`.
  /// [category:Statistics]
  [<Extension>]
  static member Max(series:Series<'K, float32>) = Series.max series

  /// Returns the greatest of all elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Max(series:Series<'K, int>) = Series.max series

  /// Returns the greatest of all elements of the series. 
  /// [category:Statistics]
  [<Extension>]
  static member Max(series:Series<'K, decimal>) = Series.max series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the mean of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the means
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MeanLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.meanLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the mean of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the means
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MeanLevel(series:Series<'K1, float32>, groupSelector:Func<'K1, 'K2>) = Series.meanLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the mean of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the means
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MeanLevel(series:Series<'K1, decimal>, groupSelector:Func<'K1, 'K2>) = Series.meanLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the standard deviation of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the standard deviations
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  static member StandardDeviationLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.sdvLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the median of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the medians
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MedianLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.medianLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the sums
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member SumLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.sumLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the sums
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member SumLevel(series:Series<'K1, float32>, groupSelector:Func<'K1, 'K2>) = Series.sumLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the sums
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member SumLevel(series:Series<'K1, int>, groupSelector:Func<'K1, 'K2>) = Series.sumLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the sums
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member SumLevel(series:Series<'K1, decimal>, groupSelector:Func<'K1, 'K2>) = Series.sumLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the smallest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MinLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.minLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the smallest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MinLevel(series:Series<'K1, float32>, groupSelector:Func<'K1, 'K2>) = Series.minLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the smallest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MinLevel(series:Series<'K1, int>, groupSelector:Func<'K1, 'K2>) = Series.minLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the smallest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MinLevel(series:Series<'K1, decimal>, groupSelector:Func<'K1, 'K2>) = Series.minLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the greatest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MaxLevel(series:Series<'K1, float>, groupSelector:Func<'K1, 'K2>) = Series.maxLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the greatest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MaxLevel(series:Series<'K1, float32>, groupSelector:Func<'K1, 'K2>) = Series.maxLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the greatest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MaxLevel(series:Series<'K1, int>, groupSelector:Func<'K1, 'K2>) = Series.maxLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the greatest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member MaxLevel(series:Series<'K1, decimal>, groupSelector:Func<'K1, 'K2>) = Series.maxLevel groupSelector.Invoke series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the counts of elements in each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the counts
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member CountLevel(series:Series<'K1, decimal>, groupSelector:Func<'K1, 'K2>) = Series.countLevel groupSelector.Invoke series

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
  /// For example see [handling missing values in the tutorial](../features.html#missing)
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
  static member Order(series:Series<'K, 'T>) = 
    Series.order series

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
    Series.resampleUniformInto fillMode keyProj.Invoke nextKey.Invoke Series.lastValue series

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
