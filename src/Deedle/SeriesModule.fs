#nowarn "77" // Static constraint in Series.sample requires special + operator
#nowarn "10002" // Custom CompilerMessage used to hide internals that are inlined

namespace Deedle
open System
open System.Collections.Generic
open System.Linq
open Deedle.Keys
open Deedle.Addressing
open Deedle.Internal
open Deedle.Vectors
open Deedle.VectorHelpers

/// The `Series` module provides F#-friendly API for working with functions. The API
/// follows the usual design for collection-processing in F#, so the functions work
/// well with the pipelining (`|>`) operator. For example, given a series with ages,
/// we can use `Series.filterValues` to filter outliers and then `Series.mean` to calculate
/// the mean:
///
///     ages
///     |> Series.filterValues (fun v -> v > 0.0 && v < 120.0)
///     |> Series.mean
///
/// The module provides comprehensive set of functions for working with series. The same
/// API is also exposed using C#-friendly extension methods. In C#, the above snippet could
/// be written as:
///
///     [lang=csharp]
///     ages
///       .Where(kvp => kvp.Value > 0.0 && kvp.Value < 120.0)
///       .Mean()
/// 
/// For more information about similar frame-manipulation functions, see the `Frame` module.
/// For more information about C#-friendly extensions, see `SeriesExtensions`. The functions 
/// in the `Series` module are grouped in a number of categories and documented below.
///
/// ## Accessing series data and lookup
///
/// Functions in this category provide access to the values in the series.
///
///  - The term _observation_ is used for a key value pair in the series. 
///  - When working with a sorted series, it is possible to perform lookup using
///    keys that are not present in the series - you can specify to search for the
///    previous or next available value using _lookup behavior_.
///  - Functions such as `get` and `getAll` have their counterparts `lookup` and
///    `lookupAll` that let you specify lookup behavior.
///  - For most of the functions that may fail, there is a `try[Foo]` variant that
///    returns `None` instead of failing.
///  - Functions with a name ending with `At` perform lookup based on the absolute
///    integer offset (and ignore the keys of the series)
///
/// ## Series transformations 
///
/// > **TODO** Write comment here  
/// 
/// 
/// ## Hierarchical index operations
///
/// When the key of a series is tuple, the elements of the tuple can be treated
/// as multiple levels of a index. For example `Series<'K1 * 'K2, 'V>` has two 
/// levels with keys of types `'K1` and `'K2` respectively.
///
/// The functions in this cateogry provide a way for aggregating values in the 
/// series at one of the levels. For example, given a series `input` indexed by
/// two-element tuple, you can calculate mean for different first-level values as
/// follows:
///
///     input |> applyLevel fst Stats.mean
///
/// Note that the `Stats` module provides helpers for typical statistical operations,
/// so the above could be written just as `input |> Stats.levelMean fst`.
/// 
/// ## Windowing, chunking and grouping
///
/// This category includes functions that group data from a series in some way. Two key
/// concepts here are _window_ and _chunk_. Window refers to (overlapping) sliding windows
/// over the input series while chunk refers to non-overlapping blocks of the series.
///
/// The boundary behavior can be specified using the `Boundary` flags. The value 
/// `Skip` means that boundaries (incomplete windows or chunks) should be skipped. The value
/// `AtBeginning` and `AtEnding` can be used to define at which side should the boundary be
/// returned (or skipped). For chunking, `AtBeginning ||| Skip` makes sense and it means that
/// the incomplete chunk at the beginning should be skipped (aligning the last chunk with the end).
///
/// The behavior may be specified in a number of ways (which is reflected in the name):
///  - `dist` - using an absolute distance between the keys
///  - `while` - using a condition on the first and last key
///  - `size` - by specifying the absolute size of the window/chunk
///
/// The functions ending with `Into` take a function to be applied to the window/chunk. 
/// The functions `window`, `windowInto` and `chunk`, `chunkInto` are simplified versions
/// that take a size. There is also `pairwise` function for sliding window of size two.
///
/// ## Missing values
///
/// This group of functions provides a way of working with missing values in a series.
/// The `dropMissing` function drops all keys for which there are no values in the series.
/// The remaining functions provide different mechanism for filling the missing values.
///
///  * `fillMissingWith` fills missing values with a specified constant
///  * `fillMissingUsing` calls a specified function for every missing value
///  * `fillMissing` and variants propagates values from previous/later keys
///
/// ## Sorting and reindexing
/// 
/// > **TODO** Write comment here
///
///
/// ## Sampling, resampling and advanced lookup
///
/// > **TODO** Write comment here
///
///
/// ## Merging and zipping
///
/// Given two series, there are two ways to combine the values. If the keys in the series
/// are not overlapping (or you want to throw away values from one or the other series), 
/// then you cna use `merge` or `mergeUsing`. To merge more than 2 series efficiently, use 
/// the `mergeAll` function, which has been optimized for large number of series.
///
/// If you want to align two series, you can use the _zipping_ operation. This aligns
/// two series based on their keys and gives you tuples of values. The default behavior
/// (`zip`) uses outer join and exact matching. For ordered series, you can specify 
/// other forms of key lookups (e.g. find the greatest smaller key) using `zipAlign`.
/// functions ending with `Into` are generally easier to use as they call a specified
/// function to turn the tuple (of possibly missing values) into a new value.
///
/// For more complicated behaviors, it is often convenient to use joins on frames instead
/// of working with series. Create two frames with single columns and then use the join
/// operation. The result will be a frame with two columns (which is easier to use than
/// series of tuples).
///
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series = 

  // ------------------------------------------------------------------------------------
  // Accessing series data and lookup
  // ------------------------------------------------------------------------------------

  /// Return observations with available values. The operation skips over 
  /// all keys with missing values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetObservations")>]
  let observations (series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings do
      let v = series.Vector.GetValue(address)
      if v.HasValue then yield key, v.Value }
  
  /// Returns all keys from the sequence, together with the associated (optional) values. 
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetAllObservations")>]
  let observationsAll (series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings ->
      key, OptionalValue.asOption (series.Vector.GetValue(address)) }

  /// Create a new series that contains values for all provided keys.
  /// Use the specified lookup semantics - for exact matching, use `getAll`
  ///
  /// ## Parameters
  ///  - `keys` - A sequence of keys that will form the keys of the retunred sequence
  ///  - `lookup` - Lookup behavior to use when the value at the specified key does not exist
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("LookupAll")>]
  let lookupAll keys lookup (series:Series<'K, 'T>) = series.GetItems(keys, lookup)

  /// Create a new series that contains values for all provided keys.
  /// Uses exact lookup semantics for key lookup - use `lookupAll` for more options
  ///
  /// ## Parameters
  ///  - `keys` - A sequence of keys that will form the keys of the retunred sequence
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetObservations")>]
  let getAll keys (series:Series<'K, 'T>) = series.GetItems(keys)

  /// Get the value for the specified key.
  /// Use the specified lookup semantics - for exact matching, use `get`
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("Lookup")>]
  let lookup key lookup (series:Series<'K, 'T>) = series.Get(key, lookup)

  /// Get the value for the specified key.
  /// Uses exact lookup semantics for key lookup - use `lookup` for more options
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("Get")>]
  let get key (series:Series<'K, 'T>) = series.Get(key)

  /// Returns the value at the specified (integer) offset.
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetAt")>]
  let getAt index (series:Series<'K, 'T>) = series.GetAt(index)

  /// Attempts to get the value for the specified key. If the value is not
  /// available, `None` is returned.
  /// Use the specified lookup semantics - for exact matching, use `tryGet`.
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("TryLookup")>]
  let tryLookup key lookup (series:Series<'K, 'T>) = series.TryGet(key, lookup) |> OptionalValue.asOption

  /// Attempts to get an observation (key value pair) based on the specified key.
  /// The search uses the specified lookup semantics and so the returned key
  /// may differ from the key searched for. If the value is not
  /// available, `None` is returned.
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("TryLookupObservation")>]
  let tryLookupObservation key lookup (series:Series<'K, 'T>) = 
    series.TryGetObservation(key, lookup) 
    |> OptionalValue.asOption 
    |> Option.map (fun kvp -> (kvp.Key, kvp.Value))

  /// Get the value for the specified key. Returns `None` when the key does not exist
  /// or the value is missing.
  /// Uses exact lookup semantics for key lookup - use `tryLookup` for more options
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("TryGet")>]
  let tryGet key (series:Series<'K, 'T>) = series.TryGet(key) |> OptionalValue.asOption

  /// Returns the value at the specified (integer) offset, or `None` if the value is missing.
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("TryGetAt")>]
  let tryGetAt index (series:Series<'K, 'T>) = series.TryGetAt(index) |> OptionalValue.asOption

  /// Returns the total number of values in the specified series. This excludes
  /// missing values or not available values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("CountValues")>]
  let countValues (series:Series<'K, 'T>) = series.ValueCount 

  /// Returns the total number of keys in the specified series. This returns
  /// the total length of the series, including keys for which there is no 
  /// value available.
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("CountKeys")>]
  let countKeys (series:Series<'K, 'T>) = series.KeyCount

  /// Returns true when the series contains value for all of the specified keys
  /// (This is useful for checking prior to performing a computation)
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("HasAll")>]
  let hasAll keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).HasValue)

  /// Returns true when the series contains value for some of the specified keys
  /// (This is useful for checking prior to performing a computation)
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("HasSome")>]
  let hasSome keys (series:Series<'K, 'T>) = 
    keys |> Seq.exists (fun k -> series.TryGet(k).HasValue)

  /// Returns true when the series does not contains value for any of the specified keys
  /// (This is useful for checking prior to performing a computation)
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("HasNone")>]
  let hasNone keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).HasValue |> not)

  /// Returns true when the series contains value for the specified key
  /// (This is useful for checking prior to performing a computation)
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("Has")>]
  let has key (series:Series<'K, 'T>) = series.TryGet(key).HasValue

  /// Returns true when the series does not contains value for the specified key
  /// (This is useful for checking prior to performing a computation)
  ///
  /// [category:Accessing series data and lookup]
  [<CompiledName("HasNot")>]
  let hasNot key (series:Series<'K, 'T>) = series.TryGet(key).HasValue

  /// Returns the last key of the series
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetLastKey")>]
  let lastKey (series:Series< 'K , 'V >) = series.KeyRange |> snd

  /// Returns the first key of the series
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetFirstKey")>]
  let firstKey (series:Series< 'K , 'V >) = series.KeyRange |> fst

  /// Returns the last value of the series. This fails if the last value is missing.
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetLastValue")>]
  let lastValue (series:Series< 'K , 'V >) = series |> get (series.KeyRange |> snd)

  /// Returns the first value of the series. This fails if the first value is missing.
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetFirstValue")>]
  let firstValue (series:Series< 'K , 'V >) = series |> get (series.KeyRange |> fst)

  /// Returns the (non-missing) values of the series as a sequence
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetValues")>]
  let values (series:Series<'K, 'T>) = series.Values

  /// Returns the series values (both missing and present) as a sequence
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetAllValues")>]
  let valuesAll (series:Series<'K, 'T>) = series.Vector.DataSequence |> Seq.map OptionalValue.asOption

  /// Returns the keys of the series as a sequence
  /// [category:Accessing series data and lookup]
  [<CompiledName("GetKeys")>]
  let keys (series:Series<'K, 'T>) = series.Keys

  // ------------------------------------------------------------------------------------
  // Series transformations 
  // ------------------------------------------------------------------------------------
  
  /// [category:Series transformations]
  let filter f (series:Series<'K, 'T>) = 
    series.Where(Func<_, _>(fun (KeyValue(k,v)) -> f k v))

  /// [category:Series transformations]
  let filterValues f (series:Series<'K, 'T>) = 
    series.Where(Func<_, _>(fun (KeyValue(k,v)) -> f v))

  /// [category:Series transformations]
  let map (f:'K -> 'T -> 'R) (series:Series<'K, 'T>) = 
    series.Select(fun (KeyValue(k,v)) -> f k v)

  /// [category:Series transformations]
  let mapValues (f:'T -> 'R) (series:Series<'K, 'T>) = 
    series.Select(fun (KeyValue(k,v)) -> f v)

  /// [category:Series transformations]
  let mapKeys (f:'K -> 'R) (series:Series<'K, 'T>) = 
    series.SelectKeys(fun kvp -> f kvp.Key)

  /// [category:Series transformations]
  let filterAll f (series:Series<'K, 'T>) = 
    series.WhereOptional(fun kvp -> f kvp.Key (OptionalValue.asOption kvp.Value))

  /// [category:Series transformations]
  let withMissingFrom other (series:Series<'K,'T>) =
    series.WithMissingFrom(other)

  /// [category:Series transformations]
  let mapAll (f:_ -> _ -> option<'R>) (series:Series<'K, 'T>) = 
    series.SelectOptional(fun kvp -> 
      f kvp.Key (OptionalValue.asOption kvp.Value) |> OptionalValue.ofOption)

  /// Flattens option values; ie, it applies the `join` operation to each value 
  /// that is a nested option monad.
  ///
  /// [category:Series transformations]
  let flatten (series:Series<'K, 'T option>) = 
    series |> mapAll (fun _ v -> match v with Some x -> x | _ -> None)

  /// [category:Series transformations]
  let tryMap (f:'K -> 'T -> 'R) (series:Series<'K, 'T>) : Series<_, _ tryval> = 
    series.Select(fun (KeyValue(k,v)) -> 
      try TryValue.Success(f k v) with e -> TryValue.Error e )

  /// Throws `AggregateException` if something goes wrong
  ///
  /// [category:Series transformations]
  let tryValues (series:Series<'K, 'T tryval>) = 
    let exceptions = series.Values |> Seq.choose (fun tv -> 
      if tv.HasValue then None else Some tv.Exception) |> List.ofSeq
    if List.isEmpty exceptions then
      series |> mapValues (fun tv -> tv.Value)
    else raise (new AggregateException(exceptions))

  /// Return a Series of all exceptions 
  ///
  /// [category:Series transformations]
  let tryErrors (series: Series<'K, TryValue<'V>>) =
    let errors = 
      series.Observations
      |> Seq.choose (function | KeyValue(k, Error(e)) -> Some(KeyValuePair(k, e))
                              | _ -> None)
    Series<_,_>(errors)

  /// Return a Series of all successful tries 
  ///
  /// [category:Series transformations]
  let trySuccesses (series: Series<'K, TryValue<'V>>) =
    let successes = 
      series.Observations
      |> Seq.choose (function | KeyValue(k, Success(v)) -> Some(KeyValuePair(k, v))
                              | _ -> None)
    Series<_,_>(successes)

  /// [category:Series transformations]
  let fillErrorsWith value (series:Series<'K, 'T tryval>) = 
    series |> mapValues (function TryValue.Error _ -> value | TryValue.Success v -> v)

  /// Internal helper used by `skip`, `take`, etc.
  let internal getRange lo hi (series:Series<'K, 'T>) = 
    if hi < lo then Series([],[]) else
      let cmd = GetRange(Return 0, (int64 lo, int64 hi))
      let vec = series.VectorBuilder.Build(cmd, [| series.Vector |])
      let newKeys = series.Index.Keys.[lo .. hi]
      let idx = series.IndexBuilder.Create(newKeys, if series.IsOrdered then Some true else None)
      Series(idx, vec, series.VectorBuilder, series.IndexBuilder)

  /// Returns a series that contains the specified number of keys from the original series. 
  ///
  /// ## Parameters
  ///  - `count` - Number of keys to take; must be smaller or equal to the original number of keys
  ///  - `series` - Input series from which the keys are taken
  ///
  /// [category:Series transformations]
  [<CompiledName("Take")>]
  let take count (series:Series<'K, 'T>) =
    if count > series.KeyCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange 0 (count - 1) series

  /// Returns a series that contains the specified number of keys from the 
  /// original series. The keys are taken from the end of the series. 
  ///
  /// ## Parameters
  ///  - `count` - Number of keys to take; must be smaller or equal to the original number of keys
  ///  - `series` - Input series from which the keys are taken
  ///
  /// [category:Series transformations]
  [<CompiledName("TakeLast")>]
  let takeLast count (series:Series<'K, 'T>) =
    if count > series.KeyCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange (series.KeyCount-count) (series.KeyCount-1) series

  /// Returns a series that contains the data from the original series,
  /// except for the first `count` keys.
  ///
  /// ## Parameters
  ///  - `count` - Number of keys to skip; must be smaller or equal to the original number of keys
  ///  - `series` - Input series from which the keys are taken
  ///
  /// [category:Series transformations]
  [<CompiledName("Skip")>]
  let skip count (series:Series<'K, 'T>) =
    if count > series.KeyCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange count (series.KeyCount-1) series

  /// Returns a series that contains the data from the original series,
  /// except for the last `count` keys.
  ///
  /// ## Parameters
  ///  - `count` - Number of keys to skip; must be smaller or equal to the original number of keys
  ///  - `series` - Input series from which the keys are taken
  ///
  /// [category:Series transformations]
  [<CompiledName("SkipLast")>]
  let skipLast count (series:Series<'K, 'T>) =
    if count > series.KeyCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange 0 (series.KeyCount-1-count) series

  /// [category:Series transformations]
  let force (series:Series<'K, 'V>) = 
    series.Materialize()


  // Scanning

  /// Applies a folding function starting with some initial value and the first value of the series,
  /// and continues to "scan" along the series, saving all values produced from the first function 
  /// application, and yielding a new series having the original index and newly produced values.
  /// Any application involving a missing value yields a missing value.
  ///
  /// ## Parameters
  ///  - `foldFunc` - A folding function 
  ///  - `init` - An initial value
  ///  - `series` - The series over whose values to scan
  ///
  /// [category:Series transformations]
  let scanValues foldFunc init (series:Series<'K,'T>) =
    series.ScanValues(Func<_,_,_>(foldFunc), init)

  /// Applies a folding function starting with some initial optional value and the first optional value of 
  /// the series, and continues to "scan" along the series, saving all values produced from the first function 
  /// application, and yielding a new series having the original index and newly produced values.
  ///
  /// ## Parameters
  ///  - `foldFunc` - A folding function 
  ///  - `init` - An initial value
  ///  - `series` - The series over whose values to scan
  ///
  /// [category:Series transformations]
  let scanAllValues foldFunc init (series:Series<'K,'T>) =
    let liftedFunc a b = foldFunc (OptionalValue.asOption a) (OptionalValue.asOption b) |> OptionalValue.ofOption
    series.ScanAllValues(Func<_,_,_>(liftedFunc), OptionalValue.ofOption init)

  /// Returns a series containing difference between a value in the original series and 
  /// a value at the specified offset. For example, calling `Series.diff 1 s` returns a 
  /// series where previous value is subtracted from the current one. In pseudo-code, the
  /// function behaves as follows:
  ///
  ///     result[k] = series[k] - series[k - offset]
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `series` - The input series, containing values that support the `-` operator.
  ///
  /// [category:Series transformations]
  [<CompiledName("Diff")>]
  let inline diff offset (series:Series<'K, ^T>) = 
    let vectorBuilder = VectorBuilder.Instance
    let newIndex, vectorR = series.Index.Builder.Shift((series.Index, Vectors.Return 0), offset)
    let _, vectorL = series.Index.Builder.Shift((series.Index, Vectors.Return 0), -offset)
    let cmd = Vectors.Combine(vectorL, vectorR, VectorValueTransform.Create< ^T >(OptionalValue.map2 (-)))
    let newVector = vectorBuilder.Build(cmd, [| series.Vector |])
    Series(newIndex, newVector, vectorBuilder, series.Index.Builder)

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
  ///
  /// [category:Series transformations]
  [<CompiledName("Shift")>]
  let shift offset (series:Series<'K, 'T>) = 
    let newIndex, vector = series.IndexBuilder.Shift((series.Index, Vectors.Return 0), offset)
    let newVector = series.VectorBuilder.Build(vector, [| series.Vector |])
    Series(newIndex, newVector, series.VectorBuilder, series.IndexBuilder)

  /// Aggregates the values of the specified series using a function that can combine
  /// individual values. 
  ///
  /// ## Parameters
  ///  - `series` - An input series to be aggregated
  ///  - `op` - A function that is used to aggregate elements of the series
  ///
  /// [category:Series transformations]
  [<CompiledName("Reduce")>]
  let reduce op (series:Series<'K, 'T>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> ReadOnlyCollection.reduce op list
    | VectorData.SparseList list -> (ReadOnlyCollection.reduceOptional op list) |> OptionalValue.get
    | VectorData.Sequence seq -> Seq.reduce op (Seq.choose OptionalValue.asOption seq)
 
  // ----------------------------------------------------------------------------------------------
  // Hierarchical index operations
  // ----------------------------------------------------------------------------------------------

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `level` and then aggregates series representing each group
  /// using the specified function `op`. The result is a new series containing
  /// the aggregates of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - An input series to be aggregated
  ///  - `op` - A function that takes a series and produces an aggregated result
  ///  - `level` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("ApplyLevel")>]
  let inline applyLevel (level:'K1 -> 'K2) op (series:Series<_, 'V>) : Series<_, 'R> = 
    series.GroupBy(fun kvp -> level kvp.Key) |> mapValues op

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `level` and then aggregates series representing each group
  /// using the specified function `op`. The result is a new series containing
  /// the aggregates of each group. The result of a group may be None, in which
  /// case the group will have no representation in the resulting series. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - An input series to be aggregated
  ///  - `op` - A function that takes a series and produces an optional aggregated result
  ///  - `level` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("ApplyLevelOptional")>]
  let inline applyLevelOptional (level:'K1 -> 'K2) op (series:Series<_, 'V>) : Series<_, 'R> = 
    series.GroupBy(fun kvp -> level kvp.Key) |> mapValues op |> flatten

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `level` and then aggregates elements in each group
  /// using the specified function `op`. The result is a new series containing
  /// the aggregates of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - An input series to be aggregated
  ///  - `op` - A function that is used to aggregate elements of each group
  ///  - `level` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("ReduceLevel")>]
  let reduceLevel (level:'K1 -> 'K2) op (series:Series<_, 'T>) = 
    series.GroupBy(fun (KeyValue(key, _)) -> level key) |> mapValues (reduce op)

  // ----------------------------------------------------------------------------------------------
  // Windowing, chunking and grouping
  // ----------------------------------------------------------------------------------------------

  /// Aggregates an ordered series using the method specified by `Aggregation<K>` and 
  /// returns the windows or chunks as nested series. A key for each window or chunk is
  /// selected using the specified `keySelector`.
  ///
  /// ## Parameters
  ///  - `aggregation` - Specifies the aggregation method using `Aggregation<K>`. This is
  ///    a discriminated union listing various chunking and windowing conditions.
  ///  - `keySelector` - A function that is called on each chunk to obtain a key.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("Aggregate")>]
  let aggregate aggregation keySelector (series:Series<'K, 'T>) : Series<'TNewKey, _> =
    series.Aggregate
      ( aggregation, System.Func<_, _>(keySelector), System.Func<_, _>(fun v -> OptionalValue(v)))

  /// Aggregates an ordered series using the method specified by `Aggregation<K>` and then
  /// applies the provided value selector `f` on each window or chunk to produce the result
  /// which is returned as a new series. A key for each window or chunk is
  /// selected using the specified `keySelector`.
  ///
  /// ## Parameters
  ///  - `aggregation` - Specifies the aggregation method using `Aggregation<K>`. This is
  ///    a discriminated union listing various chunking and windowing conditions.
  ///  - `keySelector` - A function that is called on each chunk to obtain a key.
  ///  - `f` - A value selector function that is called to aggregate each chunk or window.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("AggregateInto")>]
  let aggregateInto aggregation keySelector f (series:Series<'K, 'T>) : Series<'TNewKey, 'R> =
    series.Aggregate
      ( aggregation, System.Func<_, _>(keySelector), System.Func<_, _>(f))

  /// Creates a sliding window using the specified size and boundary behavior and then
  /// applies the provided value selector `f` on each window to produce the result
  /// which is returned as a new series. The key is the last key of the window, unless
  /// boundary behavior is `Boundary.AtEnding` (in which case it is the first key).
  ///
  /// ## Parameters
  ///  - `bounds` - Specifies the window size and bounary behavior. The boundary behavior
  ///    can be `Boundary.Skip` (meaning that no incomplete windows are produced), 
  ///    `Boundary.AtBeginning` (meaning that incomplete windows are produced at the beginning)
  ///    or `Boundary.AtEnding` (to produce incomplete windows at the end of series)
  ///  - `f` - A value selector that is called to aggregate each window.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowSizeInto")>]
  let windowSizeInto bounds f (series:Series<'K, 'T>) : Series<'K, 'R> =
    let dir = if snd bounds = Boundary.AtEnding then Direction.Forward else Direction.Backward
    let keySel = System.Func<DataSegment<Series<_, _>>, _>(fun data -> 
      if dir = Direction.Backward then data.Data.Index.KeyRange |> snd
      else data.Data.Index.KeyRange |> fst )
    series.Aggregate(WindowSize(bounds), keySel, (fun ds -> OptionalValue(f ds)))

  /// Creates a sliding window using the specified size and boundary behavior and returns
  /// the produced windows as a nested series. The key is the last key of the window, unless
  /// boundary behavior is `Boundary.AtEnding` (in which case it is the first key).
  ///
  /// ## Parameters
  ///  - `bounds` - Specifies the window size and bounary behavior. The boundary behavior
  ///    can be `Boundary.Skip` (meaning that no incomplete windows are produced), 
  ///    `Boundary.AtBeginning` (meaning that incomplete windows are produced at the beginning)
  ///    or `Boundary.AtEnding` (to produce incomplete windows at the end of series)
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowSize")>]
  let inline windowSize bounds (series:Series<'K, 'T>) = 
    windowSizeInto bounds DataSegment.data series 

  // Based on distance

  /// Creates a sliding window based on distance between keys. A window is started at each
  /// input element and ends once the distance between the first and the last key is greater
  /// than the specified `distance`. Each window is then aggregated into a value using the
  /// specified function `f`. The key of each window is the key of the first element in the window.
  ///
  /// ## Parameters
  ///  - `distance` - The maximal allowed distance between keys of a window. Note that this
  ///    is an inline function - there must be `-` operator defined between `distance` and the
  ///    keys of the series.
  ///  - `f` - A function that is used to aggregate each window into a single value.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowDistanceInto")>]
  let inline windowDistInto distance f (series:Series<'K, 'T>) =
    series.Aggregate
      ( WindowWhile(fun skey ekey -> (ekey - skey) < distance), 
        (fun d -> d.Data.Keys |> Seq.head), fun ds -> OptionalValue(f ds.Data))

  /// Creates a sliding window based on distance between keys. A window is started at each
  /// input element and ends once the distance between the first and the last key is greater
  /// than the specified `distance`. The windows are then returned as a nested series.
  /// The key of each window is the key of the first element in the window.
  ///
  /// ## Parameters
  ///  - `distance` - The maximal allowed distance between keys of a window. Note that this
  ///    is an inline function - there must be `-` operator defined between `distance` and the
  ///    keys of the series.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowDistance")>]
  let inline windowDist (distance:'D) (series:Series<'K, 'T>) = 
    windowDistInto distance id series 

  // Window using while

  /// Creates a sliding window based on a condition on keys. A window is started at each
  /// input element and ends once the specified `cond` function returns `false` when called on 
  /// the first and the last key of the window. Each window is then aggregated into a value using the
  /// specified function `f`. The key of each window is the key of the first element in the window.
  ///
  /// ## Parameters
  ///  - `cond` - A function that is called on the first and the last key of a window
  ///    to determine when a window should end.
  ///  - `f` - A function that is used to aggregate each window into a single value.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowWhileInto")>]
  let inline windowWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(WindowWhile(cond), (fun d -> d.Data.Keys |> Seq.head), fun ds -> OptionalValue(f ds.Data))

  /// Creates a sliding window based on a condition on keys. A window is started at each
  /// input element and ends once the specified `cond` function returns `false` when called on 
  /// the first and the last key of the window. The windows are then returned as a nested series.
  /// The key of each window is the key of the first element in the window.
  ///
  /// ## Parameters
  ///  - `cond` - A function that is called on the first and the last key of a window
  ///    to determine when a window should end.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowWhile")>]
  let inline windowWhile cond (series:Series<'K, 'T>) = 
    windowWhileInto cond id series 

  // Chunk based on size

  /// Aggregates the input into a series of adacent chunks using the specified size and boundary behavior and then
  /// applies the provided value selector `f` on each chunk to produce the result
  /// which is returned as a new series. The key is the last key of the chunk, unless
  /// boundary behavior is `Boundary.AtEnding` (in which case it is the first key).
  ///
  /// ## Parameters
  ///  - `bounds` - Specifies the chunk size and bounary behavior. The boundary behavior
  ///    can be `Boundary.Skip` (meaning that no incomplete chunks are produced), 
  ///    `Boundary.AtBeginning` (meaning that incomplete chunks are produced at the beginning)
  ///    or `Boundary.AtEnding` (to produce incomplete chunks at the end of series)
  ///  - `f` - A value selector that is called to aggregate each chunk.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkSizeInto")>]
  let inline chunkSizeInto bounds f (series:Series<'K, 'T>) : Series<'K, 'R> =
    series.Aggregate(ChunkSize(bounds), (fun d -> d.Data.Keys |> Seq.head), fun ds -> OptionalValue(f ds))

  /// Aggregates the input into a series of adacent chunks using the specified size and boundary behavior and returns
  /// the produced chunks as a nested series. The key is the last key of the chunk, unless
  /// boundary behavior is `Boundary.AtEnding` (in which case it is the first key).
  ///
  /// ## Parameters
  ///  - `bounds` - Specifies the chunk size and bounary behavior. The boundary behavior
  ///    can be `Boundary.Skip` (meaning that no incomplete chunks are produced), 
  ///    `Boundary.AtBeginning` (meaning that incomplete chunks are produced at the beginning)
  ///    or `Boundary.AtEnding` (to produce incomplete chunks at the end of series)
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkSize")>]
  let inline chunkSize bounds (series:Series<'K, 'T>) = 
    chunkSizeInto bounds DataSegment.data series 

  // Chunk based on distance

  /// Aggregates the input into a series of adacent chunks. A chunk is started once
  /// the distance between the first and the last key of a previous chunk is greater
  /// than the specified `distance`. Each chunk is then aggregated into a value using the
  /// specified function `f`. The key of each chunk is the key of the first element in the chunk.
  ///
  /// ## Parameters
  ///  - `distance` - The maximal allowed distance between keys of a chunk. Note that this
  ///    is an inline function - there must be `-` operator defined between `distance` and the
  ///    keys of the series.
  ///  - `f` - A value selector that is called to aggregate each chunk.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkDistanceInto")>]
  let inline chunkDistInto (distance:^D) f (series:Series<'K, 'T>) : Series<'K, 'R> =
    series.Aggregate(ChunkWhile(fun skey ekey -> (ekey - skey) < distance), (fun d -> d.Data.Keys |> Seq.head), fun ds -> OptionalValue(f ds.Data))

  /// Aggregates the input into a series of adacent chunks. A chunk is started once
  /// the distance between the first and the last key of a previous chunk is greater
  /// than the specified `distance`. The chunks are then returned as a nested series.
  /// The key of each chunk is the key of the first element in the chunk.
  ///
  /// ## Parameters
  ///  - `distance` - The maximal allowed distance between keys of a chunk. Note that this
  ///    is an inline function - there must be `-` operator defined between `distance` and the
  ///    keys of the series.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkDistance")>]
  let inline chunkDist (distance:^D) (series:Series<'K, 'T>) = 
    chunkDistInto distance id series 

  // Chunk while

  /// Aggregates the input into a series of adacent chunks based on a condition on keys. A chunk is started 
  /// once the specified `cond` function returns `false` when called on  the first and the last key of the 
  /// previous chunk. Each chunk is then aggregated into a value using the
  /// specified function `f`. The key of each chunk is the key of the first element in the chunk.
  ///
  /// ## Parameters
  ///  - `cond` - A function that is called on the first and the last key of a chunk
  ///    to determine when a window should end.
  ///  - `f` - A value selector that is called to aggregate each chunk.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkWhileInto")>]
  let inline chunkWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(ChunkWhile(cond), (fun d -> d.Data.Keys |> Seq.head), fun ds -> OptionalValue(f ds.Data))

  /// Aggregates the input into a series of adacent chunks based on a condition on keys. A chunk is started 
  /// once the specified `cond` function returns `false` when called on  the first and the last key of the 
  /// previous chunk. The chunks are then returned as a nested series.
  /// The key of each chunk is the key of the first element in the chunk.
  ///
  /// ## Parameters
  ///  - `cond` - A function that is called on the first and the last key of a chunk
  ///    to determine when a window should end.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkWhile")>]
  let inline chunkWhile cond (series:Series<'K, 'T>) = 
    chunkWhileInto cond id series 

  // Most common-case functions  

  /// Creates a sliding window using the specified size and then applies the provided 
  /// value selector `f` on each window to produce the result which is returned as a new series. 
  /// This function skips incomplete chunks - you can use `Series.windowSizeInto` for more options.
  ///
  /// ## Parameters
  ///  - `size` - The size of the sliding window.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("WindowInto")>]
  let inline windowInto size f (series:Series<'K, 'T>) : Series<'K, 'R> =
    windowSizeInto (size, Boundary.Skip) (DataSegment.data >> f) series

  /// Creates a sliding window using the specified size and returns the produced windows as 
  /// a nested series. The key in the new series is the last key of the window. This function
  /// skips incomplete chunks - you can use `Series.windowSize` for more options.
  ///
  /// ## Parameters
  ///  - `size` - The size of the sliding window.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("Window")>]
  let inline window size (series:Series<'K, 'T>) =
    windowSize (size, Boundary.Skip) series

  /// Aggregates the input into a series of adacent chunks and then applies the provided 
  /// value selector `f` on each chunk to produce the result which is returned as a new series. 
  /// The key in the new series is the last key of the chunk. This function
  /// skips incomplete chunks - you can use `Series.chunkSizeInto` for more options.
  ///
  /// ## Parameters
  ///  - `size` - The size of the chunk.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("ChunkInto")>]
  let inline chunkInto size f (series:Series<'K, 'T>) : Series<'K, 'R> =
    chunkSizeInto (size, Boundary.Skip) (DataSegment.data >> f) series

  /// Aggregates the input into a series of adacent chunks and returns the produced chunks as
  /// a nested series. The key in the new series is the last key of the chunk. This function
  /// skips incomplete chunks - you can use `Series.chunkSize` for more options.
  ///
  /// ## Parameters
  ///  - `size` - The size of the chunk.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("Chunk")>]
  let inline chunk size (series:Series<'K, 'T>) =
    chunkSize (size, Boundary.Skip) series

  // Pairwise

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
  ///     let res = input |> Series.pairwise
  ///     res = series [2 => ('a', 'b'); 3 => ('b', 'c') ]
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("Pairwise")>]
  let pairwise (series:Series<'K, 'T>) = 
    series.Pairwise()

  /// Aggregates the input into pairs containing the predecessor and an element for each input, except
  /// for the first one. Then calls the specified aggregation function `f` with a tuple and a key.
  /// The returned series is one key shorter (it does not contain a  value for the first key).
  ///
  /// ## Parameters
  ///  - `f` - A function that is called for each pair to produce result in the final series.
  ///  - `series` - The input series to be aggregated.
  ///
  /// [category:Windowing, chunking and grouping]
  [<CompiledName("PairwiseWith")>]
  let pairwiseWith f (series:Series<'K, 'T>) = 
    series.Pairwise() |> map (fun k v -> f k v)

  // Grouping

  /// Groups a series (ordered or unordered) using the specified key selector (`keySelector`) 
  /// and then aggregates each group into a single value, returned in the resulting series,
  /// using the provided `f` function.
  ///
  /// ## Parameters
  ///  - `keySelector` - Generates a new key that is used for aggregation, based on the original 
  ///    key and value. The new key must support equality testing.
  ///  - `f` - A function to aggregate each group of collected elements.
  ///  - `series` - An input series to be grouped. 
  ///
  /// [category:Windowing, chunking and grouping]
  let groupInto (keySelector:'K -> 'T -> 'TNewKey) f (series:Series<'K, 'T>) : Series<'TNewKey, 'TNewValue> =
    series.GroupBy(fun (KeyValue(k,v)) -> keySelector k v) |> map (fun k s -> f k s)

  /// Groups a series (ordered or unordered) using the specified key selector (`keySelector`) 
  /// and then returns a series of (nested) series as the result. The outer series is indexed by
  /// the newly produced keys, the nested series are indexed with the original keys.
  ///
  /// ## Parameters
  ///  - `keySelector` - Generates a new key that is used for aggregation, based on the original 
  ///    key and value. The new key must support equality testing.
  ///  - `series` - An input series to be grouped. 
  ///
  /// [category:Windowing, chunking and grouping]
  let groupBy (keySelector:'K -> 'T -> 'TNewKey) (series:Series<'K, 'T>) =
    groupInto keySelector (fun k s -> s) series


  // ----------------------------------------------------------------------------------------------
  // Handling of missing values
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
  ///     s |> Series.dropMissing 
  ///     [fsi:val it : Series<int,float> = series [ 1 => 1]
  ///
  /// [category:Missing values]
  [<CompiledName("DropMissing")>]
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
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingUsing")>]
  let fillMissingUsing f (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> Some(f k)
      | value -> value)

  /// Fill missing values in the series with a constant value.
  ///
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `value` - A constant value that is used to fill all missing values
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingWith")>]
  let fillMissingWith value (series:Series<'K, 'T>) = 
    let fillCmd = Vectors.FillMissing(Vectors.Return 0, VectorFillMissing.Constant value)
    let newVector = series.VectorBuilder.Build(fillCmd, [|series.Vector|])
    Series<_, _>(series.Index, newVector, series.VectorBuilder, series.IndexBuilder)

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
  /// [category:Missing values]
  [<CompiledName("FillMissing")>]
  let fillMissing direction (series:Series<'K, 'T>) = 
    let fillCmd = Vectors.FillMissing(Vectors.Return 0, VectorFillMissing.Direction direction)
    let newVector = series.VectorBuilder.Build(fillCmd, [|series.Vector|])
    Series<_, _>(series.Index, newVector, series.VectorBuilder, series.IndexBuilder)

  /// Fill missing values only between `startKey` and `endKey`, inclusive.
  /// 
  /// ## Parameters
  ///  - `series` - An input series that is to be filled
  ///  - `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  ///  - `startKey` - the lower bound at which values should be filled
  ///  - `endKey` - the upper bound at which values should be filled
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingBetween")>]
  let fillMissingBetween (startKey, endKey) direction (series:Series<'K, 'T>) = 
    let filled = fillMissing direction series.[startKey .. endKey]
    series.Zip(filled, JoinKind.Left).SelectOptional(fun kvp ->
      match kvp.Value with
      | OptionalValue.Present(_, OptionalValue.Present v2) -> OptionalValue v2
      | OptionalValue.Present(OptionalValue.Present v1, _) -> OptionalValue v1
      | _ -> OptionalValue.Missing )

  /// Fill missing values only between the first and last non-missing values.
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingInside")>]
  let fillMissingInside direction (series:Series<'K, 'T>) = 
    if not series.IsOrdered then invalidOp "Series must be sorted to use fillMissingInside"
    series.Observations |> Seq.tryFirstAndLast |> function
    | Some (a, b) when a <> b -> series |> fillMissingBetween (a.Key, b.Key) direction
    | _ -> series

  // ----------------------------------------------------------------------------------------------
  // Sorting
  // ----------------------------------------------------------------------------------------------

  /// [omit]
  let internal sortWithCommand compareFunc (series:Series<'K, 'T>) =
    let index = series.Index
    let values = series.Vector

    let missingCompare a b =
      let v1 = values.GetValue(snd a) |> OptionalValue.asOption
      let v2 = values.GetValue(snd b) |> OptionalValue.asOption 
      match v1, v2 with
      | Some x, Some y -> compareFunc x y
      | None,   Some y -> -1
      | Some x, None   -> 1
      | None,   None   -> 0

    let newKeys, newLocs =
      index.Mappings |> Array.ofSeq 
                     |> Array.sortWith missingCompare 
                     |> (fun arr -> arr |> Array.map fst, arr |> Array.map snd)

    let newIndex = Index.ofKeys newKeys
    let len = int64 newKeys.Length
    let reordering = Seq.zip (Addressing.Address.generateRange(0L, len-1L)) newLocs
    newIndex, VectorConstruction.Relocate(VectorConstruction.Return 0, len, reordering)

  /// [omit]
  let internal sortByCommand (f:'T -> 'V) (series:Series<'K, 'T>) =
    let index = series.Index
    let vector = series.Vector
    let fseries = Series(index, vector.SelectMissing (OptionalValue.map f), series.VectorBuilder, series.IndexBuilder)
    fseries |> sortWithCommand compare

  /// [category:Sorting and reindexing]
  let sortWith compareFunc series =
    let newIndex, cmd = sortWithCommand compareFunc series
    let vector = series.Vector
    Series(newIndex, series.VectorBuilder.Build(cmd, [| vector |]), series.VectorBuilder, series.IndexBuilder)

  /// [category:Sorting and reindexing]
  let sortBy (f:'T -> 'V) (series:Series<'K, 'T>) =
    let newIndex, cmd = series |> sortByCommand f
    let vector = series.Vector
    Series<'K,'T>(newIndex, series.VectorBuilder.Build(cmd, [| vector |]), series.VectorBuilder, series.IndexBuilder)

  /// [category:Sorting and reindexing]
  let sort series =
    series |> sortWith compare

  /// [category:Sorting and reindexing]
  let rev (series:Series<'K,'T>) = 
    series.Reversed

  /// [category:Sorting and reindexing]
  let realign keys (series:Series<'K, 'T>) = 
    series.Realign(keys)

  /// [category:Sorting and reindexing]
  let indexOrdinally (series:Series<'K, 'T>) = 
    series.IndexOrdinally()

  /// [category:Sorting and reindexing]
  let indexWith (keys:seq<'K2>) (series:Series<'K1, 'T>) = 
    series.IndexWith(keys)


  /// Returns a new series whose entries are reordered according to index order
  ///
  /// ## Parameters
  ///  - `series` - An input series to be used
  ///
  /// [category:Sorting and reindexing]
  let sortByKey (series:Series<'K, 'T>) =
    let newRowIndex, rowCmd = series.IndexBuilder.OrderIndex(series.Index, Vectors.Return 0)
    let newData = series.VectorBuilder.Build(rowCmd, [| series.Vector |])
    Series(newRowIndex, newData, series.VectorBuilder, series.IndexBuilder)

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
  /// [category:Sampling, resampling and advanced lookup]
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
  /// [category:Sampling, resampling and advanced lookup]
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
  /// [category:Sampling, resampling and advanced lookup]
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
  /// [category:Sampling, resampling and advanced lookup]
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
  ///  - `fillMode` - When set to `Lookup.ExactOrSmaller` or `Lookup.ExactOrGreater`, 
  ///     the function searches for a nearest available observation in an neighboring chunk.
  ///     Otherwise, the function `f` is called with an empty series as an argument.
  ///     Values `Lookup.Smaller` and `Lookup.Greater` are not supported.
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
  /// [category:Sampling, resampling and advanced lookup]
  let resampleUniformInto (fillMode:Lookup) (keyProj:'K1 -> 'K2) (nextKey:'K2 -> 'K2) f (series:Series<'K1, 'V>) =
    if not (fillMode.HasFlag(Lookup.Exact)) then
      invalidOp "resampleUniformInto: The value of 'fillMode' must include 'Exact'."
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
        | Lookup.ExactOrSmaller ->
            let res = reindexed.Get(k, fillMode)
            Series([res.KeyRange |> snd], [res.[res.KeyRange |> snd]])
        | Lookup.ExactOrGreater ->
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
  /// [category:Sampling, resampling and advanced lookup]
  let resampleUniform fillMode (keyProj:'K1 -> 'K2) (nextKey:'K2 -> 'K2) (series:Series<'K1, 'V>) =
    resampleUniformInto fillMode keyProj nextKey id series

  /// [omit]
  /// Module that contains an implementation of sampling for `sampleTime` and 
  /// `sampleTimeInto`. For technical reasons (`inline`) this is public..
  module Implementation = 

    let generateKeys add startOpt interval dir (series:Series<'K, 'V>) =
      let smallest, largest = series.KeyRange
      let comparer = System.Collections.Generic.Comparer<'K>.Default
      let rec genKeys current = seq {
        // For Backward, we need one more key after the end
        if dir = Direction.Backward then yield current
        if comparer.Compare(current, largest) <= 0 then
          // For Forward, we only want keys in the range
          if dir = Direction.Forward then yield current
          yield! genKeys (add current interval) }
      genKeys (defaultArg startOpt smallest)

    /// Given a specified starting time and time span, generates all keys that fit in the
    /// range of the series (and one additional, if `dir = Backward`) and then performs
    /// sampling using `resampleInto`.
    let sampleTimeIntoInternal add startOpt (interval:'T) dir f (series:Series<'K , 'V>) =
      resampleInto (generateKeys add startOpt interval dir series) dir f series

    /// Given a specified starting time and time span, generates all keys that fit in the
    /// range of the series (and one additional, if `dir = Backward`) and then performs lookup
    /// for each key using the specified direction
    let lookupTimeInternal add startOpt (interval:'T) dir lookup (series:Series<'K , 'V>) =
      lookupAll (generateKeys add startOpt interval dir series) lookup series

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
  ///  - `f` - A function that is called to aggregate each chunk into a single value.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline sampleTimeInto interval dir f (series:Series< ^K , ^V >) = 
    let add dt ts = (^K: (static member (+) : ^K * TimeSpan -> ^K) (dt, ts))
    Implementation.sampleTimeIntoInternal add None interval dir (fun _ -> f) series

  /// Performs sampling by time and aggregates chunks obtained by time-sampling into a single
  /// value using a specified function. The operation generates keys starting at the given
  /// `start` time, using the specified `interval` and then obtains chunks based on these
  /// keys in a fashion similar to the `Series.resample` function.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `start` - The initial time to be used for sampling
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///  - `f` - A function that is called to aggregate each chunk into a single value.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline sampleTimeAtInto start interval dir f (series:Series< ^K , ^V >) = 
    let add dt ts = (^K: (static member (+) : ^K * TimeSpan -> ^K) (dt, ts))
    Implementation.sampleTimeIntoInternal add (Some start) interval dir (fun _ -> f) series

  /// Performs sampling by time and returns chunks obtained by time-sampling as a nested  
  /// series. The operation generates keys starting at the first key in the source series,
  /// using the specified `interval` and then obtains chunks based on these
  /// keys in a fashion similar to the `Series.resample` function.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline sampleTime interval dir series = sampleTimeInto interval dir id series

  /// Performs sampling by time and returns chunks obtained by time-sampling as a nested  
  /// series. The operation generates keys starting at the given `start` time, using the 
  /// specified `interval` and then obtains chunks based on these
  /// keys in a fashion similar to the `Series.resample` function.
  ///
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `start` - The initial time to be used for sampling
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - If this parameter is `Direction.Forward`, then each key is
  ///    used as the smallest key in a chunk; for `Direction.Backward`, the keys are
  ///    used as the greatest keys in a chunk.
  ///
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline sampleTimeAt start interval dir series = sampleTimeAtInto start interval dir id series

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting from the smallest key of the original series, using the specified `interval`
  /// and then finds values close to such keys using the specified `lookup` and `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - Specifies how the keys should be generated. `Direction.Forward` means that the 
  ///    key is the smallest value of each chunk (and so first key of the series is returned and 
  ///    the last is not, unless it matches exactly _start + k*interval_); `Direction.Backward`
  ///    means that the first key is skipped and sample is generated at, or just before the end 
  ///    of interval and at the end of the series.
  ///  - `lookup` - Specifies how the lookup based on keys is performed. `Exact` means that the
  ///    values at exact keys will be returned; `NearestGreater` returns the nearest greater key value
  ///    (starting at the first key) and `NearestSmaller` returns the nearest smaller key value
  ///    (starting at most `interval` after the end of the series)
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline lookupTime interval dir lookup (series:Series< ^K , ^V >) = 
    let add dt ts = (^K: (static member (+) : ^K * TimeSpan -> ^K) (dt, ts))
    Implementation.lookupTimeInternal add None interval dir lookup series

  /// Finds values at, or near, the specified times in a given series. The operation generates
  /// keys starting at the specified `start` time, using the specified `interval`
  /// and then finds values close to such keys using the specified `lookup` and `dir`.
  /// 
  /// ## Parameters
  ///  - `series` - An input series to be resampled
  ///  - `start` - The initial time to be used for sampling
  ///  - `interval` - The interval between the individual samples 
  ///  - `dir` - Specifies how the keys should be generated. `Direction.Forward` means that the 
  ///    key is the smallest value of each chunk (and so first key of the series is returned and 
  ///    the last is not, unless it matches exactly _start + k*interval_); `Direction.Backward`
  ///    means that the first key is skipped and sample is generated at, or just before the end 
  ///    of interval and at the end of the series.
  ///  - `lookup` - Specifies how the lookup based on keys is performed. `Exact` means that the
  ///    values at exact keys will be returned; `NearestGreater` returns the nearest greater key value
  ///    (starting at the first key) and `NearestSmaller` returns the nearest smaller key value
  ///    (starting at most `interval` after the end of the series)
  /// 
  /// ## Remarks
  /// This operation is only supported on ordered series. The method throws
  /// `InvalidOperationException` when the series is not ordered. 
  /// 
  /// [category:Sampling, resampling and advanced lookup]
  let inline lookupTimeAt start interval dir lookup (series:Series< ^K , ^V >) = 
    let add dt ts = (^K: (static member (+) : ^K * TimeSpan -> ^K) (dt, ts))
    Implementation.lookupTimeInternal add (Some start) interval dir lookup series


  // ----------------------------------------------------------------------------------------------
  // Merging and zipping
  // ----------------------------------------------------------------------------------------------

  /// Merge two series with distinct keys. When the same key with a value occurs in both
  /// series, an exception is thrown. In that case, you can use `mergeUsing`, which allows
  /// specifying merging behavior.
  ///
  /// [category:Merging and zipping]
  [<CompiledName("Merge")>]
  let merge (series1:Series<'K, 'V>) (series2:Series<'K, 'V>) =
   series1.Merge(series2)

  /// Merge two series with possibly overlapping keys. The `behavior` parameter specifies
  /// how to handle situation when a value is definedin both series.
  ///
  /// ## Parameters
  ///  - `behavior` specifies how to handle values available in both series.
  ///    You can use `UnionBehavior.Exclusive` to throw an exception, or 
  ///    `UnionBehavior.PreferLeft` and `UnionBehavior.PreferRight` to prefer values
  ///    from the first or the second series, respectively.
  /// - `series1` - the first (left) series to be merged
  /// - `series2` - the second (right) series to be merged
  ///
  /// [category:Merging and zipping]
  [<CompiledName("MergeUsing")>]
  let mergeUsing behavior (series1:Series<'K, 'V>) (series2:Series<'K, 'V>) = 
    series1.Merge(series2, behavior)

  /// Merge multiple series with distinct keys. When the same key with a value occurs in two
  /// of the series, an exception is thrown. This function is efficient even when the number
  /// of series to be merged is large.
  ///
  /// [category:Merging and zipping]
  [<CompiledName("MergeAll")>]
  let mergeAll (series: Series<'K, 'V> seq) =
    if series |> Seq.isEmpty then 
        Series([], [])
    else 
        let series = series |> Array.ofSeq
        series.[0].Merge(series.[1 .. ])

  /// Align and zip two series using outer join and exact key matching. The function returns
  /// a series of tuples where both elements may be missing. As a result, it is often easier
  /// to use join on frames instead.
  ///
  /// [category:Merging and zipping]
  [<CompiledName("Zip")>]
  let zip (series1:Series<'K, 'V1>) (series2:Series<'K, 'V2>) =
    series1.Zip(series2)

  /// Align and zip two series using the specified joining mechanism and key matching.
  /// The function returns a series of tuples where both elements may be missing. As a result, 
  /// it is often easier to use join on frames instead.
  ///
  /// ## Parameters
  ///  - `kind` specifies the kind of join you want to use (left, right, inner or outer).
  ///    For inner join, it is better to use `zipInner` instead.
  ///  - `lookup` specifies how matching keys are found when left or right join is used
  ///    on a sorted series. Use this to find the nearest smaller or nearest greater key
  ///    in the other series.
  ///    Supported values are `Lookup.Exact`, `Lookup.ExactOrSmaller` and `Lookup.ExactOrGreater`.
  ///  - `series1` - The first (left) series to be aligned
  ///  - `series2` - The second (right) series to be aligned
  ///
  /// [category:Merging and zipping]
  [<CompiledName("ZipAlign")>]
  let zipAlign kind lookup (series1:Series<'K, 'V1>) (series2:Series<'K, 'V2>) =
    series1.Zip(series2, kind, lookup)

  /// Align and zip two series using inner join and exact key matching. The function returns
  /// a series of tuples with values from the two series.
  ///
  /// [category:Merging and zipping]
  [<CompiledName("ZipInner")>]
  let zipInner (series1:Series<'K, 'V1>) (series2:Series<'K, 'V2>) =
    series1.ZipInner(series2)
    
  /// Align and zip two series using the specified joining mechanism and key matching.
  /// The function calls the specified function `op` to combine values from the two series
  ///
  /// ## Parameters
  ///  - `kind` specifies the kind of join you want to use (left, right, inner or outer).
  ///    For inner join, it is better to use `zipInner` instead.
  ///  - `lookup` specifies how matching keys are found when left or right join is used
  ///    on a sorted series. Use this to find the nearest smaller or nearest greater key
  ///    in the other series.
  ///    Supported values are `Lookup.Exact`, `Lookup.ExactOrSmaller` and `Lookup.ExactOrGreater`.
  ///  - `op` - A function that combines values from the two series. In case of left, right
  ///    or outer join, some of the values may be missing. The function can also return 
  ///    `None` to indicate a missing result.
  ///  - `series1` - The first (left) series to be aligned
  ///  - `series2` - The second (right) series to be aligned
  ///
  /// [category:Merging and zipping]
  [<CompiledName("ZipAlignInto")>]
  let zipAlignInto kind lookup (op:'V1 option->'V2 option->'R option) (series1:Series<'K, 'V1>) (series2:Series<'K, 'V2>) : Series<'K, 'R> =
    let joined = series1.Zip(series2, kind, lookup)
    joined.SelectOptional(fun (KeyValue(_, v)) -> 
      match v with
      | OptionalValue.Present(a, b) -> OptionalValue.ofOption(op (OptionalValue.asOption a) (OptionalValue.asOption b))
      | _ -> OptionalValue.Missing )

  /// Align and zip two series using outer join and exact key matching (use `zipAlignInto`
  /// for more options). The function calls the specified function `op` to combine values 
  /// from the two series
  ///
  /// ## Parameters
  ///  - `op` - A function that combines values from the two series. 
  ///  - `series1` - The first (left) series to be aligned
  ///  - `series2` - The second (right) series to be aligned
  ///
  /// [category:Merging and zipping]
  [<CompiledName("ZipInto")>]
  let zipInto (op:'V1->'V2->'R) (series1:Series<'K, 'V1>) (series2:Series<'K, 'V2>) : Series<'K, 'R> =
    (series1, series2) ||> zipAlignInto JoinKind.Inner Lookup.Exact (fun a b ->
      match a, b with Some a, Some b -> Some (op a b) | _ -> None)