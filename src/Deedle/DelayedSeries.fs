#nowarn "86" // Allow me to locally redefine the <=, <, >, >= operators to use IComparer
namespace Deedle.Delayed

open System
open Deedle
open Deedle.Addressing
open Deedle.Vectors
open Deedle.Indices

// --------------------------------------------------------------------------------------
// Ranges
// --------------------------------------------------------------------------------------

/// Module that contains functions for working with ranges - most importantly
/// it handles flattening of trees constructed by unioning & intersecting ranges
module internal Ranges = 
  type Ranges<'T> = 
    | Range of (('T * BoundaryBehavior) * ('T * BoundaryBehavior))
    | Intersect of Ranges<'T> * Ranges<'T>
    | Union of Ranges<'T> * Ranges<'T>

  let containsRange f input =
    let rec loop = function
      | Range(l, h) -> f l h
      | Union(lo, hi) -> loop lo || loop hi
      | Intersect(lo, hi) -> loop lo && loop hi
    loop input

  /// Test if a range contains the specified sub-range
  /// (the function assumes that the sub-range is smaller than any range in the input)
  let containsSub (comparer:System.Collections.Generic.IComparer<_>) rlo rhi input =
    let (<=) a b = comparer.Compare(a, b) <= 0
    let (>=) a b = comparer.Compare(a, b) >= 0
    input |> containsRange (fun (lo, _) (hi, _) ->
      rlo >= lo && rhi <= hi)

  /// Test if a range contains the specified value
  let contains (comparer:System.Collections.Generic.IComparer<_>) x input =
    let (<) a b = comparer.Compare(a, b) < 0
    let (>) a b = comparer.Compare(a, b) > 0
    input |> containsRange (fun (lo, lob) (hi, hib) ->
      (x > lo && x < hi) || (x = lo && lob = Inclusive) || (x = hi && hib = Inclusive))

  /// Returns an ordered sequence of exclusive ranges
  let flattenRanges overallMin overallMax (comparer:System.Collections.Generic.IComparer<_>) ranges =
    let (<) a b = comparer.Compare(a, b) < 0
    let (>) a b = comparer.Compare(a, b) > 0
    let (<=) a b = comparer.Compare(a, b) <= 0
    let (>=) a b = comparer.Compare(a, b) >= 0

    // First we get all boundary points in the range tree and sort them
    let rec getBoundaries ranges = seq {
      match ranges with 
      | Range((lo, _), (hi, _)) -> yield! [lo; hi]
      | Union(l, r) | Intersect(l, r) -> 
          yield! getBoundaries l
          yield! getBoundaries r }
    let allRanges = Seq.concat [seq [overallMin; overallMax]; getBoundaries ranges]
    let sorted = System.Linq.Enumerable.Distinct(allRanges) |> Array.ofSeq
    Array.sortInPlaceWith (fun a b -> comparer.Compare(a, b)) sorted

    // Now we iterate over all regions and check if we want to include them
    // include the last point twice, otherwise we will not handle end correctly
    let includes = sorted |> Array.map (fun v -> v, if contains comparer v ranges then Inclusive else Exclusive)
    let includes = seq { yield! includes; yield System.Linq.Enumerable.Last(includes) }
    let regions = 
      [ for ((lo, loinc), (hi, hiinc)) as reg in Seq.pairwise includes ->
          (containsSub comparer lo hi ranges), reg ]

    // Walk over regions, remembering if we want to produce one or not
    // and generate non-overlapping, continuous blocks
    let rec yieldRegions sofar regions = seq { 
      match regions with
      | [] -> if sofar <> None then yield sofar.Value
      | (incl, (lo, hi))::regions ->
          match sofar, snd lo, incl with
          // ---X--- ...
          | Some(sv,_), Inclusive, true -> 
              yield! yieldRegions (Some(sv, hi)) regions
          // ---o--- ...
          | Some(sv, _), Exclusive, true ->
              yield sv, lo
              yield! yieldRegions (Some(lo, hi)) regions
          // ---?    ...
          | Some(sv, _), _, false ->
              yield sv, lo
              yield! yieldRegions None regions
          //    x    ...
          | None, Inclusive, false ->
              yield lo, lo
              yield! yieldRegions None regions
          //    o    ...
          | None, Exclusive, false ->
              yield! yieldRegions None regions
          //    ?--- ...
          | None, _, true ->
              yield! yieldRegions (Some(lo, hi)) regions }
    yieldRegions None regions       

// --------------------------------------------------------------------------------------
// Delayed source
// --------------------------------------------------------------------------------------

open Ranges
open System.Threading.Tasks
open System.Collections.Generic

/// Specifies the ranges for which data need to be provided
type internal DelayedSourceRanges<'K> = (('K * BoundaryBehavior) * ('K * BoundaryBehavior))[]
/// Result that should be returned in response to `DelayedSourceRanges` request
type internal DelayedSourceData<'K, 'V> = Async<seq<KeyValuePair<'K, 'V>>>[]

/// This type represents data source for constructing delayed series. To construct
/// a delayed series, use `DelayedSeries.Create` (this creates index and vector 
/// linked to this `DelayedSource`).
///
/// The function `loader` is called outside of the `async` (on the calling thread)
/// but the returned async computations are invoked on background thread.
type internal DelayedSource<'K, 'V when 'K : equality>
    ( rangeMin:'K, rangeMax:'K, ranges:Ranges<'K>, 
      loader:DelayedSourceRanges<'K> -> DelayedSourceData<'K, 'V>) =

  static let vectorBuilder = VectorBuilder.Instance
  static let indexBuilder = IndexBuilder.Instance

  let comparer = System.Collections.Generic.Comparer<'K>.Default
  let (<) a b = comparer.Compare(a, b) < 0
  let (>) a b = comparer.Compare(a, b) > 0
  let (<=) a b = comparer.Compare(a, b) <= 0
  let (>=) a b = comparer.Compare(a, b) >= 0

  // Lazy computation that returns started task whil loads the data 
  // (we use task here so that we can cache the result)
  let asyncData = Lazy.Create(fun () -> 
    let ranges = flattenRanges rangeMin rangeMax comparer ranges |> Array.ofSeq
    let ops = loader ranges
    async {
      let data = new ResizeArray<_>(1000)
      for ((lo, lob), (hi, hib)), op in Seq.zip ranges ops do
        let! rangeValues = op
        for KeyValue(k, v) in rangeValues do
          if (k > lo || (k >= lo && lob = Inclusive)) &&
             (k < hi || (k <= hi && hib = Inclusive)) then data.Add( (k, v) )
      let vector = vectorBuilder.Create(data.ToArray() |> Array.map snd)
      let index = indexBuilder.Create(Seq.map fst data, Some true)
      return index, vector } |> Async.StartAsTask)

  member x.Ranges = ranges
  member x.RangeMax = rangeMax
  member x.RangeMin = rangeMin

  member x.Loader = loader
  member x.AsyncData = asyncData.Value
  member x.Index = fst asyncData.Value.Result
  member x.Values = snd asyncData.Value.Result

// --------------------------------------------------------------------------------------
// Delayed vector, index & index builder
// --------------------------------------------------------------------------------------

/// A delayed vector that is linked to a DelayedSource specified during construction
/// (This simply delegates all operations to the 'source.Values' vector)
type internal DelayedVector<'K, 'V when 'K : equality> internal (source:DelayedSource<'K, 'V>) = 
  member x.Source = source
  // Boilerplate - all operations on the vector just force the retrieval 
  // of the data and then delegate the request to the actual vector
  interface IVector with
    member val ElementType = typeof<'V>
    member x.SuppressPrinting = true
    member x.GetObject(index) = source.Values.GetObject(index)
    member x.ObjectSequence = source.Values.ObjectSequence
  interface IVector<'V> with
    member x.GetValue(index) = source.Values.GetValue(index)
    member x.Data = source.Values.Data
    member x.SelectMissing(f) = source.Values.SelectMissing(f)
    member x.Select(f) = source.Values.Select(f)


/// Delayed index that is lnked to a DelayedSource specified during construction
/// (This simply delegates all operations to the 'source.Keys' index)
type internal DelayedIndex<'K, 'V when 'K : equality> internal (source:DelayedSource<'K, 'V>) = 
  member x.Source = source
  interface IIndex<'K> with
    member x.KeyAt index = source.Index.KeyAt index
    member x.KeyCount = source.Index.KeyCount
    member x.IsEmpty = false
    member x.Builder = DelayedIndexBuilder() :> IIndexBuilder
    member x.KeyRange = source.RangeMin, source.RangeMax
    member x.Keys = source.Index.Keys
    member x.Locate(key) = source.Index.Locate(key)
    member x.Lookup(key, semantics, check) = source.Index.Lookup(key, semantics, check)
    member x.Mappings = source.Index.Mappings
    member x.IsOrdered = true // source.Index.Ordered
    member x.Comparer = source.Index.Comparer
  interface IDelayedIndex<'K> with
    member x.Invoke(func) = func.Invoke<'V>(x)
    member x.SourceIndex = source.Index

/// In the DelayedIndexBuilder, we do not know the type of values, so this 
/// is a less generic interface that gives us a way for accessing it...
and internal IDelayedIndex<'K when 'K : equality> =
  abstract Invoke<'R> : DelayedIndexFunction<'K, 'R> -> 'R
  abstract SourceIndex : IIndex<'K>
  
/// A polymorphic function that is passed to IDelayedIndex.Invoke
and internal DelayedIndexFunction<'K, 'R when 'K : equality> = 
  abstract Invoke<'V> : DelayedIndex<'K, 'V> -> 'R

/// Delayed index builder - this is where interesting things happen. Most operations
/// are still delegated to LinearIndexBuilder, but the `GetRange` method looks at the
/// index and if it is DelayedIndex, then it uses the `Source` to build a new `Source`
/// with a restricted range.
and internal DelayedIndexBuilder() =
  let builder = IndexBuilder.Instance
  interface IIndexBuilder with
    member x.Create(keys, ordered) = builder.Create(keys, ordered)
    member x.Aggregate(index, aggregation, vector, selector) = builder.Aggregate(index, aggregation, vector, selector)
    member x.GroupBy(index, keySel, vector, valueSel) = builder.GroupBy(index, keySel, vector, valueSel)
    member x.GroupWith(index, keys, vec) = builder.GroupWith(index, keys, vec)
    member x.OrderIndex(sc) = builder.OrderIndex(sc)
    member x.Union(sc1, sc2) = builder.Union(sc1, sc2)
    member x.Append(sc1, sc2, transform) = builder.Append(sc1, sc2, transform)
    member x.Intersect(sc1, sc2) = builder.Intersect(sc1, sc2)
    member x.LookupLevel(sc, key) = builder.LookupLevel(sc, key)
    member x.WithIndex(index1, f, vector) = builder.WithIndex(index1, f, vector)
    member x.Reindex(index1, index2, semantics, vector, cond) = builder.Reindex(index1, index2, semantics, vector, cond)
    member x.DropItem(sc, key) = builder.DropItem(sc, key)
    member x.Resample(index, keys, close, vect, ks, vs) = builder.Resample(index, keys, close, vect, ks, vs)
    
    member x.Project(index:IIndex<'K>) = 
      // If the index is delayed, then projection evaluates it
      match index with
      | :? IDelayedIndex<'K> as index -> index.SourceIndex
      | _ -> index

    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) =
      match index with
      | :? IDelayedIndex<'K> as index ->
        // See comment below for how DelayedIndexFunction works
        { new DelayedIndexFunction<'K, _> with
            member x.Invoke<'V>(index:DelayedIndex<'K, 'V>) =
              // Start the task and return the index
              let asyncIndex = async {
                let! index, _ = Async.AwaitTask index.Source.AsyncData
                return index }

              // Return asynchronous vector construction that awaits for vector data
              // of the associated vector, or just returns any other vectors
              let cmd = Vectors.AsyncCustomCommand([vector], fun vectors -> async {
                match List.head vectors with
                | (:? DelayedVector<'K, 'V> as lv) when lv.Source = index.Source ->
                    let! _, vect = Async.AwaitTask index.Source.AsyncData
                    return vect  :> IVector
                | other -> 
                    return other })                    
              asyncIndex, cmd }
        |> index.Invoke
      | _ ->
        builder.AsyncMaterialize((index, vector))

    member x.GetRange(index, optLo:option<'K * _>, optHi:option<'K * _>, vector) = 
      match index with
      | :? IDelayedIndex<'K> as index ->
        // Use 'index.Invoke' to run the 'Invoke' method of the following
        // object expression with an appropriate type of values 'V
        { new DelayedIndexFunction<'K, _> with
            member x.Invoke<'V>(index:DelayedIndex<'K, 'V>) =

              // Calculate range for the current slicing
              let lo = defaultArg optLo (index.Source.RangeMin, BoundaryBehavior.Inclusive)
              let hi = defaultArg optHi (index.Source.RangeMax, BoundaryBehavior.Inclusive)
              let range = Intersect(index.Source.Ranges, Range(lo, hi))
              // A function that combines the current slice with another 
              // range and returns a source for this portion of data
              let restrictSource otherRange loader = 
                let ranges = Intersect(range, otherRange)
                DelayedSource<'K, 'V>(index.Source.RangeMin, index.Source.RangeMax, ranges, loader)
                
              // Create a new Delayed source for this index with more restricted range
              let source = restrictSource index.Source.Ranges index.Source.Loader
              let newIndex = DelayedIndex<'K, 'V>(source)

              let (|Singleton|) list = List.head list
              let cmd = Vectors.CustomCommand([vector], fun (Singleton vector) ->
                match vector with
                | :? DelayedVector<'K, 'V> as lv -> 
                    if lv.Source = index.Source then 
                      DelayedVector(source) :> IVector 
                    else
                      let source = restrictSource lv.Source.Ranges lv.Source.Loader
                      DelayedVector(source) :> IVector
                | _ -> 
                    //let  = builder.GetRange(index, optLo, optHi, Vectors.Return 0)
                    //ArrayVector.ArrayVectorBuilder.Instance.Build(cmd, [| vector |])
                    

                    failwith "TODO: This should probably be supported?")
              newIndex :> IIndex<'K>, cmd }
        |> index.Invoke
      | _ ->
        builder.GetRange(index, optLo, optHi, vector)

// --------------------------------------------------------------------------------------
// Public API for creating delayed series
// --------------------------------------------------------------------------------------

namespace Deedle

open System
open System.Collections.Generic
open System.Threading.Tasks
open Deedle.Delayed
open Deedle.Indices
open Deedle.Vectors.ArrayVector

/// This type exposes a single static method `DelayedSeries.Create` that can be used for
/// constructing data series (of type `Series<K, V>`) with lazily loaded data. You can
/// use this functionality to create series that represents e.g. an entire price history
/// in a database, but only loads data that are actually needed. For more information
/// see the [lazy data loading tutorial](../lazysource.html).
/// 
/// ### Example
/// 
/// Assuming we have a function `generate lo hi` that generates data in the specified
/// `DateTime` range, we can create lazy series as follows:
///
///     let ls = DelayedSeries.Create(min, max, fun (lo, lob) (hi, hib) -> 
///       async { 
///         printfn "Query: %A - %A" (lo, lob) (hi, hib)
///         return generate lo hi })
///
/// The arguments `min` and `max` specfify the complete range of the series. The 
/// function passed to `Create` is called with minimal and maximal required key
/// (`lo` and `hi`) and with two values that specify boundary behaviour.
type DelayedSeries =
  /// A C#-friendly function that creates lazily loaded series. The method requires
  /// the overall range of the series (smallest and greatest key) and a function that
  /// loads the data. In this overload, the function is a `Func` delegate taking 
  /// information about the requested range and returning `Task<T>` that produces the data.
  ///
  /// ## Parameters
  /// 
  ///  - `min` - The smallest key that should be present in the created series.
  ///  - `min` - The greatests key that should be present in the created series.
  ///  - `loader` - A delegate which returns a task that loads the data in a specified 
  ///    range. The delegate is called with four arguments specifying the minimal and
  ///    maximal key and two `BoundaryBehavior` values specifying whether the low and
  ///    high ranges are inclusive or exclusive.
  ///
  /// ## Remarks
  ///
  /// For more information see the [lazy data loading tutorial](../lazysource.html).
  /// The operation calls `loader` (and so creates the tasks) on the thread that is
  /// requesting the result.
  static member Create(min, max, loader:Func<_, _, _, _, Task<seq<KeyValuePair<'K, 'V>>>>) : Series<'K, 'V> =
    DelayedSeries.Create(min, max, fun (l, lb) (h, hb)-> Async.AwaitTask (loader.Invoke(l,lb,h,hb)))

  /// An F#-friendly function that creates lazily loaded series. The method requires
  /// the overall range of the series (smallest and greatest key) and a function that
  /// loads the data. The function is called with two tuples that specify lower and upper
  /// boundary. It returns an asynchronous workflow that produces the data.
  ///
  /// ## Parameters
  /// 
  ///  - `min` - The smallest key that should be present in the created series.
  ///  - `min` - The greatests key that should be present in the created series.
  ///  - `loader` - A function which returns an asynchronous workflow that loads the data in a 
  ///    specified range. The function is called with two tuples consisting of key and 
  ///    `BoundaryBehavior` values. The keys specify lower and upper boundary and 
  ///    `BoundaryBehavior` values can be either `Inclusive` or `Exclusive`.
  ///
  /// ## Remarks
  ///
  /// For more information see the [lazy data loading tutorial](../lazysource.html).
  static member Create(min, max, loader:_ -> _ -> Async<_>) : Series<'K, 'V> =
    let initRange = Ranges.Range((min, BoundaryBehavior.Inclusive), (max, BoundaryBehavior.Inclusive))
    let series = DelayedSource<'K, 'V>(min, max, initRange, fun ranges -> 
      ranges |> Array.map (fun (l, h) -> loader l h))
    let index = DelayedIndex(series)
    let vector = DelayedVector(series)
    let vectorBuilder = VectorBuilder.Instance
    Series<'K, 'V>(index, vector, vectorBuilder, DelayedIndexBuilder())
