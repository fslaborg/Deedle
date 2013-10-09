#nowarn "86" // Allow me to locally redefine the <=, <, >, >= operators to use IComparer
namespace FSharp.DataFrame.Delayed

open System
open System.Linq
open FSharp.DataFrame
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.Indices

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
    let sorted = allRanges.Distinct() |> Array.ofSeq
    Array.sortInPlaceWith (fun a b -> comparer.Compare(a, b)) sorted

    // Now we iterate over all regions and check if we want to include them
    // include the last point twice, otherwise we will not handle end correctly
    let includes = sorted |> Array.map (fun v -> v, if contains comparer v ranges then Inclusive else Exclusive)
    let includes = seq { yield! includes; yield includes.Last() }
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

/// This type represents data source for constructing delayed series. To construct
/// a delayed series, use `DelayedSeries.Create` (this creates index and vector 
/// linked to this `DelayedSource`).
type internal DelayedSource<'K, 'V when 'K : equality>
    (rangeMin:'K, rangeMax:'K, ranges:Ranges<'K>, loader:System.Func<'K, BoundaryBehavior, 'K, BoundaryBehavior, Task<seq<KeyValuePair<'K, 'V>>>>) =

  static let vectorBuilder = ArrayVector.ArrayVectorBuilder.Instance
  static let indexBuilder = Linear.LinearIndexBuilder.Instance

  let comparer = System.Collections.Generic.Comparer<'K>.Default
  let (<) a b = comparer.Compare(a, b) < 0
  let (>) a b = comparer.Compare(a, b) > 0
  let (<=) a b = comparer.Compare(a, b) <= 0
  let (>=) a b = comparer.Compare(a, b) >= 0

  // Lazy value that loads the data when needed
  let seriesData = Lazy.Create(fun () ->
    let ranges = flattenRanges rangeMin rangeMax comparer ranges
    let data = 
      [| for (lo, lob), (hi, hib) in ranges do
           let dataTask = loader.Invoke(lo, lob, hi, hib)
           for KeyValue(k, v) in dataTask.Result do
             if (k > lo || (k >= lo && lob = Inclusive)) &&
                (k < hi || (k <= hi && hib = Inclusive)) then
               yield k, v |] 
    let vector = vectorBuilder.Create(Array.map snd data)
    let index = indexBuilder.Create(Seq.map fst data, Some true)
    index, vector )

  member x.Ranges = ranges
  member x.RangeMax = rangeMax
  member x.RangeMin = rangeMin

  member x.Loader = loader
  member x.Index = fst seriesData.Value
  member x.Values = snd seriesData.Value

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
    member x.IsEmpty = false
    member x.Builder = DelayedIndexBuilder() :> IIndexBuilder
    member x.KeyRange = source.Index.KeyRange
    member x.Keys = source.Index.Keys
    member x.Lookup(key, semantics, check) = source.Index.Lookup(key, semantics, check)
    member x.Mappings = source.Index.Mappings
    member x.Range = source.Index.Range
    member x.Ordered = true // source.Index.Ordered
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
  let builder = Linear.LinearIndexBuilder.Instance
  interface IIndexBuilder with
    member x.Create(keys, ordered) = builder.Create(keys, ordered)
    member x.Aggregate(index, aggregation, vector, valueSel, keySel) = builder.Aggregate(index, aggregation, vector, valueSel, keySel)
    member x.GroupBy(index, keySel, vector, valueSel) = builder.GroupBy(index, keySel, vector, valueSel)
    member x.OrderIndex(sc) = builder.OrderIndex(sc)
    member x.Union(sc1, sc2) = builder.Union(sc1, sc2)
    member x.Append(sc1, sc2, transform) = builder.Append(sc1, sc2, transform)
    member x.Intersect(sc1, sc2) = builder.Intersect(sc1, sc2)
    member x.LookupLevel(sc, key) = builder.LookupLevel(sc, key)
    member x.WithIndex(index1, f, vector) = builder.WithIndex(index1, f, vector)
    member x.Reindex(index1, index2, semantics, vector) = builder.Reindex(index1, index2, semantics, vector)
    member x.DropItem(sc, key) = builder.DropItem(sc, key)
    member x.Resample(index, keys, close, vect, ks, vs) = builder.Resample(index, keys, close, vect, ks, vs)
    
    member x.Project(index:IIndex<'K>) = 
      // If the index is delayed, then projection evaluates it
      match index with
      | :? IDelayedIndex<'K> as index -> index.SourceIndex
      | _ -> index

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

namespace FSharp.DataFrame

open System
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.DataFrame.Delayed
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors.ArrayVector

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
  static member Create(min, max, loader:Func<_, _, _, _, Task<seq<KeyValuePair<'K, 'V>>>>) : Series<'K, 'V> =
    let initRange = Ranges.Range((min, BoundaryBehavior.Inclusive), (max, BoundaryBehavior.Inclusive))
    let series = DelayedSource<'K, 'V>(min, max, initRange, loader)
    let index = DelayedIndex(series)
    let vector = DelayedVector(series)
    // DelayedIndex never issues any special commands, 
    // so we can just use ArrayVector builder
    let vectorBuilder = ArrayVectorBuilder.Instance
    Series<'K, 'V>(index, vector, vectorBuilder, DelayedIndexBuilder())

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
  static member Create(min, max, loader) : Series<'K, 'V> =
    DelayedSeries.Create<'K, 'V>(min, max, fun lo lob hi hib -> 
      async { 
        let! res = loader (lo, lob) (hi, hib) 
        let proj = res |> Seq.map KeyValue.Create 
        return proj } |> Async.StartAsTask )

