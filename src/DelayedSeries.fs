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

module Ranges = 
  type Ranges<'T> = 
    | Range of (('T * BoundaryBehavior) * ('T * BoundaryBehavior))
    | Intersect of Ranges<'T> * Ranges<'T>
    | Union of Ranges<'T> * Ranges<'T>

  /// Test if a range contains the specified value
  let contains (comparer:System.Collections.Generic.IComparer<_>) x input =
    let (<) a b = comparer.Compare(a, b) < 0
    let (>) a b = comparer.Compare(a, b) > 0
    let rec loop = function
      | Range((lo, lob), (hi, hib)) ->
          (x > lo && x < hi) || (x = lo && lob = Inclusive) || (x = hi && hib = Inclusive)
      | Union(lo, hi) -> loop lo || loop hi
      | Intersect(lo, hi) -> loop lo && loop hi
    loop input

  /// Returns an ordered sequence of exclusive ranges
  let flattenRanges overallMin overallMax (comparer:System.Collections.Generic.IComparer<_>) ranges middle =
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
          (contains comparer (middle lo hi) ranges), reg ]

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

(* tests

  // skipToEndOfRegion (5, Inclusive) [ ((5, Exclusive), (5, Exclusive)) ]

  // skipToEndOfRegion (5, Inclusive) [ ((1, Inclusive), (5, Exclusive)) ]
  // skipToEndOfRegion (5, Exclusive) [ ((1, Inclusive), (5, Exclusive)) ]
  // skipToEndOfRegion (5, Exclusive) [ ((1, Inclusive), (5, Exclusive)); ((5, Exclusive), (6, Inclusive)) ]
  // skipToEndOfRegion (5, Exclusive) [ ((1, Inclusive), (6, Exclusive)); ((6, Exclusive), (6, Inclusive)) ]


  // allUntil 8 [] [ (1,2); (3,4); (6,9) ] = ([(1, 2); (3, 4); (6, 8)], [(8, 9)])
  // allUntil 5 [] [ (1,2); (3,4); (6,9) ] = ([(1, 2); (3, 4)], [(6, 9)])
  // allUntil 10 [] [ (10, 11) ] = ([10,10], [10, 11])

  let check range = 
    let flat = flattenRanges 0 100 (System.Collections.Generic.Comparer<int>.Default) range (fun a b -> (a + b) / 2)
    [ 0 .. 100 ] |> Seq.iter (fun x ->
      let expected = contains x range
      let actual = flat |> Seq.exists (fun ((lo, lob), (hi, hib)) -> 
        (x > lo && x < hi) || (x = lo && lob = Inclusive) || (x = hi && hib = Inclusive))
      if not (expected = actual) then failwithf "Mismatch at %d" x)

  let empty range = [ 0 .. 100 ] |> Seq.exists (fun v -> contains v range) |> not
  let incomplete range = [ 0 .. 100 ] |> Seq.forall (fun v -> contains v range) |> not

  check (Intersect(Range((1, Exclusive), (5, Inclusive)), Range((3, Inclusive), (7, Inclusive))))
  check (Intersect(Range((1, Exclusive), (5, Exclusive)), Range((5, Inclusive), (7, Inclusive))))
  check (Intersect(Range((1, Exclusive), (5, Exclusive)), Range((5, Exclusive), (7, Inclusive))))

  check (Union(Range((1, Exclusive), (5, Inclusive)), Range((3, Inclusive), (7, Inclusive))))
  check (Union(Range((1, Exclusive), (5, Exclusive)), Range((5, Inclusive), (7, Inclusive))))
  check (Union(Range((1, Exclusive), (5, Exclusive)), Range((5, Exclusive), (7, Inclusive))))

  check (Union(Range((1, Inclusive), (5, Exclusive)), Range((5, Inclusive), (5, Inclusive))))

  let rec randomRanges (rnd:System.Random) lo hi = 
    let mid = rnd.Next(lo, hi+1)
    let midl = rnd.Next(lo, mid+1)
    let midr = rnd.Next(mid, hi+1)
    match rnd.Next(5) with
    | 0 -> Union(randomRanges rnd midl mid, randomRanges rnd mid midr)
    | 1 -> Intersect(randomRanges rnd lo midr, randomRanges rnd midl hi)
    | _ -> 
      let lob, hib = 
        let beh() = if rnd.Next(2) = 0 then Inclusive else Exclusive
        if lo = hi then let b = beh() in b, b
        else beh(), beh()
      Range( (lo, lob), (hi, hib) )

  for i in 0 .. 10000 do 
    try randomRanges (Random(i)) 0 100 |> check
    with e -> failwithf "Failed with seed %d (%s)" i e.Message
*)
// --------------------------------------------------------------------------------------
// Delayed source
// --------------------------------------------------------------------------------------

open Ranges
open System.Threading.Tasks

/// This type represents data source for constructing delayed series. To construct
/// a delayed series, use `DelayedSeries.Create` (this creates index and vector 
/// linked to this `DelayedSource`).
type DelayedSource<'K, 'V when 'K : equality>
    (rangeMin:'K, rangeMax:'K, midpoint:'K -> 'K -> 'K, ranges:Ranges<'K>, identifier, loader:System.Func<'K * BoundaryBehavior, 'K * BoundaryBehavior, Task<seq<'K * 'V>>>) =

  static let vectorBuilder = ArrayVector.ArrayVectorBuilder.Instance
  static let indexBuilder = Linear.LinearIndexBuilder.Instance

  // Lazy value that loads the data when needed
  let seriesData = Lazy.Create(fun () ->
    let ranges = flattenRanges rangeMin rangeMax (System.Collections.Generic.Comparer<'K>.Default) ranges midpoint
    let data = 
      [| for lo, hi in ranges do
           let dataTask = loader.Invoke(lo, hi)
           let data = dataTask.Result
           yield! data |]
    let vector = vectorBuilder.CreateNonOptional(Array.map snd data)
    let index = indexBuilder.Create(Seq.map fst data, Some true)
    index, vector )

  member x.Ranges = ranges
  member x.RangeMax = rangeMax
  member x.RangeMin = rangeMin

  member x.Identifier = identifier
  member x.MidPoint = midpoint
  member x.Loader = loader
  member x.Index = fst seriesData.Value
  member x.Values = snd seriesData.Value

// --------------------------------------------------------------------------------------
// Delayed vector, index & index builder
// --------------------------------------------------------------------------------------

/// A delayed vector that is linked to a DelayedSource specified during construction
/// (This simply delegates all operations to the 'source.Values' vector)
type DelayedVector<'K, 'V when 'K : equality> internal (source:DelayedSource<'K, 'V>) = 
  member x.Source = source
  // Boilerplate - all operations on the vector just force the retrieval 
  // of the data and then delegate the request to the actual vector
  interface IVector with
    member val ElementType = typeof<float>
    member x.SuppressPrinting = true
    member x.GetObject(index) = source.Values.GetObject(index)
  interface IVector<'V> with
    member x.GetValue(index) = source.Values.GetValue(index)
    member x.Data = source.Values.Data
    member x.SelectOptional(f) = source.Values.SelectOptional(f)
    member x.Select(f) = source.Values.Select(f)


/// Delayed index that is lnked to a DelayedSource specified during construction
/// (This simply delegates all operations to the 'source.Keys' index)
type DelayedIndex<'K, 'V when 'K : equality> internal (source:DelayedSource<'K, 'V>) = 
  member x.Source = source
  interface IIndex<'K> with
    member x.Builder = DelayedIndexBuilder() :> IIndexBuilder
    member x.KeyRange = source.Index.KeyRange
    member x.Keys = source.Index.Keys
    member x.Lookup(key, semantics, check) = source.Index.Lookup(key, semantics, check)
    member x.Mappings = source.Index.Mappings
    member x.Range = source.Index.Range
    member x.Ordered = source.Index.Ordered
    member x.Comparer = source.Index.Comparer
  interface IDelayedIndex<'K> with
    member x.Invoke(func) = func.Invoke<'V>(x)

/// In the DelayedIndexBuilder, we do not know the type of values, so this 
/// is a less generic interface that gives us a way for accessing it...
and IDelayedIndex<'K when 'K : equality> =
  abstract Invoke<'R> : DelayedIndexFunction<'K, 'R> -> 'R
/// A polymorphic function that is passed to IDelayedIndex.Invoke
and DelayedIndexFunction<'K, 'R when 'K : equality> = 
  abstract Invoke<'V> : DelayedIndex<'K, 'V> -> 'R

/// Delayed index builder - this is where interesting things happen. Most operations
/// are still delegated to LinearIndexBuilder, but the `GetRange` method looks at the
/// index and if it is DelayedIndex, then it uses the `Source` to build a new `Source`
/// with a restricted range.
and DelayedIndexBuilder() =
  let builder = Linear.LinearIndexBuilder.Instance
  interface IIndexBuilder with
    member x.Create(keys, ordered) = builder.Create(keys, ordered)
    member x.Aggregate(index, aggregation, vector, valueSel, keySel) = builder.Aggregate(index, aggregation, vector, valueSel, keySel)
    member x.GroupBy(index, keySel, vector, valueSel) = builder.GroupBy(index, keySel, vector, valueSel)
    member x.OrderIndex(index, vector) = builder.OrderIndex(index, vector)
    member x.Union(index1, index2, vector1, vector2) = 
      let res = builder.Union(index1, index2, vector1, vector2)
      res

    member x.Append(index1, index2, vector1, vector2, transform) = builder.Append(index1, index2, vector1, vector2, transform)
    member x.Intersect(index1, index2, vector1, vector2) = builder.Intersect(index1, index2, vector1, vector2)
    member x.WithIndex(index1, f, vector) = builder.WithIndex(index1, f, vector)
    member x.Reindex(index1, index2, semantics, vector) = builder.Reindex(index1, index2, semantics, vector)
    member x.DropItem(index, key, vector) = builder.DropItem(index, key, vector)
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
              let restrictSource otherRange identifier loader = 
                let ranges = Intersect(range, otherRange)
                DelayedSource<'K, 'V>(index.Source.RangeMin, index.Source.RangeMax, index.Source.MidPoint, ranges, identifier, loader)
                
              // Create a new Delayed source for this index with more restricted range
              let source = restrictSource index.Source.Ranges index.Source.Identifier index.Source.Loader
              let newIndex = DelayedIndex<'K, 'V>(source)

              let (|Singleton|) list = List.head list
              let cmd = Vectors.CustomCommand([vector], fun (Singleton vector) ->
                match vector with
                | :? DelayedVector<'K, 'V> as lv -> 
                    if lv.Source.Identifier = source.Identifier then 
                      DelayedVector(source) :> IVector 
                    else
                      let source = restrictSource lv.Source.Ranges lv.Source.Identifier lv.Source.Loader
                      DelayedVector(source) :> IVector
                | _ -> 
                    failwith "TODO: This should probably be supported?")
              newIndex :> IIndex<'K>, cmd }
        |> index.Invoke
      | _ ->
        builder.GetRange(index, optLo, optHi, vector)

// --------------------------------------------------------------------------------------
// Public API for creating delayed series
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame

open FSharp.DataFrame.Delayed
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors.ArrayVector

type DelayedSeries =
  static member Create<'K, 'V when 'K : equality>(min, max, midpoint, token, loader) =
    let initRange = Ranges.Range((min, BoundaryBehavior.Inclusive), (max, BoundaryBehavior.Inclusive))
    let series = DelayedSource<'K, 'V>(min, max, midpoint, initRange, token, loader)
    let index = DelayedIndex(series)
    let vector = DelayedVector(series)
    // DelayedIndex never issues any special commands, 
    // so we can just use ArrayVector builder
    let vectorBuilder = ArrayVectorBuilder.Instance
    Series(index, vector, vectorBuilder, DelayedIndexBuilder())
