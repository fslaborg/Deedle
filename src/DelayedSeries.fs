#nowarn "86" // Allow me to locally redefine the <=, <, >, >= operators to use IComparer
namespace FSharp.DataFrame.Delayed

open System
open FSharp.DataFrame
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.Indices

// --------------------------------------------------------------------------------------
// Ranges
// --------------------------------------------------------------------------------------

module Ranges = 
  type Ranges<'T> = 
    | Range of ('T * 'T)
    | Union of Ranges<'T> * Ranges<'T>
    | Intersect of Ranges<'T> * Ranges<'T>

  /// Returns an ordered sequence of exclusive ranges
  let flattenRanges (comparer:System.Collections.Generic.IComparer<_>) ranges =
    let (<=) a b = comparer.Compare(a, b) <= 0
    let (>=) a b = comparer.Compare(a, b) >= 0
    let (>) a b = comparer.Compare(a, b) > 0
    let (|SmallerBy|) op = function
      | ((h1::_) as l1), ((h2::_) as l2) when op h1 <= op h2 -> l1, l2
      | l1, l2 -> l2, l1
    let rec endJustAfter e = function
      | ((s1, e1) as h1)::t1 ->
          if s1 > e then e, h1::t1
          elif s1 <= e && e1 >= e then e1, t1
          else (*s1 <= e && e1 < e *) endJustAfter e t1
      | [] -> e, []
    let rec allUntil e acc = function
      | ((s1, e1) as h1)::t1 when s1 > e -> List.rev acc, h1::t1
      | ((s1, e1) as h1)::t1 when e1 <= e -> allUntil e (h1::acc) t1
      | ((s1, e1) as h1)::t1 (* s1 <= e < e1 *) -> List.rev ((s1, e)::acc), (e, e1)::t1
      | [] -> List.rev acc, []
    let rec loop = function
      | Range(lo, hi) -> [ (lo, hi) ]
      | Union(l, r) ->
          let rec union = function
            // One is empty - just return the other
            | [], other | other, [] -> other
            // Assume that s1 <= s2 
            | SmallerBy fst (((s1, e1) as h1)::t1, r2) ->
                let e, r2' = endJustAfter e1 r2
                (s1, e) :: (union (t1, r2'))
            | _ -> failwith "pattern matching failed"
          union (loop l, loop r)
      | Intersect(l, r) -> 
          let rec intersect = function
            // One is empty - so the intersection is empty
            | [], _ | _, [] -> []
            // Assume that s1 <= s2
            | SmallerBy fst (((s1, e1) as h1)::t1, r2) ->
                let incl, r2' = allUntil e1 [] r2
                incl @ (intersect (t1, r2'))
            | _ -> failwith "pattern matching failed"
          intersect (loop l, loop r)
    loop ranges
(* tests


// endJustAfter 5 [ (1,2); (3,4); (6, 7) ] = 5, [(6,7)]
// endJustAfter 3 [ (0,1); (2,4); (6, 7) ] = 4, [(6,7)]

// allUntil 8 [] [ (1,2); (3,4); (6,9) ] = ([(1, 2); (3, 4); (6, 8)], [(8, 9)])
// allUntil 5 [] [ (1,2); (3,4); (6,9) ] = ([(1, 2); (3, 4)], [(6, 9)])
// allUntil 10 [] [ (10, 11) ] = ([10,10], [10, 11])

let rec contains x = function
  | Range(lo, hi) -> x >= lo && x <= hi
  | Union(lo, hi) -> contains x lo || contains x hi
  | Intersect(lo, hi) -> contains x lo && contains x hi

let check range = 
  let flat = flattenRanges range
  [ 0 .. 100 ] |> Seq.forall (fun i ->
    (contains i range) = (flat |> Seq.exists (fun (lo, hi) -> i >= lo && i <= hi)))

let empty range = [ 0 .. 100 ] |> Seq.exists (fun v -> contains v range) |> not
let incomplete range = [ 0 .. 100 ] |> Seq.forall (fun v -> contains v range) |> not

check (Intersect(Range(1, 5), Range(3, 7)))
check (Intersect(Union(Range(1, 5), Range(5, 6)), Range(3, 7)))
check (Union(Range(1, 3), Range(2, 4)))
check (Union(Range(1, 2), Range(3, 4)))
check (Union(Range(1, 2), Union(Range(3, 4), Range(2, 5))))

let rec randomRanges (rnd:System.Random) lo hi = 
  let mid = rnd.Next(lo, hi+1)
  let midl = rnd.Next(lo, mid+1)
  let midr = rnd.Next(mid, hi+1)
  match rnd.Next(5) with
  | 0 -> Union(randomRanges rnd midl mid, randomRanges rnd mid midr)
  | 1 -> Intersect(randomRanges rnd lo midr, randomRanges rnd midl hi)
  | _ -> Range(lo, hi)

for i in 0 .. 10000 do 
  if not (randomRanges (Random(i)) 0 100 |> check) then 
    failwithf "Failed with %d" i
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
    (rangeMin:'K, rangeMax:'K, ranges:Ranges<'K>, identifier, loader:System.Func<_, _, Task<_>>) =

  static let vectorBuilder = ArrayVector.ArrayVectorBuilder.Instance
  static let indexBuilder = Linear.LinearIndexBuilder.Instance

  // Lazy value that loads the data when needed
  let seriesData = Lazy.Create(fun () ->
    let ranges = flattenRanges (System.Collections.Generic.Comparer<'K>.Default) ranges
    let lo, hi = match ranges with [(lo, hi)] -> lo, hi | _ -> failwith "Unioning is TODO"
    let dataTask = loader.Invoke(lo, hi)
    let data = dataTask.Result
    let vector = vectorBuilder.CreateNonOptional(Array.map snd data) :> IVector<'V>
    let index = indexBuilder.Create(Seq.map fst data, Some true) :> IIndex<'K>
    index, vector )

  member x.Ranges = ranges
  member x.RangeMax = rangeMax
  member x.RangeMin = rangeMin

  member x.Identifier = identifier
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
    member x.GetRange(index, optLo:option<'K>, optHi:option<'K>, vector) = 
      match index with
      | :? IDelayedIndex<'K> as index ->
        // Use 'index.Invoke' to run the 'Invoke' method of the following
        // object expression with an appropriate type of values 'V
        { new DelayedIndexFunction<'K, _> with
            member x.Invoke<'V>(index:DelayedIndex<'K, 'V>) =

              // Calculate range for the current slicing
              let lo = defaultArg optLo index.Source.RangeMin
              let hi = defaultArg optHi index.Source.RangeMax
              let range = Intersect(index.Source.Ranges, Range(lo, hi))
              // A function that combines the current slice with another 
              // range and returns a source for this portion of data
              let restrictSource otherRange identifier loader = 
                let ranges = Intersect(range, otherRange)
                DelayedSource<'K, 'V>(index.Source.RangeMin, index.Source.RangeMax, ranges, identifier, loader)
                
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
open FSharp.DataFrame.Vectors.ArrayVector

type DelayedSeries =
  static member Create<'K, 'V when 'K : equality>(min, max, token, loader) =
    let series = DelayedSource<'K, 'V>(min, max, Ranges.Range(min, max), token, loader)
    let index = DelayedIndex(series)
    let vector = DelayedVector(series)
    // DelayedIndex never issues any special commands, 
    // so we can just use ArrayVector builder
    let vectorBuilder = ArrayVectorBuilder.Instance
    Series(index, vector, vectorBuilder, DelayedIndexBuilder())


