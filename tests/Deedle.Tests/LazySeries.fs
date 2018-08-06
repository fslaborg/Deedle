#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.LazySeries
#endif

open System
open System.Collections.Generic
open FsUnit
open FsCheck
open NUnit.Framework
open Deedle
open Deedle.Delayed
open Deedle.Internal
open Deedle.Indices

// ------------------------------------------------------------------------------------------------
// Basic tests
// ------------------------------------------------------------------------------------------------

let loadIntegers (lo, lob) (hi, hib) = async { 
  let lo = if lob = Inclusive then lo else lo + 1
  let hi = if hib = Inclusive then hi else hi - 1
  return seq { for x in lo .. hi -> KeyValue.Create(x, x) } }

[<Test>]
let ``No call is made when series is created and formatted`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  ls.Format() |> ignore
  r.Values |> shouldEqual []

[<Test>]
let ``No call is made when series is created and we get the key range`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  ls.KeyRange |> ignore
  r.Values |> shouldEqual []

[<Test>]
let ``After creates lower bound exclusive restriction`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  let actual = ls.After(90) |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 91 .. 100 -> i, i ]
  r.Values |> shouldEqual [(90, Exclusive), (100, Inclusive)]

[<Test>]
let ``Before creates upper bound exclusive restriction`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  let actual = ls.Before(10) |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 0 .. 9 -> i, i ]
  r.Values |> shouldEqual [(0, Inclusive), (10, Exclusive)]

[<Test>]
let ``Multiple range restrictions are combined for sample calls`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  let ls = ls.Before(90)
  let ls = ls.After(10)
  let actual = ls |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 11 .. 89 -> i, i ]
  r.Values |> shouldEqual [(10, Exclusive), (90, Exclusive)]

[<Test>]
let ``Multiple conflicting range restrictions (at the end) lead to empty results`` () =
  let ls = DelayedSeries.FromValueLoader(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> KeyValue.Create(i, i) } })   
  ls.Between(100,99).KeyCount |> shouldEqual 0

[<Test>]
let ``Multiple conflicting range restrictions (in the middle) lead to empty results`` () =
  let ls = DelayedSeries.FromValueLoader(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> KeyValue.Create(i, i) } })   
  ls.Between(90,89).KeyCount |> shouldEqual 0
  
[<Test>]
let ``Splicing syntax creates inclusive restrictions`` () = 
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  let spliced = ls.[50 .. 60]
  let actual = spliced |> Series.observations |> List.ofSeq
  actual |> shouldEqual [ for i in 50 .. 60 -> i, i ]
  r.Values |> shouldEqual [(50, Inclusive), (60, Inclusive)]

[<Test>]
let ``Adding to data frame creates restriction based on data frame key range`` () = 
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers)
  let df = Frame.ofRowKeys [ 20 .. 30 ]
  df?Test <- ls
  let actual = df?Test |> Series.observations |> List.ofSeq
  actual |> shouldEqual [ for i in 20 .. 30 -> i, float i ]
  r.Values |> shouldEqual [(20, Inclusive), (30, Inclusive)]

[<Test>]
let ``Created series does not contain out-of-range keys, even if the source provides them`` () = 
  let ls = DelayedSeries.FromValueLoader(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> KeyValue.Create(i, i) }  })
  ls.[0 .. 0] |> Series.keys |> List.ofSeq |> shouldEqual [0]
  ls.[.. 0] |> Series.keys |> List.ofSeq |> shouldEqual [0]
  ls.[100 ..] |> Series.keys |> List.ofSeq |> shouldEqual [100]
  ls.[10 .. 20] |> Series.keys |> List.ofSeq |> shouldEqual [10 .. 20]
  ls.After(90) |> Series.keys |> List.ofSeq |> shouldEqual [91 .. 100]

[<Test>]
let ``Can add projection of a lazy vector to a data frame`` () = 
  let ls = DelayedSeries.FromValueLoader(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> KeyValue.Create(i, i) }  })
  let df = Frame.ofColumns [ "Lazy" => ls ]
  df?Test <- ((+) 1) $ ls 
  df?Lazy - df?Test |> Stats.sum |> int |> shouldEqual -101

// ------------------------------------------------------------------------------------------------
// Materialization
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can materialize series asynchronously`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers).[40 .. 60]
  let ms = ls.AsyncMaterialize() |> Async.RunSynchronously 
  r.Values |> shouldEqual [(40, Inclusive), (60, Inclusive)]
  ms |> shouldEqual (series [ for i in 40 .. 60 -> i, i] )

[<Test>]
let ``Discarding async materialization does not call the loader`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers).[40 .. 60]
  ls.AsyncMaterialize() |> ignore
  System.Threading.Thread.Sleep(100)
  r.Values |> shouldEqual []

[<Test>]
let ``Materializing materialized series is a no-op`` () =
  let r = Recorder()
  let ls = DelayedSeries.FromValueLoader(0, 100, spy2 r loadIntegers).[40 .. 60]
  let ms = ls.AsyncMaterialize() |> Async.RunSynchronously 
  r.Values |> shouldEqual [(40, Inclusive), (60, Inclusive)]
  let ms2 = ms.AsyncMaterialize() |> Async.RunSynchronously
  r.Values |> shouldEqual [(40, Inclusive), (60, Inclusive)]

// ------------------------------------------------------------------------------------------------
// Unioning and intersecting ranges
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can union valid range with an invalid (negative) range`` () =
  let ranges = 
    Ranges.Union
      ( Ranges.Range((0, Inclusive), (10, Inclusive)),
        Ranges.Range((30, Inclusive), (20, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual [(0, Inclusive), (10, Inclusive)]

[<Test>]
let ``Can intersect valid range with an invalid (negative) range`` () =
  let ranges = 
    Ranges.Intersect
      ( Ranges.Range((0, Inclusive), (100, Inclusive)),
        Ranges.Range((30, Inclusive), (20, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual []

[<Test>]
let ``Can union valid range with an invalid (zero) range`` () =
  let ranges = 
    Ranges.Union
      ( Ranges.Range((0, Inclusive), (10, Inclusive)),
        Ranges.Range((20, Inclusive), (20, Exclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual [(0, Inclusive), (10, Inclusive)]

[<Test>]
let ``Can union valid range with a singleton range`` () =
  let ranges = 
    Ranges.Union
      ( Ranges.Range((0, Inclusive), (10, Inclusive)),
        Ranges.Range((20, Inclusive), (20, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual [(0, Inclusive), (10, Inclusive); (20, Inclusive), (20, Inclusive)]

[<Test>]
let ``Can intersect valid range with a singleton range`` () =
  let ranges = 
    Ranges.Intersect
      ( Ranges.Range((0, Inclusive), (100, Inclusive)),
        Ranges.Range((20, Inclusive), (20, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual [(20, Inclusive), (20, Inclusive)]

[<Test>]
let ``Can intersect two singleton ranges`` () =
  let ranges = 
    Ranges.Intersect
      ( Ranges.Range((5, Inclusive), (5, Inclusive)),
        Ranges.Range((6, Inclusive), (6, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.flattenRanges 0 100 intComp ranges |> List.ofSeq
  |> shouldEqual []

[<Test>]
let ``Contains function works on ranges with invalid high/low order`` () =
  let ranges = 
    Ranges.Intersect
      ( Ranges.Range((0, Inclusive), (100, Inclusive)),
        Ranges.Range((100, Inclusive), (99, Inclusive)) )
  let intComp = System.Collections.Generic.Comparer<int>.Default
  Ranges.contains intComp 100 ranges |> shouldEqual false


// ------------------------------------------------------------------------------------------------
// Random testing
// ------------------------------------------------------------------------------------------------

/// Generator that generates ranges by unioning/intersecting them
let internal randomRange seed =   
  let rec randomRanges (rnd:System.Random) lo hi = 
    let mid = rnd.Next(lo, hi+1)
    let midl = rnd.Next(lo, mid+1)
    let midr = rnd.Next(mid, hi+1)
    match rnd.Next(5) with
    | 0 -> Ranges.Union(randomRanges rnd midl mid, randomRanges rnd mid midr)
    | 1 -> Ranges.Intersect(randomRanges rnd lo midr, randomRanges rnd midl hi)
    | _ -> 
      let lob, hib = 
        let beh() = if rnd.Next(2) = 0 then Inclusive else Exclusive
        if lo = hi then let b = beh() in b, b
        else beh(), beh()
      Ranges.Range( (lo, lob), (hi, hib) )
  randomRanges (Random(seed)) 0 100

/// Check that, flattening works correctly for a given range
let internal check range = 
  let intComp = System.Collections.Generic.Comparer<int>.Default
  let flat = Ranges.flattenRanges 0 100 intComp range 
  [ 0 .. 100 ] |> Seq.iter (fun x ->
    let expected = Ranges.contains intComp x range
    let actual = flat |> Seq.exists (fun ((lo, lob), (hi, hib)) -> 
      (x > lo && x < hi) || (x = lo && lob = Inclusive) || (x = hi && hib = Inclusive))
    expected |> shouldEqual actual)

[<Test>]
let ``Randomly generated ranges are unioned and intersected correctly``() =
  Check.QuickThrowOnFailure(fun (seed:int) -> randomRange seed |> check)
