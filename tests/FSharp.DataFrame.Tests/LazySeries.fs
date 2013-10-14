#if INTERACTIVE
#I "../../bin/"
#load "FSharp.DataFrame.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module FSharp.DataFrame.Tests.LazySeries
#endif

open System
open System.Collections.Generic
open FsUnit
open FsCheck
open NUnit.Framework
open FSharp.DataFrame
open FSharp.DataFrame.Delayed
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Indices

// ------------------------------------------------------------------------------------------------
// Basic tests
// ------------------------------------------------------------------------------------------------

let loadIntegers (lo, lob) (hi, hib) = async { 
  let lo = if lob = Inclusive then lo else lo + 1
  let hi = if hib = Inclusive then hi else hi - 1
  return seq { for x in lo .. hi -> x, x } }

[<Test>]
let ``No call is made when series is created and formatted`` () =
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  (ls :> IFsiFormattable).Format() |> ignore
  r.Values |> shouldEqual []

[<Test>]
let ``SeriesExtensions.After creates lower bound exclusive restriction`` () =
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  let actual = SeriesExtensions.After(ls, 90) |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 91 .. 100 -> i, i ]
  r.Values |> shouldEqual [(90, Exclusive), (100, Inclusive)]

[<Test>]
let ``SeriesExtensions.Before creates upper bound exclusive restriction`` () =
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  let actual = SeriesExtensions.Before(ls, 10) |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 0 .. 9 -> i, i ]
  r.Values |> shouldEqual [(0, Inclusive), (10, Exclusive)]

[<Test>]
let ``Multiple range restrictions are combined for sample calls`` () =
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  let ls = SeriesExtensions.Before(ls, 90)
  let ls = SeriesExtensions.After(ls, 10)
  let actual = ls |> Series.observations |> List.ofSeq 
  actual |> shouldEqual [ for i in 11 .. 89 -> i, i ]
  r.Values |> shouldEqual [(10, Exclusive), (90, Exclusive)]

[<Test>]
let ``Splicing syntax creates inclusive restrictions`` () = 
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  let spliced = ls.[50 .. 60]
  let actual = spliced |> Series.observations |> List.ofSeq
  actual |> shouldEqual [ for i in 50 .. 60 -> i, i ]
  r.Values |> shouldEqual [(50, Inclusive), (60, Inclusive)]

[<Test>]
let ``Adding to data frame creates restriction based on data frame key range`` () = 
  let r = Recorder()
  let ls = DelayedSeries.Create(0, 100, spy2 r loadIntegers)
  let df = Frame.ofRowKeys [ 20 .. 30 ]
  df?Test <- ls
  let actual = df?Test |> Series.observations |> List.ofSeq
  actual |> shouldEqual [ for i in 20 .. 30 -> i, float i ]
  r.Values |> shouldEqual [(20, Inclusive), (30, Inclusive)]

[<Test>]
let ``Created series does not contain out-of-range keys, even if the source provides them`` () = 
  let ls = DelayedSeries.Create(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> i, i }  })
  ls.[0 .. 0] |> Series.keys |> List.ofSeq |> shouldEqual [0]
  ls.[.. 0] |> Series.keys |> List.ofSeq |> shouldEqual [0]
  ls.[100 ..] |> Series.keys |> List.ofSeq |> shouldEqual [100]
  ls.[10 .. 20] |> Series.keys |> List.ofSeq |> shouldEqual [10 .. 20]
  SeriesExtensions.After(ls, 90) |> Series.keys |> List.ofSeq |> shouldEqual [91 .. 100]

[<Test>]
let ``Can add projection of a lazy vector to a data frame`` () = 
  let ls = DelayedSeries.Create(0, 100, fun _ _ -> async { 
    return seq { for i in 0 .. 100 -> i, i }  })
  let df = Frame.ofColumns [ "Lazy" => ls ]
  df?Test <- ((+) 1) $ ls 
  df?Lazy - df?Test |> Series.sum |> int |> shouldEqual -101

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
