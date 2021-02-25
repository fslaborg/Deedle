#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Stats
#endif

open System
open System.Linq
open System.Collections.Generic
open System.Globalization
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle

// ------------------------------------------------------------------------------------------------
// Statistics
// ------------------------------------------------------------------------------------------------

// to avoid floating point comparison gotchas, we use checksums to within a threshold
// we use pandas as an oracle to get proper values

[<Test>]
let ``Moving count works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  let e1 = 6.0
  let e2 = 8.0
  let e3 = 8.0

  s1 |> Stats.movingCount 2 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingCount 2 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingCount 2 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving sum works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  let e1 = 12.0
  let e2 = 16.0
  let e3 = 16.0

  s1 |> Stats.movingSum 2 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingSum 2 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingSum 2 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving mean works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  // using pandas as an oracle
  let e1 = 8.0
  let e2 = 8.0
  let e3 = 8.0

  s1 |> Stats.movingMean 2 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMean 2 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingMean 2 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving stddev works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  // using pandas as an oracle
  let e1 = 1.4142135623730951
  let e2 = 2.8284271247461903
  let e3 = 2.8284271247461903

  s1 |> Stats.movingStdDev 2 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingStdDev 2 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingStdDev 2 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``describe works`` ()=
  let s = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let desc = Stats.describe s

  desc.Get("min")  |> should equal (Stats.min s)
  desc.Get("max")  |> should equal (Stats.max s)
  desc.Get("mean") |> should equal (Stats.mean s)
  desc.Get("unique") |> should equal (Stats.uniqueCount s |> float)
  desc.Get("std")  |> should beWithin  ((Stats.stdDev s) +/- 1e-9)
  desc.Get("0.25")  |> should beWithin  (1.0 +/- 1e-9)
  desc.Get("0.5")  |> should beWithin  (2.0 +/- 1e-9)
  desc.Get("0.75")  |> should beWithin  (3.0 +/- 1e-9)

[<Test>]
let ``quantile works`` () =
  let s1 = Series.ofValues [ 1.0; 2.0; 3.0; 4.0 ]
  let quantile = Stats.quantile ([|0.25; 0.5; 0.75|], s1)

  quantile.Get("0.25")  |> should beWithin (1.75 +/- 1e-9)
  quantile.Get("0.5")  |> should beWithin (2.5 +/- 1e-9)
  quantile.Get("0.75")  |> should beWithin (3.25 +/- 1e-9)

[<Test>]
let ``Moving skew works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an oracle
  let e1 = -2.7081486342972996
  let e2 = -4.6963310471597577
  let e3 = -4.6963310471597577

  s1 |> Stats.movingSkew 3 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingSkew 3 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingSkew 3 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving min works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an oracle
  let e1 = -18.0
  let e2 = -18.0
  let e3 = -18.0

  s1 |> Stats.movingMin 3 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMin 3 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingMin 3 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving max works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an oracle
  let e1 = 18.0
  let e2 = 20.0
  let e3 = 20.0

  s1 |> Stats.movingMax 3 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMax 3 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingMax 3 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving kurt works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an oracle
  let e1 = 1.9686908218659251
  let e2 = 1.3147969613040118
  let e3 = 1.3147969613040118

  s1 |> Stats.movingKurt 4 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingKurt 4 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingKurt 4 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding mean works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  // using pandas as an oracle
  let e1 = 4.333333333333333
  let e2 = 5.0
  let e3 = 5.0

  s1 |> Stats.expandingMean |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMean |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingMean |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding stddev works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  // using pandas as an  oracle
  let e1 = 4.7674806523755962
  let e2 = 4.5792400600065433
  let e3 = 4.5792400600065433

  s1 |> Stats.expandingStdDev |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingStdDev |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingStdDev |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding skew works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an  oracle
  let e1 =  0.25348662300133284
  let e2 = -0.99638293603701233
  let e3 = -0.99638293603701233

  s1 |> Stats.expandingSkew |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingSkew |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingSkew |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding kurt works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // using pandas as an  oracle
  let e1 = 0.90493406707689539
  let e2 = -1.4288501854055262
  let e3 = -1.4288501854055262

  s1 |> Stats.expandingKurt |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingKurt |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingKurt |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding min works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  let e1 = -18.0
  let e2 = -18.0
  let e3 = -18.0

  s1 |> Stats.expandingMin |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMin |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingMin |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Expanding max works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  let e1 = 18.0
  let e2 = 20.0
  let e3 = 20.0

  s1 |> Stats.expandingMax |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMax |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingMax |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Basic level statistics works on sample input`` () =
  let s1 = series [(1,0) => nan; (1,1) => 2.0; (2,0) => 3.0; (2,1) => 4.0 ]
  let s2 = series [(1,1) => 2; (2,0) => 3; (2,1) => 4 ]

  s1 |> Stats.levelCount fst |> shouldEqual <| series [ 1 => 1; 2 => 2 ]
  s1 |> Stats.levelSum fst |> shouldEqual <| series [ 1 => 2.0; 2 => 7.0 ]
  s1 |> Stats.levelMean fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.5 ]
  s2 |> Stats.levelCount fst |> shouldEqual <| series [ 1 => 1; 2 => 2 ]
  s2 |> Stats.levelSum fst |> shouldEqual <| series [ 1 => 2.0; 2 => 7.0 ]
  s2 |> Stats.levelMean fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.5 ]

[<Test>]
let ``Advanced level statistics works on sample input`` () =
  let s1 = series [(1,0) => 1.0; (1,1) => 2.0; (1,2) => 3.0; (1,4) => 4.0; (2,0) => 3.0; (2,1) => 4.0 ]
  let s2 = series [(1,0) => 1; (1,1) => 2; (1,2) => 3; (1,4) => 4; (2,0) => 3; (2,1) => 4 ]

  s1 |> Stats.levelKurt fst |> Stats.sum |> should beWithin (-1.2 +/- 1e-9)
  s1 |> Stats.levelSkew fst |> Stats.sum |> should beWithin (0.0 +/- 1e-9)
  s2 |> Stats.levelKurt fst |> Stats.sum |> should beWithin (-1.2 +/- 1e-9)
  s2 |> Stats.levelSkew fst |> Stats.sum |> should beWithin (0.0 +/- 1e-9)

[<Test>]
let ``Moving minimum works with nan values`` () =
  let s1 = series [ 0 => 1.0; 1 => nan ]
  Stats.movingMin 1 s1 |> shouldEqual <| series [ 0 => 1.0; 1 => nan ]

[<Test>]
let ``maxBy and minBy work`` () =
  let s1 = series [ 0 => 1.0; 1 => nan; 2 => 5.0 ]
  let s2 = series [ 0 => 1; 2 => 5 ]

  s1 |> Stats.maxBy id |> shouldEqual (Some (2, 5.0))
  s1 |> Stats.minBy id |> shouldEqual (Some (0, 1.0))
  s2 |> Stats.maxBy id |> shouldEqual (Some (2, 5))
  s2 |> Stats.minBy id |> shouldEqual (Some (0, 1))

  s1 |> Stats.maxBy (fun x -> -x) |> shouldEqual (Some (0, 1.0))
  s1 |> Stats.minBy (fun x -> -x) |> shouldEqual (Some (2, 5.0))
  s2 |> Stats.maxBy (fun x -> -x) |> shouldEqual (Some (0, 1))
  s2 |> Stats.minBy (fun x -> -x) |> shouldEqual (Some (2, 5))

[<Test>]
let ``sum returns NaN for empty series`` () =
  series [ ] |> Stats.sum |> should be NaN

[<Test>]
let ``sum does not return NaN for series with missing values`` () =
  series [ 1 => nan; 2 => 1.0; 3 => 2.0 ] |> Stats.sum |> shouldEqual 3.0

[<Test>]
let ``unique count work``() =
  series [1 => 1; 2 => 2; 3 => 1] |> Stats.uniqueCount |> shouldEqual 2

// ------------------------------------------------------------------------------------------------
// Statistics on frames
// ------------------------------------------------------------------------------------------------

let sampleFrame() =
  frame
    [ "A" =?> Series.ofValues [ 1.0; nan; 2.0; 3.0 ]
      "B" =?> Series.ofValues [ "hi"; "there"; "!"; "?" ]
      "C" =?> Series.ofValues [ 3.3; 4.4; 5.5; 6.6 ] ]

[<Test>]
let ``Can calulate minimum and maximum of numeric series in a frame`` () =
  let df = sampleFrame()
  df |> Stats.min |> shouldEqual <| series [ "A" => 1.0; "B" => nan; "C" => 3.3 ]
  df |> Stats.max |> shouldEqual <| series [ "A" => 3.0; "B" => nan; "C" => 6.6 ]

[<Test>]
let ``Can calulate minimum and maximum of numeric series in a frame using extension`` () =
  let df = sampleFrame()
  df.Min() |> shouldEqual <| series [ "A" => 1.0; "B" => nan; "C" => 3.3 ]
  df.Max() |> shouldEqual <| series [ "A" => 3.0; "B" => nan; "C" => 6.6 ]

// ------------------------------------------------------------------------------------------------
// Some FsCheck tests
// ------------------------------------------------------------------------------------------------

open FsCheck

[<Test>]
let ``Moving minimum using Stats.movingMin is equal to using Series.window`` () =
  Check.QuickThrowOnFailure(fun (input:float[]) ->
    let s = Series.ofValues input
    for i in 1 .. s.KeyCount - 1 do
      let actual = s |> Stats.movingMin i
      let expected = s |> Series.windowSizeInto (i, Boundary.AtBeginning) (fun s ->
        if s.Data.ValueCount = 0 then nan else Seq.min s.Data.Values)
      actual |> shouldEqual expected )

[<Test>]
let ``Moving maximum using Stats.movingMax is equal to using Series.window`` () =
  Check.QuickThrowOnFailure(fun (input:float[]) ->
    let s = Series.ofValues input
    for i in 1 .. s.KeyCount - 1 do
      let actual = s |> Stats.movingMax i
      let expected = s |> Series.windowSizeInto (i, Boundary.AtBeginning) (fun s ->
        if s.Data.ValueCount = 0 then nan else Seq.max s.Data.Values)
      actual |> shouldEqual expected )

// ------------------------------------------------------------------------------------------------
// Comparing results with Math.NET
// ------------------------------------------------------------------------------------------------

open MathNet.Numerics.Statistics

[<Test>]
let ``Mean is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let expected = Statistics.Mean(Array.map float input)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 1 then
      Double.IsNaN(Stats.mean s) |> shouldEqual true
    else
      Stats.mean s |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``StdDev and Variance is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let d = DescriptiveStatistics(Array.map float input)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 2 then
      Double.IsNaN(Stats.variance s) |> shouldEqual true
      Double.IsNaN(Stats.stdDev s) |> shouldEqual true
    else
      Stats.variance s |> should beWithin (d.Variance +/- 1e-9)
      Stats.stdDev s |> should beWithin (d.StandardDeviation +/- 1e-9) )

[<Test>]
let ``Skewness is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let expected = Statistics.Skewness(Array.map float input)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 3 then
      Double.IsNaN(Stats.skew s) |> shouldEqual true
    else
      Stats.skew s |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``Kurtosis is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let expected = Statistics.Kurtosis(Array.map float input)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 4 then
      Double.IsNaN(Stats.kurt s) |> shouldEqual true
    else
      Stats.kurt s |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``Median is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:float[]) ->
    let input = Array.filter (Double.IsNaN >> not) input
    let expected = Statistics.Median(input)
    let actual = Series.ofValues input |> Stats.median
    actual |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``Quantile is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:float[]) ->
    let tau = 0.5
    let input = Array.filter (Double.IsNaN >> not) input
    let expected = ArrayStatistics.QuantileCustomInplace (input, tau, QuantileDefinition.R7)

    let quantile = Stats.quantile ([|tau|], (Series.ofValues input))
    let quantileValue = quantile.TryGet(string tau)
    let actual =
       match quantileValue with
       | OptionalValue.Missing -> nan
       | _ -> quantileValue.Value

    actual |> should beWithin (expected +/- 1e-9) )
