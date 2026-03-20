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
let ``describe works for frame`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 10.0; 20.0; 30.0; 40.0; 50.0 ]
  let frame = Frame.ofColumns [ "A", s1; "B", s2 ]
  let desc = Stats.describe frame

  desc.ColumnKeys |> Seq.toList |> should equal ["A"; "B"]
  desc.["A", "min"] |> should equal (Stats.min s1)
  desc.["A", "max"] |> should equal (Stats.max s1)
  desc.["A", "mean"] |> should equal (Stats.mean s1)
  desc.["B", "min"] |> should equal (Stats.min s2)
  desc.["B", "max"] |> should equal (Stats.max s2)
  desc.["B", "mean"] |> should equal (Stats.mean s2)

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
let ``Moving median works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]
  let s3 = Series.ofValues [ 0; -1; 2; 3; -5; 4; 8 ]

  // expected: sum of non-missing window medians
  let e1 = 6.5   // windows: (-0.5), 1.0, (-1.0), 3.0, 4.0
  let e2 = 11.0  // windows: 0, 2, 2, 3, 4
  let e3 = 11.0

  s1 |> Stats.movingMedian 3 |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMedian 3 |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.movingMedian 3 |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Moving median - first size-1 values are missing`` () =
  let s = Series.ofValues [ 1.0; 2.0; 3.0; 4.0; 5.0 ]
  let result = s |> Stats.movingMedian 3
  result |> Series.countValues |> should equal 3  // 2 missing, 3 present

[<Test>]
let ``Expanding median works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]
  let s3 = Series.ofValues [ 0; 1; 2; 3; 4 ]

  // expected cumulative medians summed
  let e1 = 4.0   // 0, 0.5, 0.5, 1.0, 2.0
  let e2 = 5.0   // 0, 0.5, 1.0, 1.5, 2.0
  let e3 = 5.0

  s1 |> Stats.expandingMedian |> Stats.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMedian |> Stats.sum |> should beWithin (e2 +/- 1e-9)
  s3 |> Stats.expandingMedian |> Stats.sum |> should beWithin (e3 +/- 1e-9)

[<Test>]
let ``Basic level statistics works on sample input`` () =
  let s1 = series [(1,0) => nan; (1,1) => 2.0; (2,0) => 3.0; (2,1) => 4.0 ]
  let s2 = series [(1,1) => 2; (2,0) => 3; (2,1) => 4 ]

  s1 |> Stats.levelCount fst |> shouldEqual <| series [ 1 => 1; 2 => 2 ]
  s1 |> Stats.levelSum fst |> shouldEqual <| series [ 1 => 2.0; 2 => 7.0 ]
  s1 |> Stats.levelMean fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.5 ]
  s1 |> Stats.levelMin fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.0 ]
  s1 |> Stats.levelMax fst |> shouldEqual <| series [ 1 => 2.0; 2 => 4.0 ]
  s2 |> Stats.levelCount fst |> shouldEqual <| series [ 1 => 1; 2 => 2 ]
  s2 |> Stats.levelSum fst |> shouldEqual <| series [ 1 => 2.0; 2 => 7.0 ]
  s2 |> Stats.levelMean fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.5 ]
  s2 |> Stats.levelMin fst |> shouldEqual <| series [ 1 => 2.0; 2 => 3.0 ]
  s2 |> Stats.levelMax fst |> shouldEqual <| series [ 1 => 2.0; 2 => 4.0 ]

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
// tryMin / tryMax
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Stats.tryMin returns Some for non-empty series`` () =
  let s = series [ 1 => 3.0; 2 => 1.0; 3 => 2.0 ]
  Stats.tryMin s |> shouldEqual (Some 1.0)

[<Test>]
let ``Stats.tryMax returns Some for non-empty series`` () =
  let s = series [ 1 => 3.0; 2 => 1.0; 3 => 2.0 ]
  Stats.tryMax s |> shouldEqual (Some 3.0)

[<Test>]
let ``Stats.tryMin returns None for empty series`` () =
  Stats.tryMin (Series.empty<int, float>) |> shouldEqual None

[<Test>]
let ``Stats.tryMax returns None for empty series`` () =
  Stats.tryMax (Series.empty<int, float>) |> shouldEqual None

[<Test>]
let ``Stats.tryMin returns None for all-missing series`` () =
  let s = Series.ofOptionalObservations [ 1, None; 2, None ]
  Stats.tryMin s |> shouldEqual None

[<Test>]
let ``Stats.tryMax returns None for all-missing series`` () =
  let s = Series.ofOptionalObservations [ 1, None; 2, None ]
  Stats.tryMax s |> shouldEqual None

[<Test>]
let ``Stats.tryMin works on integer series`` () =
  let s = series [ "a" => 5; "b" => 2; "c" => 8 ]
  Stats.tryMin s |> shouldEqual (Some 2)

[<Test>]
let ``Stats.tryMax works on integer series`` () =
  let s = series [ "a" => 5; "b" => 2; "c" => 8 ]
  Stats.tryMax s |> shouldEqual (Some 8)

// ------------------------------------------------------------------------------------------------
// numSum
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Stats.numSum sums integer series`` () =
  let s = series [ 1 => 10; 2 => 20; 3 => 30 ]
  Stats.numSum s |> shouldEqual 60

[<Test>]
let ``Stats.numSum sums float series, same as Stats.sum for present values`` () =
  let s = series [ 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
  Stats.numSum s |> should beWithin (Stats.sum s +/- 1e-9)

[<Test>]
let ``Stats.numSum returns zero for empty float series`` () =
  Stats.numSum (Series.empty<int, float>) |> shouldEqual 0.0

[<Test>]
let ``Stats.numSum returns zero for empty integer series`` () =
  Stats.numSum (Series.empty<int, int>) |> shouldEqual 0

// ------------------------------------------------------------------------------------------------
// Stats.interpolate (generic version)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Stats.interpolate uses custom function for in-range keys`` () =
  // Series at keys 0, 2, 4 with values 0.0, 4.0, 8.0 (slope = 2 per unit)
  let s = series [ 0 => 0.0; 2 => 4.0; 4 => 8.0 ]
  // Custom linear interpolation: weighted average by distance
  let interp k (prev:(int*float) option) (next:(int*float) option) =
    match prev, next with
    | Some (k0, v0), Some (k1, v1) ->
        let t = float (k - k0) / float (k1 - k0)
        v0 + t * (v1 - v0)
    | Some (_, v), None -> v
    | None, Some (_, v) -> v
    | None, None -> nan
  let result = Stats.interpolate [0;1;2;3;4] interp s
  result.Get(1) |> should beWithin (2.0 +/- 1e-9)
  result.Get(3) |> should beWithin (6.0 +/- 1e-9)

[<Test>]
let ``Stats.interpolate passes boundary values to interpolation function`` () =
  let s = series [ 10 => 100.0; 20 => 200.0 ]
  // Just return the previous value (step interpolation)
  let stepInterp _ (prev:(int*float) option) (next:(int*float) option) =
    match prev, next with
    | Some (_, v), _ -> v
    | _, Some (_, v) -> v
    | None, None     -> nan
  let result = Stats.interpolate [10;15;20] stepInterp s
  result.Get(15) |> shouldEqual 100.0

// ------------------------------------------------------------------------------------------------
// Statistics on frames
// ------------------------------------------------------------------------------------------------

let sampleFrame() =
  frame
    [ "A" =?> Series.ofValues [ 1.0; nan; 2.0; 3.0 ]
      "B" =?> Series.ofValues [ "hi"; "there"; "!"; "?" ]
      "C" =?> Series.ofValues [ 3.3; 4.4; 5.5; 6.6 ] ]

// A frame with all-numeric columns for aggregation tests
let numericFrame() =
  frame
    [ "X" =?> Series.ofValues [ 1.0; 2.0; 3.0; 4.0 ]
      "Y" =?> Series.ofValues [ 10.0; 20.0; 30.0; 40.0 ] ]

[<Test>]
let ``Stats.count returns value count per column of a frame`` () =
  let df = sampleFrame()
  // Column A has a NaN so only 3 present values; B and C have 4 each
  let counts = Stats.count df
  counts.Get("A") |> shouldEqual 3
  counts.Get("B") |> shouldEqual 4
  counts.Get("C") |> shouldEqual 4

[<Test>]
let ``Stats.sum frame returns column-wise sums for numeric columns`` () =
  let df = numericFrame()
  let sums = Stats.sum df
  sums.Get("X") |> should beWithin (10.0 +/- 1e-9)  // 1+2+3+4
  sums.Get("Y") |> should beWithin (100.0 +/- 1e-9) // 10+20+30+40

[<Test>]
let ``Stats.mean frame returns column-wise means for numeric columns`` () =
  let df = numericFrame()
  let means = Stats.mean df
  means.Get("X") |> should beWithin (2.5 +/- 1e-9)  // (1+2+3+4)/4
  means.Get("Y") |> should beWithin (25.0 +/- 1e-9)

[<Test>]
let ``Stats.median frame returns column-wise medians for numeric columns`` () =
  let df = numericFrame()
  let medians = Stats.median df
  medians.Get("X") |> should beWithin (2.5 +/- 1e-9)
  medians.Get("Y") |> should beWithin (25.0 +/- 1e-9)

[<Test>]
let ``Stats.stdDev frame returns column-wise standard deviations`` () =
  let df = numericFrame()
  let stds = Stats.stdDev df
  stds.Get("X") |> should beWithin (Stats.stdDev (series [0=>1.0;1=>2.0;2=>3.0;3=>4.0]) +/- 1e-9)

[<Test>]
let ``Stats.variance frame returns column-wise variances`` () =
  let df = numericFrame()
  let vars = Stats.variance df
  // Var([1,2,3,4]) = 5/3 ≈ 1.6667 (sample variance)
  vars.Get("X") |> should beWithin (Stats.variance (series [0=>1.0;1=>2.0;2=>3.0;3=>4.0]) +/- 1e-9)

[<Test>]
let ``Stats.skew frame returns column-wise skewness`` () =
  let df = numericFrame()
  let skews = Stats.skew df
  skews.Get("X") |> should beWithin (Stats.skew (series [0=>1.0;1=>2.0;2=>3.0;3=>4.0]) +/- 1e-9)

[<Test>]
let ``Stats.kurt frame returns column-wise kurtosis`` () =
  let df = numericFrame()
  let kurts = Stats.kurt df
  kurts.Get("X") |> should beWithin (Stats.kurt (series [0=>1.0;1=>2.0;2=>3.0;3=>4.0]) +/- 1e-9)

[<Test>]
let ``Stats.uniqueCount frame returns per-column distinct value counts`` () =
  let df = frame [ "A" =?> series [1 => 1.0; 2 => 1.0; 3 => 2.0]; "B" =?> series [1 => "x"; 2 => "y"; 3 => "x"] ]
  let uc = Stats.uniqueCount df
  uc.Get("A") |> shouldEqual 2  // 1.0, 2.0
  uc.Get("B") |> shouldEqual 2  // "x", "y"

[<Test>]
let ``Stats.sum frame ignores NaN values`` () =
  let df = frame [ "A" =?> Series.ofValues [ 1.0; nan; 3.0 ] ]
  let sums = Stats.sum df
  sums.Get("A") |> should beWithin (4.0 +/- 1e-9)

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

// ------------------------------------------------------------------------------------------------
// Covariance and correlation
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Stats.cov computes sample covariance of two series`` () =
  // Using pandas as oracle: pd.Series([1.0,2.0,3.0,4.0]).cov(pd.Series([2.0,4.0,6.0,8.0])) = 3.333...
  let s1 = Series.ofValues [1.0; 2.0; 3.0; 4.0]
  let s2 = Series.ofValues [2.0; 4.0; 6.0; 8.0]
  Stats.cov s1 s2 |> should beWithin (10.0 / 3.0 +/- 1e-9)

[<Test>]
let ``Stats.cov returns NaN for series with fewer than 2 common pairs`` () =
  let s1 = Series.ofValues [1.0]
  let s2 = Series.ofValues [2.0]
  Stats.cov s1 s2 |> Double.IsNaN |> shouldEqual true

[<Test>]
let ``Stats.cov aligns series on keys (inner join) before computing`` () =
  let s1 = series [ 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
  let s2 = series [ 2 => 2.0; 3 => 3.0; 4 => 4.0 ]
  // Only keys 2 and 3 are in common; cov([2,3],[2,3]) = 0.5
  Stats.cov s1 s2 |> should beWithin (0.5 +/- 1e-9)

[<Test>]
let ``Stats.corr computes Pearson correlation of perfectly correlated series`` () =
  let s1 = Series.ofValues [1.0; 2.0; 3.0; 4.0]
  let s2 = Series.ofValues [2.0; 4.0; 6.0; 8.0]
  Stats.corr s1 s2 |> should beWithin (1.0 +/- 1e-9)

[<Test>]
let ``Stats.corr computes Pearson correlation of anti-correlated series`` () =
  let s1 = Series.ofValues [1.0; 2.0; 3.0; 4.0]
  let s2 = Series.ofValues [4.0; 3.0; 2.0; 1.0]
  Stats.corr s1 s2 |> should beWithin (-1.0 +/- 1e-9)

[<Test>]
let ``Stats.corr returns NaN for zero-variance series`` () =
  let s1 = Series.ofValues [2.0; 2.0; 2.0]
  let s2 = Series.ofValues [1.0; 2.0; 3.0]
  Stats.corr s1 s2 |> Double.IsNaN |> shouldEqual true

[<Test>]
let ``Stats.corr returns NaN for fewer than 2 pairs`` () =
  let s1 = Series.ofValues [1.0]
  let s2 = Series.ofValues [1.0]
  Stats.corr s1 s2 |> Double.IsNaN |> shouldEqual true

// ------------------------------------------------------------------------------------------------
// Correlation and covariance matrices
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Stats.corrMatrix diagonal entries are 1.0`` () =
  let f = frame [ "A" => series [1=>1.0; 2=>2.0; 3=>3.0]
                  "B" => series [1=>3.0; 2=>1.0; 3=>2.0] ]
  let m = Stats.corrMatrix f
  m.GetColumn<float>("A").Get("A") |> should beWithin (1.0 +/- 1e-9)
  m.GetColumn<float>("B").Get("B") |> should beWithin (1.0 +/- 1e-9)

[<Test>]
let ``Stats.corrMatrix is symmetric`` () =
  let f = frame [ "A" => series [1=>1.0; 2=>2.0; 3=>3.0]
                  "B" => series [1=>3.0; 2=>1.0; 3=>2.0] ]
  let m = Stats.corrMatrix f
  let ab = m.GetColumn<float>("A").Get("B")
  let ba = m.GetColumn<float>("B").Get("A")
  ab |> should beWithin (ba +/- 1e-9)

[<Test>]
let ``Stats.corrMatrix perfectly correlated columns give 1.0`` () =
  let f = frame [ "A" => series [1=>1.0; 2=>2.0; 3=>3.0; 4=>4.0]
                  "B" => series [1=>2.0; 2=>4.0; 3=>6.0; 4=>8.0] ]
  let m = Stats.corrMatrix f
  m.GetColumn<float>("A").Get("B") |> should beWithin (1.0 +/- 1e-9)

[<Test>]
let ``Stats.covMatrix diagonal entries equal variance`` () =
  let f = frame [ "A" => series [1=>1.0; 2=>2.0; 3=>3.0; 4=>4.0]
                  "B" => series [1=>2.0; 2=>4.0; 3=>6.0; 4=>8.0] ]
  let m = Stats.covMatrix f
  // Diagonal entry (A,A) = Var(A) = sample variance of [1,2,3,4]
  let expected = Stats.variance (Series.ofValues [1.0;2.0;3.0;4.0])
  m.GetColumn<float>("A").Get("A") |> should beWithin (expected +/- 1e-9)

[<Test>]
let ``Stats.covMatrix is symmetric`` () =
  let f = frame [ "A" => series [1=>1.0; 2=>2.0; 3=>3.0]
                  "B" => series [1=>3.0; 2=>1.0; 3=>2.0] ]
  let m = Stats.covMatrix f
  let ab = m.GetColumn<float>("A").Get("B")
  let ba = m.GetColumn<float>("B").Get("A")
  ab |> should beWithin (ba +/- 1e-9)
