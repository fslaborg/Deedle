#if INTERACTIVE
#load "../../bin/Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#r "../../packages/MathNet.Numerics.3.0.0-alpha8/lib/net40/MathNet.Numerics.dll"
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

  let e1 = 6.0
  let e2 = 8.0

  s1 |> Stats.movingCount 2 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingCount 2 |> Series.sum |> should beWithin (e2 +/- 1e-9)
 
[<Test>]
let ``Moving sum works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]

  let e1 = 12.0
  let e2 = 16.0

  s1 |> Stats.movingSum 2 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingSum 2 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving mean works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]

  // using pandas as an oracle
  let e1 = 8.0
  let e2 = 8.0

  s1 |> Stats.movingMean 2 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMean 2 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving stddev works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]

  // using pandas as an oracle
  let e1 = 1.4142135623730951
  let e2 = 2.8284271247461903

  s1 |> Stats.movingStdDev 2 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingStdDev 2 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving skew works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an oracle
  let e1 = -2.7081486342972996
  let e2 = -4.6963310471597577

  s1 |> Stats.movingSkew 3 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingSkew 3 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving min works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an oracle
  let e1 = -18.0
  let e2 = -18.0

  s1 |> Stats.movingMin 3 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMin 3 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving max works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an oracle
  let e1 = 18.0
  let e2 = 20.0

  s1 |> Stats.movingMax 3 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingMax 3 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Moving kurt works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an oracle
  let e1 = 1.9686908218659251
  let e2 = 1.3147969613040118

  s1 |> Stats.movingKurt 4 |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.movingKurt 4 |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding mean works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]

  // using pandas as an oracle
  let e1 = 4.333333333333333
  let e2 = 5.0

  s1 |> Stats.expandingMean |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMean |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding stddev works`` () =
  let s1 = Series.ofValues [ 0.0; 1.0; Double.NaN; 3.0; 4.0 ]
  let s2 = Series.ofValues [ 0.0; 1.0; 2.0; 3.0; 4.0 ]

  // using pandas as an  oracle
  let e1 = 4.7674806523755962
  let e2 = 4.5792400600065433

  s1 |> Stats.expandingStdDev |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingStdDev |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding skew works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an  oracle
  let e1 =  0.25348662300133284
  let e2 = -0.99638293603701233

  s1 |> Stats.expandingSkew |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingSkew |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding kurt works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  // using pandas as an  oracle
  let e1 = 0.90493406707689539
  let e2 = -1.4288501854055262

  s1 |> Stats.expandingKurt |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingKurt |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding min works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  let e1 = -18.0
  let e2 = -18.0

  s1 |> Stats.expandingMin |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMin |> Series.sum |> should beWithin (e2 +/- 1e-9)

[<Test>]
let ``Expanding max works`` () =
  let s1 = Series.ofValues [ 0.0; -1.0; Double.NaN; 3.0; -5.0; 4.0; 8.0 ]
  let s2 = Series.ofValues [ 0.0; -1.0; 2.0; 3.0; -5.0; 4.0; 8.0 ]

  let e1 = 18.0
  let e2 = 20.0

  s1 |> Stats.expandingMax |> Series.sum |> should beWithin (e1 +/- 1e-9)
  s2 |> Stats.expandingMax |> Series.sum |> should beWithin (e2 +/- 1e-9)

// ------------------------------------------------------------------------------------------------
// Statistics
// ------------------------------------------------------------------------------------------------

open FsCheck
open MathNet.Numerics.Statistics

[<Test>]
let ``Mean is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) -> 
    let d = DescriptiveStatistics(Array.map float input) 
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 1 then 
      Double.IsNaN(Stats.mean s) |> shouldEqual true
    else 
      Stats.mean s |> should beWithin (d.Mean +/- 1e-9) )

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
    let d = DescriptiveStatistics(Array.map float input) 
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 3 then 
      Double.IsNaN(Stats.skew s) |> shouldEqual true
    else 
      Stats.skew s |> should beWithin (d.Skewness +/- 1e-9) )

[<Test>]
let ``Kurtosis is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) -> 
    let d = DescriptiveStatistics(Array.map float input) 
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 4 then 
      Double.IsNaN(Stats.kurt s) |> shouldEqual true
    else 
      Stats.kurt s |> should beWithin (d.Kurtosis +/- 1e-9) )

