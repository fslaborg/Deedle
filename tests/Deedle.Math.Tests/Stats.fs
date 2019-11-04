#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.Stats
#endif

open System
open FsUnit
open NUnit.Framework
open FsCheck
open Deedle
open Deedle.Internal
open Deedle.Math
open MathNet.Numerics.Statistics

let stockPrices = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/stocks_weekly.csv") |> Frame.indexRowsDate "Dates"
let stockReturns = stockPrices / (stockPrices |> Frame.shift 1) - 1 |> Frame.dropSparseRows
let weights =
  let nStocks = stockPrices.ColumnCount
  let w = Array.create nStocks (1. / float nStocks)
  (stockPrices.ColumnKeys, w)
  ||> Seq.zip
  |> Series.ofObservations

[<Test>]
let ``Median is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) -> 
    let expected = Statistics.Median(Array.map float input)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 1 then 
      Double.IsNaN(Stats.median s) |> shouldEqual true
    else 
      Stats.median s |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``Quantile is the same as in Math.NET``() =
  Check.QuickThrowOnFailure(fun (input:int[]) -> 
    let expected = Statistics.QuantileCustom(Array.map float input, 0.75, QuantileDefinition.Excel)
    let s = Series.ofValues (Array.map float input)
    if s.ValueCount < 1 then 
      Double.IsNaN(Stats.quantile(s, 0.75)) |> shouldEqual true
    else 
      Stats.quantile(s, 0.75) |> should beWithin (expected +/- 1e-9) )

[<Test>]
let ``ewmMean shall work `` () =
  // pandas v0.24.2: series.ewm(alpha=0.97, adjust=False, ignore_na=True).mean()
  let s1 = Series.ofValues [ 100.; 105.; 90.; 100.; 110.; 120. ]
  let s2 = Series.ofValues [ 100.; nan; 90.; 100.; nan; 120. ]
  let s3 = Series.ofValues [ nan; nan; nan; nan; nan; 1.; 2.; 3. ]
  let lambda = 0.97
  let actual1 = Stats.ewmMean(s1, alpha = lambda)
  let actual2 = Stats.ewmMean(s2, alpha = lambda)
  let actual3 = Stats.ewmMean(s3, alpha = lambda)
  let expected1 = Series.ofValues [ 100.; 104.85; 90.4455; 99.713365; 109.691401; 119.690742 ]
  let expected2 = Series.ofValues [ 100.; 100.; 90.3; 99.709; 99.709; 119.39127 ]
  let expected3 = Series.ofValues [ nan; nan; nan; nan; nan; 1.; 1.9700; 2.9691 ]
  actual1 - expected1 |> Stats.sum |> should beWithin (0. +/- 1e-6)
  actual2 - expected2 |> Stats.sum |> should beWithin (0. +/- 1e-6)
  actual3 - expected3 |> Stats.sum |> should beWithin (0. +/- 1e-6)

[<Test>]
let ``Ex-ante vol of equally weighted portfolio using normal covariance matrix works`` () =
  let nObsAnnual = 52.
  let cov = stockReturns |> Stats.cov
  let annualVol =
    let vol = weights.Dot(cov).Dot(weights)
    Math.Sqrt(vol * nObsAnnual)
  annualVol |> should beWithin (0.13575 +/- 1e-6)  

[<Test>]
let ``Ex-ante vol of equally weighted portfolio using exponentially weighted covariance matrix works`` () =
  let halfLife = 52.
  let nObsAnnual = 52.
  let cov = Stats.ewmCovMatrix(stockReturns, halfLife = halfLife) |> Series.lastValue
  let covFrame = Stats.ewmCov(stockReturns, halfLife = halfLife) |> Series.lastValue
  let annualVol1 =
    let vol = weights.Dot(cov).Dot(weights)
    Math.Sqrt(vol * nObsAnnual)
  let annualVol2 =
    let vol = weights.Dot(covFrame).Dot(weights)
    Math.Sqrt(vol * nObsAnnual)
  annualVol1 |> should beWithin (0.14437 +/- 1e-6)
  annualVol1 |> should beWithin (annualVol2 +/- 1e-6)

[<Test>]
let ``cov2Corr and corr2Cov work`` () =
  let cov = stockReturns |> Stats.cov
  let std, corr = cov |> Stats.cov2Corr
  let actual = Stats.corr2Cov(std, corr).GetColumnAt<float>(0).GetAt(0)
  let expected = cov.GetColumnAt<float>(0).GetAt(0)
  actual |> should beWithin (expected +/- 1e-6)
  
[<Test>]
let ``ewmVariance and ewmCov's diagonal shall be identical `` () =
  let varFrame = Stats.ewmVariance(stockReturns, halfLife = 52.)
  let cov = Stats.ewmCovMatrix(stockReturns, halfLife = 52.) |> Series.lastValue
  let varSeries1 = (varFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let varSeries2 = Series(stockReturns.ColumnKeys, cov.Diagonal())
  (varSeries1 - varSeries2) |> Stats.sum |> should beWithin (0. +/- 1e-10)