#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.Finance
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Internal
open Deedle.Math

let stockPrices = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/stocks_weekly.csv") |> Frame.indexRowsDate "Dates"
let stockReturns = stockPrices / (stockPrices |> Frame.shift 1) - 1 |> Frame.dropSparseRows
let weights =
  let nStocks = stockPrices.ColumnCount
  let w = Array.create nStocks (1. / float nStocks)
  (stockPrices.ColumnKeys, w)
  ||> Seq.zip
  |> Series.ofObservations

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
  let cov = Finance.ewmCovMatrix(stockReturns, halfLife = halfLife) |> Series.lastValue
  let covFrame = Finance.ewmCov(stockReturns, halfLife = halfLife) |> Series.lastValue
  let annualVol1 =
    let vol = weights.Dot(cov).Dot(weights)
    Math.Sqrt(vol * nObsAnnual)
  let annualVol2 =
    let vol = weights.Dot(covFrame).Dot(weights)
    Math.Sqrt(vol * nObsAnnual)
  annualVol1 |> should beWithin (0.14553 +/- 1e-5)
  annualVol1 |> should beWithin (annualVol2 +/- 1e-6)

[<Test>]
let ``ewmVol on non-returns series should return standard deviation not root-mean-square (issue #555)`` () =
  // For a monotonically increasing sequence, ewmVol should give std (~5),
  // not a value near the mean (~45) as the old incorrect formula produced.
  let s = Series.ofValues [ for i in 1.0 .. 50.0 -> i ]
  let vol = Finance.ewmVol(s, span = 10.)
  let lastVol = vol |> Series.lastValue
  let ewmMeanLast = Stats.ewmMean(s, span = 10.) |> Series.lastValue
  // Std should be much smaller than the mean (~5 vs ~45)
  lastVol |> should be (lessThan 10.)
  lastVol |> should be (greaterThan 1.)
  let varFrame = Finance.ewmVar(stockReturns, halfLife = 52.)
  let cov = Finance.ewmCovMatrix(stockReturns, halfLife = 52.) |> Series.lastValue
  let varSeries1 = (varFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let varSeries2 = Series(stockReturns.ColumnKeys, cov.Diagonal())
  (varSeries1 - varSeries2) |> Stats.sum |> should beWithin (0. +/- 1e-10)
