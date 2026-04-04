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
let ``ewmVolStdDev on non-returns series should return standard deviation not root-mean-square (issue #555)`` () =
  // For a monotonically increasing sequence, ewmVolStdDev should give std (~5),
  // not a value near the mean (~45) as the old incorrect ewmVol formula produced.
  let s = Series.ofValues [ for i in 1.0 .. 50.0 -> i ]
  let vol = Finance.ewmVolStdDev(s, span = 10.)
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

[<Test>]
let ``ewmVolStdDev on monotonically increasing series gives small std (not mean)`` () =
  // For a monotonically increasing sequence, ewmVolStdDev should give values near the
  // per-step increment (~1), much smaller than the mean (~45)
  let s = Series.ofValues [ for i in 1.0 .. 50.0 -> i ]
  let vol = Finance.ewmVolStdDev(s, span = 10.)
  let lastVol = vol |> Series.lastValue
  lastVol |> should be (lessThan 10.)
  lastVol |> should be (greaterThan 0.)

[<Test>]
let ``ewmVolStdDev on frame squared equals ewmVar for last row`` () =
  let varFrame = Finance.ewmVar(stockReturns, halfLife = 52.)
  let volFrame = Finance.ewmVolStdDev(stockReturns, halfLife = 52.)
  // Check the last row: ewmVar should equal ewmVolStdDev^2 for each column
  let lastVar = (varFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let lastVol = (volFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let lastVarFromVol = lastVol |> Series.mapValues (fun v -> v * v)
  (lastVar - lastVarFromVol) |> Stats.sum |> should beWithin (0. +/- 1e-10)

[<Test>]
let ``ewmVolStdDev diagonal consistency with ewmCovMatrix`` () =
  let varFrame = Finance.ewmVar(stockReturns, halfLife = 52.)
  let cov = Finance.ewmCovMatrix(stockReturns, halfLife = 52.) |> Series.lastValue
  let varSeries1 = (varFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let varSeries2 = Series(stockReturns.ColumnKeys, cov.Diagonal())
  (varSeries1 - varSeries2) |> Stats.sum |> should beWithin (0. +/- 1e-10)

[<Test>]
let ``ewmVolRMS on zero-mean series is in same ballpark as ewmVolStdDev`` () =
  // For a zero-mean series, RMS and StdDev should be in the same ballpark
  let rng = System.Random(42)
  let zeroMean = Series.ofValues [ for _ in 1..100 -> rng.NextDouble() - 0.5 ]
  let rms = Finance.ewmVolRMS(zeroMean, span = 20.) |> Series.lastValue
  let std = Finance.ewmVolStdDev(zeroMean, span = 20.) |> Series.lastValue
  rms |> should be (greaterThan 0.)
  std |> should be (greaterThan 0.)
  abs(rms - std) / std |> should be (lessThan 0.5)

[<Test>]
let ``ewmVolRMS on monotonically increasing series is near the ewm mean (old behaviour)`` () =
  // For a monotonically increasing sequence starting at 1, RMS ≈ EWMA(x),
  // which is the old behaviour of ewmVol before the fix for issue #555.
  let s = Series.ofValues [ for i in 1.0 .. 50.0 -> i ]
  let rms = Finance.ewmVolRMS(s, span = 10.)
  let lastRMS = rms |> Series.lastValue
  let ewmMeanLast = Stats.ewmMean(s, span = 10.) |> Series.lastValue
  // RMS should be close to the mean (both ~45) for a positive monotone sequence
  lastRMS |> should be (greaterThan 10.)
  abs(lastRMS - ewmMeanLast) / ewmMeanLast |> should be (lessThan 0.05)

[<Test>]
let ``ewmVolRMS on frame columns are all positive`` () =
  let rmsFrame = Finance.ewmVolRMS(stockReturns, halfLife = 52.)
  let lastRow = (rmsFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  lastRow |> Series.values |> Seq.iter (fun v -> v |> should be (greaterThan 0.))

[<Test>]
let ``ewmCrossCov of series with itself equals ewmVar`` () =
  let col = stockReturns.GetColumnAt<float>(0)
  let crossCov = Finance.ewmCrossCov(col, col, halfLife = 52.)
  let varSeries = Finance.ewmVar(col, halfLife = 52.)
  // Both series should have the same length and nearly identical values
  crossCov.KeyCount |> should equal varSeries.KeyCount
  let maxDiff =
    (crossCov - varSeries)
    |> Series.mapValues Math.Abs
    |> Stats.max
  maxDiff |> should beWithin (0. +/- 1e-10)

[<Test>]
let ``ewmCrossCov between two columns matches off-diagonal of ewmCovMatrix`` () =
  let halfLife = 52.
  let col0 = stockReturns.GetColumnAt<float>(0)
  let col1 = stockReturns.GetColumnAt<float>(1)
  let crossCov = Finance.ewmCrossCov(col0, col1, halfLife = halfLife)
  // Get the off-diagonal element from the full EWM covariance matrix at each row
  let covMatrixSeries = Finance.ewmCovMatrix(stockReturns, halfLife = halfLife)
  let lastCrossFromMatrix = (covMatrixSeries |> Series.lastValue).[0, 1]
  let lastCrossFromPair   = crossCov |> Series.lastValue
  lastCrossFromPair |> should beWithin (lastCrossFromMatrix +/- 1e-10)

[<Test>]
let ``ewmCrossVol squared equals ewmCrossCov in magnitude`` () =
  let halfLife = 52.
  let col0 = stockReturns.GetColumnAt<float>(0)
  let col1 = stockReturns.GetColumnAt<float>(1)
  let crossCov = Finance.ewmCrossCov(col0, col1, halfLife = halfLife)
  let crossVol = Finance.ewmCrossVol(col0, col1, halfLife = halfLife)
  // |crossVol|^2 should equal |crossCov|
  let absCrossVolSq = crossVol |> Series.mapValues (fun v -> v * v)
  let absCrossCov   = crossCov |> Series.mapValues Math.Abs
  let maxDiff = (absCrossVolSq - absCrossCov) |> Series.mapValues Math.Abs |> Stats.max
  maxDiff |> should beWithin (0. +/- 1e-10)

[<Test>]
let ``ewmCrossVol preserves sign of ewmCrossCov`` () =
  let halfLife = 52.
  let col0 = stockReturns.GetColumnAt<float>(0)
  let col1 = stockReturns.GetColumnAt<float>(1)
  let crossCov = Finance.ewmCrossCov(col0, col1, halfLife = halfLife)
  let crossVol = Finance.ewmCrossVol(col0, col1, halfLife = halfLife)
  // sign(crossVol) should equal sign(crossCov) for all elements
  let signsMatch =
    crossCov.Values
    |> Seq.zip crossVol.Values
    |> Seq.forall (fun (vol, cov) -> Math.Sign cov = Math.Sign vol || (cov = 0. && vol = 0.))
  signsMatch |> should equal true
