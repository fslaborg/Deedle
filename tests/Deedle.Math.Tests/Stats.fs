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
let ``Ex-ante vol of equally weighted portfolio using normal covariance matrix works`` () =
  let cov = stockReturns |> Stats.cov
  let annualVol =
    let vol = weights.Dot(cov).Dot(weights)
    let nObs = 52.
    Math.Sqrt(vol * nObs)
  annualVol |> should beWithin (0.13575 +/- 1e-6)  

[<Test>]
let ``Ex-ante vol of equally weighted portfolio using exponentially weighted covariance matrix works`` () =
  let cov = Stats.ewCovMatrix(stockReturns, halfLife = 52.) |> Series.lastValue
  let covFrame = Stats.ewCov(stockReturns, halfLife = 52.) |> Series.lastValue
  let annualVol1 =
    let vol = weights.Dot(cov).Dot(weights)
    let nObs = 52.
    Math.Sqrt(vol * nObs)
  let annualVol2 =
    let vol = weights.Dot(covFrame).Dot(weights)
    let nObs = 52.
    Math.Sqrt(vol * nObs)
  annualVol1 |> should beWithin (0.14437 +/- 1e-6)
  annualVol1 |> should beWithin (annualVol2 +/- 1e-6)

[<Test>]
let ``cov2Corr and corr2Cov work`` () =
  let cov = stockReturns |> Stats.cov
  let std, corr = cov |> Stats.cov2Corr
  let actual = Stats.corr2Cov(std, corr).GetColumnAt<float>(0).GetAt(0)
  let expected = cov.GetColumnAt<float>(0).GetAt(0)
  actual |> should beWithin (expected +/- 1e-6)
  