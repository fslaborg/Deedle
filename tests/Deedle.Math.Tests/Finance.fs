#if INTERACTIVE
#I "../../bin/net45"
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
  annualVol1 |> should beWithin (0.14437 +/- 1e-6)
  annualVol1 |> should beWithin (annualVol2 +/- 1e-6)
  
[<Test>]
let ``Diagonals of ewmVar and ewmCov shall be identical `` () =
  let varFrame = Finance.ewmVar(stockReturns, halfLife = 52.)
  let cov = Finance.ewmCovMatrix(stockReturns, halfLife = 52.) |> Series.lastValue
  let varSeries1 = (varFrame |> Frame.takeLast 1).GetRowAt<float>(0)
  let varSeries2 = Series(stockReturns.ColumnKeys, cov.Diagonal())
  (varSeries1 - varSeries2) |> Stats.sum |> should beWithin (0. +/- 1e-10)