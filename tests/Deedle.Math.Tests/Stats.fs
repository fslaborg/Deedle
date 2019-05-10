#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.Common
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Internal
open Deedle.Math

let stockPrices =
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/stocks_weekly.csv")
  |> Frame.indexRowsDate "Dates"
let stockReturns =
  stockPrices / (stockPrices |> Frame.shift 1) - 1
  |> Frame.dropSparseRows
let cov = stockReturns |> Stats.covarianceFrame
let corr = stockReturns |> Stats.pearsonCorrelationFrame
let weights =
  let nStocks = stockPrices.ColumnCount
  let w = Array.init nStocks (fun _ -> 1. / float nStocks)
  Seq.zip stockPrices.ColumnKeys w
  |> Series.ofObservations
let annualVol =
  let vol = weights.Dot(cov).Dot(weights).Item(0,0)
  let nObs = 52.
  Math.Sqrt(vol * nObs)

[<Test>]
let ``Ex-ante vol of equally weighted portfolio works`` () =
  annualVol |> should beWithin (0.13575 +/- 1e-6)