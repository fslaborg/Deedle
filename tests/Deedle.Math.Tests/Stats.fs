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
let cov = stockReturns |> Stats.covFrame
let corr = stockReturns |> Stats.corrFrame
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
let ``Ex-ante vol of equally weighted portfolio works`` () =
  annualVol |> should beWithin (0.13575 +/- 1e-6)