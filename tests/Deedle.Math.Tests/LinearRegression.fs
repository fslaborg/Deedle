#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.LinearRegression
#endif

open Deedle
open Deedle.Math
open MathNet.Numerics.LinearAlgebra
open MathNet.Numerics.LinearRegression
open NUnit.Framework

[<Literal>]
let stockPath =
  __SOURCE_DIRECTORY__ + "/data/stocks_weekly.csv"
let stockPrices =
  Frame.ReadCsv(stockPath)
  |> Frame.indexRowsDate "Dates"
let stockReturns =
  let shiftedPrices =
    Frame.shift 1 stockPrices
  (stockPrices - shiftedPrices) - 1
  |> Frame.dropSparseRows

[<Test>]
let ``Simple linear regression coefficients between MSFT and WMT returns the same as in Math.Net when fit without y-axis intersect`` () =
  let actualCoeffs =
    LinearRegression.simple "MSFT" "WMT" None stockReturns
    |> LinearRegression.Fit.coefficients
  let actualCoeff = actualCoeffs.["MSFT"]
  let expectedCoeff =
      let xs =
        stockReturns.["MSFT"]
        |> Series.values
        |> Seq.toArray
      let ys =
        stockReturns.["WMT"]
        |> Series.values
        |> Seq.toArray
      MathNet.Numerics.LinearRegression.SimpleRegression.FitThroughOrigin(xs, ys)
  Assert.AreEqual(expectedCoeff, actualCoeff, 1e-10)

[<Test>]
let ``Simple linear regression coefficients between MSFT and WMT returns the same as in Math.Net when fit with y-axis intersect`` () =
  let actualCoeffs =
    LinearRegression.simple "MSFT" "WMT" (Some "yIntersect") stockReturns
    |> LinearRegression.Fit.coefficients
  let actualCoeff = actualCoeffs.["MSFT"]
  let (yCross, xCoeff) =
    let xs =
      stockReturns.["MSFT"]
      |> Series.values
      |> Seq.toArray
    let ys =
      stockReturns.["WMT"]
      |> Series.values
      |> Seq.toArray
    MathNet.Numerics.LinearRegression.SimpleRegression.Fit(xs, ys)
  let intersect = actualCoeffs.["yIntersect"]
  Assert.AreEqual(yCross, intersect, 1e-10)
  Assert.AreEqual(xCoeff, actualCoeff, 1e-10)

[<Test>]
let ``Multiple linear regression coefficients between MSFT, WMT and AES returns the same as in Math.Net when fit without y-axis intersect`` () =
  let actualCoeffs =
    LinearRegression.multiDim ["MSFT";"WMT"] "AES" None stockReturns
    |> LinearRegression.Fit.coefficients
  let xs =
    stockReturns
    |> Frame.filterCols (fun k _ -> List.contains k ["MSFT"; "WMT"])
    |> Frame.toMatrix
    |> Matrix.toRowArrays
  let ys =
      stockReturns.["AES"]
      |> Series.values
      |> Seq.toArray
  let results =
    MathNet.Numerics.LinearRegression.MultipleRegression.DirectMethod<float>(xs, ys, false, DirectRegressionMethod.QR)
  let expectedMsft = results.[0]
  let expectedWmt = results.[1]
  Assert.AreEqual(expectedMsft, actualCoeffs.["MSFT"], 1e-10)
  Assert.AreEqual(expectedWmt, actualCoeffs.["WMT"], 1e-10)


[<Test>]
let ``Multiple linear regression coefficients between MSFT, WMT and AES returns the same as in Math.Net when fit with y-axis intersect`` () =
  let actualCoeffs =
    LinearRegression.multiDim ["MSFT";"WMT"] "AES" (Some "yIntersect") stockReturns
    |> LinearRegression.Fit.coefficients
  let xs =
    stockReturns
    |> Frame.filterCols (fun k _ -> List.contains k ["MSFT"; "WMT"])
    |> Frame.toMatrix
    |> Matrix.toRowArrays
  let ys =
      stockReturns.["AES"]
      |> Series.values
      |> Seq.toArray
  let results =
    MathNet.Numerics.LinearRegression.MultipleRegression.DirectMethod<float>(xs, ys, true, DirectRegressionMethod.QR)
  let expectedYIntersect = results.[0]
  let expectedMsft = results.[1]
  let expectedWmt = results.[2]
  Assert.AreEqual(expectedYIntersect, actualCoeffs.["yIntersect"], 1e-10)
  Assert.AreEqual(expectedMsft, actualCoeffs.["MSFT"], 1e-10)
  Assert.AreEqual(expectedWmt, actualCoeffs.["WMT"], 1e-10)
