(*** hide ***)
#I "../../../packages/FSharp.Charting.0.87"
#r "../../../bin/MathNet.Numerics.dll"
#load "../../bin/Deedle.fsx"
#load "FSharp.Charting.fsx"

//open System
//open FSharp.Charting
//open MathNet.Numerics.Distributions

(**
R Provider interoperabilit
==========================
*)
//#I @"C:\Tomas\Projects\FSharp.RProvider\bin"
#I "../../../src/deedle.rprovider.plugin/bin/debug/"
#r "RProvider.dll"
#r "RDotNet.dll"

open Deedle
open RProvider
open RProvider.datasets
open FSharp.Charting

let mtcars : Frame<string, string> = R.mtcars.GetValue()
mtcars
|> Frame.groupRowsByInt "gear"
|> Frame.meanLevel fst
|> Frame.getSeries "mpg"
|> Series.observations |> Chart.Column



open RProvider.``base``

let df = frame [ "A" => series [ 1 => 10.0]]
R.assign("x",  df).Value
R.eval(R.parse(text="x")).GetValue<Frame<int, string>>()

open RDotNet
//open RProvider.zoo

R.zoo([10;20;30], [1;2;3])


open RProvider.datasets

open RDotNet
fsi.AddPrinter(fun (se:SymbolicExpression) -> se.Print())

R.mtcars
R.BJsales
R.AirPassengers.Class
R.DNase.Value

R.ChickWeight


R.eval(R.parse(text="EuStockMarkets")).Class
R.eval(R.parse(text="Nile")).Class
R.eval(R.parse(text="mtcars")).Class

//.Value :?> Frame<string, string>

let rdf = R.eval(R.parse(text="""data.frame(c(2, 3, 5), c("aa", "bb", "cc"), c(TRUE, FALSE, TRUE)) """))
rdf.Value

