(*** hide ***)
#I "../../../packages/FSharp.Charting.0.87"
#r "../../../bin/MathNet.Numerics.dll"
#load "../../bin/Deedle.fsx"
#load "FSharp.Charting.fsx"

open System
open Deedle
open FSharp.Charting
open MathNet.Numerics.Distributions

(**
R Provider interoperabilit
==========================
*)
#I "../../../src/deedle.rprovider.plugin/bin/debug/"
#r "RProvider.dll"
#r "RDotNet.dll"
open RProvider
//open RProvider.``base``
open RProvider.datasets

let df = frame [ "A" => series [ 1 => 10.0]]
R.assign("x",  df)


R.eval(R.parse(text="mtcars")).Value :?> Frame<string, string>


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

