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

#I "../../../packages/RProvider.1.0.3/lib"
#r "RProvider.dll"
#r "RDotNet.dll"
open RProvider
open RProvider.``base``

let df = frame [ "A" => series [ 1 => 10.0]]
R.assign("x",  df)