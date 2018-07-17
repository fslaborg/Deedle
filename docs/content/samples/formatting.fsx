(*** hide ***)
#load "../../../bin/net45/Deedle.fsx"
#load "../../../packages/FSharp.Charting/lib/net45/FSharp.Charting.fsx"
#r "../../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
open System
open System.IO
open FSharp.Data
open Deedle
open FSharp.Charting

let root = __SOURCE_DIRECTORY__ + "/../data/"

(**
Numerics and statistics
===================

*)

(*** define-output:sample ***)
let s1 = series [ for i in 1 .. 100 -> i => float (i * i) ]
let s2 = series [ for i in 1 .. 100 -> (i + 2) => float (i * i) ]
let s1' = s1 |> Series.mapAll (fun k v -> if k%3=0 then None else v)
frame [ for i in 1 .. 10 do yield "First "+(string i) => s1; yield "Another "+(string i) => s2 ]

(** 
some chat
*)

(*** include-it: sample ***)

(**
some other chat
*)

(*** include-value: s1' ***)

(*** define-output: chart ***)
Chart.Line [ for x in 0.0 .. 0.001 .. 2.0 -> x, sin x ]

(*** include-it:chart ***)

(*** define-output: chart2 ***)
Chart.Pie [ "Good", 20; "Bad", 10; "Unsure", 5 ]

(*** include-it:chart2 ***)

(**
helper to capture the last image
*)
#load "../../../packages/RProvider/RProvider.fsx"
open RProvider
open RProvider.datasets
open RProvider.``base``
open RProvider.graphics
open RProvider.grDevices

let lastPlot() =
  let png = R.eval(R.parse(text="png"))
  let file = System.IO.Path.GetTempFileName() + ".png"
  R.dev_off(R.dev_copy(namedParams [ "device", box png; "filename", box file ])) |> ignore
  R.graphics_off() |> ignore
  //R.x11()
  System.Drawing.Bitmap.FromFile(file)
  //file
  
let lastPlot2 = lastPlot
(**
Rko
*)


R.plot [ for i in 1.0 .. 0.01 .. 3.0 -> sin i ]

(*** include-value: lastPlot() ***)


R.plot [ for i in 1.0 .. 0.01 .. 3.0 -> cos i ]


(*** include-value: lastPlot2() ***)