(*** hide ***)
#I "../../../bin/"
#I "../../../packages/FSharp.Charting.0.87"
#I "../../../packages/Deedle.0.9.5"
#load "FSharp.Charting.fsx"
#load "Deedle.fsx"
open System
open Deedle
open FSharp.Charting

(**
Missing data and exceptions
==========================
*)

let msftCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../data/MSFT.csv")
let fbCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../data/FB.csv")

let msftOrd = 
  msftCsv
  |> Frame.indexRowsDate "Date"
  |> Frame.orderRows

let msft = msftOrd.Columns.[ ["Open"; "Close"] ]

// Add new column with the difference between Open & Close
msft?Difference <- msft?Open - msft?Close

// Do the same thing for Facebook
let fb = 
  fbCsv
  |> Frame.indexRowsDate "Date"
  |> Frame.orderRows
  |> Frame.getCols ["Open"; "Close"]
fb?Difference <- fb?Open - fb?Close

let msftRen = msft |> Frame.indexColsWith ["MsftOpen"; "MsftClose"; "MsftDiff"]
let fbRen = fb |> Frame.indexColsWith ["FbOpen"; "FbClose"; "FbDiff"]

// Outer join (align & fill with missing values)
let joinedOut = msftRen.Join(fbRen, kind=JoinKind.Outer)


(**

Handling errors
---------------
*)

let errors1 = 
  joinedOut?MsftOpen 
  |> Series.tryMap (fun k v -> int v / (k.Day % 5))

let errors2 = 
  joinedOut?MsftOpen 
  |> Series.tryMap (fun k v -> int v / (k.Day % 5) |> string)

let noerrors = 
  joinedOut?MsftOpen 
  |> Series.tryMap (fun k v -> v / float (k.Day % 5))

// Throws loads
errors1 |> Series.tryValues
// Works fine
noerrors |> Series.tryValues

errors1 |> Series.fillErrorsWith -1

let errorDf = frame [ "Errors1" =?> errors1; "Errors2" =?> errors2; "NoErrors" =?> noerrors ]

// 
errorDf |> Frame.fillErrorsWith -1 |> Frame.fillErrorsWith (null:string)


(**

Other TODO things
-----------------

*)

log (msftRen ** 2.0)