(*** hide ***)
#r "../../../bin/Deedle.dll"
#load "../../../packages/FSharp.Charting.0.87/FSharp.Charting.fsx"
#r "../../../packages/FSharp.Data.1.1.10/lib/net40/FSharp.Data.dll"
open System
open FSharp.Data
open Deedle
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/../data/"

(**
Analyzing Titanic data set
==========================
*)

let titanic = Frame.ReadCsv(root + "Titanic.csv")

(**

Group by class and port

*)

let byClassAndPort1 = 
  titanic.GroupRowsBy<int>("Pclass").GroupRowsBy<string>("Embarked") 
  |> Frame.mapRowKeys Pair.flatten3

let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3

(**

Get the age column

*)

let ageByClassAndPort = byClassAndPort.Columns.["Age"].As<float>()

Frame.ofColumns
  [ "AgeMeans", ageByClassAndPort |> Series.meanLevel Pair.get1And2Of3
    "AgeCounts", float $ (ageByClassAndPort |> Series.countLevel Pair.get1And2Of3) ]

(**

Mean & sum everything by class and port

*)

byClassAndPort
|> Frame.meanLevel Pair.get1And2Of3

byClassAndPort
|> Frame.sumLevel Pair.get1And2Of3

(**

Look at survived column as booleans

*)

let survivedByClassAndPort = byClassAndPort.Columns.["Survived"].As<bool>()

(**

Count number of survived/died in each group

*)

survivedByClassAndPort 
|> Series.applyLevel Pair.get1And2Of3 (fun sr -> 
    series (sr |> Seq.countBy id))
|> Frame.ofRows

(**

Count total number of passangers in each group

*)

byClassAndPort
|> Frame.applyLevel Pair.get1And2Of3 Seq.length

