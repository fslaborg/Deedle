(*** hide ***)
#I "../../bin"
#load "FSharp.DataFrame.fsx"
#load "../../packages/FSharp.Charting.0.86/FSharp.Charting.fsx"
#r "../../packages/FSharp.Data.1.1.9/lib/net40/FSharp.Data.dll"
open System
open FSharp.Data
open FSharp.DataFrame
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/../data/"

(**
Analyzing Titanic data set
==========================
*)

let titanic = Frame.readCsv(root + "Titanic.csv")

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
  [ "AgeMeans", ageByClassAndPort |> Series.meanBy Pair.get1And2Of3
    "AgeCounts", float $ (ageByClassAndPort |> Series.countBy Pair.get1And2Of3) ]

(**

Mean & sum everything by class and port

*)

byClassAndPort
|> Frame.meanBy Pair.get1And2Of3

byClassAndPort
|> Frame.sumBy Pair.get1And2Of3

(**

Look at survived column as booleans

*)

let survivedByClassAndPort = byClassAndPort.Columns.["Survived"].As<bool>()

(**

Count number of survived/died in each group

*)

survivedByClassAndPort 
|> Series.foldBy Pair.get1And2Of3 (fun sr -> 
    series (sr.Values |> Seq.countBy id))
|> Frame.ofRows

(**

Count total number of passangers in each group

*)

byClassAndPort
|> Frame.foldBy Pair.get1And2Of3 (fun sr -> sr |> Series.countValues)

