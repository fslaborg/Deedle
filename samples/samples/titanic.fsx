(*** hide ***)
#I "../../bin"
#load "FSharp.DataFrame.fsx"
#load "../../packages/FSharp.Charting.0.84/FSharp.Charting.fsx"
#r "../../packages/FSharp.Data.1.1.9/lib/net40/FSharp.Data.dll"
open System
open FSharp.Data
open FSharp.DataFrame
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/data/"

(**
TODO
*)

let titanic = Frame.readCsv(root + "Titanic.csv")

let byClassAndPort1 = titanic.GroupRowsBy<int>("Pclass").GroupRowsBy<string>("Embarked") |> Frame.mapRowKeys Tuple.flatten3
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3


let ageByClassAndPort = byClassAndPort.Columns.["Age"].As<float>()

Frame.ofColumns
  [ "AgeMeans", ageByClassAndPort |> Series.meanBy Pair.get1And2Of3
    "AgeCounts", float $ (ageByClassAndPort |> Series.countBy Pair.get1And2Of3) ]

byClassAndPort
|> Frame.meanBy Pair.get1And2Of3

byClassAndPort
|> Frame.sumBy Pair.get1And2Of3

let survivedByClassAndPort = byClassAndPort.Columns.["Survived"].As<bool>()

// survivedByClassAndPort
// |> Series.meanBy By1Of3

survivedByClassAndPort 
|> Series.foldBy Pair.get1And2Of3 (fun sr -> sprintf "%A" (sr.Values |> Seq.countBy id |> List.ofSeq))

byClassAndPort
|> Frame.foldBy Pair.get1And2Of3 (fun sr -> sr |> Series.countValues)

