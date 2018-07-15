(*** hide ***)
#load "../../../packages/FSharp.Charting/lib/net45/FSharp.Charting.fsx"
#r "../../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
#load "../../../bin/net45/Deedle.fsx"
open System
open FSharp.Data
open Deedle
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/../data/"


let levelMean (level) (frame:Frame<_,_>)= frame.GetColumns<float>()
                                                    |> Series.map (fun _ -> Stats.levelMean level)
                                                    |> Frame.ofColumns


(**
Analyzing Titanic data set
==========================
*)

let titanic = Frame.ReadCsv(root + "titanic.csv")

(**

Draw a pie chart displaying the survival rate

*)

titanic
|> Frame.groupRowsByBool "Survived"
|> Frame.getCol "PassengerId"
|> Stats.levelCount fst
|> Series.indexWith ["Died"; "Survived"]
|> Series.observations
|> Chart.Pie

(**

Group by class and port

*)

let byClassAndPort1 = 
  titanic.GroupRowsBy<int>("Pclass").GroupRowsBy<string>("Embarked") 

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
  [ "AgeMeans", ageByClassAndPort |> Stats.levelMean Pair.get1And2Of3
    "AgeCounts", float $ (ageByClassAndPort |> Stats.levelCount Pair.get1And2Of3) ]

(**

Mean & sum everything by class and port

*)

byClassAndPort |> levelMean Pair.get1And2Of3


(**

Look at survived column as booleans

*)

let survivedByClassAndPort = byClassAndPort.Columns.["Survived"].As<bool>()

(**

Count number of survived/died in each group

*)

let survivals = 
  survivedByClassAndPort 
  |> Series.applyLevel Pair.get1And2Of3 (fun sr -> 
      sr.Values |> Seq.countBy id |> series)
  |> Frame.ofRows
  |> Frame.indexColsWith ["Survived"; "Died"]
(**

Count total number of passangers in each group

*)

survivals?Total <- survivals?Survived + survivals?Died

survivals

let summary = 
  [ "Survived (%)" => survivals?Survived / survivals?Total * 100.0
    "Died (%)" => survivals?Died/ survivals?Total * 100.0 ] |> frame

round summary

summary |> levelMean fst
summary |> levelMean snd
  