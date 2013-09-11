(**
Feature overview
================

TODO

*)
#I "../bin"
#I "../lib"
#I "../packages"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"
open System
open FSharp.DataFrame
open FSharp.Charting

(**
Fancy windowing & chunking
--------------------------
*)

let s = Series.ofValues [ 'a' .. 'j' ]
s |> Series.windowSize (3, Boundary.Skip) |> Series.map (fun _ v -> String(Array.ofSeq v.Data.Values))
s |> Series.windowSize (3, Boundary.AtBeginning) |> Series.map (fun _ v -> String(Array.ofSeq v.Data.Values))


(** 
Grouping 
--------
*)

// Load the data from the Titanic data set
let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/Titanic.csv")

// Group the data frame by sex 
let grouped = titanic |> Frame.groupRowsBy "Sex"

// For each group, calculate the total number of survived & died
let survivalBySex =
  grouped 
  |> Series.map (fun sex df -> 
      // Group each sex by the Survived column & count elements in each group
      df.GetSeries<bool>("Survived") |> Series.groupBy (fun k v -> v) 
      |> Frame.ofColumns |> Frame.countValues )
  |> Frame.ofRows
  |> Frame.withColumnKeys ["Died"; "Survived"]

// Add column with Total number of males/females on Titanic
survivalBySex?Total <- grouped |> Series.map (fun _ -> Frame.countKeys)

// Build a data frame with nice summary of rates in percents
let results =
  [ "Died (%)" => survivalBySex?Died / survivalBySex?Total * 100.0
    "Survived (%)" => survivalBySex?Survived / survivalBySex?Total * 100.0 ]
  |> Frame.ofColumns


(**
Filling missing values
----------------------
*)

let nulls =
  [ Nullable(1); Nullable(); Nullable(3); Nullable(4) ]
  |> Series.ofValues
  |> Series.map (fun _ v -> v.Value)

let test = Frame.ofColumns [ "Test" => nulls ]
test?Test

[ Nullable(1); Nullable(); Nullable(3); Nullable(4) ]
|> Series.ofNullables


let air = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/AirQuality.csv", separators=";")
let ozone = air?Ozone

air?OzoneFilled <-
  ozone |> Series.mapAll (fun k -> function 
    | None -> ozone.TryGet(k, Lookup.NearestSmaller)
    | value -> value)

// Might not be super usefu but does the trick
ozone |> Series.fillMissingWith 0.0
// This works
ozone |> Series.fillMissing Lookup.NearestGreater
// but here, the first value is still missing
ozone |> Series.fillMissing Lookup.NearestSmaller
// we can be more explicit and write
ozone |> Series.fillMissingUsing (fun k -> 
  defaultArg (ozone.TryGet(k, Lookup.NearestSmaller)) 0.0)
// Or we can drop the first value
ozone |> Series.fillMissing Lookup.NearestSmaller
      |> Series.dropMissing



// air.WithRowIndex<int>("Month")

// System.Linq.Enumerable.group
// air.Rows.Aggregate(Aggregation.GroupBy(



(*

let f = Frame.ofColumns ["C" => Series.ofValues [ 1;2;3 ]]
f.Append(f)

// TODO:
let f = Frame.ofColumns ["C" => Series.ofObservations [ 1,"hi"; 2,"there"; 3,"ciao" ]]
f.Append(f)

*)

(**
Operations
----------
*)

let rnd = Random()
let s = Series.ofValues [ for i in 0 .. 100 -> rnd.NextDouble() * 10.0 ]

log s 
log10 s

let s1 = abs (log s) * 10.0
floor s1 - round s1  

Series.Log10


(**
Grouping
--------

*)
type Person = 
  { Name:string; Age:int; Countries:string list; }

let people = 
  [ { Name = "Joe"; Age = 51; Countries = [ "UK"; "US"; "UK"] }
    { Name = "Tomas"; Age = 28; Countries = [ "CZ"; "UK"; "US"; "CZ" ] }
    { Name = "Eve"; Age = 2; Countries = [ "FR" ] } ]

// Turn the list of records into data frame and use 
// the 'Name' column (containing strings) as the key
let dfList = Frame.ofRecords people 
let df = dfList |> Frame.withRowIndex column<string> "Name"

let peopleCountries = 
  df.GetSeries<string list>("Countries")
  |> Series.map (fun k countries -> 
      [ for c, k in Seq.countBy id countries -> c, k ] |> Series.ofObservations )
  |> Frame.ofRows
  |> Frame.withMissingVal 0

let sums = peopleCountries |> Frame.sum 

sums.Observations
|> Seq.sortBy (fun (c, v) -> -v)
|> Chart.Column


(* More options when working with records *)
let recdSeries = 
  Series.ofValues [ "Tomas"; null; "Joe"; "Eve" ]
  |> Series.map (fun _ v -> { Name = v; Age = -1; Countries = [] })

// Missing values are preserved at the right index..
Frame.ofRecords recdSeries




(* TEST *)

let f1 = Frame.ofRows [ 1 => Series.ofObservations [ "A" => 1; "B" => 2 ] ]
let f2 = Frame.ofRows [ 2 => Series.ofObservations [ "C" => 3 ] 
                        3 => Series.ofObservations [ "C" => 4 ]  ]
f1.Append(f2)

