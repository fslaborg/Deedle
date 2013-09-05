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
      let s = [ for c, k in Seq.countBy id countries -> c, k ] |> Series.ofValues 
      (s :> FSharp.DataFrame.Common.IFormattable).Format() |> printfn "%s\n"
      s)
  |> Frame.ofRows
  |> Frame.withMissingVal 0

let sums = peopleCountries |> Frame.sum 

sums.Observations
|> Seq.sortBy (fun (c, v) -> -v)
|> Chart.Column


(* TEST *)

let f1 = Frame.ofRows [ 1 => Series.ofValues [ "A" => 1; "B" => 2 ] ]
let f2 = Frame.ofRows [ 2 => Series.ofValues [ "C" => 3 ] 
                        3 => Series.ofValues [ "C" => 4 ]  ]
f1.Append(f2)

