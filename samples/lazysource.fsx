#I "../bin"
#I "../lib"
#I "../packages"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"
open System
open FSharp.DataFrame
open FSharp.Charting


let loadSeries (min:DateTime) max column = 
  DelayedSeries.Create(min, max, column, fun lo hi -> 
    let rnd = Random()
    printfn "QUERY %s: %A" column (lo, hi)
    [| for i in 0 .. int (hi - lo).TotalDays do 
         yield lo.AddDays(float i), rnd.NextDouble() |] )

let s1 = loadSeries (DateTime(2000,1,1)) (DateTime(2015,1,1)) "Test"
let s2 = s1.[DateTime(2013,1,1) .. ]
let s3 = s2.[ .. DateTime(2013,2,1)]


// s3.Observations |> Seq.map fst |> Seq.iter (printfn "%O")
// s3.[DateTime(2013,2,1)]

Frame.ofColumns [ "S2" => s2; "S3" => s3; "S4" => Series.ofObservations [DateTime(2013,1,1), 42.0] ]

s3.Count