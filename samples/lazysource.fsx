#I "../bin"
#load "FSharp.DataFrame.fsx"
open System

open FSharp.DataFrame
open FSharp.DataFrame.Indices


let loadSeries (min:DateTime) max column = 

  let inline midp (a:DateTime) (b:DateTime) =
    let ts = b - a in a + TimeSpan(ts.Ticks / 2L)

  DelayedSeries.Create(min, max, midp, column, fun (lo, loinc) (hi, hiinc) -> 
    Async.StartAsTask <| async {
      let rnd = Random()
      printfn "QUERY %s: %A" column ((lo, loinc), (hi, hiinc))

      let lo = if loinc = Exclusive then lo.AddDays(1.0) else lo
      let hi = if hiinc = Exclusive then hi.AddDays(-1.0) else hi
      return seq {
        for i in 0 .. int (hi - lo).TotalDays do 
          yield lo.AddDays(float i), rnd.NextDouble() } } )

let s1 = loadSeries (DateTime(2000,1,1)) (DateTime(2015,1,1)) "Test"
let s2 = s1.[DateTime(2013,1,1) .. ]
let s3 = s2.[ .. DateTime(2013,2,1)]

// s3 |> Series.observations |> Seq.iter (printfn "%O")
// s3.[DateTime(2013,2,1)]

Frame.ofColumns 
  [ "S2" => s2; "S3" => s3; "S4" => Series.ofObservations [DateTime(2012,12,1), 42.0] ]

let s4 = s1.GetSubrange(Some(DateTime(2014, 12, 25), Exclusive), None)
//s4.[DateTime(2014,12,16)]
//s4.[DateTime(2014,12,15)]

Frame.ofColumns 
  [ "S2" => s2; "S3" => s3; "S4" => s4 ]

let sept = Frame.ofRowKeys [ for d in 1 .. 30 -> DateTime(2012, 9, d) ]
sept?S1 <- s1

