module Main
#if INTERACTIVE
#I "..\\bin"
#load "DataFrame.fsx"
#endif

open FSharp.DataFrame
open System
open System
//open MathNet.Numerics.Distributions

let timed f = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn "%dms" sw.ElapsedMilliseconds
  res


do

  let titanic = Frame.readCsv(__SOURCE_DIRECTORY__ + "/../samples/data/Titanic.csv")
  let it = timed(fun () -> titanic |> Frame.groupRowsBy "Sex")

(*
  // Generate price using geometric Brownian motion
  let randomPrice drift volatility initial count = 
    let dist = Normal(0.0, 1.0, RandomSource=Random(0))  
    let dt = 1.0 / (250.0 * 24.0 * 60.0)
    let driftExp = (drift - 0.5 * pown volatility 2) * dt
    let randExp = volatility * (sqrt dt)
    (DateTimeOffset(DateTime(2013, 1, 1)), initial) |> Seq.unfold (fun (dt, price) ->
      let price = price * exp (driftExp + randExp * dist.Sample()) 
      let dt = dt.AddSeconds(1.0)
      Some((dt, price), (dt, price))) |> Seq.take count

  // Generate two "high-frequency" time series (with different volatility)
  let hfq1 = Series.ofObservations (randomPrice 0.05 0.1 20.0 (24*60*60))
  let hfq2 = Series.ofObservations (randomPrice 0.05 0.2 20.0 (24*60*60))

  let intervals = [ for i in 0.0 .. 24.0*60.0 - 1.0 -> DateTimeOffset(DateTime(2013, 1, 1)).AddMinutes(i + 0.001) ]

  let it0 = timed (fun () -> hfq1 |> Series.lookupAll intervals Lookup.NearestGreater)
  //let it1 = timed (fun () -> hfq1 |> Series.lookupAll intervals Lookup.NearestGreater)
  //let it2 = timed (fun () -> hfq1 |> Series.lookupAll intervals Lookup.NearestGreater)

  *)
  ()

(*
  // Generate price using geometric Brownian motion
  let randomPrice drift volatility initial count = 
    let dist = Normal(0.0, 1.0, RandomSource=Random(0))  
    let dt = 1.0 / 250.0
    let driftExp = (drift - 0.5 * pown volatility 2) * dt
    let randExp = volatility * (sqrt dt)
    (DateTimeOffset(DateTime(2013, 1, 1)), initial) |> Seq.unfold (fun (dt, price) ->
      let price = price * exp (driftExp + randExp * dist.Sample()) 
      let dt = dt.AddSeconds(1.0)
      Some((dt, price), (dt, price))) |> Seq.take count

  let hfq1 = Series.ofObservations (randomPrice 0.05 0.5 5.0 100)
  
  let intervals = [ for i in 0.0 .. 10.0 .. 10000.0 -> DateTimeOffset(DateTime(2013, 1, 1)).AddSeconds(i) ]
  let logs1 = hfq1 |> Series.lookupAll intervals Lookup.NearestGreater |> log
  let diffs = logs1 |> Series.pairwiseWith (fun _ (v1, v2) -> v2 - v1)

  let it0 = timed (fun () -> diffs |> Series.chunk (TimeSpan(1, 0, 0)) )
  let it1 = timed (fun () -> diffs |> Series.chunk (TimeSpan(1, 0, 0)) )
*)

  
  ()
