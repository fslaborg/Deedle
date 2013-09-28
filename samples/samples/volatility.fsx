(**

Volatility
==========

*)
#I "../../bin"
#I "../../packages/FSharp.Charting.0.84"
#r "MathNet.Numerics.dll"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"

open System
open FSharp.DataFrame
open FSharp.Charting
open MathNet.Numerics.Distributions

// Generate price using geometric Brownian motion
let randomPrice drift volatility initial count span = 
  let dist = Normal(0.0, 1.0, RandomSource=Random(0))  
  let dt = 1.0 / (250.0 * 24.0 * 60.0)
  let driftExp = (drift - 0.5 * pown volatility 2) * dt
  let randExp = volatility * (sqrt dt)
  (DateTimeOffset(DateTime(2013, 1, 1)), initial) |> Seq.unfold (fun (dt, price) ->
    let price = price * exp (driftExp + randExp * dist.Sample()) 
    let dt = dt + span
    Some((dt, price), (dt, price))) |> Seq.take count

// Generate two "high-frequency" time series (with different volatility)
let hfq1 = Series.ofObservations (randomPrice 0.05 0.1 20.0 (24*60*60) (TimeSpan(0, 0, 1)))
let hfq2 = Series.ofObservations (randomPrice 0.05 0.2 20.0 (24*60*60) (TimeSpan(0, 0, 1)))

// Chart them using F# Chart to see what they look like
Chart.Combine(
  [ Chart.FastLine(hfq1 |> Series.observations)
    Chart.FastLine(hfq2 |> Series.observations) ]).WithYAxis(Min=17.0, Max=22.0)
  
// Calculate the means of the two series (and see that they are the same)
hfq1 |> Series.mean
hfq1 |> Series.mean

// hfq1 + hfq2 

// Get all day data in 1 minute intervals
let intervals = [ for i in 0.0 .. 24.0*60.0 - 1.0 -> DateTimeOffset(DateTime(2013, 1, 1)).AddMinutes(i + 0.001) ]

// Get nearest smaller values for the specified interval & calculate logs
// Then take difference between previous and the next log value
let logs1 = hfq1 |> Series.lookupAll intervals Lookup.NearestGreater |> log
let diffs = logs1 |> Series.pairwiseWith (fun _ (v1, v2) -> v2 - v1)

Chart.Rows 
  [ Chart.Line(logs1 |> Series.observations);
    Chart.Line(diffs |> Series.observations) ]

// Get 1 hour chunks and build a data frame with one column
// for each hour (containing 60 rows for 60 minutes in the hour)
let chunkedLogs =
  diffs 
  |> Series.chunkDist (TimeSpan(1, 0, 0))
  |> Series.map (fun _ -> Series.withOrdinalIndex)
  |> Frame.ofColumns


// Means and standard deviations for each hour
let sdvs = chunkedLogs |> Frame.sdv
let means = chunkedLogs |> Frame.mean

// Display a chart that shows means and sdvs per hour of day
Chart.Rows
  [ Chart.Column(sdvs.Observations)
    Chart.Column(means.Observations) ]



// TODO: Not used

let hourly = Series.ofObservations (randomPrice 0.05 0.1 20.0 (24*365) (TimeSpan(1, 0, 0)))

let timeOfDay = TimeSpan(13, 30, 0)
let chunks = hourly |> Series.chunkWhile (fun d1 d2 -> d1.Date = d2.Date) 
chunks |> Series.map (fun day daily -> 
  daily |> Series.lookup (DateTimeOffset(day.Date + timeOfDay)) Lookup.NearestGreater)


hourly


let short = Series.ofObservations (randomPrice 0.5 10.0 20.0 10 (TimeSpan(1, 0, 0)))

short
short |> Series.shift 2
short |> Series.shift -2

let df =
  Frame.ofColumns 
    [ "Orig" => short
      "ShiftTwo" => (short |> Series.shift 2) ]

df |> Frame.shiftRows 1

df.ro

// Sampling

let dt = DateTime(2012, 2, 12)
let ts = TimeSpan.FromMinutes(5.37)
let inp = Seq.init 50 (fun i -> dt.Add(TimeSpan(ts.Ticks * int64 i)), i) |> Series.ofObservations

inp.Aggregate(WindowSize(10, Boundary.AtEnding), (fun r -> r), (fun ds -> ds.Data.KeyRange |> fst))

inp |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Forward Series.firstValue
inp |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Backward Series.lastValue

let ts2 = TimeSpan.FromHours(5.37)
let inp2 = Seq.init 20 (fun i -> dt.Add(TimeSpan(ts2.Ticks * int64 i)), i) |> Series.ofObservations
inp2 |> Series.sample [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Forward
inp2 |> Series.sample [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Backward

inp2 |> Series.sampleTimeInto (TimeSpan(24,0,0)) Direction.Forward Series.firstValue

let ts3 = TimeSpan.FromHours(48.0)
let inp3 = Seq.init 5 (fun i -> dt.Add(TimeSpan(ts3.Ticks * int64 i)), i) |> Series.ofObservations

let keys = [ for d in 12 .. 20 -> DateTime(2012, 2, d) ]
inp3 |> Series.sample keys Direction.Forward

(**
Fancy windowing & chunking
--------------------------
*)

let st = Series.ofValues [ 'a' .. 'j' ]
st |> Series.windowSize (3, Boundary.Skip) |> Series.map (fun _ v -> String(Array.ofSeq v.Values))
st |> Series.windowSize (3, Boundary.AtBeginning) |> Series.map (fun _ v -> String(Array.ofSeq v.Values))
st |> Series.windowSize (3, Boundary.AtEnding) |> Series.map (fun _ v -> String(Array.ofSeq v.Values))

let concatString = function
  | DataSegment.Complete(ser) -> String(ser |> Series.values |> Array.ofSeq)
  | DataSegment.Incomplete(ser) -> String(ser |> Series.values |> Array.ofSeq).PadRight(3, '-')

st |> Series.chunkSizeInto (3, Boundary.Skip) concatString
st |> Series.chunkSizeInto (3, Boundary.AtBeginning) concatString
st |> Series.chunkSizeInto (3, Boundary.AtEnding) concatString
