(**

Volatility
==========

*)
#I "../bin"
#I "../lib"
#I "../packages"
#r "MathNet.Numerics.dll"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"

open System
open FSharp.DataFrame
open FSharp.Charting
open MathNet.Numerics.Distributions

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
let hfq1 = Series.ofValues (randomPrice 0.05 0.1 20.0 (24*60*60))
let hfq2 = Series.ofValues (randomPrice 0.05 0.2 20.0 (24*60*60))

// Chart them using F# Chart to see what they look like
Chart.Combine(
  [ Chart.FastLine(hfq1.Observations)
    Chart.FastLine(hfq2.Observations) ]).WithYAxis(Min=17.0, Max=22.0)
  
// Calculate the means of the two series (and see that they are the same)
hfq1 |> Series.mean
hfq1 |> Series.mean

// Get all day data in 1 minute intervals
let intervals = [ for i in 0.0 .. 24.0*60.0 - 1.0 -> DateTimeOffset(DateTime(2013, 1, 1)).AddMinutes(i) ]

// Get nearest smaller values for the specified interval & calculate logs
// Then take difference between previous and the next log value
let logs1 = hfq1 |> Series.lookupAll intervals LookupSemantics.NearestGreater |> log
let diffs = logs1 |> Series.pairwiseWith (fun _ (v1, v2) -> v2 - v1)

Chart.Rows 
  [ Chart.Line(logs1.Observations);
    Chart.Line(diffs.Observations) ]

// Get 1 hour chunks and build a data frame with one column
// for each hour (containing 60 rows for 60 minutes in the hour)
let chunkedLogs =
  diffs 
  |> Series.chunk (TimeSpan(1, 0, 0))
  |> Series.map (fun _ -> Series.withOrdinalIndex)
  |> Frame.ofColumns


// Means and standard deviations for each hour
let sdvs = chunkedLogs |> Frame.sdv
let means = chunkedLogs |> Frame.mean

// Display a chart that shows means and sdvs per hour of day
Chart.Rows
  [ Chart.Column(sdvs.Observations)
    Chart.Column(means.Observations) ]
