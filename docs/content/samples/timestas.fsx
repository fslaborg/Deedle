// ----------------------------------------------------------------------------
// Load everything 
// ----------------------------------------------------------------------------
#load "../../../bin/net45/Deedle.fsx"
#load "../../../packages/FSharp.Charting/lib/net45/FSharp.Charting.fsx"
#r "../../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
#r "../../../packages/MathNet.Numerics/lib/net40/MathNet.Numerics.dll"
#load "FSharp.Charting.fsx"
#load "Deedle.fsx"





open System
open Deedle
open FSharp.Charting
open MathNet.Numerics.Distributions

// ----------------------------------------------------------------------------
// Generate stock prices using geometric Brownian motion
// ----------------------------------------------------------------------------

/// Generates price using geometric Brownian motion
///
/// ## Parameters
///  - 'seed' specifies the seed for random number generator
///  - 'drift' and 'volatility' set properties of the price movement
///  - 'initial' and 'start' specify the initial price and date
///  - 'span' specifies time span between individual observations
///  - 'count' is the number of required values to generate
let randomPrice seed drift volatility initial start span count = 
  (*[omit:(Implementation omitted)]*) 
  let dist = Normal(0.0, 1.0, RandomSource=Random(seed))  
  let dt = (span:TimeSpan).TotalDays / 250.0
  let driftExp = (drift - 0.5 * pown volatility 2) * dt
  let randExp = volatility * (sqrt dt)
  ((start:DateTimeOffset), initial) |> Seq.unfold (fun (dt, price) ->
    let price = price * exp (driftExp + randExp * dist.Sample()) 
    Some((dt, price), (dt + span, price))) |> Seq.take count(*[/omit]*)


// ----------------------------------------------------------------------------
// Sample inputs
// ----------------------------------------------------------------------------

// Two series with 1 sec data for entire day
let today = DateTimeOffset(DateTime.Today)
let sec = TimeSpan(0, 0, 1)
let hfq1 = randomPrice 1 0.1 3.0 20.0 today sec (24*60*60) |> series
let hfq2 = randomPrice 1 0.1 1.5 20.0 today sec (24*60*60) |> series

// Visualize
Chart.Combine
  [ hfq1 |> Series.observations |> Chart.FastLine
    hfq2 |> Series.observations |> Chart.FastLine ]

// ----------------------------------------------------------------------------
// Sampling and statistics
// ----------------------------------------------------------------------------

// Calculate the means of the two series (and see that they are the same)
hfq1 |> Stats.mean
hfq1 |> Stats.mean

// Get all day data in 1 minute intervals
let intervals = 
  [ for i in 0.0 .. 24.0*60.0 - 1.0 -> today.AddMinutes(i) ]

// Get values for the specified interval & calculate logs
// Then take difference between previous and the next log value
let logs1 = hfq1 |> Series.lookupAll intervals Lookup.Exact |> log
let diffs = logs1 |> Series.pairwiseWith (fun _ (v1, v2) -> v2 - v1)

// Show the logs together with the differences in the logs
Chart.Rows 
  [ Chart.Line(logs1 |> Series.observations);
    Chart.Line(diffs |> Series.observations) ]

// Get 1 hour chunks and build a data frame with one column
// for each hour (containing 60 rows for 60 minutes in the hour)
let chunkedLogs =
  diffs 
  |> Series.sampleTime (TimeSpan(1, 0, 0)) Direction.Forward 
  |> Series.mapValues Series.indexOrdinally
  |> Frame.ofColumns

// Means and standard deviations for each hour
let sdvs = chunkedLogs |> Stats.stdDev
let means = chunkedLogs |> Stats.mean

// Display a chart that shows means and sdvs per hour of day
Chart.Rows
  [ Chart.Column(sdvs |> Series.observations)
    Chart.Column(means |> Series.observations) ]
