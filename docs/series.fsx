(**
---
title: Series and time series features
category: Guides
categoryindex: 1
index: 4
description: Ordered series, time-series alignment, windowing, chunking, resampling, and arithmetic
keywords: series, time series, sampling, windowing, resampling, F#
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.MathNetNumerics.dll"
#r "nuget: MathNet.Numerics, 5.0.0"
#r "nuget: MathNet.Numerics.FSharp, 5.0.0"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"
#endif // FSX
(*** condition: prepare ***)

open System
open Deedle
open MathNet.Numerics.Distributions

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)

(**

# Series and time series features

This page covers features for working with ordered series and time series —
alignment, windowing, chunking, resampling, and arithmetic operators.
For basic series and frame operations, see the [quick start](tutorial.html).
For frame-specific features, see [data frames](frame.html).

## Sample data

We generate random stock-like prices to demonstrate time series operations.
The function below uses geometric Brownian motion:
*)

/// Generate random prices with geometric Brownian motion
let randomPrice seed drift volatility initial (start: DateTimeOffset) (span: TimeSpan) count = 
  let dist = Normal(0.0, 1.0, RandomSource=Random(seed))  
  let dt = span.TotalDays / 250.0
  let driftExp = (drift - 0.5 * pown volatility 2) * dt
  let randExp = volatility * (sqrt dt)
  (start, initial) |> Seq.unfold (fun (dt, price) ->
    let price = price * exp (driftExp + randExp * dist.Sample()) 
    Some((dt, price), (dt + span, price))) |> Seq.take count

let today = DateTimeOffset(DateTime.Today)
let stock1 = randomPrice 1 0.1 3.0 20.0 today 
let stock2 = randomPrice 2 0.2 1.5 22.0 today

(**
Call `stock1` or `stock2` with a `TimeSpan` and count to get prices
at different intervals.

## Alignment and zipping

A key time series feature is _automatic alignment_ — combining series
with different keys, matching by key or nearest available value.
*)

// Hourly, half-hourly, and 65-minute series
let s1 = stock1 (TimeSpan(1, 0, 0)) 6 |> series
let s2 = stock2 (TimeSpan(0, 30, 0)) 12 |> series
let s3 = stock1 (TimeSpan(1, 5, 0)) 6 |> series

(**
### Zipping series

`Zip` combines two series into a series of pairs, with configurable alignment:
*)
// Match values from right series to keys of the left one
s1.Zip(s2, JoinKind.Left)

// Match values from the left series to keys of the right one
s1.Zip(s2, JoinKind.Right)

// Use left series key and find the nearest previous value from the right series
s1.Zip(s2, JoinKind.Left, Lookup.ExactOrSmaller)

(**
### Joining data frames

The same alignment works at frame level via `Join`:
*)

// Contains value for each hour
let f1 = Frame.ofColumns ["S1" => s1]
// Contains value every 30 minutes
let f2 = Frame.ofColumns ["S2" => s2]
// Contains values with 65 minute offsets
let f3 = Frame.ofColumns ["S3" => s3]

// Union keys from both frames and align corresponding values
f1.Join(f2, JoinKind.Outer)

// Take only keys where both frames contain all values
f2.Join(f3, JoinKind.Inner)

// Take keys from the left frame and find nearest smaller value from the right frame
f2.Join(f3, JoinKind.Left, Lookup.ExactOrSmaller)

// Function syntax equivalents
Frame.join JoinKind.Outer f1 f2
Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller f1 f2

(**
For more on joins, see [Joining and merging](joining.html).

## Windowing, chunking, and pairwise

These operations aggregate _consecutive_ elements — unlike grouping, they
rely on ordering.

### Sliding windows
*)

// Create input series with 6 observations
let lf = stock1 (TimeSpan(0, 1, 0)) 6 |> series

// Sliding windows of size 4
lf |> Series.window 4
// Aggregate each window
lf |> Series.windowInto 4 Stats.mean
// First value of each window
lf |> Series.windowInto 4 Series.firstValue

(**
Given input `[1,2,3,4,5,6]`, windows of size 4 produce:
`[1,2,3,4]`, `[2,3,4,5]`, `[3,4,5,6]`.

> **Performance tip:** For rolling statistics, prefer the dedicated `Stats.moving*`
> functions which use O(n) online algorithms instead of materialising each window:
>
>     lf |> Stats.movingMean 4     // fast
>     lf |> Series.windowInto 4 Stats.mean  // slow
>
> See [Statistics](stats.html) for the full list.

Incomplete windows at the boundary avoid missing values:
*)
let lfm2 = 
  // Create sliding windows with incomplete windows at the beginning
  lf |> Series.windowSizeInto (4, Boundary.AtBeginning) (fun ds ->
    Stats.mean ds.Data)

Frame.ofColumns [ "Orig" => lf; "Means" => lfm2 ]

(**
The `DataSegment<T>` type tells you whether a window is `Complete` or `Incomplete`:
*)

// Simple series with characters
let st = Series.ofValues [ 'a' .. 'e' ]
st |> Series.windowSizeInto (3, Boundary.AtEnding) (function
  | DataSegment.Complete(ser) -> 
      // Return complete windows as uppercase strings
      String(ser |> Series.values |> Array.ofSeq).ToUpper()
  | DataSegment.Incomplete(ser) -> 
      // Return incomplete windows as padded lowercase strings
      String(ser |> Series.values |> Array.ofSeq).PadRight(3, '-') )  

(**
### Window size conditions

Windows can also end based on key distance or a predicate:
*)
// Generate prices for each hour over 30 days
let hourly = stock1 (TimeSpan(1, 0, 0)) (30*24) |> series

// Generate windows of size 1 day
hourly |> Series.windowDist (TimeSpan(24, 0, 0))

// Generate windows such that date in each window is the same
hourly |> Series.windowWhile (fun d1 d2 -> d1.Date = d2.Date)

(**
### Chunking

Chunking creates _non-overlapping_ groups (unlike overlapping windows):
*)

// Generate per-second observations over 10 minutes
let hf = stock1 (TimeSpan(0, 0, 1)) 600 |> series

// Create 10 second chunks with (possible) incomplete chunk at the end
hf |> Series.chunkSize (10, Boundary.AtEnding) 

// Create 10 second chunks and get the first observation for each (downsample)
hf |> Series.chunkDistInto (TimeSpan(0, 0, 10)) Series.firstValue

// Create chunks where hh:mm component is the same
hf |> Series.chunkWhile (fun k1 k2 -> 
  (k1.Hour, k1.Minute) = (k2.Hour, k2.Minute))

(**
### Pairwise

Build pairs of consecutive values — useful for computing returns or differences:
*)

// Create a series of pairs from earlier 'hf' input
hf |> Series.pairwise 

// Calculate differences between the current and previous values
hf |> Series.pairwiseWith (fun k (v1, v2) -> v2 - v1)

(**

## Sampling and resampling

### Lookup

`Series.lookupAll` retrieves values for many keys at once with flexible matching:
*)
// Generate a bit less than 24 hours of data with 13.7sec offsets
let mf = stock1 (TimeSpan.FromSeconds(13.7)) 6300 |> series
// Generate keys for all minutes in 24 hours
let keys = [ for m in 0.0 .. 24.0*60.0-1.0 -> today.AddMinutes(m) ]

// Find value for a given key, or nearest greater key with value
mf |> Series.lookupAll keys Lookup.ExactOrGreater

// Find value for nearest smaller key
mf |> Series.lookupAll keys Lookup.ExactOrSmaller

(**
### Resampling

Resample by collecting values between specified keys:
*)

// For each key, collect values for greater keys until the next one
mf |> Series.resample keys Direction.Forward

// Aggregate each chunk of preceding values using mean
mf |> Series.resampleInto keys Direction.Backward 
  (fun k s -> Stats.mean s)

(**
Resample by projecting existing keys (e.g. group by date):
*)

// Generate 2.5 months of data in 1.7 hour offsets
let ds = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample by day
ds |> Series.resampleEquiv (fun d -> d.Date)
ds.ResampleEquivalence(fun d -> d.Date)

(**
### Uniform resampling

Assign values to every key in a range, filling gaps for days with no observations:
*)

// Create input data with non-uniformly distributed keys
let days =
  [ "10/3/2013 12:00:00"; "10/4/2013 15:00:00" 
    "10/4/2013 18:00:00"; "10/4/2013 19:00:00"
    "10/6/2013 15:00:00"; "10/6/2013 21:00:00" ]
let nu = 
  stock1 (TimeSpan(24,0,0)) 10 |> series
  |> Series.indexWith days |> Series.mapKeys DateTimeOffset.Parse

// Generate uniform resampling based on dates, fill missing with nearest smaller
let sampled =
  nu |> Series.resampleUniform Lookup.ExactOrSmaller 
    (fun dt -> dt.Date) (fun dt -> dt.AddDays(1.0))

// Turn into frame with multiple columns for each day
sampled 
|> Series.mapValues Series.indexOrdinally
|> Frame.ofRows

(**
### Sampling at fixed intervals
*)
// Generate 1k observations with 1.7 hour offsets
let pr = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample at 2 hour intervals; 'Backward' specifies that we collect all previous values
pr |> Series.sampleTime (TimeSpan(2, 0, 0)) Direction.Backward

// Get the most recent value, sampled at 2 hour intervals
pr |> Series.sampleTimeInto
  (TimeSpan(2, 0, 0)) Direction.Backward Series.lastValue

(**

## Arithmetic and statistics

### Shifting and differences
*)
// Generate sample data with 1.7 hour offsets
let sample = stock1 (TimeSpan.FromHours(1.7)) 6 |> series

// Calculates: new[i] = s[i] - s[i-1]
let diff1 = sample |> Series.diff 1

// Shift series values by 1
let shift1 = sample |> Series.shift 1

// Align all results in a frame to see the results
let alignedDf = 
  [ "Shift +1" => shift1 
    "Diff +1" => diff1 
    "Diff" => sample - shift1 
    "Orig" => sample ] |> Frame.ofColumns 

(**
### Operators and functions

Series supports standard F# math functions and binary operators.
Binary operators auto-align two series by key before applying:
*)

// Subtract previous value from the current value
sample - sample.Shift(1)
// Calculate logarithm of such differences
log (sample - sample.Shift(1))
// Calculate square of differences
sample.Diff(1) ** 2.0
// Get absolute value of differences
abs (sample - sample.Shift(1))
// Get absolute value of distance from the mean
abs (sample - (Stats.mean sample))

// Apply a custom function to all elements
let adjust v = min 1.0 (max -1.0 v)
adjust $ sample.Diff(1)

(**
### Frame-level operations

Many time-series operations apply to entire frames:
*)
// Multiply all numeric columns by a given constant
alignedDf * 0.65

// Sum each column and divide results by a constant
Stats.sum alignedDf / 6.0
// Divide sum by mean of each frame column
Stats.sum alignedDf / Stats.mean alignedDf
