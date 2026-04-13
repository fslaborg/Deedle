(**
---
title: Working with series and time series data in F#
category: Guides
categoryindex: 1
index: 4
description: Creating and manipulating ordered and unordered series, time-series sampling, and windowing
keywords: series, time series, sampling, windowing, resampling, F#
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Math.dll"
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
    fmt.Invoke(o, [||]) :?> string
  else null)

(**

# Working with series and time series

In this section, we look at F# data frame library features that are useful when working
with time series data or, more generally, any ordered series. Although we mainly look at
operations on the `Series` type, many of the operations can be applied to data frame `Frame`
containing multiple series. Furthermore, data frame provides an elegant way for aligning and
joining series. 

## Generating input data

For the purpose of this tutorial, we'll need some input data. We use a function which generates
random prices using the geometric Brownian motion.
*)

/// Generates price using geometric Brownian motion
///  - 'seed' specifies the seed for random number generator
///  - 'drift' and 'volatility' set properties of the price movement
///  - 'initial' and 'start' specify the initial price and date
///  - 'span' specifies time span between individual observations
///  - 'count' is the number of required values to generate
let randomPrice seed drift volatility initial start span count = 
  let dist = Normal(0.0, 1.0, RandomSource=Random(seed))  
  let dt = (span:TimeSpan).TotalDays / 250.0
  let driftExp = (drift - 0.5 * pown volatility 2) * dt
  let randExp = volatility * (sqrt dt)
  ((start:DateTimeOffset), initial) |> Seq.unfold (fun (dt, price) ->
    let price = price * exp (driftExp + randExp * dist.Sample()) 
    Some((dt, price), (dt + span, price))) |> Seq.take count

// 12:00 AM today, in current time zone
let today = DateTimeOffset(DateTime.Today)
let stock1 = randomPrice 1 0.1 3.0 20.0 today 
let stock2 = randomPrice 2 0.2 1.5 22.0 today

(**
To get random prices, we now only need to call `stock1` or `stock2` with `TimeSpan` and 
the required number of prices.

<a name="alignment"></a>

## Data alignment and zipping

One of the key features of the data frame library for working with time series data is 
_automatic alignment_ based on the keys. When we have multiple time series with date 
as the key (here, we use `DateTimeOffset`), we can combine multiple series and align 
them automatically to specified date keys.

To demonstrate this feature, we generate random prices in 60 minute, 30 minute and 
65 minute intervals:
*)

let s1 = stock1 (TimeSpan(1, 0, 0)) 6 |> series
let s2 = stock2 (TimeSpan(0, 30, 0)) 12 |> series
let s3 = stock1 (TimeSpan(1, 5, 0)) 6 |> series

(**
### Zipping time series 

A series exposes `Zip` operation that can combine multiple series into a single series of pairs.
*)
// Match values from right series to keys of the left one
s1.Zip(s2, JoinKind.Left)

// Match values from the left series to keys of the right one
s1.Zip(s2, JoinKind.Right)

// Use left series key and find the nearest previous value from the right series
s1.Zip(s2, JoinKind.Left, Lookup.ExactOrSmaller)

(**
### Joining data frames

When we store data in data frames, we can simply use a data frame with multiple columns
instead of series of tuples. Let's first create three data frames:
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

// Equivalent using function syntax 
Frame.join JoinKind.Outer f1 f2
Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller f1 f2

(**
<a name="windowing"></a>

## Windowing, chunking and pairwise

Windowing and chunking are two operations on ordered series that allow aggregating
the values of series into groups. Both of these operations work on consecutive elements,
which contrasts with [grouping](tutorial.html#grouping) that does not use order.

### Sliding windows
*)

// Create input series with 6 observations
let lf = stock1 (TimeSpan(0, 1, 0)) 6 |> series

// Create series of series representing individual windows
lf |> Series.window 4
// Aggregate each window using 'Stats.mean'
lf |> Series.windowInto 4 Stats.mean
// Get first value in each window
lf |> Series.windowInto 4 Series.firstValue

(**
The functions above create windows of size 4 that move from the left to right.
Given input `[1,2,3,4,5,6]`, this produces the following three windows:
`[1,2,3,4]`, `[2,3,4,5]` and `[3,4,5,6]`. 

> **Performance note:** `Series.windowInto` materialises each window as a full series
> before calling the aggregation function. This gives O(n × window) time and allocation.
> When computing rolling statistics (mean, standard deviation, variance, etc.), prefer the
> dedicated `Stats.moving*` functions (e.g. `Stats.movingMean`, `Stats.movingStd`), which
> use an online algorithm and run in O(n) time:
>
>     // Fast – O(n) online algorithm
>     lf |> Stats.movingMean 4
>
>     // Slow – O(n × window), allocates a series per step
>     lf |> Series.windowInto 4 Stats.mean
>
> See the [Statistics documentation](stats.html#moving) for the full list of `Stats.moving*`
> and `Stats.expanding*` functions.

What if we want to avoid creating `<missing>` values? One approach is to 
specify that we want to generate windows of smaller sizes at the beginning 
or at the end. This way, we get _incomplete_ windows at the boundary:
*)
let lfm2 = 
  // Create sliding windows with incomplete windows at the beginning
  lf |> Series.windowSizeInto (4, Boundary.AtBeginning) (fun ds ->
    Stats.mean ds.Data)

Frame.ofColumns [ "Orig" => lf; "Means" => lfm2 ]

(**
In the previous sample, the code that performs aggregation is a lambda that takes `ds`,
which is of type `DataSegment<T>`. This type informs us whether the window is complete or not:
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

There are two other options for specifying when a window ends. 

 - Specify the maximal _distance_ between the first and the last key
 - Specify a function that is called with the first and the last key; a window ends when it returns false.
*)
// Generate prices for each hour over 30 days
let hourly = stock1 (TimeSpan(1, 0, 0)) (30*24) |> series

// Generate windows of size 1 day
hourly |> Series.windowDist (TimeSpan(24, 0, 0))

// Generate windows such that date in each window is the same
hourly |> Series.windowWhile (fun d1 d2 -> d1.Date = d2.Date)

(**
### Chunking series

Chunking is similar to windowing, but it creates non-overlapping chunks, 
rather than (overlapping) sliding windows:
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

A special form of windowing is building a series of pairs containing a current
and previous value from the input series:
*)

// Create a series of pairs from earlier 'hf' input
hf |> Series.pairwise 

// Calculate differences between the current and previous values
hf |> Series.pairwiseWith (fun k (v1, v2) -> v2 - v1)

(**
<a name="sampling"></a>

## Sampling and resampling time series

### Lookup

Given a series `hf`, you can get a value at a specified key using `hf.Get(key)`.
It is also possible to find values for larger number of keys at once using 
`Series.lookupAll` when you want more flexible lookup:
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
*)

// For each key, collect values for greater keys until the next one
mf |> Series.resample keys Direction.Forward

// Aggregate each chunk of preceding values using mean
mf |> Series.resampleInto keys Direction.Backward 
  (fun k s -> Stats.mean s)

(**
The second kind of resampling is based on a projection from existing keys in 
the series. The typical scenario is when you have time series with date time information
and want to get information for each day:
*)

// Generate 2.5 months of data in 1.7 hour offsets
let ds = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample by day
ds |> Series.resampleEquiv (fun d -> d.Date)
ds.ResampleEquivalence(fun d -> d.Date)

(**
### Uniform resampling

If you want to create sampling that assigns value to each key in the range specified
by the input sequence (including days with no observations), then you can use 
_uniform resampling_:
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
### Sampling time series
*)
// Generate 1k observations with 1.7 hour offsets
let pr = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample at 2 hour intervals; 'Backward' specifies that we collect all previous values
pr |> Series.sampleTime (TimeSpan(2, 0, 0)) Direction.Backward

// Get the most recent value, sampled at 2 hour intervals
pr |> Series.sampleTimeInto
  (TimeSpan(2, 0, 0)) Direction.Backward Series.lastValue

(**
<a name="stats"></a>

## Calculations and statistics

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

Time series supports a large number of standard F# functions such as `log` and `abs`.
You can also use standard numerical operators to apply some operation to all elements
of the series, and binary operators automatically align two series before applying:
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
### Data frame operations

Many of the time series operations can be applied to entire data frames as well:
*)
// Multiply all numeric columns by a given constant
alignedDf * 0.65

// Sum each column and divide results by a constant
Stats.sum alignedDf / 6.0
// Divide sum by mean of each frame column
Stats.sum alignedDf / Stats.mean alignedDf
