(*** hide ***)
#I "../bin"
#load "FSharp.DataFrame.fsx"
#load "../packages/FSharp.Charting.0.84/FSharp.Charting.fsx"
#r "../packages/FSharp.Data.1.1.9/lib/net40/FSharp.Data.dll"
open System
open FSharp.Data
open FSharp.DataFrame
open FSharp.Charting

let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Time series features
====================

In this section, we look at F# data frame library features that are useful when working
with time series data or, more generally, any ordered series. Although we mainly look at
operations on the `Series` type, many of the operations can be applied to data frame `Frame`
containing multiple series. Furthermore, data frame provides an elegant way for aligning and
joining series. 

You can also get this page as an [F# script file](https://github.com/BlueMountainCapital/FSharp.DataFrame/blob/master/samples/timeseries.fsx)
from GitHub and run the samples interactively.

Generating input data
---------------------

For the purpose of this tutorial, we'll need some input data. For simplicitly, we use the
following function which generates random prices using the geometric Brownian motion.
The code is adapted from the [financial tutorial on Try F#](http://www.tryfsharp.org/Learn/financial-computing#simulating-and-analyzing).

*)

// Use Math.NET for probability distributions
#r "MathNet.Numerics.dll"
open MathNet.Numerics.Distributions

/// Generates price using geometric Brownian motion
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

// 12:00 AM today, in current time zone
let today = DateTimeOffset(DateTime.Today)
let stock1 = randomPrice 1 0.1 3.0 20.0 today 
let stock2 = randomPrice 2 0.2 1.5 22.0 today
(**
The implementation of the function is not particularly important for the purpose of this
page, but you can find it in the [script file with full source](https://github.com/BlueMountainCapital/FSharp.DataFrame/blob/master/samples/timeseries.fsx).
Once we have the function, we define a date `today` (representing today's midnight) and
two helper functions that set basic properties for the `randomPrice` function. 

To get random prices, we now only need to call `stock1` or `stock2` with `TimeSpan` and 
the required number of prices:
*)
Chart.Combine
  [ stock1 (TimeSpan(0, 1, 0)) 1000 |> Chart.FastLine
    stock2 (TimeSpan(0, 1, 0)) 1000 |> Chart.FastLine ]
(**
The above snippet generates 1k of prices in one minutte intervals and plots them using the
[F# Charting library](https://github.com/fsharp/FSharp.Charting). When you run the code
and tweak the chart look, you should see something like this:

<div style="text-align:center;margin-right:100px;">
<img src="content/images/ts-chart.png" />
</div>

<a name="alignment"></a>
Data alignment and joining
--------------------------

One of the key features of the data frame library for working with time series data is 
_automatic alignment_ based on the keys. When we have multiple time series with date 
as the key (here, we use `DateTimeOffset`, but any type of date will do), we can combine
multiple series and align them automatically to specified date keys.

To demonstrate this feature, we generate random prices in 60 minut, 30 minute and 
65 minute intervals:
*)

let s1 = series <| stock1 (TimeSpan(1, 0, 0)) 6
// [fsi:val s1 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 20.76; 1:00:00 AM => 21.11; 2:00:00 AM => 22.51 ]
// [fsi:            3:00:00 AM => 23.88; 4:00:00 AM => 23.23; 5:00:00 AM => 22.68 ] ]

let s2 = series <| stock2 (TimeSpan(0, 30, 0)) 12
// [fsi:val s2 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 21.61; 12:30:00 AM => 21.64; 1:00:00 AM => 21.86 ]
// [fsi:            1:30:00 AM => 22.22;  2:00:00 AM => 22.35; 2:30:00 AM => 22.76 ]
// [fsi:            3:00:00 AM => 22.68;  3:30:00 AM => 22.64; 4:00:00 AM => 22.90 ]
// [fsi:            4:30:00 AM => 23.40;  5:00:00 AM => 23.33; 5:30:00 AM => 23.43] ]

let s3 = series <| stock1 (TimeSpan(1, 5, 0)) 6
// [fsi:val s3 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 21.37; 1:05:00 AM => 22.73; 2:10:00 AM => 22.08 ]
// [fsi:            3:15:00 AM => 23.92; 4:20:00 AM => 22.72; 5:25:00 AM => 22.79 ]

(**
### Joining time series 

Let's first look at operations that are available on the `Series<K, V>` type. A series
exposes `Join` operation that can combine multiple series into a single series of pairs.
This is not as convenient as working with data frames (which we'll see later), but it 
is useful if you only need to work with one or two columns without missing values:

*)
// Match values from right series to keys of the left one
// (this creates series with no missing values)
s1.Join(s2, JoinKind.Left)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -> (21.32, 21.61) ]
// [fsi:   1:00:00 AM -> (22.62, 21.86) ]
// [fsi:   2:00:00 AM -> (22.00, 22.35)  ]
// [fsi:  (...)]

// Match values from the left series to keys of the right one
// (right has higher resolution, so half of left values are missing)
s1.Join(s2, JoinKind.Right)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -> (21.32,     21.61) ]
// [fsi:  12:30:00 AM -> (<missing>, 21.64)  ]      
// [fsi:   1:00:00 AM -> (22.62,     21.86) ]
// [fsi:  (...)]

// Use left series key and find the nearest previous
// (smaller) value from the right series
s1.Join(s2, JoinKind.Left, Lookup.NearestSmaller)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -04:00 -> (21.32, 21.61) ]
// [fsi:   1:00:00 AM -04:00 -> (22.62, 21.86) ]
// [fsi:   2:00:00 AM -04:00 -> (22.00, 22.35)  ]
// [fsi:  (...)]

(**
Using `Join` on series is somewhat complicated. The result is a series of tuples, but each 
component of the tuple may be missing. To represent this, the library uses the `T opt` type
(a type alias for `OptionalValue<T>`). This is not necessary when we use data frame to 
work with multiple columns.

### Joining data frames

When we store data in data frames, we do not need to use tuples to represent combined values.
Instead, we can simply use data frame with multiple columns. To see how this works, let's first
create three data frames containing the three series from the previous section:
*)

// Contains value for each hour
let f1 = Frame.ofColumns ["S1" => s1]
// Contains value every 30 minutes
let f2 = Frame.ofColumns ["S2" => s2]
// Contains values with 65 minute offsets
let f3 = Frame.ofColumns ["S3" => s3]

(**
Similarly to `Series<K, V>`, the type `Frame<R, C>` has an instance method `Join` that can be
used for joining (for unordered) or aligning (for ordered) data. The same operation is also
exposed as `Frame.join` and `Frame.align` functions, but it is usually more convenient to use 
the member syntax in this case:
*)

// Union keys from both frames and align corresponding values
f1.Join(f2, JoinKind.Outer)
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 S1        S2               ]
// [fsi:  12:00:00 AM -> 21.32     21.61 ]
// [fsi:  12:30:00 AM -> <missing> 21.64 ]
// [fsi:   1:00:00 AM -> 22.62     21.86 ]
// [fsi:  (...)]

// Take only keys where both frames contain all values
// (We get only a single row, because 'f3' is off by 5 minutes)
f2.Join(f3, JoinKind.Inner)
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 S2      S3               ]
// [fsi:  12:00:00 AM -> 21.61   21.37 ]

// Take keys from the left frame and find corresponding values
// from the right frame, or value for a nearest smaller date
// ($21.37 is repeated for all values between 12:00 and 1:05)
f2.Join(f3, JoinKind.Left, Lookup.NearestSmaller)
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 S2      S3               ]
// [fsi:  12:00:00 AM -> 21.61   21.37 ]
// [fsi:  12:30:00 AM -> 21.64   21.37 ]
// [fsi:   1:00:00 AM -> 21.86   21.37 ]
// [fsi:   1:30:00 AM -> 22.22   22.73 ]
// [fsi:  (...)]

// If we perform left join as previously, but specify exact 
// matching, then most of the values are missing
f2.Join(f3, JoinKind.Left, Lookup.Exact)
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 S2      S3               ]
// [fsi:  12:00:00 AM -> 21.61   21.37]
// [fsi:  12:30:00 AM -> 21.64   <missing>        ]
// [fsi:   1:00:00 AM -> 21.86   <missing>        ]
// [fsi:  (...)]

// Equivalent to line 2, using function syntax 
Frame.join JoinKind.Outer f1 f2

// Equivalent to line 20, using function syntax
Frame.align JoinKind.Left Lookup.NearestSmaller f1 f2

(**
The automatic alignment is extremely useful when you have multiple data series with different
offsets between individual observations. You can choose your set of keys (dates) and then easily
align other data to match the keys. Another alternative to using `Join` explicitly is to create
a new frame with just keys that you are interested in (using `Frame.ofRowKeys`) and then use
the `AddSeries` member (or the `df?New <- s` syntax) to add series. This will automatically left
join the new series to match the current row keys.

When aligning data, you may or may not want to create data frame with missing values. If your
observations do not happen at exact time, then using `Lookup.NearestSmaller` or `Lookup.NearestGreater`
is a great way to avoid mismatch. 

If you have observations that happen e.g. at two times faster rate (one series is hourly and 
another is half-hourly), then you can create data frame with missing values using `Lookup.Exact` 
(the default value) and then handle missing values explicitly (as [discussed here](features.html#missing)).


<a name="windowing"></a>
Series windowing and chunking
-----------------------------

## Aa
 - ffooo

*)

let x = 42

(**
<a name="sampling"></a>
Sampling and scaling time series
--------------------------------

### Lookup

### Resampling

### Uniform resampling

### Resampling time series

*)

let inp = series <| stock1 (TimeSpan.FromHours(32.0)) 10
inp |> Series.resampleUniform Lookup.NearestSmaller (fun dt -> dt.Date) (fun dt -> dt.AddDays(1.0))


let lo, hi = inp.KeyRange
let ts = TimeSpan.FromDays(1.0)
let start = lo
let dir = Direction.Forward

let resample keys 
let keys = 
//  if dir = Direction.Forward then
    Seq.unfold (fun dt -> Some(dt, dt+ts)) start 
    |> Seq.takeWhile (fun dt -> dt <= hi)
    
    
keys |> Seq.iter (printfn "%O")

Series.mean $ (inp |> Series.sample keys Direction.Forward)
Series.mean $ (inp |> Series.sample keys Direction.Backward)


inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1))] Direction.Forward (fun k s -> s)
inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1))] Direction.Backward (fun k s -> s)

inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1)); DateTimeOffset(DateTime(2013,10,2))] Direction.Forward (fun k s -> s)
inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1)); DateTimeOffset(DateTime(2013,10,2))] Direction.Backward (fun k s -> s)
            
(**
<a name="stats"></a>
Calculations and statistics
---------------------------

*)

(*** hide ***)


// Generate two "high-frequency" time series (with different volatility)
let hfq1 = series (randomPrice 0.05 1.0 20.0 (24*60*60) (DateTimeOffset(DateTime(2013, 1, 1))) (TimeSpan(0, 0, 1)))
let hfq2 = series (randomPrice 0.05 2.0 20.0 (24*60*60) (DateTimeOffset(DateTime(2013, 1, 1))) (TimeSpan(0, 0, 1)))

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

(*
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



(*
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

