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
Windowing, chunking and pairwise
--------------------------------

Windowing and chunking are two operations on ordered series that allow aggregating
the values of series into groups. Both of these operations work on consecutive elements,
which contrast with [grouping](tutorial.html#grouping) that does not use order.

### Sliding windows

Sliding window creates windows of certain size (or certain condition). The window
"slides" over the input series and provides a view on a part of the series. The
key thing is that a single element will typically appear in multiple windows.
*)

// Create input series with 6 observations
let lf = series <| stock1 (TimeSpan(0, 1, 0)) 6

// Create series of series representing individual windows
lf |> Series.window 4
// Aggregate each window using 'Series.mean'
lf |> Series.windowInto 4 Series.mean
// Get first value in each window
lf |> Series.windowInto 4 Series.firstValue

(**
The functions used above create window of size 4 that moves from the left to right.
Given input `[1,2,3,4,5,6]` the this produces the following three windows:
`[1,2,3,4]`, `[2,3,4,5]` and `[3,4,5,6]`. By default, the `Series.window` function 
automatically chooses the key of the last element of the window as the key for 
the whole window (we'll see how to change this soon):

*)
// Calculate means for sliding windows
let lfm1 = lf |> Series.windowInto 4 Series.mean
// Construct dataframe to show aligned results
Frame.ofColumns [ "Orig" => lf; "Means" => lfm1 ]
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 Means      Orig        ]     
// [fsi:  12:00:00 AM -> <missing>  20.16]
// [fsi:  12:01:00 AM -> <missing>  20.32]
// [fsi:  12:02:00 AM -> <missing>  20.25]
// [fsi:  12:03:00 AM -> 20.30      20.45]
// [fsi:  12:04:00 AM -> 20.34      20.32]
// [fsi:  12:05:00 AM -> 20.34      20.33]

(**
What if we want to avoid creating `<missing>` values? One approach is to 
specify that we want to generate windows of smaller sizes at the beginning 
or at the end of the beginning. This way, we get _incomplete_ windows that look as 
`[1]`, `[1,2]`, `[1,2,3]` followed by the three _complete_ windows shown above:
*)
let lfm2 = 
  // Create sliding windows with incomplete windows at the beginning
  lf |> Series.windowSizeInto (4, Boundary.AtBeginning) (fun ds ->
    Series.mean ds.Data)

Frame.ofColumns [ "Orig" => lf; "Means" => lfm2 ]
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 Means  Orig        ]     
// [fsi:  12:00:00 AM -> 20.16  20.16]
// [fsi:  12:01:00 AM -> 20.24  20.32]
// [fsi:  12:02:00 AM -> 20.24  20.25]
// [fsi:  12:03:00 AM -> 20.30  20.45]
// [fsi:  12:04:00 AM -> 20.34  20.32]
// [fsi:  12:05:00 AM -> 20.34  20.33]

(**
As you can see, the values in the first column are equal, because the first
`Mean` value is just the average of singleton series.

When you specify `Boundary.AtBeginning` (this example) or `Boundary.Skip` 
(default value used in the previous example), the function uses the last key
of the window as the key of the aggregated value. When you specify 
`Boundary.AtEnding`, the last key is used, so the values can be nicely 
aligned wiht original values. When you want to specify custom key selector,
you can use a more general function `Series.aggregate`. 

In the previous sample, the code that performs aggregation is no longer
just a simple function like `Series.mean`, but a lambda that takes `ds`,
which is of type `DataSegment<T>`. This type informs us whether the window
is complete or not. For example:
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
// [fsi:val it : Series<int,string> =]
// [fsi:  0 -> ABC ]
// [fsi:  1 -> BCD ]
// [fsi:  2 -> CDE ]
// [fsi:  3 -> de- ]
// [fsi:  4 -> e-- ]

(**
### Window size conditions

The previous examples generated windows of fixed size. However, there are two other
options for specifying when a window ends. 

 - The first option is to specify the maximal
   _distance_ between the first and the last key
 - The second option is to specify a function that is called with the first
   and the last key; a window ends when the function returns false.

The two functions are `Series.windowDist` and `Series.windowWhile` (together
with versions suffixed with `Into` that call a provided function to aggregate
each window):
*)
// Generate prices for each hour over 30 days
let hourly = series <| stock1 (TimeSpan(1, 0, 0)) (30*24)

// Generate windows of size 1 day (if the source was
// irregular, windows would have varying size)
hourly |> Series.windowDist (TimeSpan(24, 0, 0))

// Generate windows such that date in each window is the same
// (windows start every hour and end at the end of the day)
hourly |> Series.windowWhile (fun d1 d2 -> d1.Date = d2.Date)

(**
### Chunking series

Chunking is similar to windowing, but it creates non-overlapping chunks, 
rather than (overlapping) sliding windows. The size of chunk can be specified
in the same three ways as for sliding windows (fixed size, distance on keys
and condition):
*)

// Generate per-second observations over 10 minutes
let hf = series <| stock1 (TimeSpan(0, 0, 1)) 600

// Create 10 second chunks with (possible) incomplete
// chunk of smaller size at the end.
hf |> Series.chunkSize (10, Boundary.AtEnding) 

// Create 10 second chunks using time span and get
// the first observation for each chunk (downsample)
hf |> Series.chunkDistInto (TimeSpan(0, 0, 10)) Series.firstValue

// Create chunks where hh:mm component is the same
// (containing observations for all seconds in the minute)
hf |> Series.chunkWhile (fun k1 k2 -> 
  (k1.Hour, k1.Minute) = (k2.Hour, k2.Minute))

(**
The above examples use various chunking functions in a very similar way, mainly
because the randomly generated input is very uniform. However, they all behave
differently for inputs with non-uniform keys. 

Using `chunkSize` means that the chunks have the same size, but may correspond
to time series of different time spans. Using `chunkDist` guarantees that there
is a maximal time span over each chunk, but it does not guarantee when a chunk
starts. That is something which can be achieved using `chunkWhile`.

Finally, all of the aggregations discussed so far are just special cases of
`Series.aggregate` which takes a discriminated union that specifies the kind
of aggregation ([see API reference](reference/fsharp-dataframe-aggregation-1.html)).
However, in practice it is more convenient to use the helpers presented here -
in some rare cases, you might need to use `Series.aggregate` as it provides
a few other options.

### Pairwise 

A special form of windowing is building a series of pairs containing a current
and previous value from the input series (in other words, the key for each pair
is the key of the later element). For example:
*)

// Create a series of pairs from earlier 'hf' input
hf |> Series.pairwise 

// Calculate differences between the current and previous values
hf |> Series.pairwiseWith (fun k (v1, v2) -> v2 - v1)

(** 
The `pairwise` operation always returns a series that has no value for
the first key in the input series. If you want more complex behavior, you
will usually need to replace `pairwise` with `window`. For example, you might
want to get a series that contains the first value as the first element, 
followed by differences. This has the nice property that summing rows,
starting from the first one gives you the current price:
*)
// Sliding window with incomplete segment at the beginning 
hf |> Series.windowSizeInto (2, Boundary.AtBeginning) (function
  // Return the first value for the first segment
  | DataSegment.Incomplete s -> s.GetAt(0).Value
  // Calculate difference for all later segments
  | DataSegment.Complete s -> s.GetAt(1).Value - s.GetAt(0).Value)

(**

<a name="sampling"></a>
Sampling and resampling time series
-----------------------------------

Given a time series with high-frequency prices, sampling or resampling makes 
it possible to get time series with representative values at lower frequency.
The library uses the following terminology:

 - **Lookup** means that we find values at specified key; if a key is not
   available, we can look for value associated with the nearest smaller or 
   the nearest greater key.

 - **Resampling** means that we aggregate values values into chunks based
   on a specified collection of keys (e.g. explicitly provided times), or 
   based on some relation between keys (e.g. date times having the same date).

 - **Uniform resampling** is similar to resampling, but we specify keys by
   providing functions that generate a uniform sequence of keys (e.g. days),
   the operation also fills value for days that have no corresponding 
   observations in the input sequence.

Finally, the library also provides a few helper functions that are specifically
desinged for series with keys of types `DateTime` and `DateTimeOffset`.

### Lookup

Given a series `hf`, you can get a value at a specified key using `hf.Get(key)`
or using `hf |> Series.get key`. However, it is also possible to find values
for larger number of keys at once. The instance member for doing this
is `hf.GetItems(..)`. Moreover, both `Get` and `GetItems` take an optional
parameter that specifies the behavior when the exact key is not found.

Using the function syntax, you can use `Series.getAll` for exact key 
lookup and `Series.lookupAll` when you want more flexible lookup:
*)
// Generate a bit less than 24 hours of data with 13.7sec offsets
let hf = series <| stock1 (TimeSpan.FromSeconds(13.7)) 6300
// Generate keys for all minutes in 24 hours
let keys = [ for m in 0.0 .. 24.0*60.0-1.0 -> today.AddMinutes(m) ]

// Find value for a given key, or nearest greater key with value
hf |> Series.lookupAll keys Lookup.NearestGreater
// [fsi:val it : Series<DateTimeOffset,float> =]
// [fsi:  12:00:00 AM -> 20.07 ]
// [fsi:  12:01:00 AM -> 19.98 ]
// [fsi:  ...         -> ...   ]
// [fsi:  11:58:00 PM -> 19.03 ]
// [fsi:  11:59:00 PM -> <missing>        ]

// Find value for nearest smaller key
// (This returns value for 11:59:00 PM as well)
hf |> Series.lookupAll keys Lookup.NearestSmaller

// Find values for exact key 
// (This only works for the first key)
hf |> Series.lookupAll keys Lookup.Exact

(**
Lookup operations only return one value for each key, so they are useful for
quick sampling of large (or high-frequency) data. When we want to calculate
a new value based on multiple values, we need to use resampling.

### Resampling

Series supports two kinds of resamplings. The first kind is similar to lookup
in that we have to explicitly specify keys. The difference is that resampling
does not find just the nearest key, but all smaller or greater keys. For example:
*)

// For each key, collect values for greater keys until the 
// next one (chunk for 11:59:00 PM is empty)
hf |> Series.resample keys Direction.Forward

// For each key, collect values for smaller keys until the 
// previous one (the first chunk will be singleton series)
hf |> Series.resample keys Direction.Backward

// Aggregate each chunk of preceding values using mean
hf |> Series.resampleInto keys Direction.Backward 
  (fun k s -> Series.mean s)

(**

The second kind of resampling is based on a projection from existing keys in 
the series. The operation then collects chunks such that the projection returns
equal keys. This is very similar to `Series.groupBy`, but resampling assumes 
that the projection preserves the ordering of the keys, and so it only aggregates
consequent keys.

The typical scenario is when you have time series with date time information
(here `DateTimeOffset`) and want to get information for each day (we use 
`DateTime` with empty time to represent dates):
*)

// Generate 2.5 months of data in 1.7 hour offsets
let ds = series <| stock1 (TimeSpan.FromHours(1.7)) 1000

// Sample by day (of type 'DateTime')
let daily = ds |> Series.resampleEquiv (fun d -> d.Date)

(**
The same operation can be easily implemented using `Series.chunkWhile`, but as
it is often used in the context of sampling, it is included in the library as a
primitive. Moreover, we'll see that it is closely related to uniform resampling.

Note that the resulting series has different type of keys than the source. The
source has keys `DateTimeOffset` (representing date with time) while the resulting
keys are of the type returned by the projection (here, `DateTime` representing just
dates).

### Uniform resampling

### Sampling time series

*)
Series.res
Series.resampleUniform
let inp = series <| stock1 (TimeSpan.FromHours(32.0)) 10
inp |> Series.resampleUniform Lookup.NearestSmaller (fun dt -> dt.Date) (fun dt -> dt.AddDays(1.0))

Series.resample

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

Series.sampleTime
inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1))] Direction.Forward (fun k s -> s)
inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1))] Direction.Backward (fun k s -> s)

inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1)); DateTimeOffset(DateTime(2013,10,2))] Direction.Forward (fun k s -> s)
inp |> Series.sampleInto [DateTimeOffset(DateTime(2013,10,1)); DateTimeOffset(DateTime(2013,10,2))] Direction.Backward (fun k s -> s)
            
(**
<a name="stats"></a>
Calculations and statistics
---------------------------

Diff and such

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

// TODO: Error in fsi output
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

