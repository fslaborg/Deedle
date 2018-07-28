(*** hide ***)
#I "../../bin/net45"
#load "Deedle.fsx"
#I "../../packages/MathNet.Numerics/lib/net40"
#load "../../packages/FSharp.Charting/lib/net45/FSharp.Charting.fsx"
open System
open FSharp.Data
open Deedle
open FSharp.Charting

let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Working with series and time series data in F#
==============================================

In this section, we look at F# data frame library features that are useful when working
with time series data or, more generally, any ordered series. Although we mainly look at
operations on the `Series` type, many of the operations can be applied to data frame `Frame`
containing multiple series. Furthermore, data frame provides an elegant way for aligning and
joining series. 

You can also get this page as an [F# script file](https://github.com/fslaborg/Deedle/blob/master/docs/content/series.fsx)
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
page, but you can find it in the [script file with full source](https://github.com/fslaborg/Deedle/blob/master/docs/content/series.fsx).
Once we have the function, we define a date `today` (representing today's midnight) and
two helper functions that set basic properties for the `randomPrice` function. 

To get random prices, we now only need to call `stock1` or `stock2` with `TimeSpan` and 
the required number of prices:
*)
(*** define-output: stocks ***)
Chart.Combine
  [ stock1 (TimeSpan(0, 1, 0)) 1000 |> Chart.FastLine
    stock2 (TimeSpan(0, 1, 0)) 1000 |> Chart.FastLine ]
(**
The above snippet generates 1k of prices in one minute intervals and plots them using the
[F# Charting library](https://github.com/fsharp/FSharp.Charting). When you run the code
and tweak the chart look, you should see something like this:
*)

(*** include-it: stocks ***)

(**
<a name="alignment"></a>
Data alignment and zipping
--------------------------

One of the key features of the data frame library for working with time series data is 
_automatic alignment_ based on the keys. When we have multiple time series with date 
as the key (here, we use `DateTimeOffset`, but any type of date will do), we can combine
multiple series and align them automatically to specified date keys.

To demonstrate this feature, we generate random prices in 60 minute, 30 minute and 
65 minute intervals:
*)

let s1 = stock1 (TimeSpan(1, 0, 0)) 6 |> series
// [fsi:val s1 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 20.76; 1:00:00 AM => 21.11; 2:00:00 AM => 22.51 ]
// [fsi:            3:00:00 AM => 23.88; 4:00:00 AM => 23.23; 5:00:00 AM => 22.68 ] ]

let s2 =stock2 (TimeSpan(0, 30, 0)) 12 |> series
// [fsi:val s2 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 21.61; 12:30:00 AM => 21.64; 1:00:00 AM => 21.86 ]
// [fsi:            1:30:00 AM => 22.22;  2:00:00 AM => 22.35; 2:30:00 AM => 22.76 ]
// [fsi:            3:00:00 AM => 22.68;  3:30:00 AM => 22.64; 4:00:00 AM => 22.90 ]
// [fsi:            4:30:00 AM => 23.40;  5:00:00 AM => 23.33; 5:30:00 AM => 23.43] ]

let s3 = stock1 (TimeSpan(1, 5, 0)) 6 |> series
// [fsi:val s3 : Series<DateTimeOffset,float> =]
// [fsi:  series [ 12:00:00 AM => 21.37; 1:05:00 AM => 22.73; 2:10:00 AM => 22.08 ]
// [fsi:            3:15:00 AM => 23.92; 4:20:00 AM => 22.72; 5:25:00 AM => 22.79 ]

(**
### Zipping time series 

Let's first look at operations that are available on the `Series<K, V>` type. A series
exposes `Zip` operation that can combine multiple series into a single series of pairs.
This is not as convenient as working with data frames (which we'll see later), but it 
is useful if you only need to work with one or two columns without missing values:

*)
// Match values from right series to keys of the left one
// (this creates series with no missing values)
s1.Zip(s2, JoinKind.Left)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -> (21.32, 21.61) ]
// [fsi:   1:00:00 AM -> (22.62, 21.86) ]
// [fsi:   2:00:00 AM -> (22.00, 22.35)  ]
// [fsi:  (...)]

// Match values from the left series to keys of the right one
// (right has higher resolution, so half of left values are missing)
s1.Zip(s2, JoinKind.Right)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -> (21.32,     21.61) ]
// [fsi:  12:30:00 AM -> (<missing>, 21.64)  ]      
// [fsi:   1:00:00 AM -> (22.62,     21.86) ]
// [fsi:  (...)]

// Use left series key and find the nearest previous
// (smaller) value from the right series
s1.Zip(s2, JoinKind.Left, Lookup.ExactOrSmaller)
// [fsi:val it : Series<DateTimeOffset,float opt * float opt>]
// [fsi:  12:00:00 AM -04:00 -> (21.32, 21.61) ]
// [fsi:   1:00:00 AM -04:00 -> (22.62, 21.86) ]
// [fsi:   2:00:00 AM -04:00 -> (22.00, 22.35)  ]
// [fsi:  (...)]

(**
Using `Zip` on series is somewhat complicated. The result is a series of tuples, but each 
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
exposed as `Frame.join` and `Frame.joinAlign` functions, but it is usually more convenient to use 
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
f2.Join(f3, JoinKind.Left, Lookup.ExactOrSmaller)
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
Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller f1 f2

(**
The automatic alignment is extremely useful when you have multiple data series with different
offsets between individual observations. You can choose your set of keys (dates) and then easily
align other data to match the keys. Another alternative to using `Join` explicitly is to create
a new frame with just keys that you are interested in (using `Frame.ofRowKeys`) and then use
the `AddSeries` member (or the `df?New <- s` syntax) to add series. This will automatically left
join the new series to match the current row keys.

When aligning data, you may or may not want to create data frame with missing values. If your
observations do not happen at exact time, then using `Lookup.ExactOrSmaller` or `Lookup.ExactOrGreater`
is a great way to avoid mismatch. 

If you have observations that happen e.g. at two times faster rate (one series is hourly and 
another is half-hourly), then you can create data frame with missing values using `Lookup.Exact` 
(the default value) and then handle missing values explicitly (as [discussed here](frame.html#missing)).


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
let lf = stock1 (TimeSpan(0, 1, 0)) 6 |> series

// Create series of series representing individual windows
lf |> Series.window 4
// Aggregate each window using 'Stats.mean'
lf |> Series.windowInto 4 Stats.mean
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
let lfm1 = lf |> Series.windowInto 4 Stats.mean
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
    Stats.mean ds.Data)

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
aligned with original values. When you want to specify custom key selector,
you can use a more general function `Series.aggregate`. 

In the previous sample, the code that performs aggregation is no longer
just a simple function like `Stats.mean`, but a lambda that takes `ds`,
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
let hourly = stock1 (TimeSpan(1, 0, 0)) (30*24) |> series

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
let hf = stock1 (TimeSpan(0, 0, 1)) 600 |> series

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
  | DataSegment.Incomplete s -> s.GetAt(0)
  // Calculate difference for all later segments
  | DataSegment.Complete s -> s.GetAt(1) - s.GetAt(0))

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
let mf = stock1 (TimeSpan.FromSeconds(13.7)) 6300 |> series
// Generate keys for all minutes in 24 hours
let keys = [ for m in 0.0 .. 24.0*60.0-1.0 -> today.AddMinutes(m) ]

// Find value for a given key, or nearest greater key with value
mf |> Series.lookupAll keys Lookup.ExactOrGreater
// [fsi:val it : Series<DateTimeOffset,float> =]
// [fsi:  12:00:00 AM -> 20.07 ]
// [fsi:  12:01:00 AM -> 19.98 ]
// [fsi:  ...         -> ...   ]
// [fsi:  11:58:00 PM -> 19.03 ]
// [fsi:  11:59:00 PM -> <missing>        ]

// Find value for nearest smaller key
// (This returns value for 11:59:00 PM as well)
mf |> Series.lookupAll keys Lookup.ExactOrSmaller

// Find values for exact key 
// (This only works for the first key)
mf |> Series.lookupAll keys Lookup.Exact

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
mf |> Series.resample keys Direction.Forward

// For each key, collect values for smaller keys until the 
// previous one (the first chunk will be singleton series)
mf |> Series.resample keys Direction.Backward

// Aggregate each chunk of preceding values using mean
mf |> Series.resampleInto keys Direction.Backward 
  (fun k s -> Stats.mean s)

// Resampling is also available via the member syntax
mf.Resample(keys, Direction.Forward)
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
let ds = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample by day (of type 'DateTime')
ds |> Series.resampleEquiv (fun d -> d.Date)

// Sample by day (of type 'DateTime')
ds.ResampleEquivalence(fun d -> d.Date)
(**
The same operation can be easily implemented using `Series.chunkWhile`, but as
it is often used in the context of sampling, it is included in the library as a
primitive. Moreover, we'll see that it is closely related to uniform resampling.

Note that the resulting series has different type of keys than the source. The
source has keys `DateTimeOffset` (representing date with time) while the resulting
keys are of the type returned by the projection (here, `DateTime` representing just
dates).

### Uniform resampling

In the previous section, we looked at `resampleEquiv`, which is useful if you want
to sample time series by keys with "lower resolution" - for example, sample date time
observations by date. However, the function discussed in the previous section only
generates values for which there are keys in the input sequence - if there is no
observation for an entire day, then the day will not be included in the result.

If you want to create sampling that assigns value to each key in the range specified
by the input sequence, then you can use _uniform resampling_.

The idea is that uniform resampling applies the key projection to the smallest and
greatest key of the input (e.g. gets date of the first and last observation) and then
it generates all keys in the projected space (e.g. all dates). Then it picks the
best value for each of the generated key.
*)

// Create input data with non-uniformly distributed keys
// (1 value for 10/3, three for 10/4 and two for 10/6)
let days =
  [ "10/3/2013 12:00:00"; "10/4/2013 15:00:00" 
    "10/4/2013 18:00:00"; "10/4/2013 19:00:00"
    "10/6/2013 15:00:00"; "10/6/2013 21:00:00" ]
let nu = 
  stock1 (TimeSpan(24,0,0)) 10 |> series
  |> Series.indexWith days |> Series.mapKeys DateTimeOffset.Parse

// Generate uniform resampling based on dates. Fill
// missing chunks with nearest smaller observations.
let sampled =
  nu |> Series.resampleUniform Lookup.ExactOrSmaller 
    (fun dt -> dt.Date) (fun dt -> dt.AddDays(1.0))

// Same thing using the C#-friendly member syntax
// (Lookup.ExactOrSmaller is the default value)
nu.ResampleUniform((fun dt -> dt.Date), (fun dt -> dt.AddDays(1.0)))

// Turn into frame with multiple columns for each day
// (to format the result in a readable way)
sampled 
|> Series.mapValues Series.indexOrdinally
|> Frame.ofRows
// [fsi:val it : Frame<DateTime,int> =]
// [fsi:             0      1          2                ]
// [fsi:10/3/2013 -> 21.45  <missing>  <missing>        ]
// [fsi:10/4/2013 -> 21.63  19.83      17.51]
// [fsi:10/5/2013 -> 17.51  <missing>  <missing>        ]
// [fsi:10/6/2013 -> 18.80  20.93      <missing>        ]

(**
To perform the uniform resampling, we need to specify how to project (resampled) keys
from original keys (we return the `Date`), how to calculate the next key (add 1 day)
and how to fill missing values.

After performing the resampling, we turn the data into a data frame, so that we can 
nicely see the results. The individual chunks have the actual observation times as keys,
so we replace those with just integers (using `Series.indexOrdinal`). The result contains
a simple ordered row of observations for each day.

The important thing is that there is an observation for each day - even for for 10/5/2013
which does not have any corresponding observations in the input. We call the resampling
function with `Lookup.ExactOrSmaller`, so the value 17.51 is picked from the last observation
of the previous day (`Lookup.ExactOrGreater` would pick 18.80 and `Lookup.Exact` would give
us an empty series for that date).

### Sampling time series

Perhaps the most common sampling operation that you might want to do is to sample time series
by a specified `TimeSpan`. Although this can be easily done by using some of the functions above,
the library provides helper functions exactly for this purpose:

*)
// Generate 1k observations with 1.7 hour offsets
let pr = stock1 (TimeSpan.FromHours(1.7)) 1000 |> series

// Sample at 2 hour intervals; 'Backward' specifies that
// we collect all previous values into a chunk.
pr |> Series.sampleTime (TimeSpan(2, 0, 0)) Direction.Backward

// Same thing using member syntax - 'Backward' is the dafult
pr.Sample(TimeSpan(2, 0, 0))

// Get the most recent value, sampled at 2 hour intervals
pr |> Series.sampleTimeInto
  (TimeSpan(2, 0, 0)) Direction.Backward Series.lastValue

(**
<a name="stats"></a>
Calculations and statistics
---------------------------

In the final section of this tutorial, we look at writing some calculations over time series. Many of the
functions demonstrated here can be also used on unordered data frames and series.

### Shifting and differences

First of all, let's look at functions that we need when we need to compare subsequent values in
the series. We already demonstrated how to do this using `Series.pairwise`. In many cases,
the same thing can be done using an operation that operates over the entire series.

The two useful functions here are:

 - `Series.diff` calcualtes the difference between current and n-_th_  previous element
 - `Series.shift` shifts the values of a series by a specified offset

The following snippet illustrates how both functions work:
*)
// Generate sample data with 1.7 hour offsets
let sample = stock1 (TimeSpan.FromHours(1.7)) 6 |> series

// Calculates: new[i] = s[i] - s[i-1]
let diff1 = sample |> Series.diff 1
// Diff in the opposite direction
let diffM1 = sample |> Series.diff -1

// Shift series values by 1
let shift1 = sample |> Series.shift 1

// Align all results in a frame to see the results
let df = 
  [ "Shift +1" => shift1 
    "Diff +1" => diff1 
    "Diff" => sample - shift1 
    "Orig" => sample ] |> Frame.ofColumns 
// [fsi:val it : Frame<DateTimeOffset,string> =]
// [fsi:                 Diff       Diff +1    Orig   Shift +1         ]
// [fsi:  12:00:00 AM -> <missing>  <missing>  21.73  <missing>        ]
// [fsi:   1:42:00 AM ->  1.73       1.73      23.47  21.73 ]
// [fsi:   3:24:00 AM -> -0.83      -0.83      22.63  23.47 ]
// [fsi:   5:06:00 AM ->  2.37       2.37      25.01  22.63 ]
// [fsi:   6:48:00 AM -> -1.57      -1.57      23.43  25.01 ]
// [fsi:   8:30:00 AM ->  0.09       0.09      23.52  23.43 ]

(**
In the above snippet, we first calcluate difference using the `Series.diff` function.
Then we also show how to do that using `Series.shift` and binary operator applied
to two series (`sample - shift`). The following section provides more details. 
So far, we also used the functional notation (e.g. `sample |> Series.diff 1`), but
all operations can be called using the member syntax - very often, this gives you
a shorter syntax. This is also shown in the next few snippets.

### Operators and functions

Time series also supports a large number of standard F# functions such as `log` and `abs`.
You can also use standard numerical operators to apply some operation to all elements
of the series. 

Because series are indexed, we can also apply binary operators to two series. This 
automatically aligns the series and then applies the operation on corresponding elements.

*)

// Subtract previous value from the current value
sample - sample.Shift(1)

// Calculate logarithm of such differences
log (sample - sample.Shift(1))

// Calculate square of differences
sample.Diff(1) ** 2.0

// Calculate average of value and two immediate neighbors
(sample.Shift(-1) + sample + sample.Shift(2)) / 3.0

// Get absolute value of differences
abs (sample - sample.Shift(1))

// Get absolute value of distance from the mean
abs (sample - (Stats.mean sample))

(**
The time series library provides a large number of functions that can be applied in this
way. These include trigonometric functions (`sin`, `cos`, ...), rounding functions
(`round`, `floor`, `ceil`), exponentials and logarithms (`exp`, `log`, `log10`) and more.
In general, whenever there is a built-in numerical F# function that can be used on 
standard types, the time series library should support it too.

However, what can you do when you write a custom function to do some calculation and
want to apply it to all series elements? Let's have a look:
*)

// Truncate value to interval [-1.0, +1.0]
let adjust v = min 1.0 (max -1.0 v)

// Apply adjustment to all function
adjust $ sample.Diff(1)

// The $ operator is a shorthand for
sample.Diff(1) |> Series.mapValues adjust

(**
In general, the best way to apply custom functions to all values in a series is to 
align the series (using either `Series.join` or `Series.joinAlign`) into a single series
containing tuples and then apply `Series.mapValues`. The library also provides the `$` operator
that simplifies the last step - `f $ s` applies the function `f` to all values of the series `s`.

### Data frame operations

Finally, many of the time series operations demonstrated above can be applied to entire
data frames as well. This is particularly useful if you have data frame that contains multiple
aligned time series of similar structure (for example, if you have multiple stock prices or 
open-high-low-close values for a given stock). 

The following snippet is a quick overview of what you can do:
*)
/// Multiply all numeric columns by a given constant
df * 0.65

// Apply function to all columns in all series
let conv x = min x 20.0
df |> Frame.mapRowValues (fun os -> conv $ os.As<float>())
   |> Frame.ofRows

// Sum each column and divide results by a constant
Stats.sum df / 6.0
// Divide sum by mean of each frame column
Stats.sum df / Stats.mean df
