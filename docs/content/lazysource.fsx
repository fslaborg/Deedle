(*** hide ***)
#load "../../bin/net45/Deedle.fsx"
open System

(**
Creating lazily loaded series
=============================

When loading data from an external data source (such as a database), you might
want to create a _virtual_ time series that represents the data source, but 
does not actually load the data until needed. If you apply some range restriction
(like slicing) to the data series before using the values, then it is not 
necessary to load the entire data set into memory.

Deedle supports lazy loading through the `DelayedSeries.FromValueLoader` 
method. It returns an ordinary data series of type `Series<K, V>` which has a 
delayed internal representation.

## Creating lazy series

We will not use a real database in this tutorial, but let's say that you have the
following function which loads data for a given day range: 
*)
open Deedle

/// Given a time range, generates random values for dates (at 12:00 AM)
/// starting with the day of the first date time and ending with the 
/// day after the second date time (to make sure they are in range)
let generate (low:DateTime) (high:DateTime) =
  let rnd = Random()
  let days = int (high.Date - low.Date).TotalDays + 1
  seq { for d in 0 .. days -> 
          KeyValue.Create(low.Date.AddDays(float d), rnd.Next()) }

(**
Using random numbers as the source in this example is not entirely correct, because
it means that we will get different values each time a new sub-range of the series
is required - but it will suffice for the demonstration.

Now, to create a lazily loaded series, we need to open the `Indices` namespace,
specify the minimal and maximal value of the series and use `DelayedSeries.FromValueLoader`:
*)
open Deedle.Indices

// Minimal and maximal values that can be loaded from the series
let min, max = DateTime(2010, 1, 1), DateTime(2013, 1, 1)

// Create a lazy series for the given range
let ls = DelayedSeries.FromValueLoader(min, max, fun (lo, lob) (hi, hib) -> async { 
    printfn "Query: %A - %A" (lo, lob) (hi, hib)
    return generate lo hi })

(**
To make the diagnostics easier, we print the required range whenever a request
is made. After running this code, you should not see any output yet.
The parameter to `DelayedSeries.FromValueLoader` is a function that takes 4 arguments:

  - `lo` and `hi` specify the low and high boundaries of the range. Their
    type is the type of the key (e.g. `DateTime` in our example)
  - `lob` and `hib` are values of type `BoundaryBehavior` and can be either
    `Inclusive` or `Exclusive`. They specify whether the boundary value should
    be included or not.

Our sample function does not handle boundaries correctly - it always includes the
boundary (and possibly more values). This is not a problem, because the lazy loader
automatically skips over such values. But if you want, you can use `lob` and `hib` 
parameters to build a more optimal SQL query.

## Using un-evaluated series

Let's now have a look at the operations that we can perform on un-evaluated series.
Any operation that actually accesses values or keys of the series (such as `Series.observations`
or lookup for a specific key) will force the evaluation of the series.

However, we can use range restrictions before accessing the data:
*)
// Get series representing January 2012
let jan12 = ls.[DateTime(2012, 1, 1) .. DateTime(2012, 2, 1)]

// Further restriction - only first half of the month
let janHalf = jan12.[.. DateTime(2012, 1, 15)]

// Get value for a specific date
janHalf.[DateTime(2012, 1, 1)]
// [fsi: Query: (1/1/2012, Inclusive) - (1/15/2012, Inclusive)]
// [fsi: val it : int = 1127670994]

janHalf.[DateTime(2012, 1, 2)]
// [fsi: val it : int = 560920727]
(**
As you can see from the output on line 9, the series obtained data for the
15 day range that we created by restricting the original series. When we requested
another value within the specified range, it was already available and it was
returned immediately. Note that `janHalf` is restricted to the specified 15 day
range, so we cannot access values outside of the range. Also, when you access a single
value, entire series is loaded. The motivation is that you probably need to access
multiple values, so it is likely cheaper to load the whole series.

Another operation that can be performed on an unevaluated series is to add it
to a data frame with some existing key range:
*)

// Create empty data frame for days of December 2011
let dec11 = Frame.ofRowKeys [ for d in 1 .. 31 -> DateTime(2011, 12, d) ]

// Add series as the 'Values' column to the data frame
dec11?Values <- ls
// [fsi: Query: (12/1/2011, Inclusive) - (12/31/2011, Inclusive)]

(**
When adding lazy series to a data frame, the series has to be evaluated (so that
the values can be properly aligned) but it is first restricted to the range of the
data frame. In the above example, only one month of data is loaded.

*)