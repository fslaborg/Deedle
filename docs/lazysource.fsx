(**
---
title: Creating lazily loaded series
category: Guides
categoryindex: 1
index: 6
description: Building series backed by lazy or virtual data sources for on-demand data loading
keywords: lazy, virtual series, data source, on-demand loading
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#endif // FSX
(*** condition: prepare ***)

open System
open System.Collections.Generic
open Deedle
open Deedle.Indices

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)

(**

# Delay-loaded series

The `DelayedSeries` type provides an efficient way to create series whose data is loaded
on-demand. For example, you may have a large time series stored in a CSV file or in a 
database and you do not want to load all the data in memory if the user only needs a
small part of it.

When you create a delayed series, you specify the overall range of the series (i.e. the
minimum and maximum key value) and you provide a function that loads a specified sub-range
of the series. When the user accesses a continuous range of the series, the loading function
is called to retrieve the data.

<a name="create"></a>

## Creating a delayed series

To create a delayed series, we need a function that generates data for a given range.
The following function generates a series with random data for a given date range with
a day frequency:
*)

let generate (low:DateTime) (high:DateTime) : seq<KeyValuePair<DateTime,float>> = 
    let rnd = Random()
    let days = int (high - low).TotalDays
    seq [ for d in 0 .. days -> KeyValuePair(low.AddDays(float d), rnd.NextDouble()) ]

(**
Now we use `DelayedSeries.FromValueLoader` to create a delayed series. It takes the overall
minimum and maximum key of the series and a function that loads data for a sub-range. The 
loading function gets the lower and upper bound as a tuple of `(key, BoundaryBehavior)` 
values where `BoundaryBehavior` is either `Inclusive` or `Exclusive`:
*)

let min = DateTime(2010, 1, 1)
let max = DateTime(2013, 1, 1)

let ls = DelayedSeries.FromValueLoader(min, max, fun (lo, lob) (hi, hib) -> async {
    printfn "Query: %A - %A" lo hi
    let lo = if lob = BoundaryBehavior.Inclusive then lo else lo.AddDays(1.0)
    let hi = if hib = BoundaryBehavior.Inclusive then hi else hi.AddDays(-1.0)
    return generate lo hi })

(**
The key thing about the above is that, so far, no data has been loaded. The loading function
is called only when we access part of the series.

<a name="slicing"></a>

## Slicing and using delayed series

We can now use the series as usual - for example, to get data for the entire year 2012:
*)

let slice = ls.[DateTime(2012, 1, 1) .. DateTime(2012, 12, 31)]
slice
(*** include-fsi-merged-output ***)

(**
Similarly, we can add the delayed series to a data frame. When doing this, Deedle will
only load the data that is needed. In the following example, we add the series to a frame
and then access only a slice:
*)

let df = frame ["Values" => ls]
let slicedDf = df.Rows.[DateTime(2012,6,1) .. DateTime(2012,6,30)]
slicedDf
(*** include-fsi-merged-output ***)
