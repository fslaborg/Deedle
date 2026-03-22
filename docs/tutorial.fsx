(**
---
title: Deedle in 10 minutes using F#
category: Documentation
categoryindex: 1
index: 2
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net9.0/Deedle.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#endif // FSX

open System
open Deedle

(**

# Quick Start

This document is a quick overview of the most important features of Deedle.

The first step is to install `Deedle` [from NuGet](https://www.nuget.org/packages/Deedle).
In F# Interactive, reference the library:

// #r "nuget: Deedle,{{fsdocs-package-version}}"

<a name="creating"></a>

## Creating series and frames

A data frame is a collection of series with unique column names (although these
do not actually have to be strings). So, to create a data frame, we first need
to create a series:
*)

(*** define-output: create1 ***)
// Create from sequence of keys and sequence of values
let dates  = 
  [ DateTime(2013,1,1); 
    DateTime(2013,1,4); 
    DateTime(2013,1,8) ]
let values = 
  [ 10.0; 20.0; 30.0 ]
let first = Series(dates, values)

// Create from a single list of observations
Series.ofObservations
  [ DateTime(2013,1,1) => 10.0
    DateTime(2013,1,4) => 20.0
    DateTime(2013,1,8) => 30.0 ]

(*** include-it: create1 ***)

(*** define-output: create2 ***)
// Shorter alternative to 'Series.ofObservations'
series [ 1 => 1.0; 2 => 2.0 ]

// Create series with implicit (ordinal) keys
Series.ofValues [ 10.0; 20.0; 30.0 ]
(*** include-it: create2 ***)

(**
Note that the series type is generic. `Series<K, T>` represents a series
with keys of type `K` and values of type `T`. Let's now generate series
with 10 day value range and random values:
*)

/// Generate date range from 'first' with 'count' days
let dateRange (first:System.DateTime) count = (*[omit:(...)]*)
  seq { for i in 0 .. (count - 1) -> first.AddDays(float i) }(*[/omit]*)

/// Generate 'count' number of random doubles
let rand count = (*[omit:(...)]*)
  let rnd = System.Random()
  seq { for i in 0 .. (count - 1) -> rnd.NextDouble() }(*[/omit]*)

// A series with values for 10 days 
let second = Series(dateRange (DateTime(2013,1,1)) 10, rand 10)

(*** include-value: (round (second*100.0))/100.0 ***)

(**
Now we can easily construct a data frame that has two columns - one representing
the `first` series and another representing the `second` series:
*)

let df1 = Frame(["first"; "second"], [first; second])

(*** include-value: df1 ***)

(** 
The type representing a data frame has two generic parameters:
`Frame<TRowKey, TColumnKey>`. The first parameter represents the type of
row keys - this can be `int` if we do not give the keys explicitly or `DateTime`
like in the example above. The second parameter is the type of column keys.
This is typically `string`, but sometimes it is useful to create a 
transposed frame with dates as column keys. Because a data frame can contain
heterogeneous data, there is no type of values - this needs to be specified
when getting data from the data frame.

As the output shows, creating a frame automatically combines the indices of 
the two series (using "outer join" so the result has all the dates that appear 
in any of the series). The data frame now contains `first` column with some 
missing values.

You can also use the following nicer syntax and create frame from rows as well as 
individual values:
*)

// The same as previously
let df2 = Frame.ofColumns ["first" => first; "second" => second]

// Transposed - here, rows are "first" and "second" & columns are dates
let df3 = Frame.ofRows ["first" => first; "second" => second]

// Create from individual observations (row * column * value)
let df4 = 
  [ ("Monday", "Tomas", 1.0); ("Tuesday", "Adam", 2.1)
    ("Tuesday", "Tomas", 4.0); ("Wednesday", "Tomas", -5.4) ]
  |> Frame.ofValues

(**
Data frame can be also easily created from a collection of F# record types (or of any classes
with public readable properties). The `Frame.ofRecords` function uses reflection to find the 
names and types of properties of a record and creates a data frame with the same structure.
*)
// Assuming we have a record 'Price' and a collection 'values'
type Price = { Day : DateTime; Open : float }
let prices = 
  [ { Day = DateTime.Now; Open = 10.1 }
    { Day = DateTime.Now.AddDays(1.0); Open = 15.1 }
    { Day = DateTime.Now.AddDays(2.0); Open = 9.1 } ]

// Creates a data frame with columns 'Day' and 'Open'
let df5 = Frame.ofRecords prices

(**
Finally, we can also load data frame from CSV:
*)
let root = __SOURCE_DIRECTORY__ + "/data/"

let msftCsv = Frame.ReadCsv(root + "stocks/msft.csv")
let fbCsv = Frame.ReadCsv(root + "stocks/fb.csv")

(*** include-value: fbCsv ***)

(**
When loading the data, the data frame analyses the values and automatically converts
them to the most appropriate type. However, no conversion is automatically performed
for dates and times - the user needs to decide what is the desirable representation
of dates (e.g. `DateTime`, `DateTimeOffset` or some custom type).

<a name="reindexing-and-joins"></a>

## Specifying index and joining

Now we have `fbCsv` and `msftCsv` frames containing stock prices, but they are
indexed with ordinal numbers. This means that we can get e.g. 4th price. 
However, we would like to align them using their dates (in case there are some 
values missing). This can be done by setting the row index to the "Date" column.
Once we set the date as the index, we also need to order the index. The Yahoo 
Finance prices are ordered from the newest to the oldest, but our data-frame 
requires ascending ordering.

When a frame has ordered index, we can use additional functionality that will 
be needed later (for example, we can select sub-range by specifying dates that 
are not explicitly included in the index).
*)

// Use the Date column as the index & order rows
let msftOrd = 
  msftCsv
  |> Frame.indexRowsDate "Date"
  |> Frame.sortRowsByKey

(**
The `indexRowsDate` function uses a column of type `DateTime` as a new index.
The library provides other functions for common types of indices (like `indexRowsInt`)
and you can also use a generic function - when using the generic function, some 
type annotations may be needed, so it is better to use a specific function.
Next, we sort the rows using another function from the `Frame` module. The module
contains a large number of useful functions that you'll use all the time - it
is a good idea to go through the list to get an idea of what is supported.

Now that we have properly indexed stock prices, we can create a new data frame that
only has the data we're interested (Open & Close) prices and we add a new column 
that shows their difference:
*)

// Create data frame with just Open and Close prices
let msft = msftOrd.Columns.[ ["Open"; "Close"] ]

// Add new column with the difference between Open & Close
msft?Difference <- msft?Open - msft?Close

// Do the same thing for Facebook
let fb = 
  fbCsv
  |> Frame.indexRowsDate "Date"
  |> Frame.sortRowsByKey
  |> Frame.sliceCols ["Open"; "Close"]
fb?Difference <- fb?Open - fb?Close

(**
When selecting columns using `f.Columns.[ .. ]` it is possible to use a list of columns
(as we did), a single column key, or a range (if the associated index is ordered). 
Then we use the `df?Column <- (...)` syntax to add a new column to the data frame. 
This is the only mutating operation that is supported on data frames - all other 
operations create a new data frame and return it as the result.

Next we would like to create a single data frame that contains (properly aligned) data
for both Microsoft and Facebook. This is done using the `Join` method - but before we
can do this, we need to rename their columns, because duplicate keys are not allowed:
*)

// Change the column names so that they are unique
let msftNames = ["MsftOpen"; "MsftClose"; "MsftDiff"]
let msftRen = msft |> Frame.indexColsWith msftNames

let fbNames = ["FbOpen"; "FbClose"; "FbDiff"]
let fbRen = fb |> Frame.indexColsWith fbNames

// Outer join (align & fill with missing values)
let joinedOut = msftRen.Join(fbRen, kind=JoinKind.Outer)

// Inner join (remove rows with missing values)
let joinedIn = msftRen.Join(fbRen, kind=JoinKind.Inner)

(**
<a name="selecting"></a>

## Selecting values and slicing

The data frame provides two key properties that we can use to access the data. The 
`Rows` property returns a series containing individual rows (as a series) and `Columns`
returns a series containing columns (as a series). We can then use various indexing and
slicing operators on the series:
*)

// Look for a row at a specific date
joinedIn.Rows.[DateTime(2013, 1, 2)]

// Get opening Facebook price for 2 Jan 2013
joinedIn.Rows.[DateTime(2013, 1, 2)]?FbOpen

(**
In the previous example, we used an indexer with a single key. You can also specify multiple
keys (using a list) or a range (using the slicing syntax):
*)

// Get values for the first three days of January 2013
let janDates = [ for d in 2 .. 4 -> DateTime(2013, 1, d) ]
let jan234 = joinedIn.Rows.[janDates]

// Calculate mean of Open price for 3 days
jan234?MsftOpen |> Stats.mean

// Get values corresponding to entire January 2013
let jan = joinedIn.Rows.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)] 

(*** include-value:round (jan*100.0)/100.0 |> Frame.mapRowKeys (fun dt -> dt.ToShortDateString()) ***)

// Calculate means over the period
jan?FbOpen |> Stats.mean
jan?MsftOpen |> Stats.mean

(**
<a name="timeseries"></a>

## Using ordered time series

As already mentioned, if we have an ordered series or an ordered data frame, then we can
leverage the ordering in a number of ways. In the previous example, slicing used lower
and upper bounds rather than exact matching. Similarly, it is possible to get nearest
smaller (or greater) element when using direct lookup.

For example, let's create two series with 10 values for 10 days. The `daysSeries` 
contains keys starting from `DateTime.Today` (12:00 AM) and `obsSeries` has dates
with time component set to the current time:
*)

let daysSeries = Series(dateRange DateTime.Today 10, rand 10)
let obsSeries = Series(dateRange DateTime.Now 10, rand 10)

(*** include-value: (round (daysSeries*100.0))/100.0 ***)
(*** include-value: (round (obsSeries*100.0))/100.0 ***)

(**
The indexing operation written as `daysSeries.[date]` uses _exact_ semantics so it will 
fail if the exact date is not available. When using `Get` method, we can provide an
additional parameter to specify the required behaviour:
*)

// This works - we get the value for DateTime.Today (12:00 AM)
daysSeries.Get(DateTime.Now, Lookup.ExactOrSmaller)

(**
<a name="projections"></a>

## Projection and filtering

For filtering and projection, series provides `Where` and `Select` methods and 
corresponding `Series.map` and `Series.filter` functions. The following adds a new 
column that contains the name of the stock with greater price ("FB" or "MSFT"):
*)

joinedOut?Comparison <- joinedOut |> Frame.mapRowValues (fun row -> 
  if row?MsftOpen > row?FbOpen then "MSFT" else "FB")

(**
When projecting or filtering rows, we need to be careful about missing data. The row
accessor `row?MsftOpen` reads the specified column (and converts it to `float`), but when
the column is not available, it throws the `MissingValueException` exception. Projection
functions such as `mapRowValues` automatically catch this exception and mark the 
corresponding series value as missing.

Now we can get the number of days when Microsoft stock prices were above Facebook:
*)

joinedOut.GetColumn<string>("Comparison")
|> Series.filterValues ((=) "MSFT") |> Series.countValues

joinedOut.GetColumn<string>("Comparison")
|> Series.filterValues ((=) "FB") |> Series.countValues

(**
<a name="grouping"></a>

## Grouping and aggregation

As a last thing, we briefly look at grouping and aggregation. For more information
about grouping of time series data, see [the time series features tutorial](series.html)
and [the data frame features](frame.html) contains more about grouping of unordered frames.

The following snippet groups rows by month and year:
*)
let monthly =
  joinedIn
  |> Frame.groupRowsUsing (fun k _ -> DateTime(k.Year, k.Month, 1))

(**
As you can see, we get back a frame that has a tuple `DateTime * DateTime` as the row key.
This is treated in a special way as a _hierarchical_ (or multi-level) index. A number of 
operations can be used on hierarchical indices. For example, we can get rows in a specified 
group (say, May 2013) and calculate means of columns in the group:
*)
monthly.Rows.[DateTime(2013,5,1), *] |> Stats.mean

(**
We can also use `Frame.getNumericColumns` in combination with 
`Stats.levelMean` to get means for all first-level groups:
*)
monthly 
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofColumns

(**

## Further reading

This quick-start covers the most common Deedle patterns. Dive deeper with:

 * [Data frame features](frame.html) — full coverage of frame construction, slicing,
   grouping, aggregation with `Frame.aggregateRowsBy`, pivot tables, and more.
 * [Series features](series.html) — windowing, chunking, resampling, and time-series alignment.
 * [Statistics](stats.html) — moving and expanding window statistics, multi-level aggregation.
 * [Handling missing values](missing.html) — sentinel types, all fill strategies, and how
   missing values interact with joins.
 * [Joining and merging](joining.html) — inner, outer, left, and right joins.
 * [Apache Arrow / Feather](arrow.html) — zero-copy columnar I/O.
 * [Deedle.Math](math.html) — linear algebra, correlation matrices, EWM statistics, PCA,
   and linear regression via the `Deedle.Math` package.
*)
