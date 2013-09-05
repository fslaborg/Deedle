(**
Tutorial & stock price demo
===========================

First, we need to load the DataFrame library - in F# Interactive, this is done by
loading an `.fsx` file that loads the actual `.dll` with the library and registers
pretty printers for types representing data frame and series. In this sample, we
also need F# Charting, which works similarly:

*)
#I "../bin"
#I "../lib"
#I "../packages"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"
open FSharp.DataFrame
open FSharp.Charting

(**
Creating series & data frames
-----------------------------

You can see a data frame as a collection of series with unique column names (although 
these do not actually have to be strings). So, to create a data frame, we first need
to create a series:
*)

open System

/// Generate date range from 'first' with 'count' days
let dateRange (first:System.DateTime) count = (*[omit:(...)]*)
  seq { for i in 0 .. (count - 1) -> first.AddDays(float i) }(*[/omit]*)
/// Generate 'count' number of random doubles
let rand count = (*[omit:(...)]*)
  let rnd = System.Random()
  seq { for i in 0 .. (count - 1) -> rnd.NextDouble() }(*[/omit]*)

// A simple series with just three values
let first = Series([DateTime(2013,1,1); DateTime(2013,1,4); DateTime(2013,1,8)], [ 10.0; 20.0; 30.0 ])

// A series with values for 10 days 
let second = Series(dateRange (DateTime(2013,1,1)) 10, rand 10)

(**
Now we can easily construct a data frame that has two columns - one representing
the `first` series and another representing the `second` series:
*)

let df1 = Frame(["first"; "second"], [first; second])

(** 
This automatically combines the indices of the two series (using "outer join" so the
result has all the dates that appear in any of the series). The data frame now contains
`first` column with some missing values.

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
let values = 
  [ { Day = DateTime.Now; Open = 10.1 }
    { Day = DateTime.Now.AddDays(1.0); Open = 15.1 }
    { Day = DateTime.Now.AddDays(2.0); Open = 9.1 } ]

// Creates a data frame with columns 'Day' and 'Open'
let df5 = Frame.ofRecords values

(**
Finally, we can also load data frame from CSV:
*)
let msftCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv")
let fbCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/FB.csv")

(**
At the moment, this just stores all data as strings, but we could easily reuse 
CSV type inference and parse the data to an appropriate type - but when we later 
get the data, the data frame automatically converts the strings to the required type, 
so it will work as it is - but it will be slower.

Reindexing & joins
------------------

The loaded stock prices are automatically indexed with ordinal numbers, but we would
like to align them using their dates (in case there are some values missing). This
can be done by setting the row index to the "Date" column. Once we set the date as the
index, we also need to order the index. The Yahoo prices are ordered from the newest to
the oldest, but our data-frame requires ascending ordering.

When a frame has ordered index, we can use additional functionality that will be needed 
later (for example, we can select sub-range by specifying dates that are not explicitly 
included in the index).
*)

// Use the Date column as the index for the data frame
let msftDate = msftCsv.WithRowIndex<DateTime>("Date")
let msftOrd = msftDate.WithOrderedRows()

(**
Now that we have properly indexed stock prices, we can create a new data frame that
only has the data we're interested (Open & Close) prices and we add a new column 
that shows their difference:
*)

// Create data frame with just Open and Close prices
let msft = msftOrd.Columns.["Open", "Close"] |> Frame.ofColumns

// Add new column with the difference between Open & Close
msft?Difference <- msft?Open - msft?Close

// Do the same thing for Facebook
let fb = 
  fbCsv.WithRowIndex<DateTime>("Date").WithOrderedRows().Columns.["Open", "Close"] 
  |> Frame.ofColumns
fb?Difference <- fb?Open - fb?Close

// Now we can easily plot the differences
Chart.Combine
  [ Chart.Line(msft?Difference.Observations) 
    Chart.Line(fb?Difference.Observations) ]

(**
Next we would like to create a single data frame that contains (properly aligned) data
for both Microsoft and Facebook. This is done using the `Join` method - but before we
can do this, we need to rename their columns, because duplicate keys are not allowed:
*)

// Change the column names so that they are unique
let msftRen = msft.WithColumnIndex(["MsftOpen"; "MsftClose"; "MsftDiff"])
let fbRen = fb.WithColumnIndex(["FbOpen"; "FbClose"; "FbDiff"])

// Outer join (align & fill with missing values)
let joinedOut = msftRen.Join(fbRen, kind=JoinKind.Outer)

// Inner join (remove rows with missing values)
let joinedIn = msftRen.Join(fbRen, kind=JoinKind.Inner)

// Visualize daily differences on available values only
Chart.Rows
  [ Chart.Line(joinedIn?MsftDiff.Observations) 
    Chart.Line(joinedIn?FbDiff.Observations) ]

(**
Indexing & slicing
------------------

The data frame provides two key properties that we can use to access the data. The 
`Rows` property returns a series containing individual rows (as series) and `Columns`
returns a series containing columns (as series). We can then use various indexing and
slicing operators on the series:
*)

// Look for a row at a specific date
joinedIn.Rows.[DateTime(2013, 1, 2)]
// Look for a row at a specific date & get opening FB price
joinedIn.Rows.[DateTime(2013, 1, 2)]?FbOpen

(**

The return type of the first expression is `ObjectSeries<string>` which is inherited from
`Series<string, obj>` and represents an untyped series. We can use `GetAs<int>("FbOpen")` to
get a value for a specifed key and convert it to a required type (or `TryGetAs`). The untyped
series also hides the default `?` operator (which returns the value using the statically known
value type) and provides `?` that automatically converts anything to `float`.

In the previous example, we used an indexer with a single key. You can also specify multiple
keys (using a tuple) or a range (using the slicing syntax):
*)

// Get values corresponding to first three days of January 2013
let jan234 = joinedIn.Rows.[DateTime(2013, 1, 2), DateTime(2013, 1, 3), DateTime(2013, 1, 4)] |> Frame.ofRows
jan234?MsftOpen |> Series.mean

// Get values corresponding to January 2013
let jan = joinedIn.Rows.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)] |> Frame.ofRows
jan?FbOpen |> Series.mean
jan?MsftOpen |> Series.mean

(**
The result of the indexing operation is a single data series when you use just a single
date (the previous example) or a series of series when you specify multiple indices or a 
range (this example). Here, we always convert the structure back to a data frame using 
`Frame.FromRows` (you could also transpose it using `Frame.FromColumns`).

The `Series` module used here should (in the future) include more useful functions for working
with data series - but for now, it just contains `mean` and `sum`.

Note that the slicing using range (the second case) does not actually generate a sequence
of dates from 1 January to 31 January - it passes these to the index. Because our data frame
has an ordered index, the index looks for all keys that are greater than 1 January and smaller
than 31 January (this matters here, because the data frame does not contain 1 January - the 
first day is 2 January)

Indexing and joining ordered series
-----------------------------------

As already mentioned, if we have an ordered series or an ordered data frame, then we can
leverage the ordering in a number of ways. In the previous example, slicing used lower
and upper bounds rather than exact matching. Similarly, it is possible to get nearest
smaller (or greater) element when using direct lookup.

For example, let's create two series with 10 values for 10 days. The `daysSeries` 
contains keys starting from `DateTime.Today` (12:00 AM) and `obsSeries` has dates
with time component set to the current time (this is wrong representation, but it 
can be used to ilustrate the idea):
*)

let daysSeries = Series(dateRange DateTime.Today 10, rand 10)
let obsSeries = Series(dateRange DateTime.Now 10, rand 10)

(**
The indexing operation written as `daysSeries.[date]` uses _exact_ semantics so it will 
fail if the exact date is not available. When using `Get` method, we can provide an
additional parameter to specify the required behaviour:
*)

// Fails, because current time is not present
daysSeries.[DateTime.Now]
obsSeries.[DateTime.Now]

// This works - we get the value for DateTime.Today (12:00 AM)
daysSeries.Get(DateTime.Now, LookupSemantics.NearestSmaller)
// This does not - there is no nearest smaller value than Today 12:00 AM
obsSeries.Get(DateTime.Today, LookupSemantics.NearestSmaller)

(**
Similarly, you can specify the semantics when calling `TryGet` (to get an optional value)
or when using `GetItems` (to lookup multiple keys at once). Note that this behaviour is
only supported for series or frames with ordered index. For unordered, all operations use
the exact semantics.

The semantics can be also specified when using left or right join on data frames. To 
demonstrate this, let's create two data frames with columns indexed by 1 and 2, respectively:
*)

let daysFrame = [ 1 => daysSeries ] |> Frame.ofColumns
let obsFrame = [ 2 => obsSeries ] |> Frame.ofColumns

// All values in column 2 are missing (because the times do not match)
let obsDaysExact = daysFrame.Join(obsFrame, kind=JoinKind.Left)

// All values are available - for each day, we find the nearest smaller
// time in the frame indexed by later times in the day
let obsDaysPrev = daysFrame.Join(obsFrame, kind=JoinKind.Left, lookup=LookupSemantics.NearestSmaller)

// The first value is missing (because there is no nearest value with 
// greater key - the first one has the smallest key) but the rest is available
let obsDaysNext = daysFrame.Join(obsFrame, kind=JoinKind.Left, lookup=LookupSemantics.NearestGreater)

(**
The optional parameter `?lookup` is ignored when the join `?kind` is other
than `Left` or `Right`. Also, if the data frame is not ordered, the behaviour 
defaults to exact matching.

Projection and filtering
------------------------

A series provides `Where` and `Select` methods (there should be `map` and `filter` in the
`Series` module too) for projection and filtering. These are not available directly on 
data frame, so you always need to write `df.Rows` or `df.Columns` (depending on which one
you want). The following adds a new Boolean column that is true when the MSFT opening
price is greater than FB:
*)

joinedOut?Comparison <- joinedOut.Rows.Select(fun kv -> 
  kv.Value?MsftOpen > kv.Value?FbOpen)

joinedOut.GetSeries<bool>("Comparison").Where(fun kv -> kv.Value).Count
joinedOut.GetSeries<bool>("Comparison").Where(fun kv -> not kv.Value).Count

(**
Once we add the series, we can get it as a series of booleans and count `true` and
`false` values (there should be some nice pivoting to make this easier). We do not
use `joinedOut?Comparison` because then we would get object series and we would have
to write `unbox kv.Value`.

But - this is actually not as easy as it looks - the problem is that `joinedOut.Rows` returns
all rows, including those where some value is missing (because we are using `joinedOut`
which is the result of the outer join). So, in some cases `kv.Value?FbOpen` throws an
exception - this is caught by `Select` and turned into a missing value.

Here, we should probably have used `joinedIn` which only has rows where the values are 
available. But if you want to work with data frame that has missing values, there are
some other options (which should be faster):

*)
joinedOut.RowsDense.Where(fun kv -> kv.Value?MsftOpen > kv.Value?FbOpen).Count
joinedOut.RowsDense.Where(fun kv -> kv.Value?MsftOpen < kv.Value?FbOpen).Count

let joinedOpens = joinedOut.Columns.["MsftOpen", "FbOpen"] |> Frame.ofColumns 
joinedOpens.RowsDense.Where(fun kv -> kv.Value?MsftOpen > kv.Value?FbOpen).Count

(**
First of all, the first two lines use the `RowsDense` property instead of `Rows`. This
behaves similarly to `Rows`, but it first checks each row and when the row contains
some missing values, then it skips it. This means that accessing `MsftOpen` and 
`FbOpen` will never fail.

The problem is that this would skip rows that have missing values in columns that we
do not really need (e.g. `MsftClose`). So, to do this more properly, we should first
build a data frame that contains just the values we actually need - this is done on
the last two lines.

*)