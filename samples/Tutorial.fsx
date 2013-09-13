(**
F# DataFrame in 10 minutes
==========================

The first step is to install `FSharp.DataFrame.dll` [from NuGet](https://www.nuget.org/packages/FSharp.DataFrame).
Next, we need to load the library - in F# Interactive, this is done by loading 
an `.fsx` file that loads the actual `.dll` with the library and registers 
pretty printers for types representing data frame and series. In this sample, 
we also need  [F# Charting](http://fsharp.github.io/FSharp.Charting), which 
works similarly:

*)
#I "../bin"
#I "../packages/FSharp.Charting.0.84"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"

open System
open FSharp.DataFrame
open FSharp.Charting

(**
<a name="creating"></a>

Creating series and frames
--------------------------

A data frame is a collection of series with unique column names (although these
do not actually have to be strings). So, to create a data frame, we first need
to create a series:
*)

// Create from sequence of keys and sequence of values
let dates  = [ DateTime(2013,1,1); DateTime(2013,1,4); DateTime(2013,1,8) ]
let values = [ 10.0; 20.0; 30.0 ]
let first = Series(dates, values)

// Create from a single list of observations
Series.ofObservations
  [ DateTime(2013,1,1) => 10.0
    DateTime(2013,1,4) => 20.0
    DateTime(2013,1,8) => 30.0 ]
// [fsi:val it : Series<DateTime,float> =]
// [fsi:  1/1/2013 12:00:00 AM -> 10 ]
// [fsi:  1/4/2013 12:00:00 AM -> 20 ]
// [fsi:  1/8/2013 12:00:00 AM -> 30 ]

// Create series with implicit (ordinal) keys
Series.ofValues [ 10.0; 20.0; 30.0 ]
// [fsi:val it : Series<int,float> =]
// [fsi:  0 -> 10 ]
// [fsi:  1 -> 20 ]
// [fsi:  2 -> 30 ]

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
// [fsi:val it : Series<DateTime,float> =]
// [fsi:  1/1/2013 12:00:00 AM  -> 0.957571796121808  ]
// [fsi:  1/2/2013 12:00:00 AM  -> 0.424694305017914  ]
// [fsi:  ...                      ...              ]
// [fsi:  1/9/2013 12:00:00 AM  -> 0.874667080526551  ]
// [fsi:  1/10/2013 12:00:00 AM -> 0.308210378656262 ]

(**
Now we can easily construct a data frame that has two columns - one representing
the `first` series and another representing the `second` series:
*)

let df1 = Frame(["first"; "second"], [first; second])
// [fsi:val it : Frame<DateTime,strubg> =]
// [fsi:                           first     second             ]
// [fsi:  1/1/2013 12:00:00 AM ->  10        0.287487044598668  ]
// [fsi:  1/2/2013 12:00:00 AM ->  <missing> 0.66628182011949   ]
// [fsi:  ...                      ...       ...              ]
// [fsi:  1/10/2013 12:00:00 AM -> <missing> 0.104357822846788  ]

(** 
The type representing a data frame has two generic parameters:
`Frame<TRowKey, TColumnKey>`. The first parameter is represents the type of
row keys - this can be `int` if we do not give the keys explicitly or `DateTime`
like in the example above. The second parameter is the type of column keys.
This is typically `string`, but sometimes it is useful to can create a 
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
let msftCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv")
let fbCsv = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/FB.csv")
// [fsi:val fbCsv : Frame<int,string> =]
// [fsi:         Date       Open  High  Low   Close Volume    Adj Close ]
// [fsi:  0 ->   2013-08-30 42.02 42.26 41.06 41.29 67587400  41.29     ]
// [fsi:  1 ->   2013-08-29 40.89 41.78 40.80 41.28 58303400  41.28     ]
// [fsi:  :      ...        ...   ...   ...   ...   ...       ...       ]
// [fsi:  321 -> 2012-05-21 36.53 36.66 33.00 34.03 168192700 34.03     ]
// [fsi:  322 -> 2012-05-18 42.05 45.00 38.00 38.23 573576400 38.23     ]

(**
When loading the data, data frame analyses the values and automatically converts
them to the most appropriate type. However, no conversion is automatically performed
for dates and times - the user needs to decide what is the desirable representation
of dates (e.g. `DateTime`, `DateTimeOffset` or some custom type).

<a name="reindexing-and-joins"></a>

Specifying index and joining
----------------------------

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
  msftCsv.WithRowIndex<DateTime>("Date")
  |> Frame.orderRows

(**
The `WithRowIndex` operation is written using a method call, because this makes
it possible to easily specify the required type of the key - in the above
snippet, we parse the column into `DateTime` values. Then we use a function
from the `Frame` module which contains a large number of useful functions.

Now that we have properly indexed stock prices, we can create a new data frame that
only has the data we're interested (Open & Close) prices and we add a new column 
that shows their difference:
*)

// Create data frame with just Open and Close prices
let msft = msftOrd.Columns.[ ["Open"; "Close"] ] |> Frame.ofColumns

// Add new column with the difference between Open & Close
msft?Difference <- msft?Open - msft?Close

// Do the same thing for Facebook
let fb = 
  fbCsv.WithRowIndex<DateTime>("Date").WithOrderedRows().Columns.[ ["Open"; "Close"] ]
  |> Frame.ofColumns
fb?Difference <- fb?Open - fb?Close

// Now we can easily plot the differences
Chart.Combine
  [ Chart.Line(msft?Difference.Observations) 
    Chart.Line(fb?Difference.Observations) ]

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
As a result, you should see a chart that looks something like this:

<div style="text-align:center">
<img src="content/images/tutorial-chart.png" />
</div>

<a name="selecting"></a>

Selecting values and slicing
----------------------------

The data frame provides two key properties that we can use to access the data. The 
`Rows` property returns a series containing individual rows (as a series) and `Columns`
returns a series containing columns (as a series). We can then use various indexing and
slicing operators on the series:
*)

// Look for a row at a specific date
joinedIn.Rows.[DateTime(2013, 1, 2)]
// [fsi:val it : ObjectSeries<string> =]
// [fsi:  FbOpen    -> 28.00            ]  
// [fsi:  FbClose   -> 27.44   ]           
// [fsi:  FbDiff    -> -0.5599 ]
// [fsi:  MsftOpen  -> 27.62   ]           
// [fsi:  MsftClose -> 27.25    ]          
// [fsi:  MsftDiff  -> -0.3700 ]

// Get opening Facebook price for 2 Jan 2013
joinedIn.Rows.[DateTime(2013, 1, 2)]?FbOpen
// [fsi:val it : float = 28.0]

(**

The return type of the first expression is `ObjectSeries<string>` which is inherited from
`Series<string, obj>` and represents an untyped series. We can use `GetAs<int>("FbOpen")` to
get a value for a specifed key and convert it to a required type (or `TryGetAs`). The untyped
series also hides the default `?` operator (which returns the value using the statically known
value type) and provides `?` that automatically converts anything to `float`.

In the previous example, we used an indexer with a single key. You can also specify multiple
keys (using a list) or a range (using the slicing syntax):
*)

// Get values for the first three days of January 2013
let jan234 = 
  joinedIn.Rows.[ [ DateTime(2013, 1, 2); DateTime(2013, 1, 3);
                    DateTime(2013, 1, 4)] ] |> Frame.ofRows
// Calculate mean of Open price for 3 days
jan234?MsftOpen |> Series.mean

// Get values corresponding to entire January 2013
let jan = 
  joinedIn.Rows.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)] 
  |> Frame.ofRows
// [fsi:val jan : Frame<DateTime,string> =]
// [fsi:               FbOpen FbClose FbDiff MsftOpen MsftClose MsftDiff ]
// [fsi:  1/2/2013  -> 28.00  27.44   -0.55  27.62    27.25     -0.37 ]
// [fsi:  1/3/2013  -> 27.77  27.88   0.10   27.25    27.63     0.37 ]
// [fsi:  ...       -> ...    ...     ...    ...      ...       ... ]
// [fsi:  1/30/2013 -> 31.24  30.98   -0.25  27.85    28.01     0.16 ]              
// [fsi:  1/31/2013 -> 30.98  29.15   -1.83  27.45    27.79     0.34  ]              

// Calculate means over the period
jan?FbOpen |> Series.mean
jan?MsftOpen |> Series.mean

(**
The result of the indexing operation is a single data series when you use just a single
date (the previous example) or a series (of series) when you specify multiple indices or a 
range (this example). Here, we always convert the structure back to a data frame using 
`Frame.ofRows` (you could also transpose it using `Frame.ofColumns`).

The `Series` module used here includes more useful functions for working
with data series, including (but not limited to) statistical functions like `mean`,
`sdv` and `sum`.

Note that the slicing using range (the second case) does not actually generate a sequence
of dates from 1 January to 31 January - it passes these to the index. Because our data frame
has an ordered index, the index looks for all keys that are greater than 1 January and smaller
than 31 January (this matters here, because the data frame does not contain 1 January - the 
first day is 2 January)

<a name="timeseries"></a>

Using ordered time series
-------------------------

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
// [fsi:  ]
// [fsi:val daysSeries : Series<DateTime,float> =]
// [fsi:  9/12/2013 12:00:00 AM -> 0.834164640323336  ]
// [fsi:  9/13/2013 12:00:00 AM -> 0.871085483520797  ]
// [fsi:  ...                   -> ...                ]
// [fsi:  9/21/2013 12:00:00 AM -> 0.939047194523293  ]
// [fsi:  ]
// [fsi:val obsSeries : Series<DateTime,float> =]
// [fsi:  9/12/2013 2:38:27 PM -> 0.433889241625503 ]
// [fsi:  9/13/2013 2:38:27 PM -> 0.318957257233075 ]
// [fsi:  ...                   -> ...               ] 
// [fsi:  9/21/2013 2:38:27 PM -> 0.227506533836716 ]

(**
The indexing operation written as `daysSeries.[date]` uses _exact_ semantics so it will 
fail if the exact date is not available. When using `Get` method, we can provide an
additional parameter to specify the required behaviour:
*)

// Fails, because current time is not present
daysSeries.[DateTime.Now]
obsSeries.[DateTime.Now]

// This works - we get the value for DateTime.Today (12:00 AM)
daysSeries.Get(DateTime.Now, Lookup.NearestSmaller)
// This does not - there is no nearest key <= Today 12:00 AM
obsSeries.Get(DateTime.Today, Lookup.NearestSmaller)

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
let obsDaysPrev = daysFrame.Join(obsFrame, kind=JoinKind.Left, lookup=Lookup.NearestSmaller)

// The first value is missing (because there is no nearest value with 
// greater key - the first one has the smallest key) but the rest is available
let obsDaysNext = daysFrame.Join(obsFrame, kind=JoinKind.Left, lookup=Lookup.NearestGreater)

(**
The optional parameter `?lookup` is ignored when the join `?kind` is other
than `Left` or `Right`. Also, if the data frame is not ordered, the behaviour 
defaults to exact matching.

<a name="projections"></a>

Projection and filtering
------------------------

For filtering and projection, series provides `Where` and `Select` methods and 
corresponding `Series.map` and `Series.filter` functions. On data frame

These are not available directly on 
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
