(**

F# Data Frame
=============

This is the first prototype of F# Data Frame library that was discussed here earlier. 
For more information, please have a look at the following document:

 * [Structured Data Library for F#/C#](https://docs.google.com/document/d/1M_hQinAQQrxYm7Ajn7yj38vpnMj32WfEp372OLG00rU/edit)

The current prototype implements only basic functionality, but it (hopefully) provides
the right "core" internals that should make it easier to add all the additional 
(useful) features. Here are a few notes about the desing:

### Library internals

The following types are (mostly) not directly visible to the user, but you could
use them when extending the library: 

 * `IVector<'TAddress, 'TValue>` represents a vector (essentially an abstract data
   storage) that contains values `'TValue` that can be accessed via an address
   `'TAddress`. A simple concrete implementation is an array with `int` addresses,
   but we aim to make this abstract - one could use an array of arrays with `int64`
   index for large data sets, lazy vector that loads data from a stream or even 
   a virtual vector with e.g. Cassandra data source). 
   
   An important thing about vectors is that they handle missing values, so vector 
   of integers is actually more like `array<option<int>>` (but we have a custom value 
   type so that this is continuous block of memory). We decided that handling missing
   values is something that is so important for data frame, that it should be directly
   supported rather than done by e.g. storing optional or nullable values. Our 
   implementation actually does a simple optimization - if there are no missing values,
   it just stores `array<int>`.

 * `VectorConstruction<'TAddress>` is a discriminated union (DSL) that describes
   construction of vector. For every vector type, there is an `IVectorBuilder<'TAddress>`
   that knows how to construct vectors using the construction instructions (these 
   include things like re-shuffling of elements, appending vectors, getting a sub-range
   etc.)

 * `IIndex<'TKey, 'TAddress>` represents an index - that is, a mapping from keys
   of a series or data frame to addresses in a vector. In the simple case, this is just
   a hash table that returns the `int` offset in an array when given a key (e.g.
   `string` or `DateTime`). A super-simple index would just map `int` offsets to 
   `int` addresses via an identity function (not implemented yet!) - if you have
   series or data frame that is simply a list of recrods.

Now, the following types are directly used:

 * `Series<'TKey, 'TValue>` represents a series of values `'TValue` indexed by an
   index `'TKey`. Although this is not quite fully implemented yet, a series uses
   an abstract vector, index and vector builder, so it should work with any data
   representation. A series provides some standard slicing operators, projection, 
   filtering etc. There are also some binary operators (multiply by a scalar, add series, etc.)

 * `Frame<'TRowKey, 'TColumnKey>` represents a data frame with rows indexed using
   `TRowKey` (this could be `DateTime` or just ordinal numbers like `int`) and columns
   indexed by `TColumnKey` (typically a `string`). The data in the frame can be
   hetrogeneous (e.g. different types of values in different columns) and so accessing
   data is dynamic - but you can e.g. get a typed series.
   
   The operations available on the data frame include adding & removing series (which 
   aligns the new series according to the row index), joins (again - aligns the series) 
   etc. You can also get all rows as a series of (column) series and all columns as a 
   series of (row) series.

### Questions & call to action

As mentioned earlier, the current version is just a prototype. We're hoping that the 
design of the internals is now reasonable, but the end user API is missing most of
the important functions. So:

 * **Send comments & samples** - if you have some interesting problem that might be
   a good fit for data frames, then please share a sample or problem description so
   that we can make sure that we support all you might need. We plan to share the code
   publicly in ~1 week so you can submit pull requests then too! Also, if you have
   some particularly nice Scala/Python/R/Matlab code that you'd like to do in F#,
   share it too so that we can copy clever ideas :-).

 * **Time series vs. pivot table** - there is some mismatch between two possible
   interpretations and uses of the library. One is for time-series data (e.g. in finance)
   where one typically works with dates as row indices. More generally, you can see this
   as _continous_ index. It makes sense to do interpolation, sort the observations,
   align them, re-scale them etc.
   
   The other case is when we have some _discrete_ observations (perhaps a list of 
   records with customer data, a list of prices of different stock prices etc.) In this
   case, we need more "pivot table" functions etc.

   Although these two uses are quite different, we feel that it might make sense to use
   the same type for both (just with a different index). The problem is that this might
   make the API more complex. Although, if we can keep the distincion in the type, we can
   use F# 3.1 extension methods that extend just "discrete data frame" or "continous data 
   frame". Also, F# functions could be structured in two modules like `df |> Discrete.groupBy`
   and `df |> Continuous.interoplateMissingValues`.

 * **Immutability** - in the current version, a series is immutable data type, but a
   data frame supports limited mutation - you can add new series, drop a series & replace
   a series (but you cannot mutate the series). This seems to be useful because it works
   nicely with the `?<-` operator and you do not have to re-bind when you're writing some
   research script.

 * **Type provider** - we are thinking about using type providers to give some additional
   safety (like checking column names and types in a data frame). This is currently
   on the TODO list - I think we can do something useful here, although it will 
   certainly be limited. 

   The current idea is that you migth want to do some research/prototyping using a 
   dynamic data frame, but once you're done and have some more stable data, you should
   be able to write, say `DataFrame<"Open:float,Close:float">(dynamicDf)` and get a
   new typed data frame. 

If you have any comments regarding the topics above, please discuss them on the mailing
list - this is currently a prototype and we are certainly open to changing things.   

Tutorial & stock price demo
---------------------------

First, we need to load the DataFrame library - in F# Interactive, this is done by
loading an `.fsx` file that loads the actual `.dll` with the library and registers
pretty printers for types representing data frame and series. In this sample, we
also need F# Charting, which works similarly:

*)
#I "../bin"
#I "../lib"
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
let msftCsv = Frame.ReadCsv("http://ichart.finance.yahoo.com/table.csv?s=MSFT")
let fbCsv = Frame.ReadCsv("http://ichart.finance.yahoo.com/table.csv?s=FB")

(**
At the moment, this just stores all data as strings, but we could easily reuse 
CSV type inference and parse the data to an appropriate type - but when we later 
get the data, the data frame automatically converts the strings to the required type, 
so it will work as it is - but it will be slower.

Reindexing & joins
------------------

The loaded stock prices are automatically indexed with ordinal numbers, but we would
like to align them using their dates (in case there are some values missing). This
can be done by setting the row index to the "Date" column.
*)

// Use the Date column as the index for the data frame
let msftDate = msftCsv.WithRowIndex<DateTime>("Date")

(**
Now that we have properly indexed stock prices, we can create a new data frame that
only has the data we're interested (Open & Close) prices and we add a new column 
that shows their difference:
*)

// Create data frame with just Open and Close prices
let msft = msftDate.Columns.["Open", "Close"] |> Frame.FromColumns

// Add new column with the difference between Open & Close
msft?Difference <- msft?Open - msft?Close

// Do the same thing for Facebook
let fb = fbCsv.WithRowIndex<DateTime>("Date").Columns.["Open", "Close"] |> Frame.FromColumns
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

// Get values corresponding to January 2013
let jan = joinedIn.Rows.[DateTime(2013, 1, 31) .. DateTime(2013, 1, 2)] |> Frame.FromRows
jan?FbOpen |> Series.mean
jan?MsftOpen |> Series.mean

// Get values corresponding to first three days of January 2013
let jan234 = joinedIn.Rows.[DateTime(2013, 1, 2), DateTime(2013, 1, 3), DateTime(2013, 1, 4)] |> Frame.FromRows
jan234?MsftOpen |> Series.mean

(**

The result of the indexing operation is a single data series when you use just a single
date (the previous example) or a series of series when you specify multiple indices or a 
range (this example). Here, we always convert the structure back to a data frame using 
`Frame.FromRows` (you could also transpose it using `Frame.FromColumns`).

The `Series` module used here should (in the future) include more useful functions for working
with data series - but for now, it just contains `mean` and `sum`.

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

let joinedOpens = joinedOut.Columns.["MsftOpen", "FbOpen"] |> Frame.FromColumns 
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