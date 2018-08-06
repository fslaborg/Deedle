Data frame manipulation in C#
=============================

In this section, we look at working with Deedle data frame. Data frame lets you manipulate and
analyze data consisting of multiple features (properties) with multiple observations (records).
You can think of data frame as a data table or a spreadsheet. When working with data frames, you'll
often need to work on individual series (either rows or columns) of the frame, so it is recommended
to look at the [page discussing series](csharpseries.html) first.

You can also get the samples on this page as a 
[C# source file](https://github.com/fslaborg/Deedle/blob/master/docs/csharp/Frame.cs)
from GitHub and run the samples.

<a name="understanding"></a>

What is a data frame
--------------------

 * **Row and column key to values** - data frame is represented using a type `Frame<TRowKey, TColKey>` and
   you can view it as a mapping from row and column keys to values. Note that the values in data frame can
   be heterogeneous and Deedle does not track this information statically - when accessing column/row, you 
   need to explicitly specify the type of values you want to get (although Deedle makes this easier when
   you work with numeric data).

 * **Typical uses** - although you can use any type for column and row keys, the typical use is having column 
   keys of type `string` representing different (named) properties and row keys of type `int` (unique IDs)
   or `DateTimeOffset` for time series data.

 * **Series collection** - another way to look at data frame is that it is a collection of series with 
   the same (row) index. This is also how frames are represented internally, so using this intuition will 
   probably lead you to faster and more idiomatic code. For example, you can store multiple series with 
   different stock prices in a data frame and they will all be aligned to the same (row) index.

 * **Limited mutability** - the internal data structures of data frame are immutable (i.e. series and a type
   representing indices). However, when working with data frame, you can mutate the frame and add/remove 
   columns. When adding column, a new index is created and local field of the frame pointing to the index is
   updated, but no data series or indices (that may be shared by other types) are changed.
   This makes research-style operations more convenient and makes the library more practical.

<a name="creating"></a>

Creating and loading data frames
--------------------------------

Let's start with a number of examples showing how to create data frames. The most common scenario is that you
already have some code that reads the data - perhaps from a database or some other source - and you want 
to convert it to data frame. Any collection of .NET objects can be turned to data frame using `Frame.FromRecords`:

    [lang=csharp,file=../csharp/Frame.cs,key=create-records]
    
In this sample, we use simple LINQ construction to generate collection with anonymous types containing properties
`Key` and `Number`. The `FromRecords` method uses reflection to get public readable properties of the type and
so the result of the `Print` method looks as follows:

    [lang=text]
    -    Key   Number
    0 -> ID_0  997104221
    1 -> ID_1  50365464
    2 -> ID_2  1777994880
    (...)

As an alternative, you can also construct data frame by generating a collection of explicitly created rows. 
A row is just a series of type `Series<TColKey, TValue>` so you can use any of the techniques described in 
[creating series](csharpseries.html#creating). Here, we use `SeriesBuilder<string>` which is the easiest way
to create series imperatively by adding columns:

    [lang=csharp,file=../csharp/Frame.cs,key=create-rows]

Finally, you can also easily load data frames from a CSV file. The `Frame.ReadCsv` function

    [lang=csharp,file=../csharp/Frame.cs,key=create-csv]

The function automatically recognizes the names of columns (if the CSV file does not have headers, you can
specify optional parameter `hasHeaders:false`). It also infers the type of values, so that you can later work
with numeric columns in a standard way. Here, we are reading Yahoo stock prices, so the resulting frame looks
as follows:

    [lang=text]
    -       Date       Open  High  Low   Close Volume     Adj Close
    0    -> 2013-11-07 37.96 38.01 37.43 37.50 60437400   37.50
    1    -> 2013-11-06 37.24 38.22 37.06 38.18 88615100   38.18
    :       ...        ...   ...   ...   ...   ...        ...
    6972 -> 1986-03-17 29.00 29.75 29.00 29.50 133171200  0.08
    6973 -> 1986-03-14 28.00 29.50 28.00 29.00 308160000  0.07
    6974 -> 1986-03-13 25.50 29.25 25.50 28.00 1031788800 0.07

<a name="indices"></a>

Working with row and column indices
-----------------------------------

Reading data from CSV file or from .NET objects typically gives us data frame `Frame<int, string>` where the
rows are indexed by `int` (representing the number of the row) and columns are names (`string` values). 
When we want to combine data from multiple data sources or perform some further processing, this is not 
always what we need.

For example, for the MSFT and FB stock prices, we want the row index to be `DateTime` (so that we can 
align the prices based on dates) and we also need to order the rows (because aligning that we'll do in
the next step is only allowed on ordered frames and series):

    [lang=csharp,file=../csharp/Frame.cs,key=index-date]

The `IndexRows<T>(..)` method takes the name of the column that we want to use as an index and it also takes
a type parameter `T` that specifies the type of the column (because this is not statically known). We use
`DateTime` and benefit from the fact that the CSV reader already recognized the column type. Next, we sort
the entire data frame by the new row index using `OrderRows`.

The second part of the snippet renames the columns (using a mutating `RenameSeries` operation) so that the
column name includes the name of the company. We need this, because we later want to join the two data frames
and that is only possible when column keys do not overlap.

Before looking at the joining, let's look at one more example of loading data from a CSV file. This time,
the source file has ordered rows, but has poor header names, so we reanme the column names: 

    [lang=csharp,file=../csharp/Frame.cs,key=index-cols]

The `IndexColumnsWith` method takes a collection of names - here, we use C# array expression to specify
the names explicitly. Note that the names do not have to be `string` values. It is perfectly fine to use
other types as column indices. The resulting data set looks as follows:

    [lang=text]
    -       Year GDP     Population Debt   ?
    1900 -> 1900 20.567  76.212     19.60  i
    1901 -> 1901 22.269  77.680     18.60  i
    :       ...  ...     ...        ...    ...
    2013 -> 2013 16202.7 316.847    124.84 g
    2014 -> 2014 17011.4 319.594    125.16 g

<a name="joining"></a>

Joining and aligning data frames
--------------------------------

A common scenario is when you have multiple data sets from different data sources and want to join
them into a single data frame. For example, we earlier loaded stock prices for Microsoft and Facebook
into two data frames named `msft` and `fb`. To align the data, we can use one of the overloads of the
`Join` method.

The two data frames share the same keys (`DateTime` representing trading days), but their ranges
are different, because we have more historical data for Microsoft. We can perform _inner_ or 
_outer_ join as follows:

    [lang=csharp,file=../csharp/Frame.cs,key=join-inout]

When using inner join, the resulting data frame will contain only keys that are available in both
of the source frames. On the other hand, outer join takes the union of the keys and marks all 
other values as missing. Note that the column keys of the two joined frames need to be distinct
- we guaranteed this earlier by calling `RenameSeries`.

Another option that is available lets you align (and join) two ordered data frames where the keys
do not exactly match. The following snippet demonstrates this by shifting one of the data frames
by 1 hour (the keys are always at 12:00am, representing just time)

    [lang=csharp,file=../csharp/Frame.cs,key=join-lookup]

After calculating the `msftShift` frame, we first try using just an ordinary left join. This should
align data from the right frame to the keys in the left data frame (`fb`). However, this produces
frame where all Microsoft values are missing, because the frame does not contain any data for exactly
the same keys.

The problem can be easily solved by using overload that takes `Lookup` - using `Lookup.NearestSmaller`,
we specify that, for a given key, the join operation should find the nearest available value with a 
smaller key. So for example, given a key 12:00am at 23 January 2012 (in the `fb` frame), the operation
will find values for a key 1:00am at 22 January 2012 (because this is the nearest smaller key with
a value). You can also use `Lookup.NearestGreater` to search in the opposite direction.

<a name="data"></a>

Accessing data and series operations
------------------------------------

Now that we looked at loading (or generating) data and combining data from multiple data sources,
let's look how we can obtain data from the data frame. First, we look at getting data for a specified
column - this allows you to get `Series<K, V>` where `K` is the row key and `V` is a type of values
in the series. When getting a series, you need to specify the required type of values:

    [lang=csharp,file=../csharp/Frame.cs,key=series-get]

Here, we get values as `double` (which matches with the internal representation), however data frame
will attempt to automatically convert the data to the specified type, so we could get the series as
a series of `decimal` or `single` values. 

The last line calculates the difference between opening and closing price. We can perform a few more
mutations on the original data frame and remove two series we do not use (using `DropSeries`) and
add the difference as a new series (using `AddSeries`):

    [lang=csharp,file=../csharp/Frame.cs,key=series-dropadd]

For more information about working with series, see [tutorial on working with series](csharpseries.html).
Working with series is very common, so the data frame provides the operations discussed above. However,
you can also work with columns and rows of the frame (more generally) using the `Rows` and `Columns`
properties.

The following example shows different options for getting row representing a specified date:

    [lang=csharp,file=../csharp/Frame.cs,key=series-rows]

We start by using indexer on `joinIn.Rows`. This can be used when the exact key (here January 4)
exists in the data frame. The result is a series containing `object` values, because the contents
of a row is often heterogeneous. To get a specified column, you can use `GetAs`, which casts the
value to a specified type. You can access columns similarly using `joinIn.Columns`.

The second part of the snippet shows the `Get` method, which behaves similarly to the indexer,
but has an additional parameter that can be used to specify `Lookup`. Similarly to joining, this
can be used (on an ordered frame) to find the nearest available value when the exact key is not
present (or has no value).

Finally, the data frame also supports indexer directly, which can be used to get a numeric value
for a given pair of row and column keys. This is just a useful shortcut that can be used instead
of the indexer and `GetAs` when you need to obtain a numeric value for a fixed pair of keys.

<a name="linq"></a>

LINQ to data frame
------------------

The type representing a collection of rows and columns (obtained using `df.Rows` and `df.Columns`)
implements some of the well-known LINQ operations. These can be used to transform data in the
frame or filter the contents. The `Select` operation can be used when you need to perform some 
operation that is not directly available on series. For example, to perform point-wise comparison
of Microsoft and Facebook stock prices, you can write:

    [lang=csharp,file=../csharp/Frame.cs,key=linq-select]

The result is a series of type `Series<DateTime, bool>` - the return type is inferred to be `bool`
(because that's what the lambda function returns) and the `Select` method typically returns just
a single value, so the result is a series. However, you could also return a new series and then
use `Frame.FromRows` to re-create a frame.

The library also provides `SelectKeys`, which can be used to transform the row (or column) keys
and `SelectOptional` which can be used to explicitly handle missing values in the data frame.

If we wanted to find only the days when Microsoft stock prices were more expensive than Facebook
stock prices (and create a new frame containing such data), we can use the other familiar LINQ
method, called `Where`:

    [lang=csharp,file=../csharp/Frame.cs,key=linq-where]

The result of the filtering is a series containing individual rows. Such nested series can be turned
back into data frame using `Frame.FromRows`. Now you could use the `RowCount` property to compare
the number of days when Microsoft was more expensive with the number of days when Facebook price
was higher.

Calculating with data frames
----------------------------

Finally, we can also write calculations that work over the entire data frame. The methods are
similar to the methods for calculating with series [discussed in another article](csharpseries.html).
We look at a single example that calculates daily returns of Microsoft stock prices and then applies
rounding to all values in the resulting data frame.

    [lang=csharp,file=../csharp/Frame.cs,key=ops-returns]

To calculate daily returns, we need to subtract the price on previous day from the price on the
current day. This is done by using the `Diff` extension method (another option is to use `Shift` 
together with the overloaded subtraction operator). Then we divide the difference by the current
price and multiply the result by 100.0 to get value in percents.

Implementing the rounding is slightly more complicated - there is no built-in function for doing
this, so we need to implement it using other operations. The `SeriesApply` operation is similar
to `Select`, but it transforms entire columns at once. The operation is applied to all columns of
a specified type - in the above example, we specify the type `double` by using an explicit type
specification on the lambda function. For each numeric series, we then use the `Select` method
to round the value to two fractional digits.

Ignoring a number of columns from the frame, the result looks something like follows:

    [lang=text]
    -            MsftDate    MsftOpen   MsftClose  
    3/13/1986 -> 1986-03-13  <missing>  <missing>  
    3/14/1986 -> 1986-03-14  8.93       3.45       
    3/17/1986 -> 1986-03-17  3.45       1.69       
    :            ...         ...        ...        
    11/6/2013 -> 2013-11-06  3.89       4.03       
    11/7/2013 -> 2013-11-07  1.9        -1.81      

It is worth noting that the `SeriesApply` function is applied on all numerical columns, but
all other columns (such as `MsftDate`) are left unchanged. You can also see that the first 
row does not contian any value (and is explicitly marked as missing). This is because there is
no value for the previous day and so daily return is not defined.
You could fill the missing values using the overloaded `FillMissing` method or drop the row
using `DropSparseRows` method.