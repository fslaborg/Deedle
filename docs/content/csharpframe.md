Data frame manipulation in C#
=============================

In this section, we look at working with Deedle data frame. Data frame lets you manipulate and
analyze data consisting of multiple features (properties) with multiple observations (records).
You can think of data frame as a data table or a spreadsheet. When working with data frames, you'll
often need to work on individual series (either rows or columns) of the frame, so it is recommended
to look at the [page discussing series](csharpseries.html) first.

You can also get the samples on this page as a [C# source file](https://github.com/BlueMountainCapital/Deedle/blob/master/docs/content/csharp/Frame.cs)
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

    [lang=csharp,file=csharp/Frame.cs,key=create-records]
    
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

    [lang=csharp,file=csharp/Frame.cs,key=create-rows]

Finally, you can also easily load data frames from a CSV file. The `Frame.ReadCsv` function

    [lang=csharp,file=csharp/Frame.cs,key=create-csv]

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

    [lang=csharp,file=csharp/Frame.cs,key=index-date]

The `IndexRows<T>(..)` method takes the name of the column that we want to use as an index and it also takes
a type parameter `T` that specifies the type of the column (because this is not statically known). We use
`DateTime` and benefit from the fact that the CSV reader already recognized the column type. Next, we sort
the entire data frame by the new row index using `OrderRows`.

The second part of the snippet renames the columns (using a mutating `RenameSeries` operation) so that the
column name includes the name of the company. We need this, because we later want to join the two data frames
and that is only possible when column keys do not overlap.



    [lang=csharp,file=csharp/Frame.cs,key=index-cols]


<a name="joining"></a>

Joining and aligning data frames
--------------------------------

    [lang=csharp,file=csharp/Frame.cs,key=join-inout]

a

    [lang=csharp,file=csharp/Frame.cs,key=join-lookup]


<a name="data"></a>

Accessing data and series operations
------------------------------------

    [lang=csharp,file=csharp/Frame.cs,key=series-dropadd]

a

    [lang=csharp,file=csharp/Frame.cs,key=series-get]

a

    [lang=csharp,file=csharp/Frame.cs,key=series-rows]

<a name="linq"></a>

LINQ to data frame
------------------

    [lang=csharp,file=csharp/Frame.cs,key=linq-select]

`SelectKeys`
and `SelectOptional`

    [lang=csharp,file=csharp/Frame.cs,key=linq-where]