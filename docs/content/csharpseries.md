Time series manipulation in C#
==============================

In this section, we look at Deedle features that are useful when working with series data 
in C#. A series can be either ordered (e.g. time series) or unordered. Although we mainly
look at operations on the `Series` type here, many of the operations can be applied to 
data frame `Frame` containing multiple series. Furthermore, data frame provides an elegant 
way for aligning and joining series. 

You can also get the samples on this page as a 
[C# source file](https://github.com/fslaborg/Deedle/blob/master/docs/csharp/Series.cs)
from GitHub and run the samples.

<a name="understanding"></a>

What is a series
----------------

 * **Key value mapping** - a series is represented by a type `Series<K, V>` from the `Deedle` namespace. The type
  represents a data series mapping keys of type `K` to values of type `V`. There are no 
  restriction on the types of keys and values, but some operations are only available for
  keys that can be ordered (implement the `IComparable<K>` interface).

 * **Typical uses** - typical keys include `int` for _ordinally_ indexed keys and `DateTimeOffset` when working 
  with time series. The most common types of values are `double` or `decimal` for numeric
  data. Another common use of series is with keys of type `string` and values of type `object`
  to represent heterogeneous data set - typically a column in a data frame that stores
  multiple named properties of different types.

 * **Immutable** - the type `Series<K, V>` is _immutable_. Once you create a series object, it cannot be 
  changed. All operations that operate on series return a copy (and typically also copy
  the data of the series, although this is an internal aspect of the implementation). So,
  working with series is similar to workinig with .NET `string` type or with the 
  `IEnumerable<T>` type using LINQ.

 * **Missing values** - series is desinged to automatically support and handle missing
  data. This means that you can create a series where values are missing for some keys
  (e.g. when data is not available) and then handle missing values (provide defaults or
  fill with previous values). All series operations automatically propagate or handle
  missing data.

<a name="creating"></a>

Creating and loading series
---------------------------

Once you referenced the [Deedle NuGet package](http://nuget.org/packages/Deedle) and opened
the `Deedle` namespace, you can create series in a number of ways. The Deedle library implements
the builder object pattern, so if you want to create a series explicitly, you can use
the generic `SeriesBuilder` type.

### Using series builder

If you want to create a series with explicitly given list of key-value pairs, you can use the C# 
collection initializer syntax and `SeriesBuilder<K, V>`. The series builder exposes a property
`Series` that returns (a cloned) series containing the values added so far:

    [lang=csharp,file=../csharp/Series.cs,key=create-builder]

The `SeriesBuilder<K, V>` type implements the `Add` method, so you can also easily use it 
if you want to add elements one by one in a loop. The above snippet uses extension method
`Print` to output the series to a console. In this case, the output will look as follows:

    [lang=text]
    1 -> one
    2 -> two
    3 -> three

Another feature supported by the series builder is the C# `dynamic` keyword. If you want to 
create a series that maps `string` keys to values (e.g. when building a row that you want
to append to a data frame), you can do so as follows:

    [lang=csharp,file=../csharp/Series.cs,key=create-heterogen]

Here, we assing `SeriesBuilder<K, V>` to a variable `nameNumsDyn` of type `dynamic` and
use property setter syntax to add values for strng keys `One`, `Two` and `Three`. Then we
convert the original series builder to a series and print it.

### Converting collections to series

Using the series builder is useful if you want to create series with some data explicitly from
code. However, more commonly, you already have the data you want to use in some collection.
In that case, you can use one of the extension methods exposed by Deedle. 

If you only care about the values, you can use `ToSeriesOrdinal` which s defined for any
`IEnumerable<V>` and automatically generates keys of type `int`. For example, here we create
a series containing random `double` values:

    [lang=csharp,file=../csharp/Series.cs,key=create-ordinal]

If you want to create a series with specified keys and values, you can use extension method
`ToSeries` on `IEnumerable<KeyValuePair<K, V>>`. The following snippet uses a helper method
`KeyValue.Create` that is exposed by Deedle and makes it easier to create key value pairs:

    [lang=csharp,file=../csharp/Series.cs,key=create-kvp]

To create a series where values are missing for some keys, you need to
use the type `OptionalValue<K>`. You can use two C#-friendly methods - to create an empty 
value, you can use `OptionalValue.Empty<T>()` and to convert a value `value` to optional,
use `OptionalValue.Create(value)`. Alternatively, you can also use `OptionalValue.OfNullable`:

    [lang=csharp,file=../csharp/Series.cs,key=create-sparse]

Note that the sample uses extension method `ToSparseSeries` to indicate that we are creating
series from a collection of key value pairs where the values may be missing. The resulting 
series has a type `Series<int, int>` (the fact that there are missing values has no effect
on the type).

Finally, our last example uses the `Frame` type (you can find more about it in a [separate data 
frame tutorial](csharpframe.html). We load data frame from a given CSV file, specify that we
want to use the "Date" column as the index of type `DateTime`, order the rows and then get a 
time series representing the "Open" column:

    [lang=csharp,file=../csharp/Series.cs,key=create-csv]

The result is an ordered time series of type `Series<DateTime, float>` that we'll use in some
of the later examples in this tutorial.

<a name="lookup"></a>

Lookup and slicing
------------------

Given a series, the first thing that we might want to do is to access the data in the series.
In this section, we look at _lookup_ operations that can be used to retrieve values from
series and _slicing_ operations that give us a sub-series.

### Lookup by key and index

A series supports C# indexer that takes the series _key_ as an argument. Given a series
`Series<K, V>` and a key of type `K`, you can access the associated value using indexer.
Series also supports random access using index, which can be done using the `GetAt` method:

    [lang=csharp,file=../csharp/Series.cs,key=lookup-key]

Accessing an element may fail for two reasons. When the key is not present in the series,
you get `KeyNotFoundException`. When the key is present, but the series does not contain
any value for the key, the access operations throw `MissingValueException` (defined in 
`Deedle` namespace). To avoid handling exceptions, you can use `TryGet` and `TryGetAt` 
methods that return the result as `OptionalValue<T>`:

    [lang=csharp,file=../csharp/Series.cs,key=lookup-opt]

As the snippet shows, `OptionalValue<T>` can be processed easily using `HasValue` and `Value`
properties. If the value contained in a series is a value type, then you can also turn the
result into a more convenient nullable type using `AsNullable` extension method.

### Lookup and slicing for ordered series

The operations discussed so far work on any series. However, more is available if the series 
is ordered (e.g. time series representing MSFT stock prices that we loaded in the previous 
section).

First of all, if the value is not available for a specified key (say January 1, 2012) then
we can ask the series to give us the value for nearest greater or smaller key that has a value.
This is done using the `Get` method (which behaves as the indexer in the simple case):

    [lang=csharp,file=../csharp/Series.cs,key=lookup-ord]

Even though no value is available for January 1, 2012 (because it was not a business day),
the last two operations succeed and return a value.

The next set of operations that are available on ordered series perform _slicing_. Given a
series representing the entire history of Microsoft stock prices (from 1975 to the present
date), we can easily get a sub-series that represents values only for some sub-range of the
original dates:

    [lang=csharp,file=../csharp/Series.cs,key=lookup-slice]
    
An important aspect of the slicing operations is that they can operate on _lazily loaded_
series without evaluating it. For example, you can create a series that represents data from
a database and then perform slicing without fetching the data. The fetching will only happen
when other operations are performed and it will only fetch the data needed. For more information,
see [lazy data loading tutorial](lazysource.html).


<a name="calc"></a>

Statistics and calculations
---------------------------

If a series contains numeric values (typically `double`) then we can perform various statistical
operations and calculations with the series. The Deedle library supports standard numeric 
operators for series, basic statistical calculations (as extension methods) and it also gives
you access to the underlying observations, in case you need to implement some calculation
that is not directly supported.

The following example demonstrates the basic functionality by calculating the mean price of
Microsoft stock prices over 2012 and then calculating the sum of squared differenc from the
mean:

    [lang=csharp,file=../csharp/Series.cs,key=calc-stat]

The snippet first uses extension method `Mean` (and also `Median`). Then it subtracts scalar
value (number `msftAvg`) from a series (`msft2012`) to get a new series where each value is 
the result of subtracting the scalar from an original value. 

The next line applies point-wise multiplication on two series - the result is a series where
a value at each key is the multiplication of values at the same key in the two multiplied
series. Finally, we use `Sum` to add all the differences.

> **Missing values.** Note that all numerical operations on the `Series<K, V>` type 
> carefuly handle missing data. If you have a series where value is not available for
> some dates, then the value is skipped when calculating statistics such as mean or
> sum. Point-wise and scalar operators automatically propagate missing data. When
> calculating `s1 + s2` and one of the series does not contain data for a key `k`, then
> the resulting series will not contain data for `k`. For more about missing data, 
> see the [next section](#missing).

When calculating with time series, it is also useful to transform keys. For example, here
is one possible approach to writing a calculation that calculates how the price changes 
between two days:

    [lang=csharp,file=../csharp/Series.cs,key=calc-diff]

The `Shift` operation creates a new series where the index is shifted by the specified
offset. Using `ser.Shift(1)` creates a new series where element at index _i_ is the element
from index _i - 1_ in `ser`. In the above example, this means that the value in 
`msft2012.Shift(1)` for a certain day is the value for the previous day in `msft2012`. 
This means that the code takes prices at a specified day and subtracts yesterday's prices
from them. 

The operations available for series cover most of the standard operations, but if you
need a more advanced functionality, you can always access the underlying data. For example,
the `Observations` property gives you access to all key-value pairs of the series. The
following calculates the price, divided by the number of days since the first day 
for which we have a value (this is just an example of an unusual calculation):

    [lang=csharp,file=../csharp/Series.cs,key=calc-custom]

The following properties and methods are useful when writing custom calculations:

 - `Values` - returns all values (skipping over missing data) in the series
 - `Observations` - returns all observations (key-value pairs), skipping over missing data
 - `GetAllObservations()` - returns all data, including keys with missing values
 - `FirstKey()` - returns the first key (works only for ordered series)
 - `LastKey()` - returns the last key (works only for ordered series)
 - `KeyCount` - returns the number of keys in the series
 - `ValueCount` - returns the number of values (may be smaller than `KeyCount` when
   the series contains missing values)

<a name="missing"></a>

Handling missing values
-----------------------

When discussing [what is a series](#understanding), we noted that series can contains
missing values. This can happen when creating series from `OptionalValue<T>` values or,
more frequently, when aligning data using [data frames](csharpframe.html) and then 
obtaining a series from a frame.

In this sample, we'll use an ordered series `opts` from [earlier sample](#creating) that
contains keys from 0 to 9 and values only for even elements of the series - this means, the
values are `[0; NA; 2; NA; 4; NA; 6; NA; 8; NA]`.

For any series (oredered or unordered) we can drop the missing values or replace them with
a constant:

    [lang=csharp,file=../csharp/Series.cs,key=fill-const-drop]
     
The first operation returns a series with values `[0; -1; 2; -1; 4; -1; 6; -1; 8; -1]`
and the second operation returns a series with keys `[0; 2; 4; 6; 8]`.

If the series is ordered, we have one more option. We can fill missing values with the
first previous available value, or with the first subsequent available value. This is
done using an overlaod that takes `Direction`:

    [lang=csharp,file=../csharp/Series.cs,key=fill-dir]

It is worth noting that this does not always fill _all_ missing values in the series.
If you use `Direction.Forward` and the input series contains `[NA; 0; NA; 1]` then 
the result  is `[NA; 0; 0; 1]` - the first value is still missing, because there is no
preceeding available value. However, you can be sure that the only missing values are
at the beginning (or the end) of the series.

<a name="linq"></a>

LINQ to series
--------------

The `Series<K, V>` type implements some of the methods supported by the C# LINQ pattern,
which means that you can process series in a familiar way and, to some extent, you can 
also use the C# query syntax. 

The following example shows how to count the number of days when the Microsoft stock price
was below the average (which we calculated earlier, using the `msft2012.Mean()` extension
method). First, let's look at using the LINQ methods directly:

    [lang=csharp,file=../csharp/Series.cs,key=linq-methods]

Both of the methods are defined on the `Series<K, V>` type - this means that the result is
also a series and we can get the number of keys on the resulting series using the `KeyCount`
property (the `Where` method drops the keys for which the condition does not hold).

> **Efficiency.** Note that both `Select` and `Where` copy the series and so long method 
> chaining will be less efficient. In that case, it is more desirable to use 
> `series.Values` and operate on `IEnumerable<T>` before converting the result back to a series.

The same code can be also written using the C# query syntax as follows (this time, we get
the number of days when the price was _below_ the average):

    [lang=csharp,file=../csharp/Series.cs,key=linq-query]

The `Series<K, V>` type does not support all query operations, but you can certainly use
`from`, `where` and `select` to transform and filter series. One tricky aspect is that the
variable bound in the `from` clause is key value pair containing the key (index) and value
(the value in the series) to allow projection/filtering based on both the key and the value.

<a name="aggregation"></a>

Grouping, windowing and chunking
--------------------------------

Deedle supports a number of operations that can be used to group or aggregate data. There
are two operations - for any (possibly unordered) series, _grouping_ works by obtaining a new 
key for each observation and then grouping the input by such keys; _aggregation_ works only
on ordered series. It aggregates consecutive elements (possibly with overlap) of the series -
a typical use of aggregation is getting floating windows of certain length.

### Grouping series

The grouping operation is similar to `GroupBy` from LINQ. It takes a _key selector_ that produces
a new key and a _value selector_ that produces new value for a group of values with the same key.
The following example uses `randNums` which is a series of 100 randomly generated values between
0 and 1. We group them by the first digit and count number of elements in each group to get the 
distribution of the random number generator:

    [lang=csharp,file=../csharp/Series.cs,key=aggreg-group]

Note that the aggregation function needs to return `OptionalValue<T>`. This makes it possible to
write aggregation that returns series with missing values for some key (e.g. when the group 
does not contain any valid value).

### Floating windows and chunking

When working with time series (e.g. stock prices), floating windows can be used to take the 
average value over certain number of previous values. The following example takes 5 last 
values for each day and averages them (skipping over the first 4 items in the series where 
there is not enough past values available):

    [lang=csharp,file=../csharp/Series.cs,key=aggreg-win]

The chunking operation is similar to windowing, but it builds chunks that do not overlap. For
example, given `[1; 2; 3; 4]` a floating window of size two returns `[[1; 2]; [2; 3]; [3; 4]]`
while chunks of size two return `[[1; 2]; [3; 4]]`. The chunking operations look very similar
to windowing operations:

    [lang=csharp,file=../csharp/Series.cs,key=aggreg-chunk]

Finally, it is very common to use windows of size two, which gives us the current value together
with the previous value. In Deedle, this is available via the `Pairwise` operation which turns
a series of values into a series of tuples (type `Tuple<V, V>`). Here we take the average of the
current and previous value:

    [lang=csharp,file=../csharp/Series.cs,key=aggreg-pair]

### General (ordered) aggregation

For chunking and windowing, previous examples always used a fixed number of elements to specify
when a window/chunk ends. However, you might want to use more advanced conditions. This can be
done using the fully general `Aggregate` operation. The `Aggregation` type in the following 
example provides methods for specifying additional conditions. 

The options include windowing and chunking of fixed size where boundaries are handled differently,
and windowing/chunking where each window/chunk ends when a certain property holds between the 
keys. For example, the following sample creates chunks such that the year and month are equal
for each chunk:

    [lang=csharp,file=../csharp/Series.cs,key=aggreg-any]

The result of the operation is a series that has at most one value for each year/month which
represents the average value in that month. When building the chunks, the aggregation calls
the provided function (argument of `ChunkWhile`) on the first and the last key of the chunk
until the function returns `false` and then it starts a new chunk.

<a name="sampling"></a>

Indexing and sampling
---------------------

In the last section of the series overview, we look at a number of operations that can be 
performed with the index of the series such as transformations and sampling. Index transformation
is particularly important when working with multiple series in data frames (you might need 
to transform the keys so that you can align multiple series). Sampling is useful when you have
a series with higher resolution of data than necessary, or when you need to transform data
to uniform observations.

### Transforming the index

The first operation on the index is similar to `Select`, but instead of selecting new values,
we select new keys. For example, given our `msft2012` series which has `DateTime` values as
keys, we might want to transform the keys to `DateTimeOffset`. Another useful operation
drops the index and replaces it with ordinal numbers:

    [lang=csharp,file=../csharp/Series.cs,key=index-keys]

Both of the operations in the snippet return series of a different type. Here, the type of
`byOffs` is `Series<DateTimeOffset, double>` because the type of keys has changed from 
`DateTime` to `DateTimeOffset` (this is all inferred by the C# compiler, so we do not need
to write the type explicitly). In the second example, the resulting type is `Series<int, double>`,
because the keys are dropped and replaced with numbers in range `0 .. KeyCount-1`.

Finally, if we want to replace an existing series of keys with a new series of keys (of the
same length), we can use the `IndexWith` method. Here, we replace the index of a series
`numNames` which has three observations with three dates:

    [lang=csharp,file=../csharp/Series.cs,key=index-with]

Just like the two previous operations, `IndexWith` also changes the type of the series.
It can also change whether the series is ordered or not (here, the resulting series has
`DateTime` keys and is ordered).

### Time series sampling 

When a series is ordered and the keys represent (typically) dates or times, we can use a number
of sampling operations. There are two kinds of sampling operations:

 * **Resampling** means that we aggregate values values into chunks based on a specified collection 
   of keys (e.g. explicitly provided times), or based on some relation between keys (e.g. date times 
   having the same date).

 * **Uniform resampling** is similar to resampling, but we specify keys by providing functions that 
   generate a uniform sequence of keys (e.g. days), the operation also fills value for days that 
   have no corresponding observations in the input sequence.

Given a series `ts`, the sampling operations are available via the extension methods `ts.Sample(..)`, 
`ts.SampleInto(..)`, and `ts.ResampleUniform(..)`. For more information about these methods, 
[see the API reference](reference/deedle-seriesextensions.html#section0) and also the [F# samples](series.html#sampling)
which are written using corresponding F# functions in the `Series` module.