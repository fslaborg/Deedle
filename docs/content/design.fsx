(**

F# Data Frame design notes
==========================

This is the first version of F# Data Frame library and so we are still actively looking
at how to improve the design. The best place for discussions is either the [issue list
on GitHub](https://github.com/BlueMountainCapital/Deedle/issues) or the
mailing list of the [F# for Data and Machine Learning](http://fsharp.org/technical-groups/)
group (for more broader topics).

The current version of the library implements most of the basic functionality,
but it hopefully provides the right "core" internals that should make it easier to add
all the additional (useful) features.

When developing the library, we follow the principle that there should be a small number
of _primitive_ or _fundamental_ functions (these are typically provided as members on
the basic objects) that can be used to provide a wide range of useful functions (typically
available as extension members and in F# modules). We are generally quite happy to include
more extension members and functions for commonly used operations, so feel free to contribute!

Library principles
------------------

 * **F# and C# friendly** - We want to make sure that the library works from both F# and C#.
   For this reason, most functionality is exposed as extension members (using the C# `Extension`
   attribute - so they are only visible in C# and F# 3.1) and as functions in modules
   (`Frame` and `Series`). These are generally very similar. One difference is that functions
   use tuples and F# `option<T>` and more abbreviations, while extensions use `KeyValuePair<K, V>`,
   `OptionalValue<T>` (a C#-friendly `struct` defined in the library).

 * **Symmetry between rows and columns** - The data in data frame is stored as a list of
   columns and it is good idea to use the data frame in a column-wise way (and there are
   more functions for working with column-based frames).

   However, the data type `Frame<'TRowKey, 'TColKey>` is symmetric in that it uses custom
   index for access by both columns (series) and rows. You can also access columns/rows as
   a series of (nested) series via `df.Columns` and `df.Rows`. Although the column key is
   typically going to be a string (series name), this is not required and you can e.g. transpose
   frame using the `df.Transpose()` method.

 * **Missing and NaN values** we assume that data frames can always contain missing values and
   so there is no type distinction between frame/series that may have missing values and one
   that may not have missing values. Operations available on the frame and series are designed
   to handle missing values well - they generally skip over missing values unless you explicitly
   try to read a value by a key.

   The current version treats certain values as "missing" values, including `Double.NaN`
   (for numeric values) and `null` (for `Nullable<'T>` types and reference types). This
   means that when you create a series from `Double.NaN`, this is turned into a _missing_ value
   and the value is skipped when doing aggregation such as `Series.sum`. (An alternative would be
   to support both `NaN` and _missing_, but there is no clear conclusion about what is the
   most useful option.)

 * **Immutability** - A series is fully immutable data type, but a
   data frame supports limited mutation - you can add new series, drop a series & replace
   a series (but you cannot mutate the series). The row index of a data frame is mostly immutable -
   the only case when it changes is when you create an empty data frame and than add the first
   series.

   This seems to be useful because it works nicely with the `?<-` operator and you do not have
   to re-bind when you're writing some research script.

Library internals
-----------------

The Deedle implementation uses two layers. In the public API, you work with series and frames.
Under the cover, all of the operations are provided by indices (providng lookup and alignment)
and vectors (providing data storage). These are implemented in multiple different ways - most
notably, Deedle supports in-memory version and [BigDeedle provides virtualised
implementation](bigdeedle.html) that can be used for big time series without fully loading it
into memory.

### Vectors and indices

The following types are (mostly) not directly visible to the user, but they represent the
"minimal" core that changes infrequently. You could use them when extending the library:

 * `IVector<'TValue>` represents a vector (essentially an abstract data
   storage) that contains values `'TValue` that can be accessed via an address
   `Address`. A simple concrete implementation is an array with `int` addresses,
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

 * `VectorConstruction` is a discriminated union (DSL) that describes
   construction of vector. For every vector type, there is an `IVectorBuilder`
   that knows how to construct vectors using the construction instructions (these
   include things like re-shuffling of elements, appending vectors, getting a sub-range
   etc.)

 * `IIndex<'TKey>` represents an index - that is, a mapping from keys
   of a series or data frame to addresses in a vector. In the simple case, this is just
   a hash table that returns the `int` offset in an array when given a key (e.g.
   `string` or `DateTime`). A super-simple index would just map `int` offsets to
   `int` addresses via an identity function (not implemented yet!) - if you have
   series or data frame that is simply a list of recrods.

### Series and frames

The following types are the public API, wrapping the vectors and indices:

 * `Series<'TKey, 'TValue>` represents a series of values `'TValue` indexed by an
   index `'TKey`. A series uses an abstract vector, index and vector builder, so it
   should work with any data representation. A series provides some standard slicing
   operators, projection, filtering etc. There are also some binary operators (multiply
   by a scalar, add series, etc.) and addtional operations in the `Series` module.

 * `Frame<'TRowKey, 'TColumnKey>` represents a data frame with rows indexed using
   `TRowKey` (this could be `DateTime` or just ordinal numbers like `int`) and columns
   indexed by `TColumnKey` (typically a `string`). The data in the frame can be
   hetrogeneous (e.g. different types of values in different columns) and so accessing
   data is dynamic - but you can e.g. get a typed series.

   The operations available on the data frame include adding & removing series (which
   aligns the new series according to the row index), joins (again - aligns the series)
   etc. You can also get all rows as a series of (column) series and all columns as a
   series of (row) series - they are available as extension methods and in the `Frame` module.

### Vector builders and index builders

The indices and vectors are constructed using `IIndexBuilder` and `IVectorBuilder`. Typically,
index builder creates a new index and returns a `VectorConstruction` that can then be passed
to `IVectorBuilder.Build` to perform the corresponding transformation on all vectors
(for example, transform all vectors of a data frame according to an operation applied to the
index).

Indices and vectors are connected via an address. Addresses are represented by the `Address`
type and they are used to map keys (of the index) to values (in the vector). Here is a brief
summary of what we assume (and don't assume) about addresses:

 - Address is `int64` (although we might need to generalize this in the future)
 - Different data sources can use different addressing schemes
   (as long as both index and vector use the same scheme)
 - Addresses don't have to be continuous (e.g. if the source is partitioned, it
   can use 32bit partition index + 32bit offset in the partition)
 - In the in-memory representation, address is just index into an array
 - In the BigDeedle representation, address is abstracted and comes with
   `AddressOperations` that specifies how to use it (tests use linear
   offset and partitioned representation)

To capture the different options, Deedle has an interface called `IAddressingScheme`.
This represents the addressing mode used (it has no methods - it is just an object
that can be compared). Vectors and indices both expose `AddressingScheme` property
that can be used to check that their addressing schemes match.

Whenever we have index and vector, we need to make sure that they share the same
addressing scheme. There is a number of things that ensure this:

 - Series and frames keep an `IIndexBuilder` and a `IVectorBuilder` to be used for
   performing operations with the internal structures. These should always match, i.e.
   they should produce indices/vectors with the same addressing scheme.

 - `IIndexBuilder` may not be able to perform all operations without fully materializing
   the index (e.g. BigDeedle can only do some operations). In that case, it returns
   an index with different addressing scheme. When calling `IVectorBuilder.Build`,
   this takes `VectorConstruction` together with `IAddressingScheme` and so it can
   ensure that the created vector has the correct addressing scheme (i.e. if the
   required addressing scheme is linear, partitioned virtual index can materialize
   the vector before returning it).

 - `IVector` can also materialize vectors (when using `Select` or `Convert` methods). When
   those are called, the caller should apply `IIndexBuilder.Project` on the index, which
   performs the corresponding materialization on the index.

 - When the calling code creates a new index or vector directly (typically in-memory
   index/vector calculated from keys or values), it needs to make sure that the other structure
   matches too. When a new vector is created using `IVectorBuilder.Create`,
   the index needs to be transformed using `IIndexBuilder.Recreate`.


BigDeedle and virtual series and frames
---------------------------------------

The "BigDeedle" project implements virtualized data sources that can perform many operations
of Deedle without actually evaluating the data. This consists of `VirtualVector<'T>` and
`VirtualOrderedIndex<'T>` and `VirtualOrdinalIndex<'T>` (there are two indices - one for
ordinal keys and one for user-specified keys) together with their builders.

BigDeedle is extensible and you only need to provide fairly small number of operations to 
use it. The interfaces to look at are: 

 - `IVirtualVectorSource<'T>` where `'T` is the type of values of the source. This interface
   is used as a source for both virtual vectors (data access) and virtual indices (keys).
   Once you have a value of this, you can use `Virtual.CreateSeries` and similar methods
   to build a series or a frame.

The most tricky thing of the implementation is often `IAddressOperations`:

 - If you already have some underlying infrastructure that handles merging and slicing of
   virtual sources, then you can implement the interface directly on top of this (and
   use the infrastructure in merging and slicing operations of `IVirtualVectorSource`).

 - More frequently, you'll want to use the `Ranges<'K>` type provided by Deedle. This
   represents a mapping from some keys `'K` to offsets and it supports merging and slicing
   (so you do not have to write your own code to merge slices of virtual soruces).
   To use this, you can use `RangesAddressOperations(...)` which gives you an implementation
   of `IAddressOperations`. For a reasonably simple example, see the code in 
   [tests using partitioned addressing scheme](https://github.com/tpetricek/Deedle/blob/big-deedle-addr/tests/Deedle.Tests/VirtualPartitioned.fs).

Discussion and open questions
-----------------------------

We're hoping that the design of the internals is now reasonable, but the end user API may
still be missing some useful functionality (let us know if you need some!) Here are a few
things that we discussed earlier and that we may still look into at some point:

 * **Time series vs. pivot table** - there is some mismatch between two possible
   interpretations and uses of the library. One is for time-series data (e.g. in finance)
   where one typically works with dates as row indices. More generally, you can see this
   as _continous_ index. It makes sense to do interpolation, sort the observations,
   align them, re-scale them etc. (Note that _continuous_ is stronger than _ordered_ -
   aside from time, the only continuous measure we can think of is distance-dependent
   series.)

   The other case is when we have some _discrete_ observations (perhaps a list of
   records with customer data, a list of prices of different stock prices etc.) In this
   case, we need more "pivot table" functions etc.

   Although these two uses are quite different, we feel that it might make sense to use
   the same type for both (just with a different index). The problem is that this might
   make the API more complex. Although, if we can keep the distincion in the type, we can
   use F# 3.1 extension methods that extend just "discrete data frame" or "continous data
   frame". However, for now all functions are available in `Frame`/`Series` module and
   as extension methods that extend any type.

 * **Type provider** - we are thinking about using type providers to give some additional
   safety (like checking column names and types in a data frame). This is currently
   on the TODO list - we think we can do something useful here, although it will
   certainly be limited.

   The current idea is that you migth want to do some research/prototyping using a
   dynamic data frame, but once you're done and have some more stable data, you should
   be able to write, say `DataFrame<"Open:float,Close:float">(dynamicDf)` and get a
   new typed data frame.

If you have any comments regarding the topics above, please [submit an issue
on GitHub](https://github.com/BlueMountainCapital/Deedle/issues) or, if you
are interested in more actively contributing, join
the [F# for Data and Machine Learning](http://fsharp.org/technical-groups/) working
group. *)
