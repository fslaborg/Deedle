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

*)