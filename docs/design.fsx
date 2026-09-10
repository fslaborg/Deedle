(**
---
title: Design notes
category: Design
categoryindex: 3
index: 1
description: Internal design principles and architecture of the Deedle library
keywords: design, architecture, internals, data frame, implementation
---

## Deeedle's Design Principles

The Deedle library is a data manipulation library for F# and C# .NET users. There are
a number of different design choices, some of which are discussed in this article. The
key points are:

 - **Missing values** - the library supports working with missing values and distinguishes 
   between `NaN` (not-a-number) and "missing" (absence of a value). Operations propagate 
   missing values by default.

 - **Column and row index types** - data frames and series can be indexed by any orderable
   type (integers, strings, dates, etc.), and frame columns and rows are equally first-class.

 - **Immutability** - Deedle objects are (mostly) immutable. Most operations return a new 
   series or frame rather than modifying in place.

 - **Structural typing** - Accessing a frame column with `frame?Name` returns a series of
   type `Series<'R, float>` (weakly typed), while `frame.GetColumn<'T>("Name")` returns a 
   strongly typed series.

## Library internals

### Vectors

In Deedle, a *vector* stores the actual data. It is a one-dimensional collection of values
that can be accessed by an integer index. The `IVector<'T>` interface is defined in the
`Deedle` namespace and has the following key operations:

 - `vector.GetValue(index)` - returns the value at the specified integer location
 - `vector.Data` - returns the vector data as a `VectorData<'T>` discriminated union
   that can be either `DenseList` (an immutable array) or `SparseList` (a list of
   `OptionalValue<'T>` items)

The key design principle is that the actual storage mechanism is abstracted and multiple
storage strategies can be plugged in. The default implementation uses a simple array.

### Indices

An *index* maps from keys (of type `'K`) to integer locations (offsets into a vector). The
`IIndex<'K>` interface represents an index and provides operations like:

 - `index.Lookup(key, ...)` - find the integer location of a key
 - `index.Mappings` - returns all key-location mappings 
 - `index.Keys` - returns the sequence of keys in order

The default index uses a sorted array with binary search for ordered access and a 
dictionary for O(1) lookup by key.

### Series and frames

A **series** (`Series<'K, 'V>`) consists of:
 - An index of type `IIndex<'K>` 
 - A vector of type `IVector<'V>`

A **data frame** (`Frame<'R, 'C>`) consists of:
 - A row index of type `IIndex<'R>`
 - A column index of type `IIndex<'C>`
 - A vector of vectors, where each inner vector stores one column's data

When a column is accessed, Deedle looks up the column key in the column index to get
an integer offset, then uses that to retrieve the inner vector for that column.

## Vector and index builders

Most transformations on frames and series are implemented using *builder* interfaces.
The `IVectorBuilder` and `IIndexBuilder` interfaces provide factory methods used 
throughout Deedle.

The builder pattern allows Deedle to support pluggable storage backends (e.g. a
BigDeedle backend could store data in a database rather than in-memory arrays).

### Vector builder operations

The `IVectorBuilder` interface includes operations like:
 - `Create` - creates a vector from a sequence of values
 - `AsyncMaterialize` - asynchronously materializes a lazy vector
 - `MergeWith` - merges multiple vectors
 - `Combine` - combines two vectors element-wise using a function
 - `Build` - executes a `VectorConstruction` command tree (used for alignment)

The `VectorConstruction` type is a discriminated union that represents a tree of
operations to perform on vectors. This deferred execution model enables efficient
execution of complex operations that involve joins and alignment.

### Index builder operations

The `IIndexBuilder` interface includes operations like:
 - `Create` - creates an index from keys
 - `Aggregate` - groups the index into chunks or windows
 - `GroupBy` - groups the index by a key projection
 - `OrderIndex` - sorts the index keys
 - `Union`, `Intersect`, `Append` - set operations on indices
 - `Reindex` - creates a new index from another, building a `VectorConstruction` 
   that can be used to reorder a corresponding vector

The `Reindex` operation is particularly important for frame joins and alignment —
it returns both a new index and a `VectorConstruction` recipe that can be applied
to any vector with the old index to produce a vector aligned with the new index.


## BigDeedle

Big Deedle is the out-of-core path: large frames backed by **`IVirtualVectorSource`**
implementations (CSV, Parquet, or a custom file/API source). You plug in **sources** that
know how to answer `ValueAt`, `GetSubVector`, and preferably `LookupRange` for filters;
`Virtual.CreateFrame` / `Virtual.CreateOrdinalFrame` (and `Virtual.ReadCsv` /
`Virtual.ReadParquet`) wrap those sources in virtual indices and vectors.

The pluggable `IVectorBuilder` / `IIndexBuilder` stack still matters — virtual frames use
`VirtualVectorBuilder` and `VirtualIndexBuilder` so slices and filters stay on the virtual
addressing scheme — but day-to-day extension is **source-first**, not a full custom builder
rewrite. User-facing workflow, LookupRange cookbook, and what materializes vs stays lazy:
[Big Deedle — virtual frames](bigdeedle.html).

The `DelayedSeries` module is a simpler in-memory range loader (different mechanism) —
see the [Creating lazily loaded series](lazysource.html) page.

*)
