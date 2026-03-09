(**
---
title: Deedle: Exploratory data library for .NET
category: Documentation
categoryindex: 1
index: 1
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

let root = __SOURCE_DIRECTORY__ + "/data/"

(**

Deedle is an easy to use library for data and time series manipulation and for scientific 
programming. It supports working with structured data frames, ordered and unordered data, 
as well as time series. Deedle is designed to work well for exploratory programming using 
F# and C# interactive console, but can be also used in efficient compiled .NET code.

The library implements a wide range of operations for data manipulation including 
advanced indexing and slicing, joining and aligning data, handling of missing values,
grouping and aggregation, statistics and more. 

### Titanic survivor analysis in 20 lines

Assume we loaded [Titanic data set](http://www.kaggle.com/c/titanic-gettingStarted) 
into a data frame called `titanic` (the data frame has numerous columns including int
`Pclass` and Boolean `Survived`). Now we can calculate the survival rates for three different
classes of tickets:
*)

// Read Titanic data & group rows by 'Pclass'
let titanic = Frame.ReadCsv(root + "titanic.csv").GroupRowsBy<int>("Pclass")

// Get 'Survived' column and count survival count per class
let byClass =
  titanic.GetColumn<bool>("Survived")
  |> Series.applyLevel fst (fun s ->
      // Get counts for 'True' and 'False' values of 'Survived'
      series (Seq.countBy id s.Values))
  // Create frame with 'Pclass' as row and 'Died' & 'Survived' columns
  |> Frame.ofRows 
  |> Frame.sortRowsByKey
  |> Frame.indexColsWith ["Died"; "Survived"]

// Add column with Total number of males/females on Titanic
byClass?Total <- byClass?Died + byClass?Survived

// Build a data frame with nice summary of rates in percents
frame [ "Died (%)" => round (byClass?Died / byClass?Total * 100.0)
        "Survived (%)" => round (byClass?Survived / byClass?Total * 100.0) ]

(**
We first group data by the `Pclass` and get the `Survived` column as a series
of Boolean values. Then we reduce each group using `applyLevel`. This calls a specified
function for each passenger class. We count the number of survivors and casualties.
Then we add nice namings, sort the frame and build a new data frame with a nice summary.

### How to get Deedle

 * The library is available as [Deedle on NuGet](https://www.nuget.org/packages/Deedle).
   You can also [get the code from GitHub](https://github.com/fslaborg/Deedle/)
   or download [the source as a ZIP file](https://github.com/fslaborg/Deedle/zipball/master).

 * If you want to use Deedle with F# Data, R type provider and other F# components for data science,
   consider using the [FsLab package](https://www.nuget.org/packages/FsLab).

## Samples & documentation

The library comes with comprehensible documentation. The tutorials and articles are 
automatically generated from `*.fsx` files in [the docs folder][docs]. The API
reference is automatically generated from XML documentation in the library implementation.

 * [Quick start tutorial](tutorial.html) shows how to use the most important 
   features of F# DataFrame library. Start here to learn how to use the library in 10 minutes.

 * [Data frame features](frame.html) provides more examples that use general data frame 
   features. These includes slicing, joining, grouping, aggregation.

 * [Series features](series.html) provides more details on operations that are 
   relevant when working with time series data (such as stock prices). This includes sliding
   windows, chunking, sampling and statistics.

 * [Calculating frame and series statistics](stats.html) shows how to calculate statistical
   indicators such as mean, variance, skewness and other. The tutorial also covers moving
   window and expanding window statistics.

 * [Creating lazily loaded series](lazysource.html) explains how to create virtual series
   backed by an external data source that loads data on demand.

Automatically generated documentation for all types, modules and functions in the library 
is available in the [API Reference](reference/index.html). The three most important modules
that are fully documented are the following:

 * [`Series` module](reference/deedle-seriesmodule.html) provides functions for working
   with individual data series and time-series values.
 * [`Frame` module](reference/deedle-framemodule.html) provides functions that are similar
   to those in the `Series` module, but operate on entire data frames.
 * [`Stats` module](reference/deedle-stats.html) implements standard statistical functions,
   moving windows and a lot more.

More functions related to linear algebra, statistical analysis and financial analysis can be 
found in Deedle.Math extension. Deedle.Math has dependency on MathNet.Numerics.

## Contributing and copyright

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding new public API, please also 
consider adding [samples][samples] that can be turned into documentation. You might
also want to read [library design notes](design.html) to understand how it works.

The library has been developed by [BlueMountain Capital](https://www.bluemountaincapital.com/)
and contributors. It is available under the BSD license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [docs]: https://github.com/fslaborg/Deedle/tree/master/docs
  [samples]: https://github.com/fslaborg/Deedle/tree/master/docs
  [gh]: https://github.com/fslaborg/Deedle
  [issues]: https://github.com/fslaborg/Deedle/issues
  [readme]: https://github.com/fslaborg/Deedle/blob/master/README.md
  [license]: https://github.com/fslaborg/Deedle/blob/master/LICENSE.md
*)
