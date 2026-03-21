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

## Deedle

Deedle is an F# library for data frames and time series.

Deedle supports working with structured data frames, ordered and unordered data, 
and time series. It implements a wide range of operations for data manipulation including 
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

## Samples & documentation

 * [Quick start tutorial](tutorial.html)

 * [Data frame features](frame.html)

 * [Series features](series.html)

 * [Calculating frame and series statistics](stats.html)

 * [Handling missing values](missing.html)

 * [Creating lazily loaded series](lazysource.html)

 * [`Series` module](reference/deedle-seriesmodule.html) provides functions for working
   with individual data series and time-series values.
 * [`Frame` module](reference/deedle-framemodule.html) provides functions that are similar
   to those in the `Series` module, but operate on entire data frames.
 * [`Stats` module](reference/deedle-stats.html) implements standard statistical functions,
   moving windows and a lot more.

More functions related to linear algebra, statistical analysis and financial analysis can be 
found in Deedle.Math extension. Deedle.Math has dependency on MathNet.Numerics.

## Contributing and copyright

The project is hosted on [GitHub][gh].

The library was originally developed by [BlueMountain Capital](https://www.bluemountaincapital.com/)
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
