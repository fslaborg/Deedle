(*** hide ***)
#load "../../bin/net45/Deedle.fsx"
open System
open Deedle
let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Deedle: Exploratory data library for .NET
=========================================

<img src="http://www.bluemountaincapital.com/media/logo.gif" style="float:right;margin:10px" />

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

<div id="hp-snippeta">
*)
(*** define-output: sample ***)
// Read Titanic data & group rows by 'Pclass'
let titanic = Frame.ReadCsv(root + "titanic.csv").GroupRowsBy<int>("Pclass")

// Get 'Survived' column and count survival count per clsas
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
</div>

<style type="text/css">
.hp-table th, .hp-table td { width: 140px; }
.hp-table th:first-child, .hp-table td:first-child { width: 90px; }
</style>
<div class="hp-table">
*)

(*** include-it: sample ***)

(**
</div>

We first group data by the `Pclass` and get the `Survived` column as a series
of Boolean values. Then we reduce each group using `applyLevel`. This calls a specified
function for each passenger class. We count the number of survivors and casualties.
Then we add nice namings, sort the frame and build a new data frame with a nice summary:

### How to get Deedle

 * The library is available as [Deedle on NuGet](https://www.nuget.org/packages/Deedle). To get the
   can also [get the code from GitHub](https://github.com/fslaborg/Deedle/)
   or download [the source as a ZIP file](https://github.com/fslaborg/Deedle/zipball/master).
   Compiled binaries are also available for [download as a ZIP file](https://github.com/fslaborg/Deedle/zipball/release).

 * If you want to use Deedle with F# Data, R type provider and other F# components for data science,
   consider using the [FsLab package](https://www.nuget.org/packages/FsLab). When using Visual Studio,
   you can also install [the FsLab project template](http://visualstudiogallery.msdn.microsoft.com/45373b36-2a4c-4b6a-b427-93c7a8effddb).

Samples & documentation
-----------------------

The library comes with comprehensible documentation. The tutorials and articles are 
automatically generated from `*.fsx` files in [the docs folder][docs]. The API
reference is automatically generated from Markdown comments in the library implementation.

 * [Quick start tutorial](tutorial.html) shows how to use the most important 
   features of F# DataFrame library. Start here to learn how to use the library in 10 minutes.

 * [Data frame features](frame.html) provides more examples that use general data frame 
   features. These includes slicing, joining, grouping, aggregation.

 * [Series features](series.html) provides more details on operations that are 
   relevant when working with time series data (such as stock prices). This includes sliding
   windows, chunking, sampling and statistics.

 * [Calculating frame and series statistics](stats.html) shows how to calculate statistical
   indicators such as mean, variance, skweness and other. The tutorial also covers moving
   window and expanding window statistics.

 * The Deedle library can be used from both F# and C#. We aim to provide idiomatic API for
   both of the languages. Read the [using Deedle from C#](csharpintro.html) page for more 
   information about the C#-friendly API.

Automatically generated documentation for all types, modules and functions in the library 
is available in the [API Reference](reference/index.html). The three most important modules
that are fully documented are the following:

 * [`Series` module](reference/deedle-seriesmodule.html) provides functions for working
   with individual data series and time-series values.
 * [`Frame` module](reference/deedle-framemodule.html) provides functions that are similar
   to those in the `Series` module, but operate on entire data frames.
 * [`Stats` module](reference/deedle-stats.html) implements standard statistical functions,
   moving windows and a lot more. It contains functions for both series and frames.

 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding new public API, please also 
consider adding [samples][samples] that can be turned into a documentation. You might
also want to read [library design notes](design.html) to understand how it works.

If you are interested in F# and data science more generally, consider also joining
the [F# data science and machine learning][fsharp-dwg] working group, which coordinates
work on data science projects for F#.

The library has been developed by [BlueMountain Capital](https://www.bluemountaincapital.com/)
and contributors. It is available under the BSD license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 


  [docs]: https://github.com/fslaborg/Deedle/tree/master/docs/content
  [samples]: https://github.com/fslaborg/Deedle/tree/master/docs/content/samples
  [gh]: https://github.com/fslaborg/Deedle
  [issues]: https://github.com/fslaborg/Deedle/issues
  [readme]: https://github.com/fslaborg/Deedle/blob/master/README.md
  [license]: https://github.com/fslaborg/Deedle/blob/master/LICENSE.md
  [fsharp-dwg]: http://fsharp.org/technical-groups/
*)
