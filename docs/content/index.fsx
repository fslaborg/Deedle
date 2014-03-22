(*** hide ***)
#load "../../bin/Deedle.fsx"
open System
open Deedle
let data = __SOURCE_DIRECTORY__ + "/data/"

(**
Deedle: Exploratory data library for .NET
=========================================

Deedle is an easy to use library for data and time series manipulation and for scientific 
programming. It supports working with structured data frames, ordered and unordered data, 
as well as time series. Deedle is designed to work well for exploratory programming using 
F# and C# interactive console, but can be also used in efficient compiled .NET code.

The library implements a wide range of operations for data manipulation including 
advanced indexing and slicing, joining and aligning data, handling of missing values,
grouping and aggregation, statistics and more. 

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The F# DataFrame library can be <a href="https://nuget.org/packages/Deedle">installed from NuGet</a>:
      <pre>PM> Install-Package Deedle</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Titanic survivor analysis in 20 lines
-------------------------------------

Assume we loaded [Titanic data set](http://www.kaggle.com/c/titanic-gettingStarted) 
into a data frame called `titanic` (the data frame has numerous columns including int
`Pclass` and Boolean `Survived`). Now we can calculate the survival rates for three different
classes of tickets:

<div id="hp-snippeta">
*)
(*** define-output: sample ***)
// Read Titanic data & group rows by 'Sex'
let titanic = Frame.ReadCsv(data + "titanic.csv").GroupRowsBy<int>("Pclass")

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

We first group data by the `Pclass` and get the `Survived` column as a series
of Boolean values. Then we reduce each group using `applyLevel`. This calls a specified
function for each passenger class. We count the number of survivors and casualties.
Then we add nice namings, sort the frame and build a new data frame with a nice summary:

<style type="text/css">
.hp-table th, .hp-table td { width: 140px; font-size:125%; padding:5px 10px 5px 10px; }
.hp-table th:first-child, .hp-table td:first-child { width: 90px; }
</style>
<div class="hp-table">
*)

(*** include-it: sample ***)

(**
</div>

Samples & documentation
-----------------------

The library comes with comprehensible documentation. The tutorials and articles are 
automatically generated from `*.fsx` files in [the samples folder][samples]. The API
reference is automatically generated from Markdown comments in the library implementation.

 * [Quick start tutorial](tutorial.html) shows how to use the most important 
   features of F# DataFrame library. Start here to learn how to use the library in 10 minutes.

 * [Data frame features](frame.html) provides more examples that use general data frame 
   features. These includes slicing, joining, grouping, aggregation.

 * [Time series features](series.html) provides more details on operations that are 
   relevant when working with time series data (such as stock prices). This includes sliding
   windows, chunking, sampling and statistics.

 * The Deedle library can be used from both F# and C#. We aim to provide idiomatic API for
   both of the languages. Read the [using Deedle from C#](csharpintro.html) page for more 
   information about the C#-friednly API.

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

<img src="http://www.bluemountaincapital.com/media/logo.gif" style="float:right;margin:10px" />

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


  [samples]: https://github.com/blueMountainCapital/Deedle/tree/master/samples
  [gh]: https://github.com/blueMountainCapital/Deedle
  [issues]: https://github.com/blueMountainCapital/Deedle/issues
  [readme]: https://github.com/blueMountainCapital/Deedle/blob/master/README.md
  [license]: https://github.com/blueMountainCapital/Deedle/blob/master/LICENSE.md
  [fsharp-dwg]: http://fsharp.org/technical-groups/
*)