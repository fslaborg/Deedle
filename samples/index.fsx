(*** hide ***)
#I "../bin"
#load "FSharp.DataFrame.fsx"
open System
open FSharp.DataFrame
/// Titanic data set loaded from a CSV file
let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/Titanic.csv")

(**
F# DataFrame: Easy data manipulation
=====================================

The F# DataFrame library (`FSharp.DataFrame.dll`) is an easy to use library for data 
and time series manipulation and for scientific programming. It supports working with 
structured data frames, ordered and unordered data, as well as time series.

The library implements a wide range of operations for data manipulation including 
advanced indexing and slicing, joining and aligning data, handling of missing values,
grouping and aggregation, statistics and more. The library can be used from F# and C#.

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The F# DataFrame library can be <a href="https://nuget.org/packages/FSharp.DataFrame">installed from NuGet</a>:
      <pre>PM> Install-Package FSharp.DataFrame</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Titanic survivor analysis in 20 lines
-------------------------------------

Assume we loaded [Titanic data set](http://www.kaggle.com/c/titanic-gettingStarted) 
into a data frame called `titanic` (the data frame has numerous columns including string 
`Sex` and Boolean `Survived`). Now we can calculate the survival rates for males and females:

<div id="hp-snippet">
*)
// Group the data frame by sex 
let grouped = titanic |> Frame.groupRowsByString "Sex" |> Frame.unstack

// For each group, calculate the total number of survived & died
let bySex =
  grouped
  |> Series.map (fun sex df -> 
      // Group by the 'Survived' column & count by Boolean values
      df.GetSeries<bool>("Survived") |> Series.groupBy (fun k v -> v) 
      |> Frame.ofColumns |> Frame.countValues )
  |> Frame.ofRows
  |> Frame.indexColsWith ["Died"; "Survived"]

// Add column with Total number of males/females on Titanic
bySex?Total <- Frame.countKeys $ grouped

// Build a data frame with nice summary of rates in percents
[ "Died (%)" => bySex?Died / bySex?Total * 100.0
  "Survived (%)" => bySex?Survived / bySex?Total * 100.0 ]
|> Frame.ofColumns

(**
We first group data by the `Sex` column and get a series (with rows `male` and `female`)
containing data frames for individual groups. Then we count the number of survivors in
each group, add the total number of males and females and, finally, build a new data frame
with the summary:

<table class="table table-bordered table-striped">
<thead><tr><td></td><td>Died (%)</td><td>Survived (%)</td></tr></thead>
<tbody>
  <tr><td class="title">Female</td><td>25.80</td><td>74.20</td></tr></thead>
  <tr><td class="title">Male</td><td>81.11</td><td>18.89</td></tr></thead>
</tbody>
</table>

</div>

Samples & documentation
-----------------------

The library comes with comprehensible documentation. The tutorials and articles are 
automatically generated from `*.fsx` files in [the samples folder][samples]. The API
reference is automatically generated from Markdown comments in the library implementation.

 * [Quick start tutorial](tutorial.html) shows how to use the most important 
   features of F# DataFrame library. Start here to learn how to use the library in 10 minutes.

 * [Data frame features](features.html) provides more examples that use general data frame 
   features. These includes slicing, joining, grouping, aggregation.

 * [Time series features](timeseries.html) provides more details on operations that are 
   relevant when working with time series data (such as stock prices). This includes sliding
   windows, chunking, sampling and statistics.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
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
and contributors. It is available under an open-source license that allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 


  [samples]: https://github.com/blueMountainCapital/FSharp.DataFrame/tree/master/samples
  [gh]: https://github.com/blueMountainCapital/FSharp.DataFrame
  [issues]: https://github.com/blueMountainCapital/FSharp.DataFrame/issues
  [readme]: https://github.com/blueMountainCapital/FSharp.DataFrame/blob/master/README.md
  [license]: https://github.com/blueMountainCapital/FSharp.DataFrame/blob/master/LICENSE.md
  [fsharp-dwg]: http://fsharp.org/technical-groups/
*)