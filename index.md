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

```fsharp
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
```

We first group data by the `Pclass` and get the `Survived` column as a series
of Boolean values. Then we reduce each group using `applyLevel`. This calls a specified
function for each passenger class. We count the number of survivors and casualties.
Then we add nice namings, sort the frame and build a new data frame with a nice summary.

## Tutorials &amp; documentation

* [Quick start tutorial](tutorial.html) — a 10-minute tour of Deedle
  

* [Deedle in C# — Cookbook](csharp.html) — creating frames/series, slicing rows and columns,
missing values, statistics, windowing, and more for C# developers
  

* [Data frame features](frame.html) — creating, slicing, grouping, pivoting
  

* [Series features](series.html) — time series, windowing, resampling, alignment
  

* [Calculating frame and series statistics](stats.html) — summary statistics, moving and expanding windows
  

* [Handling missing values](missing.html) — sentinel values, filling, propagation
  

* [Joining and merging frames](joining.html) — inner/outer/left/right joins and appending
  

* [Creating lazily loaded series](lazysource.html) — virtual series backed by custom loaders
  

* [`Series` module](reference/deedle-seriesmodule.html) provides functions for working
with individual data series and time-series values.
  

* [`Frame` module](reference/deedle-framemodule.html) provides functions that are similar
to those in the `Series` module, but operate on entire data frames.
  

* [`Stats` module](reference/deedle-stats.html) implements standard statistical functions,
moving windows and a lot more.

## Library Integrations

These optional packages integrate Deedle with other libraries:

* [Deedle.MathNetNumerics — MathNet.Numerics integration](math.html) — linear algebra, correlation,
EWM statistics, PCA, and linear regression via `Deedle.MathNetNumerics`

## Data Format Integrations

These optional packages integrate Deedle with other data formats:

* [Deedle.Arrow](arrow.html) — zero-copy columnar I/O with Apache Arrow
  

* [Deedle.Parquet](parquet.html) — columnar storage I/O with Apache Parquet
  

* [Excel integration](excel.html) — reading `.xlsx`/`.xls` files cross-platform with `Deedle.ExcelReader`;
writing `.xlsx` files cross-platform with `Deedle.ExcelWriter`;
live read/write via `Deedle.Excel` (Windows-only)

## Tooling Integrations

These optional packages integrate Deedle with other interactive tools:

* [Deedle.Excel](excel.html) — live read/write via `Deedle.Excel` (Windows-only)
  

* [Deedle.DotNetInteractive](dotnetinteractive.html) — Deedle support for .NET Interactive notebooks (Jupyter, VS Code, Azure Data Studio)

## Contributing and copyright

The project is hosted on [GitHub](https://github.com/fslaborg/Deedle).

The library was originally developed by [BlueMountain Capital](https://www.bluemountaincapital.com/)
and contributors. It is available under the BSD license, which allows modification and
redistribution for both commercial and non-commercial purposes. For more information see the
[License file](https://github.com/fslaborg/Deedle/blob/master/LICENSE.md) in the GitHub repository.
