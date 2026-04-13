(**
---
title: Deedle in 10 minutes
category: Guides
categoryindex: 1
index: 2
description: A quick-start tutorial covering data frames, series, grouping, and missing values in F#
keywords: tutorial, quick start, getting started, F#, data frame, series
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#endif // FSX
(*** condition: prepare ***)

open System
open Deedle

let root = __SOURCE_DIRECTORY__ + "/data/"

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    fmt.Invoke(o, [||]) :?> string
  else null)

(**

# Deedle in 10 minutes

This quick-start gets you productive with Deedle fast.
We use the classic [Titanic passenger data set](http://www.kaggle.com/c/titanic-gettingStarted)
to show the most important features — loading data, exploring columns,
filtering, grouping, handling missing values, and basic statistics.

## Setup

Install from NuGet and open the namespace:

    [lang=text]
    dotnet add package Deedle

*)

(*** do-not-eval ***)
#r "nuget: Deedle"
open Deedle

(**

## Loading data

The fastest way to get data into Deedle is `Frame.ReadCsv`:
*)

let titanic = Frame.ReadCsv(root + "titanic.csv")

(**
Let's see what we have:
*)

titanic.RowCount
(*** include-it ***)

titanic.ColumnCount
(*** include-it ***)

titanic.ColumnKeys |> Seq.toList
(*** include-it ***)

(**
Print the first few rows:
*)
titanic |> Frame.take 5
(*** include-it ***)

(**

## Accessing columns

Use `?` to grab a column as a `Series`. Deedle reads numeric columns as `float`
and text columns as `string`:
*)

// A numeric column
titanic?Age
(*** include-it ***)

// A text column
titanic.GetColumn<string>("Name") |> Series.take 3
(*** include-it ***)

(**

## Basic statistics

Compute summary statistics on any numeric series:
*)

titanic?Age |> Stats.mean
(*** include-it ***)

titanic?Fare |> Stats.mean
(*** include-it ***)

titanic?Age |> Stats.median
(*** include-it ***)

titanic?Age |> Stats.stdDev
(*** include-it ***)

(**
Missing values (like missing `Age` entries in the Titanic data) are automatically
skipped by all statistical functions.

## Filtering rows

Use `Frame.filterRowValues` to keep only rows matching a condition:
*)

// Passengers who survived
let survived = titanic |> Frame.filterRowValues (fun row ->
  row.GetAs<bool>("Survived"))

survived.RowCount
(*** include-it ***)

// First-class passengers
let firstClass = titanic |> Frame.filterRowValues (fun row ->
  row.GetAs<int>("Pclass") = 1)

firstClass.RowCount
(*** include-it ***)

(**

## Adding computed columns

The `?<-` operator adds or replaces a column. Let's add a `HasCabin` flag:
*)

titanic?HasCabin <- titanic.GetColumn<string>("Cabin")
  |> Series.mapAll (fun _ v -> Some(v.IsSome))

titanic.Columns.[ ["Name"; "Pclass"; "HasCabin"] ] |> Frame.take 5
(*** include-it ***)

(**

## Grouping and aggregation

Group rows by a column and aggregate — one of Deedle's most powerful features.

### Survival rate by passenger class
*)

titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Fare"] Stats.mean
(*** include-it ***)

(**
### Survival counts by class and sex
*)

let byClassAndSex = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Sex"
  |> Frame.mapRowKeys Pair.flatten3

byClassAndSex.GetColumn<bool>("Survived")
|> Series.applyLevel Pair.get1And2Of3 (fun s ->
    series (Seq.countBy id s.Values))
|> Frame.ofRows
(*** include-it ***)

(**
### Average age by class
*)

titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Age"] Stats.mean
(*** include-it ***)

(**

### Pivot tables

Pivot tables cross-tabulate two categorical variables with an aggregation:
*)

titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex"))
    (fun k r -> r.GetAs<int>("Pclass"))
    Frame.countRows

(*** include-it ***)

(**

## Handling missing values

Real data has gaps. The Titanic `Age` and `Cabin` columns both contain missing
entries. Deedle makes this explicit — missing values show as `<missing>` and are
automatically skipped by statistics:
*)

// How many ages are missing?
let ageCol = titanic?Age
let total = ageCol |> Series.countKeys
let present = ageCol |> Series.countValues
(*** include-value: total ***)
(*** include-value: present ***)
(*** include-value: total - present ***)

(**
Fill strategies let you choose how to handle the gaps:
*)

// Replace missing ages with a constant
ageCol |> Series.fillMissingWith 0.0 |> Series.take 5
(*** include-it ***)

// Fill forward (propagate last known value)
ageCol |> Series.fillMissing Direction.Forward |> Series.take 5
(*** include-it ***)

// Drop rows with missing values
ageCol |> Series.dropMissing |> Series.countKeys
(*** include-it ***)

(**

## Creating frames and series from scratch

You can also build data frames from scratch rather than loading CSV.
*)

// A simple series
let ages = series [ "Alice" => 30.0; "Bob" => 25.0; "Carol" => 35.0 ]
ages
(*** include-it ***)

// Build a frame from columns
let people = 
  Frame.ofColumns [
    "Age" => ages
    "Score" => series [ "Alice" => 90.0; "Bob" => 85.0; "Carol" => 92.0 ]
  ]
people
(*** include-it ***)

// Build a frame from records
type Person = { Name: string; Age: int }
let records = 
  [ { Name = "Alice"; Age = 30 }
    { Name = "Bob"; Age = 25 } ]
Frame.ofRecords records
(*** include-it ***)

(**

## Selecting columns and rows

Pick specific columns with `Frame.sliceCols` or the indexer:
*)

titanic.Columns.[ ["Name"; "Age"; "Fare"] ] |> Frame.take 3
(*** include-it ***)

titanic |> Frame.sliceCols ["Pclass"; "Survived"] |> Frame.take 3
(*** include-it ***)

(**

## Sorting

Sort a frame by column values:
*)

titanic |> Frame.sortRowsBy "Fare" (fun (v: float) -> -v) |> Frame.take 5
(*** include-it ***)

(**

## Further reading

This quick-start covers the most common Deedle patterns. Dive deeper with:

 * [Data frame features](frame.html) — full coverage of frame construction, slicing,
   grouping, aggregation with `Frame.aggregateRowsBy`, pivot tables, and more.
 * [Series features](series.html) — windowing, chunking, resampling, and time-series alignment.
 * [Statistics](stats.html) — moving and expanding window statistics, multi-level aggregation.
 * [Handling missing values](missing.html) — sentinel types, all fill strategies, and how
   missing values interact with joins.
 * [Joining and merging](joining.html) — inner, outer, left, and right joins.
 * [Apache Arrow / Feather](arrow.html) — zero-copy columnar I/O.
 * [Deedle.MathNetNumerics](math.html) — linear algebra, correlation matrices, EWM statistics, PCA,
   and linear regression via the `Deedle.MathNetNumerics` package.
*)
