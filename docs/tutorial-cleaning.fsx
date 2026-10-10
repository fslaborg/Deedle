(**
---
title: Tutorial: Cleaning messy data
category: Tutorials
categoryindex: 0
index: 2
description: Find and fix missing values, derive columns, filter, group and aggregate a real-world data set
keywords: tutorial, data cleaning, missing values, groupby, pandas, F#
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
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)
(**

# Tutorial: Cleaning messy data

*This is the Deedle equivalent of the classic "pandas data cleaning" tutorial.*
Real data sets have missing values, odd separators and columns that need transforming.
We work through the New York air quality measurements (May–September 1973), a well-known
data set that has gaps in the `Ozone` and `Solar.R` columns.

## Load the data

The file is semicolon-separated and marks missing values as `#N/A`.
Deedle recognises `#N/A`, `NA` and empty fields as missing values automatically:
*)

let air = Frame.ReadCsv(root + "airquality.csv", separators = ";")

air |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
How big is it?
*)

air.RowCount, air.ColumnCount
(*** include-fsi-merged-output ***)

(**
## Find the gaps

`ValueCount` on a series counts only the values that are present. Compare it with
the 153 rows to see which columns are incomplete:
*)

air.Columns |> Series.mapValues (fun col -> col.ValueCount)
(*** include-fsi-merged-output ***)

(**
`Stats.describe` also skips missing values, so its statistics describe the data that is there:
*)

Stats.describe air
(*** include-fsi-merged-output ***)

(**
## Option 1: drop incomplete rows

`Frame.dropSparseRows` removes every row that has a missing value:
*)

let complete = air |> Frame.dropSparseRows
complete.RowCount
(*** include-fsi-merged-output ***)

(**
This is simple, but we lost 43 of 153 rows. Often it is better to repair the gaps.

## Option 2: fill the gaps

Replace missing ozone readings with a constant:
*)

air?Ozone |> Series.fillMissingWith 0.0 |> Series.take 6
(*** include-fsi-merged-output ***)

(**
Or carry the previous measurement forward, which suits sensor readings:
*)

air?Ozone |> Series.fillMissing Direction.Forward |> Series.take 6
(*** include-fsi-merged-output ***)

(**
See [Handling missing values](missing.html) for interpolation and other strategies.

## Add a derived column

Series support arithmetic, so converting Fahrenheit to Celsius is a one-liner.
Assign to a new column name with `<-`:
*)

air?TempC <- (air?Temp - 32.0) * 5.0 / 9.0
air |> Frame.take 2
(*** include-fsi-merged-output ***)

(**
## Filter rows

Keep only the scorching days, i.e. those above 90°F:
*)

let hot = air |> Frame.filterRowValues (fun row -> row.GetAs<float>("Temp") > 90.0)
hot.RowCount
(*** include-fsi-merged-output ***)

(**
Sort to see the three hottest days:
*)

air |> Frame.sortRowsBy "Temp" (fun t -> -t) |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
## Group and aggregate

Which month has the most ozone? `Frame.aggregateRowsBy` groups by the given columns and
applies a function to the listed columns (missing values are skipped by `Stats.mean`):
*)

air |> Frame.aggregateRowsBy ["Month"] ["Ozone"; "Temp"] Stats.mean
(*** include-fsi-merged-output ***)

(**
## Relationships between columns

Ozone rises with temperature:
*)

Stats.corr air?Ozone air?Temp
(*** include-fsi-merged-output ***)

(**
## Save the cleaned result
*)

(*** do-not-eval ***)
air.SaveCsv("airquality-clean.csv", separator = ',')

(**
## Where next?

 * [Handling missing values](missing.html) — all the ways of detecting and filling gaps
 * [Data frame features](frame.html) — grouping, pivoting and reshaping
 * [Joining and merging frames](joining.html) — combine several data sources
*)
