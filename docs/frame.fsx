(**
---
title: Data frame features
category: Guides
categoryindex: 1
index: 3
description: Creating, transforming, filtering, and aggregating data frames using the Deedle F# API
keywords: data frame, columns, rows, filtering, grouping, aggregation, F#
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
open System.IO
open Deedle

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    fmt.Invoke(o, [||]) :?> string
  else null)

let root = __SOURCE_DIRECTORY__ + "/data/"

(**

# Data frame features

This page is a comprehensive reference for `Frame<'R,'C>` operations.
If you are new to Deedle, start with the [quick start tutorial](tutorial.html)
which introduces loading, filtering, grouping, and missing values
using the Titanic data set.

## Loading data

### CSV files

`Frame.ReadCsv` loads CSV (and TSV) files. The most common usage is a
single path, but it accepts many optional parameters:

 * `path` — file path or URL
 * `indexCol` — column to use as row index (the type is inferred from the type parameter)
 * `inferTypes` — whether to auto-detect column types (default `true`)
 * `inferRows` — number of rows used for type inference (default 100; 0 = all)
 * `schema` — explicit CSV schema string
 * `separators` — column separator characters (e.g. `";"`)
 * `culture` — culture name for parsing (default invariant)
*)

let titanic = Frame.ReadCsv(root + "titanic.csv")

// Use a typed index column and sort
let msft = 
  Frame.ReadCsv(root + "stocks/msft.csv") 
  |> Frame.indexRowsDateTime "Date"
  |> Frame.sortRowsByKey

// Semicolon-separated file
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")

// Shorthand: specify index column via type parameter
let msftSimpler = 
  Frame.ReadCsv<DateTime>(root + "stocks/msft.csv", indexCol="Date") 
  |> Frame.sortRowsByKey

(**
### Saving CSV files
*)

// Save with semicolon separator
air.SaveCsv(Path.GetTempFileName(), separator=';')

// Include the row key as a column named "Date"
msft.SaveCsv(Path.GetTempFileName(), keyNames=["Date"], separator='\t')

(**
By default `SaveCsv` omits the row key. Pass `includeRowKeys=true` or
provide `keyNames` to include it.

### From F# records or .NET objects

`Frame.ofRecords` turns any sequence of records or objects into a frame,
using public properties as columns:
*)

type Person = 
  { Name: string; Age: int; Countries: string list }

let peopleRecds = 
  [ { Name = "Joe"; Age = 51; Countries = ["UK"; "US"; "UK"] }
    { Name = "Tomas"; Age = 28; Countries = ["CZ"; "UK"; "US"; "CZ"] }
    { Name = "Eve"; Age = 2; Countries = ["FR"] }
    { Name = "Suzanne"; Age = 15; Countries = ["US"] } ]

let people = 
  Frame.ofRecords peopleRecds 
  |> Frame.indexRowsString "Name"

(*** include-value: people ***)

(**
### Expanding nested objects

If a column contains complex .NET objects, `Frame.expandCols` flattens
their properties into new columns:
*)

let peopleNested = 
  [ "People" => Series.ofValues peopleRecds ] |> frame

peopleNested |> Frame.expandCols ["People"]
(*** include-it ***)

(**

## Getting and setting data

### Columns and rows
*)

// Get column as float series (using ?)
people?Age

// Get column with explicit type
people.GetColumn<string list>("Countries")

// All columns as a series of series
people.Columns

(**
### Adding and replacing columns
*)

// Add a computed column
people?AgePlusOne <- people?Age |> Series.mapValues ((+) 1.0)

// Add from a list (must match row count)
people?Siblings <- [0; 2; 1; 3]

// Replace an existing column
people.ReplaceColumn("Siblings", [3; 2; 1; 0])

(**
### Adding rows
*)

let newRow = 
  [ "Name" => box "Jim"; "Age" => box 51;
    "Countries" => box ["US"]; "Siblings" => box 5 ]
  |> series

people.Merge("Jim", newRow)

(**

## Slicing and lookup

### Indexing into series
*)

let ages = people?Age

// Single key
ages.["Tomas"]

// Multiple keys
ages.[ ["Tomas"; "Joe"] ]

// Safe lookup (returns None for missing keys)
ages |> Series.tryGet "John"

// Series that may contain missing values for unknown keys
ages |> Series.getAll [ "Tomas"; "John" ]

(**
### Iterating observations
*)

// All key-value pairs
ages |> Series.observations

// Including missing values as None
ages |> Series.observationsAll

(**
### Slicing ordered series by range
*)

let opens = msft?Open
opens.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)]
|> Series.mapKeys (fun k -> k.ToShortDateString())
(*** include-it ***)

(**

## Grouping and aggregation

The [quick start tutorial](tutorial.html) shows basic `Frame.groupRowsByString`,
`Frame.aggregateRowsBy`, and `Frame.pivotTable`. This section covers deeper
features: hierarchical keys, `Frame.nest`/`Frame.unnest`, and multi-level
aggregation.

### Hierarchical (multi-level) keys

Grouping produces tuple keys treated as a hierarchical index:
*)

// Group MSFT stock prices by decade
let decades = msft |> Frame.groupRowsUsing (fun k _ -> 
  sprintf "%d0s" (k.Year / 10))

// Mean Close price per decade
decades?Close |> Stats.levelMean fst
(*** include-it ***)

// Means for all numeric columns
decades
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofColumns
(*** include-it ***)

(**
### Multi-level grouping
*)

// Group Titanic by class then port of embarkation
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3

// Average age per (Embarked, Pclass) group
byClassAndPort?Age
|> Stats.levelMean Pair.get1And2Of3
(*** include-it ***)

// Survival counts per group
byClassAndPort.GetColumn<bool>("Survived")
|> Series.applyLevel Pair.get1And2Of3 (Series.values >> Seq.countBy id >> series)
|> Frame.ofRows
(*** include-it ***)

(**
### Nesting and unnesting

`Frame.nest` splits a grouped frame into a series of sub-frames;
`Frame.unnest` reverses this:
*)

let bySex = titanic |> Frame.groupRowsByString "Sex"
let nested = bySex |> Frame.nest      // Series of frames
let flat = nested |> Frame.unnest     // Back to single frame

(**

### Grouping series
*)

let travels = people.GetColumn<string list>("Countries")

// Country visit frequency per person, as a frame
travels
|> Series.mapValues (Seq.countBy id >> series)
|> Frame.ofRows
|> Frame.fillMissingWith 0
(*** include-it ***)

(**
### aggregateRowsBy

`Frame.aggregateRowsBy` is Deedle's equivalent of SQL
`GROUP BY … SELECT aggregate(col)`:
*)

titanic
|> Frame.aggregateRowsBy ["Pclass"; "Sex"] ["Fare"] Stats.mean
(*** include-it ***)

titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Age"] Stats.mean
(*** include-it ***)

(**
### Pivot tables

Cross-tabulate two categorical variables with an aggregation function:
*)

// Passenger counts by Sex × Survived
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    Frame.countRows 
(*** include-it ***)

// Mean age by Sex × Survived
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    (fun frame -> frame?Age |> Stats.mean)
|> round
(*** include-it ***)

(**

## Handling missing values

The [quick start](tutorial.html) covers `fillMissingWith`, `fillMissing`, and `dropMissing`.
This section shows how missing values arise and the more advanced `fillMissingUsing` strategy.

### How missing values arise

`Double.NaN`, `null`, and empty `Nullable<T>` are all treated as missing:
*)

Series.ofValues [ Double.NaN; 1.0; 3.14 ]
(*** include-it ***)

[ Nullable(1); Nullable(); Nullable(3) ] |> Series.ofValues
(*** include-it ***)

(**
### Custom fill with interpolation

`Series.fillMissingUsing` calls a function for each missing key, enabling
strategies like linear interpolation:
*)

let ozone = air?Ozone

ozone |> Series.fillMissingUsing (fun k -> 
  let prev = ozone.TryGet(k, Lookup.ExactOrSmaller)
  let next = ozone.TryGet(k, Lookup.ExactOrGreater)
  match prev, next with 
  | OptionalValue.Present(p), OptionalValue.Present(n) -> (p + n) / 2.0
  | OptionalValue.Present(v), _ 
  | _, OptionalValue.Present(v) -> v
  | _ -> 0.0)

(**
For the full missing-value reference (sentinel types, all fill strategies,
interaction with joins), see [Handling missing values](missing.html).
*)
