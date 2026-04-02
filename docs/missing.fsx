(**
---
title: Handling missing values
category: Documentation
categoryindex: 1
index: 9
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

(**

# Handling missing values

Missing-value support is a first-class feature of Deedle. Every series and frame
can contain missing values; the library tracks them explicitly and handles them
consistently across all operations.

<a name="representation"></a>

## How missing values are represented

Deedle uses `OptionalValue<'T>` to represent a value that may or may not be
present. You rarely construct `OptionalValue` directly; instead, certain input
values are automatically treated as missing:

| Input type | Treated as missing when |
|---|---|
| `float` / `double` | Value is `Double.NaN` |
| Reference type (string, obj …) | Value is `null` |
| `Nullable<T>` | `.HasValue` is `false` |

The following examples show series created from inputs that include missing values:
*)
// float NaN becomes a missing value
Series.ofValues [ 1.0; Double.NaN; 3.0 ]
(*** include-it ***)

// null in a reference-type series
Series.ofValues [ "a"; null; "c" ]
(*** include-it ***)

// Nullable<int> without a value
[ Nullable(1); Nullable(); Nullable(3) ] |> Series.ofValues
(*** include-it ***)

(**
You can also construct a series with explicit missing values using `None`:
*)
Series.ofOptionalObservations
  [ 1 => Some(10.0)
    2 => None
    3 => Some(30.0) ]
(*** include-it ***)

(**

<a name="counting"></a>

## Counting present and missing values

`Stats.count` counts the number of **present** values; `Series.countValues`
is an alias. The total number of keys (including missing) is `KeyCount`:
*)
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone

// total keys (rows) in the series
ozone.KeyCount
(*** include-it ***)

// present (non-missing) values
Stats.count ozone
(*** include-it ***)

// number of missing values
ozone.KeyCount - int (Stats.count ozone)
(*** include-it ***)

(**

<a name="statistics"></a>

## Statistics skip missing values

All functions in the `Stats` module, as well as common projections such as
`Series.mapValues` and `Series.filter`, automatically skip missing values.
The operation is applied only to present observations:
*)
Stats.mean ozone      // mean of the 116 present values
(*** include-it ***)

Stats.max ozone       // maximum of the present values
(*** include-it ***)

(**

<a name="accessing"></a>

## Accessing values with `TryGet`

The safe way to look up a single value by key is `TryGet`, which returns an
`OptionalValue<'T>`:
*)
let v = ozone.TryGet(1)
match v with
| OptionalValue.Present x -> sprintf "present: %g" x
| OptionalValue.Missing   -> "missing"
(*** include-it ***)

(**
You can also use `Series.observationsAll` to iterate over all key-value pairs
including missing ones (as `option`), or `Series.observations` to skip
missing values:
*)
ozone
|> Series.observationsAll
|> Seq.truncate 6
|> Seq.map (fun (k, v) ->
    match v with
    | Some x -> sprintf "%d => %g" k x
    | None   -> sprintf "%d => <missing>" k)
|> Seq.toList
(*** include-it ***)

(**

<a name="mapall"></a>

## Custom handling with `Series.mapAll`

`Series.mapValues` skips missing values. When you need to transform the value
**or** the missing-ness of each element, use `Series.mapAll`, which receives an
`option<'T>` for each key:
*)
ozone
|> Series.mapAll (fun k v ->
    match v with
    | None   -> Some 0.0     // replace missing with zero
    | Some x -> Some (x * 2.0)) // double present values
|> Series.take 5
(*** include-it ***)

(**

<a name="filling"></a>

## Filling missing values

### Fill with a constant

The simplest strategy replaces every missing value with a fixed constant:
*)
ozone |> Series.fillMissingWith 0.0 |> Series.take 6
(*** include-it ***)

(**
### Forward and backward fill

`Series.fillMissing` propagates the most recent available value in the
specified direction:
*)
// Carry the last known value forward
ozone |> Series.fillMissing Direction.Forward |> Series.take 6
(*** include-it ***)

// Fill from the next available value backward
ozone |> Series.fillMissing Direction.Backward |> Series.take 6
(*** include-it ***)

(**
### Custom fill strategy with `fillMissingUsing`

For interpolation or other context-sensitive strategies, use
`Series.fillMissingUsing`. The function receives the missing key and should
return a replacement value:
*)
ozone
|> Series.fillMissingUsing (fun k ->
    // Linear interpolation from neighbours
    let prev = ozone.TryGet(k, Lookup.ExactOrSmaller)
    let next = ozone.TryGet(k, Lookup.ExactOrGreater)
    match prev, next with
    | OptionalValue.Present p, OptionalValue.Present n -> (p + n) / 2.0
    | OptionalValue.Present v, _
    | _, OptionalValue.Present v -> v
    | _ -> 0.0)
|> Series.take 6
(*** include-it ***)

(**
### Combining fill and drop

Often the cleanest approach is to fill as much as possible in one direction
and then discard the remaining missing values:
*)
ozone
|> Series.fillMissing Direction.Forward
|> Series.dropMissing
|> Series.countValues
(*** include-it ***)

(**

<a name="dropping"></a>

## Dropping missing values

### Drop from a series

`Series.dropMissing` removes all missing observations from a series:
*)
ozone |> Series.dropMissing |> Series.countValues
(*** include-it ***)

(**
### Drop sparse rows and columns from a frame

`Frame.dropSparseRows` removes any row that contains at least one missing
value; `Frame.dropSparseCols` removes columns with any missing value.

After reading the air quality CSV the frame has missing values in several
columns:
*)
air.RowCount
(*** include-it ***)

// Keep only rows that are fully observed
let airComplete = air |> Frame.dropSparseRows
airComplete.RowCount
(*** include-it ***)

(**

<a name="frames"></a>

## Missing values in frames

The same filling functions are available at the frame level and operate
column-by-column:
*)
// Fill every missing cell with 0.0
air
|> Frame.fillMissingWith 0.0
|> Frame.dropSparseRows   // now no rows should be dropped
|> fun f -> f.RowCount
(*** include-it ***)

// Forward-fill each column independently
air
|> Frame.fillMissing Direction.Forward
|> Frame.dropSparseRows
|> fun f -> f.RowCount
(*** include-it ***)

(**
`Frame.fillMissingUsing` accepts a function `Series<'R,'T> -> 'R -> 'T` so it
can base the fill value on the whole column series:
*)
air
|> Frame.fillMissingUsing (fun col key ->
    // Fill with that column's mean
    Stats.mean col)
|> Frame.dropSparseRows
|> fun f -> f.RowCount
(*** include-it ***)

(**

<a name="joins"></a>

## Missing values in joins

When two frames or series are joined, rows that exist in one source but not
the other receive missing values for the absent columns. The join kind controls
which rows are retained:

| Join kind | Rows kept | Missing values introduced |
|---|---|---|
| `JoinKind.Inner` | Only rows present in **both** | None |
| `JoinKind.Left` | All rows from the **left** | Right columns for unmatched rows |
| `JoinKind.Right` | All rows from the **right** | Left columns for unmatched rows |
| `JoinKind.Outer` | All rows from **either** | Both sides for unmatched rows |

*)
let s1 = series [ 1 => 10.0; 2 => 20.0; 3 => 30.0 ]
let s2 = series [ 2 => 200.0; 3 => 300.0; 4 => 400.0 ]

// Outer join introduces missing values for key 1 (not in s2) and key 4 (not in s1)
Frame.ofColumns ["A" => s1; "B" => s2]
(*** include-it ***)

// Inner join keeps only keys present in both
let f1 = frame ["A" => s1]
let f2 = frame ["B" => s2]
f1.Join(f2, JoinKind.Inner)
(*** include-it ***)

(**
After an outer join, `Frame.dropSparseRows` or `Frame.fillMissing` can be
used to bring the frame back to a fully populated state.

## Summary

| Goal | Function |
|---|---|
| Count present values | `Stats.count series` |
| Count keys (incl. missing) | `series.KeyCount` |
| Safe lookup | `series.TryGet(key)` → `OptionalValue<T>` |
| Iterate with missing | `Series.observationsAll` |
| Handle in a transform | `Series.mapAll` |
| Fill with constant | `Series.fillMissingWith value` / `Frame.fillMissingWith value` |
| Carry forward/back | `Series.fillMissing Direction.*` / `Frame.fillMissing Direction.*` |
| Custom fill | `Series.fillMissingUsing f` / `Frame.fillMissingUsing f` |
| Remove missing rows | `Series.dropMissing` / `Frame.dropSparseRows` |
| Remove missing cols | `Frame.dropSparseCols` |

## See also

 * [Joining and merging frames](joining.html) — how joins interact with missing values.
 * [Data frame features](frame.html) — the frame overview including a shorter introduction
   to missing values in context.
 * [Series features](series.html) — windowing and resampling functions that produce missing
   values at boundaries.
 * [Statistics](stats.html) — how `Stats.*` functions skip over missing values.
*)
