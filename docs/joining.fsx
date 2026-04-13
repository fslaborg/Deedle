(**
---
title: Joining, merging and appending frames
category: Guides
categoryindex: 1
index: 8
description: Inner, outer, and ordered joins on frames and series, plus merging and appending
keywords: join, merge, append, inner join, outer join, align, concatenate
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

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)

(**

# Joining, Merging and Appending Frames

Deedle provides a rich set of operations for combining data frames and series. This page covers:

* [Frame joins](#joins) — align two frames on their row/column keys
* [Series zipping](#zipping) — combine two series element-by-element
* [Append / concatenate](#append) — stack frames vertically or horizontally
* [Merge series](#merge) — merge sparse series into one

<a name="joins"></a>

## Frame joins

`Frame.join` combines two frames that share the same row-key type. The `JoinKind`
controls which rows appear in the result.

*)

// Stock prices for two securities
let aapl =
    Frame.ofColumns [
        "AAPL Open",  Series.ofValues [ 150.0; 152.0; 151.0; 153.0 ]
        "AAPL Close", Series.ofValues [ 151.5; 151.0; 153.5; 154.0 ]
    ]
    |> Frame.indexRowsWith [| "2024-01-15"; "2024-01-16"; "2024-01-17"; "2024-01-18" |]
(*** include-fsi-merged-output ***)

let msft =
    Frame.ofColumns [
        "MSFT Open",  Series.ofValues [ 370.0; 375.0; 373.0 ]
        "MSFT Close", Series.ofValues [ 374.0; 373.0; 376.0 ]
    ]
    |> Frame.indexRowsWith [| "2024-01-15"; "2024-01-16"; "2024-01-18" |]
(*** include-fsi-merged-output ***)

(**

### Outer join — keep all rows from both frames

*)

let outerJoin = Frame.join JoinKind.Outer aapl msft
(*** include-fsi-merged-output ***)

(**

An outer join includes all row keys from either frame. Columns from each frame
are combined. When column names clash, they are suffixed with `.1` and `.2`.

Missing values appear where one frame had no data for a given key.

### Inner join — keep only rows present in both frames

*)

let innerJoin = Frame.join JoinKind.Inner aapl msft
(*** include-fsi-merged-output ***)

(**

### Left join — keep all rows from the left frame

*)

let leftJoin = Frame.join JoinKind.Left aapl msft
(*** include-fsi-merged-output ***)

(**

### Lookup join — align on nearest key

When frames have different but ordered key sets, you can fill missing values by
looking up the nearest available value using `Frame.joinAlign`.

*)

let daily =
    frame [ "Price" => Series.ofValues [ 100.0; 101.0; 102.0; 103.0; 104.0 ] ]
    |> Frame.indexRowsWith [| 1; 2; 3; 4; 5 |]
(*** include-fsi-merged-output ***)

let sparse =
    frame [ "Signal" => Series.ofValues [ 0.0; 1.0 ] ]
    |> Frame.indexRowsWith [| 1; 3 |]
(*** include-fsi-merged-output ***)

// Fill missing signal values by carrying forward the last known value
let aligned = Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller daily sparse
(*** include-fsi-merged-output ***)

(**

`Lookup.ExactOrSmaller` picks the nearest key that is ≤ the row key (forward fill).
Use `Lookup.ExactOrGreater` for backward fill.

<a name="zipping"></a>

## Series zipping

`Series.zip` and `Series.zipInto` combine two series into a series of tuples or
aggregated values, aligning on matching keys.

*)

let s1 = Series.ofValues [ 1.0; 2.0; 3.0 ] |> Series.indexWith [| "a"; "b"; "c" |]
let s2 = Series.ofValues [ 10.0; 30.0 ]    |> Series.indexWith [| "a"; "c" |]
(*** include-fsi-merged-output ***)

// Pair up values — missing in s2 where key is absent
let zipped = Series.zip s1 s2
(*** include-fsi-merged-output ***)

// Sum corresponding values (inner join semantics — only where both have values)
let summed = Series.zipInto (fun a b -> a + b) s1 s2
(*** include-fsi-merged-output ***)

(**

<a name="append"></a>

## Appending and concatenating frames

### `Frame.appendRowsBy` / `Frame.merge`

Use `Frame.appendRowsBy` to stack two frames with compatible columns vertically.

*)

let q1 =
    frame [ "Sales" => Series.ofValues [ 100.0; 120.0; 130.0 ] ]
    |> Frame.indexRowsWith [| 1; 2; 3 |]
(*** include-fsi-merged-output ***)

let q2 =
    frame [ "Sales" => Series.ofValues [ 140.0; 110.0; 150.0 ] ]
    |> Frame.indexRowsWith [| 4; 5; 6 |]
(*** include-fsi-merged-output ***)

// Concatenate the two frames vertically (row-wise)
let fullYear = Frame.merge q1 q2
(*** include-fsi-merged-output ***)

(**

### Adding columns from another frame

Use `Frame.addCol` or `Frame.join` to add columns to an existing frame.

*)

let withExtra =
    fullYear
    |> Frame.addCol "Month" (Series.ofValues [ "Jan"; "Feb"; "Mar"; "Apr"; "May"; "Jun" ]
                              |> Series.indexWith [| 1; 2; 3; 4; 5; 6 |])
(*** include-fsi-merged-output ***)

(**

<a name="merge"></a>

## Merging sparse series

`Series.merge` fills gaps in one series with values from another. The *first* series
takes priority; values from the *second* are used only where the first is missing.

*)

let primary   = Series.ofOptionalObservations [ (1, Some 10.0); (2, None); (3, Some 30.0); (4, None) ]
let secondary = Series.ofOptionalObservations [ (2, Some 20.0); (4, Some 40.0) ]
(*** include-fsi-merged-output ***)

let merged = Series.merge primary secondary
(*** include-fsi-merged-output ***)

(**

The result contains values from `primary` where present, and falls back to `secondary`
for missing positions.

You can also use `Frame.mergeAll` to merge a list of frames, or combine `Series.merge`
with `Series.map` and `Series.zip` for more complex merge strategies.

## See also

 * [Handling missing values](missing.html) — how to fill or drop the missing values
   introduced by outer joins.
 * [Data frame features](frame.html) — frame construction, slicing, grouping, and aggregation.
 * [Series features](series.html) — alignment, resampling, and windowing operations.

*)
