(*** hide ***)
#load "../../bin/Deedle.fsx"
#load "../../packages/FSharp.Charting.0.90.6/FSharp.Charting.fsx"
open System
open System.IO
open FSharp.Data
open Deedle
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Numerics and statistics
===================

The `Stats` type contains functions for fast calculation of statistics over
series and frames as well as over a moving and an expanding window in a series. 

The resulting series has the same keys as the input series. When there are
no values, or missing values, different functions behave in different ways.
Statistics (e.g. mean) return missing value when any value is missing, while min/max
functions return the minimal/maximal element (skipping over missing values).

## Series statistics

Functions such as `count`, `mean`, `kurt` etc. return the
statistics calculated over all values of a series. The calculation skips
over missing values (or `nan` values), so for example `mean` returns the
average of all _present_ values.

## Frame statistics

The standard functions are exposed as static members and are 
overloaded. This means that they can be applied to both `Series<'K, float>` and 
to `Frame<'R, 'C>`. When applied to data frame, the functions apply the 
statistical calculation to all numerical columns of the frame.

## Moving windows

Moving window means that the window has a fixed size and moves over the series.
In this case, the result of the statisitcs is always attached to the last key
of the window. The function names are prefixed with `moving`.

## Expanding windows

Expanding window means that the window starts as a single-element sized window
and expands as it moves over the series. In this case, statistics is calculated
for all values up to the current key. This means that the result is attached
to the key at the end of the window. The function names are prefixed
with `expanding`.

## Multi-level statistics

For a series with multi-level (hierarchical) index, the
functions prefixed with `level` provide a way to apply statistical operation on 
a single level of the index. (For you can sum values along the `'K1` keys in a 
series `Series<'K1 * 'K2, float>` and get `Series<'K1, float>` as the result.)

## Remarks

The windowing functions in the `Stats` type support calculations over a fixed-size
windows specified by the size of the window. If you need more complex windowing 
behavior (such as window based on the distance between keys), different handling
of boundary, or chunking (calculation over adjacent chunks), you can use chunking and
windowing functions from the `Series` module such as `Series.windowSizeInto` or
`Series.chunkSizeInto`.

*)