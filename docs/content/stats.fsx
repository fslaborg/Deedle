(*** hide ***)
#load "../../bin/net45/Deedle.fsx"
#load "../../packages/FSharp.Charting/lib/net45/FSharp.Charting.fsx"
open System
open System.Globalization
open System.IO
open FSharp.Data
open Deedle
open FSharp.Charting
let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Calculating frame and series statistics
=======================================

The `Stats` type contains functions for fast calculation of statistics over
series and frames as well as over a moving and an expanding window in a series. 
The standard statistical functions that are available in the `Stats` type 
are overloaded and can be applied to both data frames and series. More advanced
functionality is available only for series (but can be applied to frame columns
easily using the `Frame.getNumericCols` function.

<a name="stats"></a>
Series and frame statistics
---------------------------

In this section, we look at calculating simple statistics over data frame and
series. An important aspect is handling of missing values, so we demonstrate that
using a data set about air quality that contains missing values. The following
snippet loads `airquality.csv` and shows the values in the `Ozone` column:
*)
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone
(*** include-value: ozone ***)

(**
### Series statistics

Given a series `ozone`, we can use a number of `Stats` functions to calculate 
statistics. The following example creates a series (indexed by strings) that
stores mean extremes and median of the input series:
*)

(*** define-output: ozinfo ***)
series [
  "Mean" => round (Stats.mean ozone)
  "Max" => Stats.max ozone
  "Min" => Stats.min ozone
  "Median" => Stats.median ozone ]

(*** include-it: ozinfo ***)

(**
To make the output simpler, we round the value of the mean (although the result is
a floating point number). Note that the value is calculated from the *available* values
in the series. All of the statistical functions skip over missing values in the 
input series.

As the above example demonstrates, `Stats.max` and `Stats.min` return `option<float>`
rather than just `float`. The result value is `None` when the series contains no values.
This makes it possible to use the functions not just on floating point numbers, but
also on series of integers and other types. Other statistical functions such as 
`Stats.mean` return `nan` when no values are available.

### Frame statistics

Functions such as `Stats.mean` can be called on series, but also on entire data frames.
In that case, they calculate the statistics for each column of a data frame and return
`Series<'C, float>` where `'C` is the column key of the original frame. 

In the following snippet, we calculate means and standard deviations of all columns of
the `air` data set and build a frame that shows the values (series) in two columns:
*)
(*** define-output: airinfo ***)
let info = 
  [ "Min" => Stats.min air
    "Max" => Stats.max air
    "Mean" => Stats.mean air
    "+/-" => Stats.stdDev air ] |> frame
(*** include-value: round(info*100.0)/100.0 ***)

(**
Missing values are handled in the same way as when calculating statistics of a series
and are skipped. If this is not desirable, you can use functions from the [Series 
module](reference/deedle-seriesmodule.html) for working with missing values to treat 
missing values in different ways.

The `Stats` module provides basic statistical functionality such as mean, standard
deviation and variance, but also more advanced functions including skewness and kurtosis.
You can find a complete list in the [Series statistics](reference/deedle-stats.html#section5)
and [Frame statistics](reference/deedle-stats.html#section1) sections of the API reference.

<a name="moving"></a>
Moving window statistics
------------------------

The `Stats` type provides an efficient implementation of moving window statistics. The
implementation uses an online algorithm so that it does not have to re-calculate the 
statistics for each window separately, but instead updates the value as it iterates over
the input (and so this is faster than using `Series.window`). 

The moving window function names are pre-fixed with the word `moving` and calculate moving
statistics over a window of a fixed length. The following example calculates means over a
moving window of length 3:
*)
(*** define-output:mvmozone ***)
ozone
|> Stats.movingMean 3
(*** include-it:mvmozone ***)

(**
The keys of the resulting series are the same as the keys of the input series. Statistical
moving functions (count, sum, mean, variance, standard deviation, skewness and kurtosis)
over a window of size _n_ always mark the first _n-1_ values with missing (i.e. they only
perform the calculation over complete windows). This explains why the value associated with
the key _1_ is _N/A_. For the key _2_, the mean is calculated from all available values in 
the window, which is: _(36+12)/2_.

The boundary behavior of the functions that calculate minimum and maximum over a moving window
differs. Rather than returning _N/A_ for the first _n-1_ values, they return the extreme 
value over a smaller window:
*)
(*** define-output:mvxozone ***)
ozone
|> Stats.movingMin 3
(*** include-it:mvxozone ***)

(**
Here, the first value is missing, because the one-element window containing just the first value
contains only missing values. However, the value for the key _1_, because the two-element window
(starting from the beginning of the series) contains two elements.

### Remarks

The windowing functions in the `Stats` type support an efficient calculations over a fixed-size
windows specified by the size of the window. They also provide one, fixed, boundary behavior.
If you need more complex windowing behavior (such as window based on the distance between keys), 
different handling of boundaries, or chunking (calculation over adjacent chunks), you can use 
chunking and windowing functions from the `Series` module such as `Series.windowSizeInto` or
`Series.chunkSizeInto`. For more information, see [Grouping, windowing and 
chunking](reference/deedle-seriesmodule.html#section1) section in the API reference.

<a name="exp"></a>
Expanding windows
-----------------

Expanding window means that the window starts as a single-element sized window at the beginning
of a series and expands as it moves over the series. For a time-series data ordered by time,
this gives you statistics calculated over all previous known observations. 
In other words, the statistics is calculated for all values up to the current key and the 
result is attached to the key at the end of the window. The expanding window functions are
prefixed with `expanding`. 

The following example demonstrates how to calculate expanding mean and expanding standard
deviation over the Ozone series. The resulting series has the same keys as the input series.
Here, we align the two series using a frame, so that we can easily see the results aligned:
*)
let exp =
  [ "Ozone" => ozone 
    "Mean" => Stats.expandingMean(ozone)
    "+/-" => Stats.expandingStdDev(ozone) ] |> frame
(*** include-value:(round(exp*100.0))/100.0 ***)

(**
As the example illustrates, expanding window statistics typically returns a series that starts
with some missing values. Here, the first mean is missing (because one-element window contains
no values) and the first two standard deviations are missing (`stdDev` is define only for two
and more values). The only exception is `expandingSum`, because the sum of no elements is zero.

<a name="multi"></a>
Multi-level indexed statistics
------------------------------

For a series with multi-level (hierarchical) index, the functions prefixed with `level` provide 
a way to apply statistical operation on a single level of the index. Series with multi-level 
index can be created directly by using a tuple (such as `'K1 * 'K2`) as the key, or they can
be produced by a grouping operation such as `Frame.groupRowsBy`.

For example, you can create two-level index that represents time-series data with month as the
first part of the key and day as the second part of the key. Then you can use multi-level
statistical functions to calculate means (and other statistics) for each month separately.

The following example demonstrates the idea - the `air` data set contains data for each
day between May and September. We can create a frame with two-level row key using 
`Frame.indexRowsUsing` and returning a tuple as the index:
*)
let dateFormat = CultureInfo.CurrentCulture.DateTimeFormat
let byMonth = air |> Frame.indexRowsUsing (fun r ->
    dateFormat.GetMonthName(r.GetAs("Month")), r.GetAs<int>("Day"))
(**
The type of the `byMonth` value is `Frame<string * int, string>` meaning that the row index
has two levels. To make the output a little nicer, we use the `GetMonthName` function to 
turn the first level of the index into a string representing the month name.

We can now access individual columns and calculate statistics over the 
first level (individual months) using functions prefixed with `level`:
*)

(*** define-output:lvlozone ***)
byMonth?Ozone
|> Stats.levelMean fst
(*** include-it:lvlozone ***)

(**
Currently, the `Stats` type does not include a function that would let you apply multi-level
statistical functions on entire data frames, but this can easily be implemented using the
`Frame.getNumericalCols` function and `Series.mapValues`:
*)
(*** define-output:lvlall ***)
byMonth
|> Frame.sliceCols ["Ozone";"Solar.R";"Wind";"Temp"]
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofRows
(*** include-it:lvlall ***)
(**
If we used `Frame.getNumericCols` directly, we would also calculate the mean of "Day" and 
"Month" columns, which does not make much sense in this example. For that reason, the snippet
first calls `sliceCols` to get only relevant columns.
*)
