(**
---
title: Calculating frame and series statistics
category: Guides
categoryindex: 1
index: 5
description: Computing mean, variance, standard deviation, correlation, and other statistics on frames and series
keywords: statistics, mean, variance, standard deviation, correlation, moving average
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
open System.Globalization
open System.IO
open Deedle

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)

let root = __SOURCE_DIRECTORY__ + "/data/"

(**

# Calculating frame and series statistics

The `Stats` type contains functions for fast calculation of statistics over
series and frames as well as over a moving and an expanding window in a series. 
The standard statistical functions that are available in the `Stats` type 
are overloaded and can be applied to both data frames and series. More advanced
functionality is available only for series (but can be applied to frame columns
easily using the `Frame.getNumericCols` function).

<a name="stats"></a>

## Series and frame statistics

In this section, we look at calculating simple statistics over data frame and
series. An important aspect is handling of missing values, so we demonstrate that
using a data set about air quality that contains missing values:
*)
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone
(*** include-fsi-merged-output ***)

(**
### Series statistics

Given a series `ozone`, we can use a number of `Stats` functions to calculate 
statistics. The following example creates a series (indexed by strings) that
stores mean, extremes and median of the input series:
*)

series [
  "Mean" => round (Stats.mean ozone)
  "Max" => Stats.max ozone
  "Min" => Stats.min ozone
  "Median" => Stats.median ozone ]

(*** include-fsi-merged-output ***)

(**
To make the output simpler, we round the value of the mean (although the result is
a floating point number). Note that the value is calculated from the *available* values
in the series. All of the statistical functions skip over missing values in the 
input series.

`Stats.max` and `Stats.min` return `option<float>` rather than just `float`. The result 
value is `None` when the series contains no values.

### Frame statistics

Functions such as `Stats.mean` can be called on series, but also on entire data frames.
In that case, they calculate the statistics for each column of a data frame and return
`Series<'C, float>` where `'C` is the column key of the original frame. 
*)
let info = 
  [ "Min" => Stats.min air
    "Max" => Stats.max air
    "Mean" => Stats.mean air
    "+/-" => Stats.stdDev air ] |> frame
(*** include-fsi-merged-output ***)

(**
<a name="moving"></a>

## Moving window statistics

The `Stats` type provides an efficient implementation of moving window statistics using
an online algorithm. The moving window function names are prefixed with the word `moving`:
*)
ozone |> Stats.movingMean 3
(*** include-fsi-merged-output ***)

(**
Statistical moving functions (count, sum, mean, variance, standard deviation, skewness 
and kurtosis) over a window of size _n_ always mark the first _n-1_ values with missing 
(i.e. they only perform the calculation over complete windows).

The boundary behavior of the functions that calculate minimum and maximum over a moving window
differs. Rather than returning _N/A_ for the first _n-1_ values, they return the extreme 
value over a smaller window:
*)
ozone |> Stats.movingMin 3
(*** include-fsi-merged-output ***)

(**
<a name="exp"></a>

## Expanding windows

Expanding window means that the window starts as a single-element sized window at the beginning
of a series and expands as it moves over the series. The expanding window functions are
prefixed with `expanding`. 
*)
let exp =
  [ "Ozone" => ozone 
    "Mean" => Stats.expandingMean(ozone)
    "+/-" => Stats.expandingStdDev(ozone) ] |> frame
(*** include-fsi-merged-output ***)

(**
<a name="multi"></a>

## Multi-level indexed statistics

For a series with multi-level (hierarchical) index, the functions prefixed with `level` provide 
a way to apply statistical operation on a single level of the index.

The following example demonstrates the idea - the `air` data set contains data for each
day between May and September. We create a frame with two-level row key using 
`Frame.indexRowsUsing` and returning a tuple as the index:
*)
let dateFormat = CultureInfo.CurrentCulture.DateTimeFormat
let byMonth = air |> Frame.indexRowsUsing (fun r ->
    dateFormat.GetMonthName(r.GetAs<int>("Month")), r.GetAs<int>("Day"))

(**
We can now access individual columns and calculate statistics over the 
first level (individual months) using functions prefixed with `level`:
*)

byMonth?Ozone |> Stats.levelMean fst
(*** include-fsi-merged-output ***)

byMonth
|> Frame.sliceCols ["Ozone";"Solar.R";"Wind";"Temp"]
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofRows
(*** include-fsi-merged-output ***)

(**

<a name="advanced"></a>

## Advanced statistics with Deedle.MathNetNumerics

The `Deedle.MathNetNumerics` package extends Deedle's statistical capabilities using
[MathNet.Numerics](https://numerics.mathnetchr.net). It adds:

 * **Correlation and covariance matrices** — `Stats.corr`, `Stats.cov` for full-frame
   Pearson or Spearman correlation and covariance.
 * **Exponentially weighted moving statistics** — `Stats.ewmMean`, `Finance.ewmVolStdDev`,
   `Finance.ewmCov` etc. for time-decayed calculations common in quantitative finance.
 * **Quantiles and ranks** — `Stats.quantile`, `Stats.ranks` backed by MathNet's exact
   algorithms.
 * **Linear algebra** — transpose, inverse, decompositions (LU, QR, SVD, Cholesky, Eigen)
   directly on `Frame<'R,'C>` values.
 * **PCA** — principal component analysis via `PCA.pca`.
 * **Linear regression** — ordinary least squares via `LinearRegression.ols`.

Install with:

```
dotnet add package Deedle.MathNetNumerics
```

See the [Deedle.MathNetNumerics documentation](math.html) for detailed examples.

*)
