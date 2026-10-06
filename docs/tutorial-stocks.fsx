(**
---
title: Tutorial: Analysing stock prices
category: Tutorials
categoryindex: 0
index: 1
description: Load, align, resample and analyse stock price time series: returns, volatility, rolling averages, drawdown
keywords: tutorial, stock prices, time series, finance, returns, pandas, F#
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

# Tutorial: Analysing stock prices

*This is the Deedle equivalent of the classic "pandas stock analysis" tutorial.*
We load daily prices for four companies, align them into a single frame, compute
returns and volatility, smooth them with rolling windows, and downsample to monthly data.
The CSV files in `docs/data/stocks` come from Yahoo Finance (2013).

## Load one file per ticker

Each file has `Date,Open,High,Low,Close,Volume,Adj Close` columns, newest row first.
We turn `Date` into the row index and sort so that time runs forward:
*)

let loadStock (ticker: string) =
  Frame.ReadCsv(root + "stocks/" + ticker + ".csv")
  |> Frame.indexRowsDateTime "Date"
  |> Frame.sortRowsByKey

let msft = loadStock "msft"
msft |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
## Combine adjusted closing prices into one frame

Building a frame from named series *aligns them by date automatically*. Dates where a
company did not trade yet (Facebook only listed in 2012) become missing values:
*)

let closes =
  frame [ for t in ["msft"; "goog"; "aapl"; "fb"] ->
            t => (loadStock t)?``Adj Close`` ]

closes |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
We only want dates where every stock has a price, so drop rows with any missing value:
*)

let prices = closes |> Frame.dropSparseRows
prices.RowCount
(*** include-fsi-merged-output ***)

(**
## Slice by date

Row slicing with dates works on ordered indices, and is inclusive at both ends:
*)

prices.Rows.[DateTime(2013, 10, 1) .. DateTime(2013, 10, 4)]
(*** include-fsi-merged-output ***)

(**
## Daily returns

`Frame.shift 1` moves every value one row forward, so dividing the frame by its shifted copy
gives day-over-day price ratios:
*)

let returns = closes / (closes |> Frame.shift 1) - 1.0 |> Frame.dropSparseRows

returns |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
## Summary statistics, volatility and correlation

`Stats.describe` summarises every column at once:
*)

Stats.describe returns
(*** include-fsi-merged-output ***)

(**
The standard deviation of daily returns is the usual measure of volatility.
Here Facebook is clearly the most volatile:
*)

returns |> Frame.getNumericCols |> Series.mapValues Stats.stdDev
(*** include-fsi-merged-output ***)

(**
How closely do Microsoft and Google move together?
*)

Stats.corr returns?msft returns?goog
(*** include-fsi-merged-output ***)

(**
## Best days

Sort a series by value to find the largest one-day gains:
*)

returns?msft |> Series.sortBy (fun v -> -v) |> Series.take 3
(*** include-fsi-merged-output ***)

(**
## Rolling windows

`Stats.movingMean` computes a trailing average. Comparing a price to its 20-day average
is a simple trend signal. Here we count how many days Microsoft traded above its average:
*)

let ma20 = prices?msft |> Stats.movingMean 20
let aboveTrend =
  (prices?msft - ma20)
  |> Series.dropMissing
  |> Series.filterValues (fun v -> v > 0.0)

aboveTrend.KeyCount
(*** include-fsi-merged-output ***)

(**
Expanding functions use *all* history up to each point. The running maximum gives the
maximum drawdown (the worst fall from a previous peak):
*)

let drawdown = prices?msft / Stats.expandingMax prices?msft - 1.0
Stats.min drawdown
(*** include-fsi-merged-output ***)

(**
## Downsample to monthly data

`Series.resampleEquiv` groups observations whose keys map to the same value.
Mapping every date to the first of its month and taking the last price in each group
gives month-end prices:
*)

let monthly =
  prices
  |> Frame.mapCols (fun _ s ->
      s.As<float>()
      |> Series.resampleEquiv (fun d -> DateTime(d.Year, d.Month, 1))
      |> Series.mapValues Series.lastValue
      :> ISeries<_>)

monthly |> Frame.take 3
(*** include-fsi-merged-output ***)

(**
## Where next?

 * [Series and time series features](series.html) covers windowing, chunking and
   sampling in depth.
 * [Calculating statistics](stats.html) lists all moving-window and expanding functions.
 * [Deedle.MathNetNumerics](math.html) adds exponentially weighted statistics and regression.
*)
