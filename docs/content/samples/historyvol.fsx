// ----------------------------------------------------------------------------
// Load everything 
// ----------------------------------------------------------------------------

#I "../../../packages/FSharp.Charting/lib/net45"
#r "../../../packages/MathNet.Numerics/lib/net40/MathNet.Numerics.dll"
#load "../../../bin/net45/Deedle.fsx"
#r "../../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
#load "FSharp.Charting.fsx"

open System
open Deedle
open FSharp.Data
open FSharp.Charting

[<AutoOpen>]
module FsLabExtensions =
  type FSharp.Charting.Chart with
    static member Line(data:Series<'K, 'V>, ?Name, ?Title, ?Labels, ?Color, ?XTitle, ?YTitle) =
      Chart.Line(Series.observations data, ?Name=Name, ?Title=Title, ?Labels=Labels, ?Color=Color, ?XTitle=XTitle, ?YTitle=YTitle)
    static member Column(data:Series<'K, 'V>, ?Name, ?Title, ?Labels, ?Color, ?XTitle, ?YTitle) =
      Chart.Column(Series.observations data, ?Name=Name, ?Title=Title, ?Labels=Labels, ?Color=Color, ?XTitle=XTitle, ?YTitle=YTitle)

// ----------------------------------------------------------------------------
// Getting stock prices from Yahoo & working with time series
// ----------------------------------------------------------------------------

type Stocks = CsvProvider<"../data/stocks/fb.csv">

// Get historical stock prices using F# Data
let stockPrices name = 
  let prices = Stocks.Load(__SOURCE_DIRECTORY__ + "/../data/stocks/" + name + ".csv")
  [ for p in prices.Rows -> p.Date, float p.Open ]
  |> List.rev |> series

// Basic statistics using series
let msft = stockPrices "MSFT"
let msft13 = msft.[DateTime(2013, 1, 1) ..]

Stats.mean msft
Stats.mean msft13
Chart.Line(msft13)

// Compare current price with the mean over 2013
let avg13 = Stats.mean msft13

Chart.Combine
  [ Chart.Line(msft13)
    Chart.Line(series [ for k in msft13.Keys -> k, avg13 ] ) ]

// Plot the current price together with weekly floating window
let msft13w = msft13 |> Series.windowInto 5 Stats.mean

Chart.Combine
  [ Chart.Line(msft13)
    Chart.Line(msft13w) ]

// Calculate the daily returns
let returns = (msft13 - msft13.Shift(1)) / msft13 * 100.0 
Chart.Line(returns)

// Average daily returns over 1 month
returns
|> Series.windowInto 20 Stats.mean
|> Chart.Line

// ----------------------------------------------------------------------------
// Working with entire data frames
// ----------------------------------------------------------------------------

// Get data frame with some technology stocks
let tech =
  let stocks = [ "YHOO"; "GOOG"; "MSFT"; "FB" ]
  [ for stock in stocks -> stock => stockPrices stock ]
  |> frame |> Frame.sortRowsByKey

// Calculate the daily returns for days when we have all data
let techAll = tech.RowsDense.[*]
let techRet = (techAll - techAll.Shift(1)) / techAll * 100.0

// Average daily returns per company
techRet |> Stats.mean |> Chart.Column

// Average daily returns per day
techRet |> Frame.transpose |> Stats.mean |> Chart.Column

// ----------------------------------------------------------------------------
// Hierarchical indexing
// ----------------------------------------------------------------------------
  
let names = 
  [ "Technology", "YHOO"; "Technology", "GOOG"; "Technology", "MSFT"; "Technology", "FB"
    "Financial", "PRU"; "Financial", "V"; "Financial", "AXP.MX";
    "Consumer Goods", "AAPL"; "Consumer Goods", "CCE"; "Consumer Goods", "MCD" ]

let stocks = 
  [ for cat, stock in names ->
      (cat, stock) => stockPrices stock ]
  |> frame |> Frame.sortRowsByKey |> Frame.sortColsByKey

// Calculate the daily returns for days when we have all data
let stocksAll = stocks.RowsDense.[*]
let stocksRet = (stocksAll - stocksAll.Shift(1)) / stocksAll * 100.0

// Average daily returns per company
let perComp = stocksRet |> Stats.mean
Chart.Column(perComp |> Series.mapKeys snd)

// Average daily returns per sector
let perSect = perComp.MeanLevel (fun (a,_)-> a)
Chart.Column(perSect)
