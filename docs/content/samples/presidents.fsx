// ----------------------------------------------------------------------------
// Load everything & add extensions for F# charting
// ----------------------------------------------------------------------------
#nowarn "58"
#I "../../../bin/net45/"
#I "../../../packages/FSharp.Data/lib/net45"
#I "../../../packages/FSharp.Charting/lib/net45"

#r "FSharp.Data.dll"
#load "FSharp.Charting.fsx"
#load "Deedle.fsx"
#load "RProvider.fsx"

open System
open System.Linq
open FSharp.Data
open FSharp.Charting
open Deedle
open RProvider

(*[omit:(Charting extensions)]*)
[<AutoOpen>]
module FsLabExtensions =
  type FSharp.Charting.Chart with
    static member Line(data:Series<'K, 'V>, ?Name, ?Title, ?Labels, ?Color, ?XTitle, ?YTitle) =
      Chart.Line(Series.observations data, ?Name=Name, ?Title=Title, ?Labels=Labels, ?Color=Color, ?XTitle=XTitle, ?YTitle=YTitle)
    static member Column(data:Series<'K, 'V>, ?Name, ?Title, ?Labels, ?Color, ?XTitle, ?YTitle) =
      Chart.Column(Series.observations data, ?Name=Name, ?Title=Title, ?Labels=Labels, ?Color=Color, ?XTitle=XTitle, ?YTitle=YTitle)
    static member Area(data:Series<'K, 'V>, ?Name, ?Title, ?Labels, ?Color, ?XTitle, ?YTitle) =
      Chart.Area(Series.observations data, ?Name=Name, ?Title=Title, ?Labels=Labels, ?Color=Color, ?XTitle=XTitle, ?YTitle=YTitle)
(*[/omit]*)

// ----------------------------------------------------------------------------
// Getting debt data
// ----------------------------------------------------------------------------

// Loading US debt data from CSV file & making the data set nicer
let debtData = 
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../data/us-debt.csv")
  |> Frame.indexRowsInt "Year"
  |> Frame.indexColsWith ["Year"; "GDP"; "Population"; "Debt"; "?" ]

// Compare the GDP data with what we can get from WorldBank
let wb = WorldBankData.GetDataContext()
let wbGdp = series wb.Countries.``United States``.Indicators.``GDP (current US$)``

debtData?GDP_WB <- wbGdp / 1.0e9

Chart.Combine
  [ Chart.Line(debtData?GDP)
    Chart.Line(debtData?GDP_WB) ]

// For the rest of the analysis, we need just the Debt
let debt = debtData.Columns.[ ["Debt"] ]

Chart.Line(debt?Debt)

// ----------------------------------------------------------------------------
// Get information about US presidents
// ----------------------------------------------------------------------------

// Get president list from Freebase!
let fb = FreebaseData.GetDataContext()
fb.Society.Government.``US Presidents`` |> List.ofSeq

// When were they presidents & party information
let abe = fb.Society.Government.``US Presidents``.First()

abe.Name
abe.``Government Positions Held``.First().``Basic title``
abe.``Government Positions Held``.First().From
abe.``Government Positions Held``.First().To
[ for p in abe.Party -> p.Party.Name ]

// Get presidents since around 1900
let presidentInfos = 
  query { for p in fb.Society.Government.``US Presidents`` do
          sortBy (Seq.max p.``President number``)
          skip 23 }

// Get list of pres
let presidentTerms =
  [ for pres in presidentInfos do
    for pos in pres.``Government Positions Held`` do
    if string pos.``Basic title`` = "President" then
      // Get start and end year of the position
      let starty = DateTime.Parse(pos.From).Year
      let endy = if pos.To = null then 2013 else
                   DateTime.Parse(pos.To).Year
      // Get their party
      let dem = pres.Party |> Seq.exists (fun p -> p.Party.Name = "Democratic Party")
      let rep = pres.Party |> Seq.exists (fun p -> p.Party.Name = "Republican Party")
      let pty = if dem then "Democrat" elif rep then "Republican" else null

      // Return three element tuple with the info
      yield (pres.Name, starty, endy, pty) ]

// Build a data frame containing the information
let presidents =
  presidentTerms
  |> Frame.ofRecords
  |> Frame.indexColsWith ["President"; "Start"; "End"; "Party"]

// ----------------------------------------------------------------------------
// Analyising debt change with Deedle
// ----------------------------------------------------------------------------

// Get debt at the end of the presidential term
let byEnd = presidents |> Frame.indexRowsInt "End"
let endDebt = byEnd.Join(debt, JoinKind.Left)

// Total accumulated debt by president
endDebt 
|> Frame.indexRowsString "President"
|> Frame.getSeries "Debt"
|> Chart.Column

// Calculate change for each president
endDebt?Difference <-
  endDebt?Debt |> Series.pairwiseWith (fun _ (prev, curr) -> curr - prev)

// Difference by president
endDebt
|> Frame.indexRowsString "President"
|> Frame.getSeries "Difference"
|> Chart.Column

// Get only presidents for who the difference is defined
let ed = endDebt.RowsDense.[*]

// Compare Republican and Democratic presidents
let debtByParty = endDebt |> Frame.groupRowsByString "Party"

// How Dem/Rep presidents contribute to the debt, in average?
debtByParty?Difference 
|> Series.meanLevel fst
|> Chart.Column

// Create nicer plot using R's ggplot2 library
open RProvider.ggplot2
open RProvider.``base``

let labels = 
  ed |> Frame.mapRows (fun y row -> sprintf "(%d) %s" y (row.GetAs("President")))

let qp =
  namedParams [
    "x", box labels.Values
    "y", box ed?Difference.Values
    "fill", box (ed.GetSeries<string>("Party").Values) 
    "geom",  box [| "bar" |] ]
  |> R.qplot

R.assign("q", qp)
R.eval(R.parse(text="q" + 
  """ + scale_fill_manual(values=c("#2080ff", "#ff6040"))""" + 
  """ + theme(axis.text.x = element_text(angle = 90, hjust = 1))"""))

// ----------------------------------------------------------------------------
// Plotting debt by president 
// ----------------------------------------------------------------------------

// For each year, find the corresponding president and party
let byStart = presidents |> Frame.indexRowsInt "Start"
let aligned = debt.Join(byStart, JoinKind.Left, Lookup.NearestSmaller)

// Format labels that we want to show
let infos = aligned.Rows |> Series.map (fun _ row ->
  sprintf "(%d) %s" (row.GetAs "Start") (row.GetAs "President"))

// Create chunks of years when the president has not changed
let chunked = aligned?Debt |> Series.chunkWhile(fun y1 y2 -> 
  infos.[y1] = infos.[y2])

// Combine all data into a single chart
chunked 
|> Series.observations
|> Seq.map (fun (startYear, chunkDebts) -> 
    Chart.Area(chunkDebts, Name=infos.[startYear]))
|> Chart.Combine


// Creating similar charts using R's ggplot2
open RProvider.ggplot2

let qr = 
  namedParams [
    "x", box aligned.RowKeys
    "y", box aligned?Debt.Values
    "fill", box (aligned.GetSeries<string>("Party").Values)
    "stat", box "identity"
    "geom",  box [| "bar" |] ]
  |> R.qplot

R.assign("q", qr)
R.eval(R.parse(text="q" + 
  """ + scale_fill_manual(values=c("#2080ff", "#ff6040"))""" + 
  """ + theme(axis.text.x = element_text(angle = 90, hjust = 1))"""))

// Or we can calculate & plot the differences in debt per year
aligned?Difference <- 
  aligned?Debt |> Series.pairwiseWith (fun _ (v1, v2) -> v2 - v1)

let qd = 
  namedParams [
    "x", box aligned.RowKeys
    "y", box (aligned?Difference |> Series.fillMissingWith 0.0 |> Series.values)
    "fill", box (aligned.GetSeries<string>("Party").Values)
    "stat", box "identity"
    "geom",  box [| "bar" |] ]
  |> R.qplot

R.assign("q", qd)
R.eval(R.parse(text="q" + 
  """ + scale_fill_manual(values=c("#2080ff", "#ff6040"))""" + 
  """ + theme(axis.text.x = element_text(angle = 90, hjust = 1))"""))