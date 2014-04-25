module Formatters
#I "../../packages/FSharp.Formatting.2.4.7/lib/net40"
#r "FSharp.Markdown.dll"
#r "FSharp.Literate.dll"
#r "../../bin/Deedle.dll"
#r "../../packages/FAKE/tools/FakeLib.dll"
#load "../../packages/FSharp.Charting.0.90.6/FSharp.Charting.fsx"

open Fake
open System.IO
open Deedle
open Deedle.Internal
open FSharp.Literate
open FSharp.Markdown
open FSharp.Charting

// --------------------------------------------------------------------------------------
// Implements Markdown formatters for common FsLab things - including Deedle series
// and frames, F# Charting charts and System.Image values
// --------------------------------------------------------------------------------------

// How many columns and rows from frame should be rendered
let startColumnCount = 3
let endColumnCount = 3

let startRowCount = 8
let endRowCount = 4

// How many items from a series should be rendered
let startItemCount = 5
let endItemCount = 3

// --------------------------------------------------------------------------------------
// Helper functions etc.
// --------------------------------------------------------------------------------------

open System.Windows.Forms
open FSharp.Charting.ChartTypes

/// Extract values from any series using reflection
let (|SeriesValues|_|) (value:obj) = 
  let iser = value.GetType().GetInterface("ISeries`1")
  if iser <> null then
    let keys = value.GetType().GetProperty("Keys").GetValue(value) :?> System.Collections.IEnumerable
    let vector = value.GetType().GetProperty("Vector").GetValue(value) :?> IVector
    Some(Seq.zip (Seq.cast<obj> keys) vector.ObjectSequence)
  else None

/// Format value as a single-literal paragraph
let formatValue def = function
  | Some v -> [ Paragraph [Literal (v.ToString()) ]] 
  | _ -> [ Paragraph [Literal def] ]

/// Format body of a single table cell
let td v = [ Paragraph [Literal v] ]

/// Use 'f' to transform all values, then call 'g' with Some for 
/// values to show and None for "..." in the middle
let mapSteps (startCount, endCount) f g input = 
  input 
  |> Seq.map f |> Seq.startAndEnd startCount endCount
  |> Seq.map (function Choice1Of3 v | Choice3Of3 v -> g (Some v) | _ -> g None)
  |> List.ofSeq

// Tuples with the counts, for easy use later on
let fcols = startColumnCount, endColumnCount
let frows = startRowCount, endRowCount
let sitms = startItemCount, endItemCount

/// Reasonably nice default style for charts
let chartStyle ch =
  let grid = ChartTypes.Grid(LineColor=System.Drawing.Color.LightGray)
  ch 
  |> Chart.WithYAxis(MajorGrid=grid)
  |> Chart.WithXAxis(MajorGrid=grid)

// --------------------------------------------------------------------------------------
// Build FSI evaluator
// --------------------------------------------------------------------------------------

/// Builds FSI evaluator that can render System.Image, F# Charts, series & frames
let createFsiEvaluator root output =

  /// Counter for saving files
  let imageCounter = 
    let count = ref 0
    (fun () -> incr count; !count)

  let transformation (value:obj, typ:System.Type) =
    match value with 
    | :? System.Drawing.Image as img ->
        // Pretty print image - save the image to the "images" directory 
        // and return a DirectImage reference to the appropriate location
        let id = imageCounter().ToString()
        let file = "chart" + id + ".png"
        ensureDirectory (output @@ "images")
        img.Save(output @@ "images" @@ file, System.Drawing.Imaging.ImageFormat.Png) 
        Some [ Paragraph [DirectImage ("Chart", (root + "/images/" + file, None))]  ]

    | :? ChartTypes.GenericChart as ch ->
        // Pretty print F# Chart - save the chart to the "images" directory 
        // and return a DirectImage reference to the appropriate location
        let id = imageCounter().ToString()
        let file = "chart" + id + ".png"
        ensureDirectory (output @@ "images")
      
        // We need to reate host control, but it does not have to be visible
        ( use ctl = new ChartControl(chartStyle ch, Dock = DockStyle.Fill, Width=500, Height=300)
          ch.CopyAsBitmap().Save(output @@ "images" @@ file, System.Drawing.Imaging.ImageFormat.Png) )
        Some [ Paragraph [DirectImage ("Chart", (root + "/images/" + file, None))]  ]

    | SeriesValues s ->
        // Pretty print series!
        let heads  = s |> mapSteps sitms fst (function Some k -> td (k.ToString()) | _ -> td " ... ")
        let row    = s |> mapSteps sitms snd (function Some v -> formatValue "N/A" (OptionalValue.asOption v) | _ -> td " ... ")
        let aligns = s |> mapSteps sitms id (fun _ -> AlignDefault)
        [ InlineBlock "<div class=\"deedleseries\">"
          TableBlock(Some ((td "Keys")::heads), AlignDefault::aligns, [ (td "Values")::row ]) 
          InlineBlock "</div>" ] |> Some

    | :? IFrame as f ->
      // Pretty print frame!
      {new IFrameOperation<_> with
        member x.Invoke(f) = 
          let heads  = f.ColumnKeys |> mapSteps fcols id (function Some k -> td (k.ToString()) | _ -> td " ... ")
          let aligns = f.ColumnKeys |> mapSteps fcols id (fun _ -> AlignDefault)
          let rows = 
            f.Rows |> Series.observationsAll |> mapSteps frows id (fun item ->
              let def, k, data = 
                match item with 
                | Some(k, Some d) -> "N/A", k.ToString(), Series.observationsAll d |> Seq.map snd 
                | Some(k, _) -> "N/A", k.ToString(), f.ColumnKeys |> Seq.map (fun _ -> None)
                | None -> " ... ", " ... ", f.ColumnKeys |> Seq.map (fun _ -> None)
              let row = data |> mapSteps fcols id (function Some v -> formatValue def v | _ -> td " ... ")
              (td k)::row )
          Some [ 
            InlineBlock "<div class=\"deedleframe\">"
            TableBlock(Some ([]::heads), AlignDefault::aligns, rows) 
            InlineBlock "</div>"
          ] }
      |> f.Apply
    | _ -> None 
    
  // Create FSI evaluator, register transformations & return
  let fsiEvaluator = FsiEvaluator()
  fsiEvaluator.RegisterTransformation(transformation)
  fsiEvaluator