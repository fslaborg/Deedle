module Deedle.VirtualPreservation.Main

open System
open System.Diagnostics
open System.IO
open System.Text
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Tests.VirtualInstrumentation

/// One row of the virtual-preservation report.
type ResultRow =
  { Operation: string
    Shape: string
    Verdict: string
    ValueAt: int
    LookupValue: int
    LookupRange: int
    GetSubVector: int
    MergeWith: int
    Notes: string }

module private Report =
  let nSmall = 10_000L
  let nMed = 100_000L

  let shapeOfSeries (s: Series<'K, 'V>) =
    match SeriesProbe.classify s with
    | FullyVirtual -> "FullyVirtual"
    | FullyLinear -> "FullyLinear"
    | Mixed(i, v) -> sprintf "Mixed(%A,%A)" i v

  let shapeOfFrame (f: Frame<'R, 'C>) =
    if FrameProbe.rowIndexIsVirtual f then "FrameRowVirtual" else "FrameRowLinear"

  /// VIRTUAL = scheme preserved and no full-length ValueAt scan.
  /// MATERIALIZE = linear/mixed scheme and/or ValueAt ≈ length.
  let verdict (shape: string) (valueAt: int) (length: int64) =
    let fullPull = int64 valueAt >= length && length > 0L
    match shape with
    | "FullyVirtual" | "FrameRowVirtual" when fullPull -> "VIRTUAL (pull)"
    | "FullyVirtual" | "FrameRowVirtual" -> "VIRTUAL"
    | _ when fullPull -> "MATERIALIZE"
    | _ -> "MATERIALIZE"

  let fromDelta op shape (d: AccessSnapshot) length notes =
    { Operation = op
      Shape = shape
      Verdict = verdict shape d.ValueAtCount length
      ValueAt = d.ValueAtCount
      LookupValue = d.LookupValueCount
      LookupRange = d.LookupRangeCount
      GetSubVector = d.GetSubVectorCount
      MergeWith = d.MergeWithCount
      Notes = notes }

  let measureSeries op (setup: unit -> AccessCounters * Series<'K, 'V>) (run: Series<'K, 'V> -> Series<'K2, 'V2>) length notes =
    let c, s = setup()
    c.Reset()
    let before = c.Snapshot()
    let result = run s
    let d = AccessSnapshot.delta before (c.Snapshot())
    fromDelta op (shapeOfSeries result) d length notes

  let measureFrame op (setup: unit -> AccessCounters * Frame<'R, 'C>) (run: Frame<'R, 'C> -> Frame<'R2, 'C2>) length notes =
    let c, f = setup()
    c.Reset()
    let before = c.Snapshot()
    let result = run f
    let d = AccessSnapshot.delta before (c.Snapshot())
    fromDelta op (shapeOfFrame result) d length notes

  let measurePull op (setup: unit -> AccessCounters * Series<'K, 'V>) (run: Series<'K, 'V> -> unit) length notes =
    let c, s = setup()
    c.Reset()
    let before = c.Snapshot()
    run s
    let d = AccessSnapshot.delta before (c.Snapshot())
    // Result shape: keep source series classification after the pull.
    fromDelta op (shapeOfSeries s) d length notes

  let tryRow op f =
    try f()
    with e ->
      { Operation = op
        Shape = "n/a"
        Verdict = "INCOMPLETE"
        ValueAt = 0
        LookupValue = 0
        LookupRange = 0
        GetSubVector = 0
        MergeWith = 0
        Notes = e.GetType().Name + ": " + e.Message }

  let runAll () =
    [ tryRow "Slice / GetRange" (fun () ->
        measureSeries "Slice / GetRange"
          (fun () -> InstrumentedOrdinalSource.createOrdinalSeries nMed)
          (fun s -> s.[100L .. 199L])
          nMed
          "GetSubVector only; no ValueAt")

      tryRow "Lookup / TryGet" (fun () ->
        let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
        c.Reset()
        let before = c.Snapshot()
        s.TryGet(12345L) |> ignore
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "Lookup / TryGet" (shapeOfSeries s) d nMed "Single ValueAt; series stays virtual")

      tryRow "Metadata (KeyCount)" (fun () ->
        let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
        c.Reset()
        let before = c.Snapshot()
        s.KeyCount |> ignore
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "Metadata (KeyCount)" (shapeOfSeries s) d nMed "No data touches")

      tryRow "SelectValues / map" (fun () ->
        measureSeries "SelectValues / map"
          (fun () -> InstrumentedOrdinalSource.createFloatSeries nMed)
          (fun s -> s |> Series.mapValues (fun v -> v + 1.0))
          nMed
          "Lazy map; no ValueAt until read")

      tryRow "Merge (ordinal slices)" (fun () ->
        let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
        let a = s.[0L .. 99L]
        let b = s.[200L .. 299L]
        c.Reset()
        let before = c.Snapshot()
        let merged = Series.merge a b
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "Merge (ordinal slices)" (shapeOfSeries merged) d nMed "MergeWith; no data pull")

      tryRow "filterRowsBy (ordered + LookupRange)" (fun () ->
        let c, f, _ = InstrumentedOrdinalSource.createOrderedSearchFrame nMed
        c.Reset()
        let before = c.Snapshot()
        let filtered = f |> Frame.filterRowsBy "S2" "lorem"
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "filterRowsBy (ordered + LookupRange)" (shapeOfFrame filtered) d nMed
          "LookupRange + GetSubVector; no scan")

      tryRow "filterRowsBy (ordinal + LookupRange)" (fun () ->
        let c, f, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame nMed
        c.Reset()
        let before = c.Snapshot()
        let filtered = f |> Frame.filterRowsBy "S2" "lorem"
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "filterRowsBy (ordinal + LookupRange)" (shapeOfFrame filtered) d nMed
          "B14 ordinal LookupRange fast path")

      tryRow "filterRowsBy (non-search float, scan)" (fun () ->
        let c, f, _, floatFilter, _ = InstrumentedOrdinalSource.createOrderedSearchWithScanColumnsFrame nMed
        c.Reset()
        let before = c.Snapshot()
        let filtered = f |> Frame.filterRowsBy "S3" floatFilter
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "filterRowsBy (non-search float, scan)" (shapeOfFrame filtered) d nMed
          "Scan fallback; ValueAt ≈ length")

      tryRow "filterRowsBy (non-search string, scan)" (fun () ->
        let c, f, _, _, labelFilter = InstrumentedOrdinalSource.createOrderedSearchWithScanColumnsFrame nMed
        c.Reset()
        let before = c.Snapshot()
        let filtered = f |> Frame.filterRowsBy "S4" labelFilter
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "filterRowsBy (non-search string, scan)" (shapeOfFrame filtered) d nMed
          "Scan fallback on second string column; ValueAt ≈ length")

      tryRow "sampleTimeInto (chunks)" (fun () ->
        let c, s = InstrumentedOrdinalSource.createOrderedFloatSeries nMed
        c.Reset()
        let before = c.Snapshot()
        let sampled = s |> Series.sampleTimeInto (TimeSpan.FromDays 365.0) Direction.Forward id
        let d = AccessSnapshot.delta before (c.Snapshot())
        let chunk = sampled.GetAt(0)
        let shape = shapeOfSeries chunk
        { fromDelta "sampleTimeInto (chunks)" shape d nMed "First chunk shape; KeyRange may probe a few addresses"
            with Shape = sprintf "chunk=%s" shape })

      tryRow "GroupBy" (fun () ->
        let c, s = InstrumentedOrdinalSource.createFloatSeries nSmall
        c.Reset()
        let before = c.Snapshot()
        let grouped = s |> Series.groupBy (fun _k v -> int v % 10)
        let first = grouped.GetAt(0)
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "GroupBy" (shapeOfSeries first) d nSmall "Nested series shape")

      tryRow "WindowSize (nested)" (fun () ->
        let c, s = InstrumentedOrdinalSource.createFloatSeries 1_000L
        c.Reset()
        let before = c.Snapshot()
        let windows = s |> Series.windowSizeInto (5, Boundary.Skip) DataSegment.data
        let first = windows.GetAt(0)
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "WindowSize (nested)" (shapeOfSeries first) d 1_000L "Nested window series")

      tryRow "Window aggregate (sum)" (fun () ->
        measureSeries "Window aggregate (sum)"
          (fun () -> InstrumentedOrdinalSource.createFloatSeries 1_000L)
          (fun s -> s |> Series.windowSizeInto (5, Boundary.AtEnding) (fun w -> Stats.sum w.Data))
          1_000L
          "Scalar aggregate result series")

      tryRow "Shift" (fun () ->
        measureSeries "Shift"
          (fun () -> InstrumentedOrdinalSource.createFloatSeries nSmall)
          (fun s -> s |> Series.shift 1)
          nSmall
          "B9 virtual GetAddressRange")

      tryRow "Slice then Stats.sum" (fun () ->
        let c, s = InstrumentedOrdinalSource.createFloatSeries nMed
        let sliced = s.[100L .. 199L]
        c.Reset()
        let before = c.Snapshot()
        Stats.sum sliced |> ignore
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "Slice then Stats.sum" (shapeOfSeries sliced) d 100L "Pull should equal slice length")

      tryRow "DropMissing" (fun () ->
        let c = AccessCounters()
        let src = InstrumentedOrdinalSource<float>(nSmall, float, c, hasMissing=true)
        let s = Virtual.CreateOrdinalSeries(src)
        c.Reset()
        let before = c.Snapshot()
        let dropped = s |> Series.dropMissing
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "DropMissing" (shapeOfSeries dropped) d nSmall "B9 scan + virtual subvector")

      tryRow "SortBy" (fun () ->
        measureSeries "SortBy"
          (fun () -> InstrumentedOrdinalSource.createFloatSeries 1_000L)
          (fun s -> s |> Series.sortBy (fun v -> -v))
          1_000L
          "Value-ordered sort")

      tryRow "Intersect (key+value)" (fun () ->
        let _, s = InstrumentedOrdinalSource.createOrdinalSeries nSmall
        let a = s.[0L .. 500L]
        let b = s.[250L .. 750L]
        // Intersect does not use our counters on a/b; still report result shape.
        let inter = Series.intersect a b
        { Operation = "Intersect (key+value)"
          Shape = shapeOfSeries inter
          Verdict = if SeriesProbe.isVirtual inter then "VIRTUAL" else "MATERIALIZE"
          ValueAt = 0
          LookupValue = 0
          LookupRange = 0
          GetSubVector = 0
          MergeWith = 0
          Notes = "Counters not on inputs; shape only" })

      tryRow "ZipAlign (identical ordinal)" (fun () ->
        let _, s1 = InstrumentedOrdinalSource.createFloatSeries nSmall
        let _, s2 = InstrumentedOrdinalSource.createFloatSeries nSmall
        let zipped = Series.zipAlign JoinKind.Inner Lookup.Exact s1 s2
        { Operation = "ZipAlign (identical ordinal)"
          Shape = shapeOfSeries zipped
          Verdict = if SeriesProbe.isVirtual zipped then "VIRTUAL" else "MATERIALIZE"
          ValueAt = 0
          LookupValue = 0
          LookupRange = 0
          GetSubVector = 0
          MergeWith = 0
          Notes = "B9 identical ordinal ranges" })

      tryRow "Frame Join (identical ordinal)" (fun () ->
        let _, s1 = InstrumentedOrdinalSource.createFloats nSmall
        let _, s2 = InstrumentedOrdinalSource.createFloats nSmall
        let f1 = Virtual.CreateOrdinalFrame(["A"], [s1 :> IVirtualVectorSource])
        let f2 = Virtual.CreateOrdinalFrame(["B"], [s2 :> IVirtualVectorSource])
        let joined = f1.Join(f2, JoinKind.Outer)
        { Operation = "Frame Join (identical ordinal)"
          Shape = shapeOfFrame joined
          Verdict = if FrameProbe.rowIndexIsVirtual joined then "VIRTUAL" else "MATERIALIZE"
          ValueAt = 0
          LookupValue = 0
          LookupRange = 0
          GetSubVector = 0
          MergeWith = 0
          Notes = "B9 structural ordinal equality" })

      tryRow "Stats.sum (full series)" (fun () ->
        measurePull "Stats.sum (full series)"
          (fun () -> InstrumentedOrdinalSource.createFloatSeries nSmall)
          (fun s -> Stats.sum s |> ignore)
          nSmall
          "ValueAt ≈ Length")

      tryRow "Materialize()" (fun () ->
        let c, s = InstrumentedOrdinalSource.createOrdinalSeries 500L
        c.Reset()
        let before = c.Snapshot()
        let mat = s.Materialize()
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "Materialize()" (shapeOfSeries mat) d 500L "Explicit flip to linear")

      tryRow "FillMissing (Forward)" (fun () ->
        let c = AccessCounters()
        let src = InstrumentedOrdinalSource<float>(1_000L, float, c, hasMissing=true)
        let s = Virtual.CreateOrdinalSeries(src)
        c.Reset()
        let before = c.Snapshot()
        let filled = s |> Series.fillMissing Direction.Forward
        let d = AccessSnapshot.delta before (c.Snapshot())
        fromDelta "FillMissing (Forward)" (shapeOfSeries filled) d 1_000L "B10 virtual wrappers") ]

  let toMarkdown (rows: ResultRow list) (deedleSha: string) =
    let sb = StringBuilder()
    let append (line: string) = sb.AppendLine(line) |> ignore
    append "# Virtual preservation report"
    append ""
    append (sprintf "- **Generated:** %s" (DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")))
    append (sprintf "- **Deedle tip:** `%s`" deedleSha)
    append "- **Harness:** `tests/Deedle.VirtualPreservation` + `VirtualInstrumentation`"
    append (sprintf "- **Sizes:** nSmall=%d, nMed=%d" nSmall nMed)
    append ""
    append "## Results"
    append ""
    append "| Operation | Shape | Verdict | ValueAt | LookupValue | LookupRange | GetSubVector | MergeWith | Notes |"
    append "|-----------|-------|---------|--------:|------------:|------------:|-------------:|----------:|-------|"
    for r in rows do
      append (
        sprintf "| %s | %s | **%s** | %d | %d | %d | %d | %d | %s |"
          r.Operation r.Shape r.Verdict r.ValueAt r.LookupValue r.LookupRange r.GetSubVector r.MergeWith r.Notes)
    append ""
    append "## Legend"
    append ""
    append "| Verdict | Meaning |"
    append "|---------|---------|"
    append "| **VIRTUAL** | Result keeps virtual addressing; no full-length `ValueAt` scan |"
    append "| **VIRTUAL (pull)** | Still virtual storage, but `ValueAt` ≈ length (e.g. Stats on a virtual series) |"
    append "| **MATERIALIZE** | Linear/mixed addressing and/or full pull |"
    append "| **INCOMPLETE** | Operation threw |"
    append ""
    append "Metric definitions: [`harness/metrics.md`](../metrics.md)."
    append ""
    sb.ToString()

[<EntryPoint>]
let main argv =
  let outPath =
    match argv |> Array.tryFind (fun a -> a.StartsWith("--out=", StringComparison.OrdinalIgnoreCase)) with
    | Some a -> a.Substring("--out=".Length)
    | None ->
        let here = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."))
        // Prefer sibling big-deedle next to Deedle checkout.
        let sibling = Path.GetFullPath(Path.Combine(here, "..", "big-deedle", "harness", "results", "virtual-preservation.md"))
        if Directory.Exists(Path.GetDirectoryName sibling) || Directory.Exists(Path.Combine(here, "..", "big-deedle")) then
          sibling
        else
          Path.Combine(here, "harness-out", "virtual-preservation.md")

  let sha =
    try
      let psi = ProcessStartInfo(FileName = "git", Arguments = "rev-parse --short HEAD", RedirectStandardOutput = true, UseShellExecute = false)
      psi.WorkingDirectory <- Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."))
      use p = Process.Start(psi)
      let text = p.StandardOutput.ReadToEnd().Trim()
      p.WaitForExit()
      if String.IsNullOrWhiteSpace text then "unknown" else text
    with _ -> "unknown"

  printfn "Running virtual preservation matrix..."
  let rows = Report.runAll ()
  let md = Report.toMarkdown rows sha
  let dir = Path.GetDirectoryName outPath
  if not (String.IsNullOrEmpty dir) then Directory.CreateDirectory dir |> ignore
  File.WriteAllText(outPath, md, Encoding.UTF8)
  printfn "Wrote %s (%d rows)" outPath rows.Length
  for r in rows do
    printfn "  [%s] %s  ValueAt=%d LookupRange=%d" r.Verdict r.Operation r.ValueAt r.LookupRange
  0
