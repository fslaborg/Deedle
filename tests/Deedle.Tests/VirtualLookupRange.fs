#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualLookupRange
#endif

open System
open System.Diagnostics
open System.IO
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Addressing
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Tests.VirtualInstrumentation

module Address = LinearAddress

module private Range =
  let step offset step =
    RangeRestriction.Custom { Offset = offset; Step = step } : RangeRestriction<Address>

  let fixedRange lo hi =
    RangeRestriction.Fixed(Address.ofInt64 lo, Address.ofInt64 hi)

// ------------------------------------------------------------------------------------------------
// VirtualLookupRange configuration (basic)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``forRepeatingCycle returns step offset for known value`` () =
  match VirtualLookupRange.forRepeatingCycle [| "a"; "b"; "c" |] with
  | LookupRangeStep f ->
      f "b" |> shouldEqual (1, 3)
      VirtualLookupRange.classifyLookupRange (VirtualLookupRange.forRepeatingCycle [| "a"; "b"; "c" |])
      |> shouldEqual (VirtualColumnLookupRange.Step 3)
  | _ -> failwith "expected LookupRangeStep"

[<Test>]
let ``forRepeatingCycle returns empty range for unknown value`` () =
  match VirtualLookupRange.forRepeatingCycle [| "a"; "b" |] with
  | LookupRangeStep f ->
      f "missing" |> shouldEqual (-1, 2)
      VirtualLookupRange.classifyLookupRange (VirtualLookupRange.forRepeatingCycle [| "a"; "b" |])
      |> shouldEqual (VirtualColumnLookupRange.Step 2)
  | _ -> failwith "expected LookupRangeStep"

[<Test>]
let ``tryInferStringLookupRange returns None for empty column`` () =
  VirtualLookupRange.tryInferStringLookupRange 0L (fun _ -> "")
  |> Option.isNone
  |> shouldEqual true

[<Test>]
let ``tryInferStringLookupRange infers repeating cycle for periodic strings`` () =
  let valueAt i = if i % 2L = 0L then "x" else "y"
  match VirtualLookupRange.tryInferStringLookupRange 10L valueAt with
  | Some (mode, _) ->
      VirtualLookupRange.classifyLookupRange mode |> shouldEqual (VirtualColumnLookupRange.Step 2)
  | None -> failwith "expected inference to produce a LookupRange mode"

[<Test>]
let ``tryInferStringLookupRange returns None when distinct count exceeds cap`` () =
  let valueAt i = sprintf "value-%d" (int i)
  VirtualLookupRange.tryInferStringLookupRange 100L valueAt
  |> Option.isNone
  |> shouldEqual true

[<Test>]
let ``resolveSearchColumnsLookupRange returns empty for non-search columns`` () =
  let searchColumns = [ VirtualSearchColumn.infer "Category" ]
  VirtualLookupRange.resolveSearchColumnsLookupRange
    "Test.Read" searchColumns "Id" "string"
    (fun () -> Some(VirtualLookupRange.forRepeatingCycle [| "a" |], "cycle"))
    (fun () -> None) (fun () -> None)
  |> fun resolved -> resolved.String.IsNone
  |> shouldEqual true

[<Test>]
let ``resolveSearchColumnsLookupRange configures search columns on frames`` () =
  let path = Path.GetTempFileName() + ".csv"
  try
    File.WriteAllLines(path, [| "Category,Code"; "a,1"; "b,2"; "a,3" |])
    let frame =
      Virtual.ReadCsv(
        path,
        searchColumns =
          [ VirtualSearchColumn.infer "Category"
            VirtualSearchColumn.withInt64 "Code" (VirtualLookupRange.forRepeatingCycle [| 1L; 2L; 3L |]) ],
        columnKeys = [ "Category"; "Code" ])
    match Virtual.TryGetLookupRange(frame, "Category") with
    | Some (VirtualColumnLookupRange.Step _) | Some VirtualColumnLookupRange.IndexList -> ()
    | actual -> Assert.Fail(sprintf "expected Category Step or IndexList, got %A" actual)
    Virtual.TryGetLookupRange(frame, "Code") |> shouldEqual (Some (VirtualColumnLookupRange.Step 3))
  finally
    if File.Exists path then File.Delete path

[<Test>]
let ``tryInferInt64LookupRange infers Step for repeating cycle`` () =
  let valueAt i = Some (i % 3L + 1L)
  match VirtualLookupRange.tryInferInt64LookupRange 12L valueAt with
  | Some (mode, _) ->
      VirtualLookupRange.classifyLookupRange mode |> shouldEqual (VirtualColumnLookupRange.Step 3)
  | None -> failwith "expected inference to produce a LookupRange mode"

[<Test>]
let ``tryInferInt64LookupRange builds IndexList for non-cyclic low cardinality`` () =
  let valueAt i = Some (if i < 4L then i else 99L)
  match VirtualLookupRange.tryInferInt64LookupRange 8L valueAt with
  | Some (mode, _) ->
      VirtualLookupRange.classifyLookupRange mode |> shouldEqual VirtualColumnLookupRange.IndexList
  | None -> failwith "expected inference to produce a LookupRange mode"

[<Test>]
let ``tryInferInt64LookupRange infers Step for int64 modulo cycle`` () =
  let valueAt i = Some (i % 4L)
  match VirtualLookupRange.tryInferInt64LookupRange 20L valueAt with
  | Some (mode, _) ->
      VirtualLookupRange.classifyLookupRange mode |> shouldEqual (VirtualColumnLookupRange.Step 4)
  | None -> failwith "expected inference to produce a LookupRange mode"

[<Test>]
let ``tryInferFloatLookupRange returns None for high cardinality`` () =
  let valueAt i = Some (float i)
  VirtualLookupRange.tryInferFloatLookupRange 100L valueAt
  |> Option.isNone
  |> shouldEqual true

// ------------------------------------------------------------------------------------------------
// LookupRange quality sensitivity
// Compare tight Custom/Fixed vs naive full-range vs linear-scan fallback.
// ------------------------------------------------------------------------------------------------

module private LookupRangeFixture =
  let nLarge = 100_000L
  let nTiming = 100_000L
  let searchValue = "lorem"

  let expectedMatchCount (length: int64) (step: int) =
    if length <= 0L then 0
    else int ((length - 1L) / int64 step) + 1

  let filterBy (frame: Frame<DateTimeOffset, string>) (c: AccessCounters) (readCount: int) (value: string) =
    c.Reset()
    let before = c.Snapshot()
    let filtered = frame |> Frame.filterRowsBy "S2" value
    let afterFilter = c.Snapshot()
    let filterDelta = AccessSnapshot.delta before afterFilter
    for i in 0 .. readCount - 1 do
      if int64 i < filtered.RowIndex.KeyCount then
        filtered?S1.GetAt(i) |> ignore
    let afterRead = c.Snapshot()
    let readDelta = AccessSnapshot.delta afterFilter afterRead
    filtered, filterDelta, readDelta

  let filterAndRead frame c readCount =
    filterBy frame c readCount searchValue

  let filterAndReadOrdinal (frame: Frame<int64, string>) (c: AccessCounters) (readCount: int) =
    c.Reset()
    let before = c.Snapshot()
    let filtered = frame |> Frame.filterRowsBy "S2" searchValue
    let afterFilter = c.Snapshot()
    let filterDelta = AccessSnapshot.delta before afterFilter
    for i in 0 .. readCount - 1 do
      if int64 i < filtered.RowIndex.KeyCount then
        filtered?S1.GetAt(i) |> ignore
    let afterRead = c.Snapshot()
    let readDelta = AccessSnapshot.delta afterFilter afterRead
    filtered, filterDelta, readDelta

  let elapsedMs (f: unit -> unit) =
    let sw = Stopwatch.StartNew()
    f()
    sw.Stop()
    float sw.ElapsedMilliseconds

// ------------------------------------------------------------------------------------------------
// Profile baseline reporter — writes metrics for all data profiles
// ------------------------------------------------------------------------------------------------

module LookupRangeProfileReport =
  open System.IO
  open System.Text

  type Row =
    { Profile: string
      LookupRange: string
      N: int64
      Search: string
      VirtualFilter: bool
      FilterValueAt: int
      FilterLookupRange: int
      ResultRows: int
      ExpectedRows: int
      ReadValueAt20: int
      FilterMs: float }

  let private n = LookupRangeFixture.nLarge
  let private readN = 20

  let private runFilter (setup: unit -> AccessCounters * Frame<DateTimeOffset, string> * string * int) =
    let c, frame, search, expected = setup ()
    let filterMs =
      LookupRangeFixture.elapsedMs (fun () ->
        c.Reset()
        frame |> Frame.filterRowsBy "S2" search |> ignore)
    let filtered, filterDelta, readDelta = LookupRangeFixture.filterBy frame c readN search
    { Profile = ""
      LookupRange = ""
      N = n
      Search = search
      VirtualFilter = FrameProbe.rowIndexIsVirtual filtered
      FilterValueAt = filterDelta.ValueAtCount
      FilterLookupRange = filterDelta.LookupRangeCount
      ResultRows = filtered.RowCount
      ExpectedRows = expected
      ReadValueAt20 = readDelta.ValueAtCount
      FilterMs = filterMs }

  let private runOrdinal () =
    let c, frame, words = InstrumentedOrdinalSource.createOrdinalSearchFrame n
    let expected = LookupRangeFixture.expectedMatchCount n words.Length
    let filterMs =
      LookupRangeFixture.elapsedMs (fun () ->
        c.Reset()
        frame |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue |> ignore)
    let filtered, filterDelta, readDelta = LookupRangeFixture.filterAndReadOrdinal frame c readN
    { Profile = "Default 8-word (ordinal index)"
      LookupRange = "Step (Custom stride)"
      N = n
      Search = LookupRangeFixture.searchValue
      VirtualFilter = FrameProbe.rowIndexIsVirtual filtered
      FilterValueAt = filterDelta.ValueAtCount
      FilterLookupRange = filterDelta.LookupRangeCount
      ResultRows = filtered.RowCount
      ExpectedRows = expected
      ReadValueAt20 = readDelta.ValueAtCount
      FilterMs = filterMs }

  let private runMapped () =
    let c, frame, words = InstrumentedOrdinalSource.createOrderedMappedSearchFrame n
    let expected = LookupRangeFixture.expectedMatchCount n words.Length
    let search = LookupRangeFixture.searchValue.ToUpperInvariant()
    let filterMs =
      LookupRangeFixture.elapsedMs (fun () ->
        c.Reset()
        frame |> Frame.filterRowsBy "S2" search |> ignore)
    c.Reset()
    let before = c.Snapshot()
    let filtered = frame |> Frame.filterRowsBy "S2" search
    let afterFilter = c.Snapshot()
    let filterDelta = AccessSnapshot.delta before afterFilter
    for i in 0 .. readN - 1 do
      if int64 i < filtered.RowIndex.KeyCount then filtered?S1.GetAt(i) |> ignore
    let readDelta = AccessSnapshot.delta afterFilter (c.Snapshot())
    { Profile = "Default 8-word (mapped column)"
      LookupRange = "Scan (no reverse map)"
      N = n
      Search = search
      VirtualFilter = FrameProbe.rowIndexIsVirtual filtered
      FilterValueAt = filterDelta.ValueAtCount
      FilterLookupRange = filterDelta.LookupRangeCount
      ResultRows = filtered.RowCount
      ExpectedRows = expected
      ReadValueAt20 = readDelta.ValueAtCount
      FilterMs = filterMs }

  let collect () : Row list =
    let words11 = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let expected11 = LookupRangeFixture.expectedMatchCount n words11.Length

    let step11 =
      let r =
        runFilter (fun () ->
          let c, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrame n
          c, frame, LookupRangeFixture.searchValue, LookupRangeFixture.expectedMatchCount n words.Length)
      { r with Profile = "Default 8-word"; LookupRange = "Step (Custom stride)" }

    let exactFixed =
      let r =
        runFilter (fun () ->
          let c, frame, _ =
            InstrumentedOrdinalSource.createOrderedSearchFrameWith n (LookupRangeExactFixed (fun v ->
              let o = words11 |> Array.findIndex ((=) v) |> int64
              o, o))
          c, frame, LookupRangeFixture.searchValue, 1)
      { r with Profile = "Default 8-word"; LookupRange = "ExactFixed (first hit)" }

    let fullFixed =
      let r =
        runFilter (fun () ->
          let c, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrameWith n LookupRangeFullFixed
          c, frame, LookupRangeFixture.searchValue, int n)
      { r with Profile = "Default 8-word"; LookupRange = "FullFixed (naive [0..N-1])" }

    let vocab256 =
      let r =
        runFilter (fun () ->
          let c, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrameLargeVocab n 256
          let search = words.[0]
          c, frame, search, LookupRangeFixture.expectedMatchCount n 256)
      { r with Profile = "Large vocab (256 labels)"; LookupRange = "Step (stride 256)" }

    let sparseIdx =
      let r =
        runFilter (fun () ->
          let c, frame, trueCount = InstrumentedOrdinalSource.createOrderedSearchFrameSparse n 997L 42L
          c, frame, "lorem", trueCount)
      { r with Profile = "Sparse (mod 997)"; LookupRange = "IndexList (precomputed)" }

    let sparseWrong =
      let r =
        runFilter (fun () ->
          let c, frame, trueCount = InstrumentedOrdinalSource.createOrderedSearchFrameSparseWrongStep n 997L 42L
          c, frame, "lorem", trueCount)
      { r with Profile = "Sparse (mod 997)"; LookupRange = "Step (wrong stride 11)" }

    [ step11; exactFixed; fullFixed; runOrdinal(); runMapped(); vocab256; sparseIdx; sparseWrong ]

  let toMarkdown (rows: Row list) (runDate: string) =
    let sb = StringBuilder()
    sb.AppendLine("| Profile | LookupRange | Virtual? | Filter ValueAt | Filter LookupRange | Result rows | Expected | Read ValueAt (20) | Filter ms |") |> ignore
    sb.AppendLine("|---------|-------------|----------|----------------|-------------------|-------------|----------|-------------------|-----------|") |> ignore
    for r in rows do
      let virt = if r.VirtualFilter then "Yes" else "No"
      let ok = if r.ResultRows = r.ExpectedRows then "✓" else "✗"
      sb.AppendLine(
        sprintf "| %s | %s | %s | %d | %d | %d %s | %d | %d | %.0f |"
          r.Profile r.LookupRange virt r.FilterValueAt r.FilterLookupRange r.ResultRows ok r.ExpectedRows r.ReadValueAt20 r.FilterMs)
      |> ignore
    sb.AppendLine() |> ignore
    sb.AppendLine(sprintf "*Generated: %s · N = %d · filter + read 20 rows where applicable*" runDate n) |> ignore
    sb.ToString()

  let writeBigDeedleResults () =
    let rows = collect ()
    let repoRoot = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "..", ".."))
    let outDir = Path.Combine(repoRoot, "big-deedle")
    let outPath = Path.Combine(outDir, "b4-profile-metrics.md")
    // Optional sibling checkout — CI that clones only Deedle must not fail.
    if Directory.Exists outDir then
      File.WriteAllText(outPath, toMarkdown rows (DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm UTC")))
      Some outPath
    else None

[<Test>]
let ``Can write LookupRange profile baseline when sibling repo exists`` () =
  match LookupRangeProfileReport.writeBigDeedleResults() with
  | Some path ->
      File.Exists(path) |> shouldEqual true
      LookupRangeProfileReport.collect() |> List.length |> shouldEqual 8
  | None ->
      // Sibling big-deedle/ not present (typical CI) — still verify collect() shape.
      LookupRangeProfileReport.collect() |> List.length |> shouldEqual 8

[<Test>]
let ``Can filter with Step LookupRange without ValueAt at filter time`` () =
  let c, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrame LookupRangeFixture.nLarge
  let filtered, filterDelta, _ = LookupRangeFixture.filterAndRead frame c 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  filterDelta.ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual (LookupRangeFixture.expectedMatchCount LookupRangeFixture.nLarge words.Length)

[<Test>]
let ``Can filter with ExactFixed LookupRange retaining first match only`` () =
  let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
  let c, frame, _ =
    InstrumentedOrdinalSource.createOrderedSearchFrameWith LookupRangeFixture.nLarge (LookupRangeExactFixed (fun v ->
      let o = words |> Array.findIndex ((=) v) |> int64
      o, o))
  let filtered, filterDelta, _ = LookupRangeFixture.filterAndRead frame c 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  filterDelta.ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual 1

[<Test>]
let ``Can filter with FullFixed LookupRange retaining entire series`` () =
  let c, frame, _ =
    InstrumentedOrdinalSource.createOrderedSearchFrameWith LookupRangeFixture.nLarge LookupRangeFullFixed
  let filtered, filterDelta, _ = LookupRangeFixture.filterAndRead frame c 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  filterDelta.ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  // Naive FullFixed keeps every row (unlike Step, which shrinks to ~N/period).
  filtered.RowCount |> shouldEqual (int LookupRangeFixture.nLarge)
  let _, stepFrame, words = InstrumentedOrdinalSource.createOrderedSearchFrame LookupRangeFixture.nLarge
  let stepCount = (stepFrame |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue).RowCount
  stepCount |> shouldEqual (LookupRangeFixture.expectedMatchCount LookupRangeFixture.nLarge words.Length)
  stepCount |> should be (lessThan filtered.RowCount)

[<Test>]
let ``Can filter ordinal frame using LookupRange like ordered index`` () =
  let c, frame, words = InstrumentedOrdinalSource.createOrdinalSearchFrame LookupRangeFixture.nLarge
  let filtered, filterDelta, _ = LookupRangeFixture.filterAndReadOrdinal frame c 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  filterDelta.ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual (LookupRangeFixture.expectedMatchCount LookupRangeFixture.nLarge words.Length)

[<Test>]
let ``Can read only requested rows after Step filter`` () =
  let c, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame LookupRangeFixture.nLarge
  let readN = 20
  let _, filterDelta, readDelta = LookupRangeFixture.filterAndRead frame c readN
  filterDelta.ValueAtCount |> shouldEqual 0
  readDelta.ValueAtCount |> should be (greaterThan 0)
  readDelta.ValueAtCount |> should be (lessThan (readN * 3))

[<Test>]
let ``Can pay more ValueAt cost draining FullFixed filter than Step filter`` () =
  let n = 10_000L
  let cFull, frameFull, _ =
    InstrumentedOrdinalSource.createOrderedSearchFrameWith n LookupRangeFullFixed
  let cStep, frameStep, words = InstrumentedOrdinalSource.createOrderedSearchFrame n
  let fullFiltered = frameFull |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue
  let stepFiltered = frameStep |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue
  fullFiltered.RowCount |> shouldEqual (int n)
  stepFiltered.RowCount |> shouldEqual (LookupRangeFixture.expectedMatchCount n words.Length)
  cFull.Reset()
  fullFiltered.GetColumn<int64>("S1").Values |> Seq.length |> ignore
  let fullReads = cFull.Snapshot().ValueAtCount
  cStep.Reset()
  stepFiltered.GetColumn<int64>("S1").Values |> Seq.length |> ignore
  let stepReads = cStep.Snapshot().ValueAtCount
  // FullFixed keeps all rows, so draining S1 touches at least every row; Step only the matches.
  fullReads |> should be (greaterThanOrEqualTo (int n))
  stepReads |> should be (greaterThanOrEqualTo stepFiltered.RowCount)
  fullReads |> should be (greaterThan stepReads)

[<Test>]
let ``Can scan mapped search column at filter time without reverse lookup`` () =
  let c, frame, words = InstrumentedOrdinalSource.createOrderedMappedSearchFrame LookupRangeFixture.nLarge
  c.Reset()
  let filtered = frame |> Frame.filterRowsBy "S2" (LookupRangeFixture.searchValue.ToUpperInvariant())
  let d = c.Snapshot()
  d.LookupRangeCount |> shouldEqual 0
  d.ValueAtCount |> should be (greaterThanOrEqualTo (int LookupRangeFixture.nLarge))
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual (LookupRangeFixture.expectedMatchCount LookupRangeFixture.nLarge words.Length)

[<Test>]
let ``Can filter ordinal Step within same order of magnitude as ordered Step`` () =
  let orderedMs =
    LookupRangeFixture.elapsedMs (fun () ->
      let c, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame LookupRangeFixture.nTiming
      c.Reset()
      frame |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue |> ignore)
  let ordinalMs =
    LookupRangeFixture.elapsedMs (fun () ->
      let c, frame, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame LookupRangeFixture.nTiming
      c.Reset()
      frame |> Frame.filterRowsBy "S2" LookupRangeFixture.searchValue |> ignore)
  ordinalMs |> should be (lessThan (max 50.0 (orderedMs * 5.0)))

[<Test>]
let ``Can match ordered partial-read cost on ordinal Step filter`` () =
  let orderedMs =
    LookupRangeFixture.elapsedMs (fun () ->
      let c, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame LookupRangeFixture.nTiming
      let filtered, _, _ = LookupRangeFixture.filterAndRead frame c 50
      filtered.RowCount |> ignore)
  let ordinalMs =
    LookupRangeFixture.elapsedMs (fun () ->
      let c, frame, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame LookupRangeFixture.nTiming
      let filtered, _, _ = LookupRangeFixture.filterAndReadOrdinal frame c 50
      filtered.RowCount |> ignore)
  ordinalMs |> should be (lessThan (max 50.0 (orderedMs * 5.0)))

// ------------------------------------------------------------------------------------------------
// Additional data profiles (beyond the ideal 8-word cycle)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can filter large vocabulary periodic data with Step LookupRange`` () =
  let vocabSize = 256
  let c, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrameLargeVocab LookupRangeFixture.nLarge vocabSize
  let search = words.[0]
  let filtered, filterDelta, _ = LookupRangeFixture.filterBy frame c 0 search
  filterDelta.ValueAtCount |> shouldEqual 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual (LookupRangeFixture.expectedMatchCount LookupRangeFixture.nLarge vocabSize)

[<Test>]
let ``Can filter sparse irregular matches with IndexList LookupRange`` () =
  let modulus = 997L
  let remainder = 42L
  let c, frame, trueCount = InstrumentedOrdinalSource.createOrderedSearchFrameSparse LookupRangeFixture.nLarge modulus remainder
  let filtered, filterDelta, _ = LookupRangeFixture.filterBy frame c 0 "lorem"
  filterDelta.ValueAtCount |> shouldEqual 0
  filterDelta.LookupRangeCount |> shouldEqual 1
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual trueCount
  trueCount |> should be (lessThan (int LookupRangeFixture.nLarge / 100))

[<Test>]
let ``Can over-filter sparse data with wrong Step LookupRange`` () =
  let modulus = 997L
  let remainder = 42L
  let c, frame, trueCount = InstrumentedOrdinalSource.createOrderedSearchFrameSparseWrongStep LookupRangeFixture.nLarge modulus remainder
  let filtered, filterDelta, _ = LookupRangeFixture.filterBy frame c 0 "lorem"
  filterDelta.ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> should be (greaterThan trueCount)
  // Wrong Step (period 11 from offset 42) keeps ~N/11 rows, not the ~N/997 true matches
  filtered.RowCount |> should be (greaterThan (int LookupRangeFixture.nLarge / 200))

[<Test>]
let ``Can remap IndexList via clipLookupRange after Fixed slice`` () =
  let modulus = 997L
  let remainder = 42L
  let n = 10_000L
  let _, frame, trueCount = InstrumentedOrdinalSource.createOrderedSearchFrameSparse n modulus remainder
  let filtered = frame |> Frame.filterRowsBy "S2" "lorem"
  filtered.RowCount |> shouldEqual trueCount
  let filtered2 = filtered |> Frame.filterRowsBy "S2" "lorem"
  filtered2.RowCount |> shouldEqual trueCount
  FrameProbe.rowIndexIsVirtual filtered2 |> shouldEqual true
  // Values must still resolve through the remapped IndexList (not absolute parent addrs).
  filtered2.GetColumn<string>("S2").Values |> Seq.distinct |> Seq.toList |> shouldEqual [ "lorem" ]

[<Test>]
let ``Can return same row count for ordinal and ordered Step filters`` () =
  let n = 10_000L
  let search = "lorem"
  let _, ordered, _ = InstrumentedOrdinalSource.createOrderedSearchFrame n
  let _, ordinal, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame n
  let orderedCount = (ordered |> Frame.filterRowsBy "S2" search).RowCount
  let ordinalCount = (ordinal |> Frame.filterRowsBy "S2" search).RowCount
  ordinalCount |> shouldEqual orderedCount
  ordinalCount |> shouldEqual (int ((n - 1L) / int64 8) + 1)

[<Test>]
let ``ordinal filterRowsBy without LookupRange scans rows and stays virtual`` () =
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let c = AccessCounters()
  let s2 = InstrumentedOrdinalSource<string>(100L, (fun i -> words.[int (i % int64 words.Length)]), c, hasMissing=false)
  let _, s1 = InstrumentedOrdinalSource.createLongs 100L
  let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
  c.Reset()
  let filtered = frame |> Frame.filterRowsBy "S2" "lorem"
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> should be (greaterThan 0)

[<Test>]
let ``Can filter ordinal row index when LookupRange is configured`` () =
  let c, frame, words = InstrumentedOrdinalSource.createOrdinalSearchFrame 1000L
  c.Reset()
  let filtered = frame |> Frame.filterRowsBy "S2" words.[0]
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  c.Snapshot().LookupRangeCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0

// ------------------------------------------------------------------------------------------------
// LookupRangeExecutor (src/Deedle/VirtualLookupRange.fs)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can intersect identical Step LookupRanges`` () =
  match LookupRangeExecutor.intersect (Range.step 0 8) (Range.step 0 8) with
  | RangeRestriction.Custom(:? StepRange as s) ->
      s.Offset |> shouldEqual 0
      s.Step |> shouldEqual 8
  | other -> failwithf "expected StepRange, got %A" other

[<Test>]
let ``Can intersect disjoint Step LookupRanges to empty range`` () =
  match LookupRangeExecutor.intersect (Range.step 0 8) (Range.step 1 8) with
  | RangeRestriction.Custom ar -> Seq.length ar |> shouldEqual 0
  | other -> failwithf "expected empty custom range, got %A" other

[<Test>]
let ``Can intersect Step LookupRange with IndexList without enumerating Step`` () =
  let step = RangeRestriction.Custom { Offset = 0; Step = 2 } : RangeRestriction<Address>
  let listAddrs = [ 0L; 2L; 3L; 4L; 7L ] |> List.map Address.ofInt64
  let list =
    ({ new IRangeRestriction<Address> with
        member _.Count = int64 listAddrs.Length
       interface seq<Address> with
         member _.GetEnumerator() = (listAddrs :> seq<_>).GetEnumerator()
       interface System.Collections.IEnumerable with
         member _.GetEnumerator() = (listAddrs :> seq<_>).GetEnumerator() :> System.Collections.IEnumerator }
     |> RangeRestriction.Custom)
  let addrsOf = function
    | RangeRestriction.Custom ar -> ar |> Seq.map Address.asInt64 |> Seq.toList
    | _ -> failwith "expected Custom range"
  addrsOf (LookupRangeExecutor.intersect step list) |> shouldEqual [ 0L; 2L; 4L ]
  addrsOf (LookupRangeExecutor.intersect list step) |> shouldEqual [ 0L; 2L; 4L ]

[<Test>]
let ``Can intersect overlapping Fixed LookupRanges`` () =
  match LookupRangeExecutor.intersect (Range.fixedRange 0 10) (Range.fixedRange 5 15) with
  | RangeRestriction.Fixed(lo, hi) ->
      Address.asInt64 lo |> shouldEqual 5
      Address.asInt64 hi |> shouldEqual 10
  | other -> failwithf "expected Fixed overlap, got %A" other

[<Test>]
let ``Can intersect disjoint Fixed LookupRanges to empty range`` () =
  match LookupRangeExecutor.intersect (Range.fixedRange 0 3) (Range.fixedRange 10 12) with
  | RangeRestriction.Fixed _ -> failwith "expected empty intersection"
  | RangeRestriction.Custom ar -> Seq.isEmpty ar |> shouldEqual true
  | other -> failwithf "expected empty Fixed intersection, got %A" other

[<Test>]
let ``LookupRangeExecutor returns empty range for invalid Step offset`` () =
  let mode = LookupRangeStep (fun _ -> (-1, 4))
  match LookupRangeExecutor.lookupRange 16L mode "x" "test" with
  | RangeRestriction.Custom ar -> Seq.isEmpty ar |> shouldEqual true
  | other -> failwithf "expected empty custom range, got %A" other

[<Test>]
let ``LookupRangeExecutor raises NotSupportedException when LookupRange is unsupported`` () =
  (fun () -> LookupRangeExecutor.lookupRange 8L LookupRangeUnsupported "x" "test" |> ignore)
  |> should throw typeof<NotSupportedException>

[<Test>]
let ``StepRange Count and enumeration raise NotSupportedException`` () =
  let range = { Offset = 1; Step = 3 } :> IRangeRestriction<Address>
  (fun () -> range.Count |> ignore) |> should throw typeof<NotSupportedException>
  (fun () -> (range :> seq<Address>) |> Seq.toList |> ignore)
  |> should throw typeof<NotSupportedException>

[<Test>]
let ``scan LookupRange returns matching row indices`` () =
  let valueAt i = if i % 3L = 0L then "hit" else "miss"
  match VirtualLookupRange.scan 9L valueAt with
  | LookupRangeIndexList f -> f "hit" |> shouldEqual [ 0L; 3L; 6L ]
  | _ -> failwith "expected LookupRangeIndexList"

[<Test>]
let ``forCategorical returns indices from the provided map`` () =
  let mode =
    VirtualLookupRange.forCategorical (Map.ofList [ "a", [ 0L; 4L ]; "b", [ 1L ] ])
  match mode with
  | LookupRangeIndexList f ->
      f "a" |> shouldEqual [ 0L; 4L ]
      f "missing" |> shouldEqual []
  | _ -> failwith "expected LookupRangeIndexList"

[<Test>]
let ``tryInferStringLookupRange infers categorical IndexList when not a cycle`` () =
  // Same vocabulary repeatedly clustered, not a repeating cycle by distinct order.
  let valueAt i =
    if i < 3L then "a"
    elif i < 6L then "b"
    else "a"
  match VirtualLookupRange.tryInferStringLookupRange 8L valueAt with
  | Some(_, desc) -> desc |> should haveSubstring "categorical IndexList"
  | None -> failwith "expected categorical inference"

[<Test>]
let ``Can remap Step LookupRange after Step GetSubVector for chained filter`` () =
  let words = [| "a"; "b"; "c" |]
  let n = 24L
  let mode = VirtualLookupRange.forRepeatingCycle words
  let range = LookupRangeExecutor.lookupRange n mode "b" "test"
  match LookupRangeExecutor.getSubVector n mode None range with
  | Choice1Of2 spec ->
      spec.Length |> shouldEqual 8L
      // Local domain is the "b" stride; filtering "b" again should keep all local rows.
      match LookupRangeExecutor.lookupRange spec.Length spec.LookupRange "b" "test" with
      | RangeRestriction.Custom(:? StepRange as s) ->
          s.Offset |> shouldEqual 0
          s.Step |> shouldEqual 1
      | other -> failwithf "expected local StepRange for same value, got %A" other
      // Disjoint cycle value should yield empty after remap.
      match LookupRangeExecutor.lookupRange spec.Length spec.LookupRange "a" "test" with
      | RangeRestriction.Custom ar -> Seq.isEmpty ar |> shouldEqual true
      | other -> failwithf "expected empty custom range for disjoint value, got %A" other
  | Choice2Of2 _ -> failwith "expected SubVectorSpec"

[<Test>]
let ``Can remap ExactFixed LookupRange after Step GetSubVector`` () =
  let n = 20L
  let mode = LookupRangeExactFixed (fun _ -> (2L, 14L))
  let stepRange = RangeRestriction.Custom { Offset = 2; Step = 3 } : RangeRestriction<Address>
  match LookupRangeExecutor.getSubVector n mode None stepRange with
  | Choice1Of2 spec ->
      // Parent Step 2,5,8,11,14,17 — ExactFixed [2,14] keeps 2,5,8,11,14 → local 0..4
      match LookupRangeExecutor.lookupRange spec.Length spec.LookupRange "x" "test" with
      | RangeRestriction.Fixed(lo, hi) ->
          Address.asInt64 lo |> shouldEqual 0L
          Address.asInt64 hi |> shouldEqual 4L
      | other -> failwithf "expected remapped Fixed, got %A" other
  | Choice2Of2 _ -> failwith "expected SubVectorSpec"

[<Test>]
let ``getSubVector raises when Fixed hi is less than lo`` () =
  let mode = LookupRangeFullFixed
  let bad = RangeRestriction.Fixed(Address.ofInt64 5L, Address.ofInt64 1L)
  (fun () -> LookupRangeExecutor.getSubVector 10L mode None bad |> ignore)
  |> should throw typeof<InvalidOperationException>
