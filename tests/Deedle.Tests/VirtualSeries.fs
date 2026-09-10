#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualSeries
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Vectors
open Deedle.Vectors.Virtual
open Deedle.Virtual
open Deedle.Tests.VirtualInstrumentation

// ------------------------------------------------------------------------------------------------
// Construction & metadata (lazy until read)
// ------------------------------------------------------------------------------------------------

let private nSmall = 10_000L
let private nMed = 100_000L

[<Test>]
let ``Can read boundary keys on small virtual ordinal series`` () =
  let _, series = InstrumentedOrdinalSource.createOrdinalSeries 5L
  series.TryGet(0L) |> shouldEqual (OptionalValue 0L)
  series.TryGet(4L) |> shouldEqual (OptionalValue 4L)
  series.TryGet(5L) |> shouldEqual OptionalValue.Missing
  series.TryGet(-1L) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Can get KeyCount on virtual series without reading values`` () =
  let c, series = InstrumentedOrdinalSource.createOrdinalSeries 1_000_000L
  c.Reset()
  series.KeyCount |> shouldEqual 1_000_000
  c.Snapshot().ValueAtCount |> shouldEqual 0
  SeriesProbe.isVirtual series |> shouldEqual true

[<Test>]
let ``Can format virtual series without evaluating entire series`` () =
  let c, series = InstrumentedOrdinalSource.createOrdinalSeries 1_000_000L
  c.Reset()
  series.Format(3, 3, false) |> ignore
  c.Snapshot().ValueAtCount |> should be (lessThan 20)
  SeriesProbe.isVirtual series |> shouldEqual true

[<Test>]
let ``Can lookup one key on virtual ordinal series`` () =
  let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
  c.Reset()
  s.TryGet(12345L) |> shouldEqual (OptionalValue 12345L)
  c.Snapshot().ValueAtCount |> shouldEqual 1
  SeriesProbe.isVirtual s |> shouldEqual true

[<Test>]
let ``Can materialize virtual series to linear storage`` () =
  let c, series = InstrumentedOrdinalSource.createOrdinalSeries 100L
  SeriesProbe.isVirtual series |> shouldEqual true
  let mat = series.Materialize()
  SeriesProbe.isLinear mat |> shouldEqual true
  c.Snapshot().ValueAtCount |> should be (greaterThan 0)

[<Test>]
let ``Can async materialize virtual series to linear storage`` () =
  let _, s = InstrumentedOrdinalSource.createOrdinalSeries 32L
  let mat = s.AsyncMaterialize() |> Async.RunSynchronously
  SeriesProbe.isLinear mat |> shouldEqual true
  mat.TryGet(7L) |> shouldEqual (OptionalValue 7L)

// ------------------------------------------------------------------------------------------------
// Indexing & slicing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can slice virtual series without ValueAt at slice time`` () =
  let c, series = InstrumentedOrdinalSource.createOrdinalSeries 1_000_000L
  c.Reset()
  let sliced = series.[10L .. 20L]
  c.Snapshot().GetSubVectorCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  SeriesProbe.isVirtual sliced |> shouldEqual true
  sliced.KeyCount |> shouldEqual 11

[<Test>]
let ``Can slice large virtual series preserving virtual storage`` () =
  let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
  c.Reset()
  let sliced = s.[100L .. 199L]
  c.Snapshot().GetSubVectorCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  SeriesProbe.isVirtual sliced |> shouldEqual true
  sliced.KeyCount |> shouldEqual 100

[<Test>]
let ``Can sum sliced virtual series touching only slice rows`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries nMed
  let sliced = s.[100L .. 199L]
  c.Reset()
  Stats.sum sliced |> shouldEqual 14950.0
  c.Snapshot().ValueAtCount |> shouldEqual 100

// ------------------------------------------------------------------------------------------------
// Operations - map, shift, diff, merge, zip
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can map values on virtual series without reading at map time`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries nMed
  c.Reset()
  let mapped = s |> Series.mapValues (fun v -> v + 1.0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  SeriesProbe.isVirtual mapped |> shouldEqual true

[<Test>]
let ``Can shift virtual ordinal series without ValueAt at shift time`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries nSmall
  c.Reset()
  let shifted = s |> Series.shift 1
  SeriesProbe.isVirtual shifted |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  shifted.KeyCount |> shouldEqual (int nSmall - 1)
  shifted.[1L] |> shouldEqual s.[0L]

[<Test>]
let ``Can shift ordered virtual series with positive and negative offsets`` () =
  let c, s = InstrumentedOrdinalSource.createOrderedFloatSeries 64L
  c.Reset()
  let shifted = s |> Series.shift 1
  SeriesProbe.isVirtual shifted |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  shifted.KeyCount |> shouldEqual 63
  c.Reset()
  let back = s |> Series.shift -1
  SeriesProbe.isVirtual back |> shouldEqual true
  back.GetAt(0) |> shouldEqual (s.GetAt(1))

[<Test>]
let ``Can diff virtual series staying virtual until read`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries 32L
  c.Reset()
  let d = s |> Series.diff 1
  SeriesProbe.isVirtual d |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  d.[1L] |> shouldEqual (s.[1L] - s.[0L])

[<Test>]
let ``Can merge non-overlapping virtual series slices`` () =
  let c, s = InstrumentedOrdinalSource.createOrdinalSeries nMed
  let a = s.[0L .. 99L]
  let b = s.[200L .. 299L]
  c.Reset()
  let merged = Series.merge a b
  c.Snapshot().MergeWithCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  SeriesProbe.isVirtual merged |> shouldEqual true
  merged.KeyCount |> shouldEqual 200

[<Test>]
let ``Can zipAlign overlapping virtual series slices`` () =
  let _, s = InstrumentedOrdinalSource.createOrdinalSeries 1_000L
  let a = s.[0L .. 500L]
  let b = s.[250L .. 750L]
  let zipped = Series.zipAlign JoinKind.Inner Lookup.Exact a b
  SeriesProbe.isVirtual zipped |> shouldEqual true
  zipped.KeyCount |> shouldEqual 251
  zipped.GetAt(0) |> shouldEqual (OptionalValue 250L, OptionalValue 250L)

[<Test>]
let ``Can intersect overlapping virtual series materializing result`` () =
  let _, s = InstrumentedOrdinalSource.createOrdinalSeries nSmall
  let a = s.[0L .. 500L]
  let b = s.[250L .. 750L]
  SeriesProbe.isLinear (Series.intersect a b) |> shouldEqual true

// ------------------------------------------------------------------------------------------------
// Fill missing & drop missing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can fill missing with constant on virtual series`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(32L, float, c, hasMissing=true)
  let s = Virtual.CreateOrdinalSeries(src)
  c.Reset()
  let filled = s |> Series.fillMissingWith 0.0
  SeriesProbe.isVirtual filled |> shouldEqual true
  filled.TryGet(0L) |> shouldEqual (OptionalValue 0.0)
  filled.TryGet(1L) |> shouldEqual (OptionalValue 1.0)

[<Test>]
let ``Can fill missing forward on virtual series`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(32L, float, c, hasMissing=true)
  let s = Virtual.CreateOrdinalSeries(src)
  let filled = s |> Series.fillMissing Direction.Forward
  SeriesProbe.isVirtual filled |> shouldEqual true
  filled.TryGet(3L) |> shouldEqual (OptionalValue 2.0)

[<Test>]
let ``Can drop missing on virtual series after presence scan`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(nSmall, float, c, hasMissing=true)
  let s = Virtual.CreateOrdinalSeries(src)
  let dropped = s |> Series.dropMissing
  SeriesProbe.isVirtual dropped |> shouldEqual true
  dropped.ValueCount |> shouldEqual dropped.KeyCount
  c.Snapshot().ValueAtCount |> should be (greaterThan 0)

[<Test>]
let ``Can dropMissing on virtual series with no missings as identity`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries 64L
  c.Reset()
  let dropped = s |> Series.dropMissing
  Object.ReferenceEquals(dropped, s) |> shouldEqual true
  SeriesProbe.isVirtual dropped |> shouldEqual true
  dropped.KeyCount |> shouldEqual 64
  // Presence scan touches every address once; no rebuild.
  c.Snapshot().ValueAtCount |> shouldEqual 64

[<Test>]
let ``Can dropMissing on virtual series when all values are missing`` () =
  let src =
    OrdinalVirtualSource(16L, (fun _ -> OptionalValue.Missing), "all-missing")
    :> IVirtualVectorSource<float>
  let s = Virtual.CreateOrdinalSeries(src)
  let dropped = s |> Series.dropMissing
  dropped.KeyCount |> shouldEqual 0
  dropped.ValueCount |> shouldEqual 0

[<Test>]
let ``Can pctChange virtual series staying virtual until read`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(32L, (fun i -> float (i + 1L)), c, hasMissing=false)
  let s = Virtual.CreateOrdinalSeries(src)
  c.Reset()
  let pct = s |> Series.pctChange 1
  SeriesProbe.isVirtual pct |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  // (2-1)/1 = 1.0 at key 1
  pct.[1L] |> shouldEqual 1.0

// ------------------------------------------------------------------------------------------------
// Windowing, grouping, sorting
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can window virtual series keeping nested windows virtual`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries 64L
  c.Reset()
  let windows = s |> Series.windowSizeInto (4, Boundary.Skip) DataSegment.data
  c.Snapshot().ValueAtCount |> shouldEqual 0
  let w0 = windows.GetAt(0)
  SeriesProbe.isVirtual w0 |> shouldEqual true
  Stats.sum w0 |> shouldEqual (0.0 + 1.0 + 2.0 + 3.0)

[<Test>]
let ``Can aggregate window sums materializing result series`` () =
  let _, s = InstrumentedOrdinalSource.createFloatSeries 1_000L
  let windows = s |> Series.windowSizeInto (5, Boundary.AtEnding) (fun w -> Stats.sum w.Data)
  SeriesProbe.isLinear windows |> shouldEqual true

[<Test>]
let ``Can group virtual series materializing nested groups`` () =
  let n = 64L
  let c, s = InstrumentedOrdinalSource.createFloatSeries n
  let grouped = s |> Series.groupBy (fun _k v -> int v % 4)
  // Outer series and every nested group are linear (documented materialize).
  SeriesProbe.isLinear grouped |> shouldEqual true
  grouped.KeyCount |> shouldEqual 4
  for KeyValue(_, nested) in grouped.Observations do
    SeriesProbe.isLinear nested |> shouldEqual true
  // Grouping must visit every value at least once.
  c.Snapshot().ValueAtCount |> should be (greaterThanOrEqualTo (int n))

[<Test>]
let ``Can sort virtual series by value materializing`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries 1_000L
  let sorted = s |> Series.sortBy (fun v -> -v)
  SeriesProbe.isLinear sorted |> shouldEqual true
  c.Snapshot().ValueAtCount |> should be (greaterThan 0)

[<Test>]
let ``Can sort already ordered virtual series by key as no-op`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries 1_000L
  c.Reset()
  let sorted = s |> Series.sortByKey
  SeriesProbe.isVirtual sorted |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  sorted.KeyCount |> shouldEqual s.KeyCount

// ------------------------------------------------------------------------------------------------
// Sampling & full pull
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can sample time into virtual chunks without full scan`` () =
  let c, s = InstrumentedOrdinalSource.createOrderedFloatSeries nMed
  c.Reset()
  let sampled = s |> Series.sampleTimeInto (TimeSpan.FromDays 365.0) Direction.Forward id
  let d = c.Snapshot()
  d.ValueAtCount |> should be (lessThan 20)
  d.ValueAtCount |> should be (lessThan (int nMed / 100))
  SeriesProbe.isVirtual (sampled.GetAt(0)) |> shouldEqual true

[<Test>]
let ``Can sum entire virtual series pulling every value`` () =
  let c, s = InstrumentedOrdinalSource.createFloatSeries nSmall
  c.Reset()
  Stats.sum s |> ignore
  c.Snapshot().ValueAtCount |> shouldEqual (int nSmall)

// ------------------------------------------------------------------------------------------------
// Mapped column without reverse lookup (scan at filter)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can filter mapped virtual column by scanning values`` () =
  let n = 64L
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let c = AccessCounters()
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
  let idx =
    InstrumentedOrdinalSource<DateTimeOffset>
      (n, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
  let inner =
    InstrumentedOrdinalSource<string>
      (n, (fun i -> words.[int (i % int64 words.Length)]), c,
       lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)
  let mapped =
    VirtualVectorSource.map None (fun _ (ov: OptionalValue<string>) ->
      ov |> OptionalValue.map (fun s -> s.ToUpperInvariant())) (inner :> IVirtualVectorSource<string>)
  let frame = Virtual.CreateFrame(idx, ["UP"], [mapped :> IVirtualVectorSource])
  c.Reset()
  let filtered = frame |> Frame.filterRowsBy "UP" "LOREM"
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> should be (greaterThan 0)
  let snap = c.Snapshot()
  snap.ValueAtCount |> should be (greaterThan 0)
  snap.LookupRangeCount |> shouldEqual 0
