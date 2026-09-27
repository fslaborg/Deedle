#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualIndex
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Tests.VirtualInstrumentation

// ------------------------------------------------------------------------------------------------
// Virtual index builder (src/Deedle/Indices/VirtualIndex.fs)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can shift virtual frame without ValueAt at shift time`` () =
  let c, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrameWith 64L (LookupRangeStep (fun _ -> 0, 1))
  c.Reset()
  let shifted = frame |> Frame.shift 1
  FrameProbe.rowIndexIsVirtual shifted |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  shifted.RowCount |> shouldEqual 63

[<Test>]
let ``Can diff virtual frame staying virtual until read`` () =
  let n = 32L
  let c = AccessCounters()
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
  let idx =
    InstrumentedOrdinalSource<DateTimeOffset>
      (n, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
  let vals = InstrumentedOrdinalSource<float>(n, float, c, hasMissing=false)
  let frame = Virtual.CreateFrame(idx, ["V"], [vals :> IVirtualVectorSource])
  c.Reset()
  let d = frame |> Frame.diff 1
  FrameProbe.rowIndexIsVirtual d |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  d.GetColumn<float>("V").GetAt(0) |> shouldEqual 1.0

[<Test>]
let ``Can filterRowsBy2 fusing two predicates on same column`` () =
  let _, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrame 64L
  let fused = frame |> Frame.filterRowsBy2 "S2" words.[0] "S2" words.[0]
  FrameProbe.rowIndexIsVirtual fused |> shouldEqual true
  fused.RowCount |> shouldEqual (int ((64L - 1L) / int64 words.Length) + 1)

[<Test>]
let ``Can filterRowsBy2 on disjoint values yielding empty frame`` () =
  let _, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrameWith 64L (LookupRangeStep (fun _ -> 0, 1))
  (frame |> Frame.filterRowsBy2 "S2" words.[0] "S2" words.[1]).RowCount |> shouldEqual 0

[<Test>]
let ``Can chain filterRowsBy on Step index preserving count after remap`` () =
  let n = 1000L
  let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
  let _, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame n
  let once = frame |> Frame.filterRowsBy "S2" words.[0]
  let twice = frame |> Frame.filterRowsBy "S2" words.[0] |> Frame.filterRowsBy "S2" words.[0]
  twice.RowCount |> shouldEqual once.RowCount
  FrameProbe.rowIndexIsVirtual twice |> shouldEqual true
  (frame |> Frame.filterRowsBy2 "S2" words.[0] "S2" words.[0]).RowCount |> shouldEqual once.RowCount

[<Test>]
let ``Can chain filterRowsBy on Step then disjoint value yielding empty`` () =
  let n = 1000L
  let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
  let _, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame n
  let chained =
    frame
    |> Frame.filterRowsBy "S2" words.[0]
    |> Frame.filterRowsBy "S2" words.[1]
  chained.RowCount |> shouldEqual 0

[<Test>]
let ``Can filterRowsBy2 combining Step and IndexList columns`` () =
  let n = 64L
  let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
  let valueAt i = words.[int (i % int64 words.Length)]
  let c = AccessCounters()
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
  let idx =
    InstrumentedOrdinalSource<DateTimeOffset>
      (n, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
  let stepCol =
    InstrumentedOrdinalSource<string>
      (n, valueAt, c, lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)
  let listCol =
    InstrumentedOrdinalSource<string>
      (n, valueAt, c, lookupRange=VirtualLookupRange.forCategoricalScan n valueAt, hasMissing=false)
  let frame = Virtual.CreateFrame(idx, ["Step"; "List"], [stepCol :> IVirtualVectorSource; listCol :> IVirtualVectorSource])
  let fused = frame |> Frame.filterRowsBy2 "Step" "lorem" "List" "lorem"
  FrameProbe.rowIndexIsVirtual fused |> shouldEqual true
  fused.RowCount |> shouldEqual (frame |> Frame.filterRowsBy "Step" "lorem").RowCount

[<Test>]
let ``Can filter virtual frame without reading unused columns`` () =
  let n = 64L
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let cUnused = AccessCounters()
  let cSearch = AccessCounters()
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
  let idx =
    InstrumentedOrdinalSource<DateTimeOffset>
      (n, (fun i -> start.AddTicks(i * 123456789L)), AccessCounters(),
       asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
  let unused = InstrumentedOrdinalSource<int64>(n, id, cUnused, asLong=id, hasMissing=false)
  let search =
    InstrumentedOrdinalSource<string>
      (n, (fun i -> words.[int (i % int64 words.Length)]), cSearch,
       lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)
  let frame = Virtual.CreateFrame(idx, ["U"; "S2"], [unused :> IVirtualVectorSource; search :> IVirtualVectorSource])
  cUnused.Reset()
  cSearch.Reset()
  frame |> Frame.filterRowsBy "S2" "lorem" |> ignore
  cUnused.Snapshot().ValueAtCount |> shouldEqual 0
  cSearch.Snapshot().ValueAtCount |> shouldEqual 0

[<Test>]
let ``Can outer join mismatched ordinal virtual frames materializing index`` () =
  let _, s1 = InstrumentedOrdinalSource.createFloats 64L
  let _, s2 = InstrumentedOrdinalSource.createFloats 32L
  let f1 = Virtual.CreateOrdinalFrame(["A"], [s1 :> IVirtualVectorSource])
  let f2 = Virtual.CreateOrdinalFrame(["B"], [s2 :> IVirtualVectorSource])
  FrameProbe.rowIndexIsVirtual (f1.Join(f2, JoinKind.Outer)) |> shouldEqual false

[<Test>]
let ``Can outer join identical ordinal virtual frames staying virtual`` () =
  let _, s1 = InstrumentedOrdinalSource.createFloats 10_000L
  let _, s2 = InstrumentedOrdinalSource.createFloats 10_000L
  let f1 = Virtual.CreateOrdinalFrame(["A"], [s1 :> IVirtualVectorSource])
  let f2 = Virtual.CreateOrdinalFrame(["B"], [s2 :> IVirtualVectorSource])
  FrameProbe.rowIndexIsVirtual (f1.Join(f2, JoinKind.Outer)) |> shouldEqual true

[<Test>]
let ``Can filter ordered virtual frame via LookupRange without ValueAt`` () =
  let c, (f: Frame<DateTimeOffset, string>), _ = InstrumentedOrdinalSource.createOrderedSearchFrame 100_000L
  c.Reset()
  let filtered = f |> Frame.filterRowsBy "S2" "lorem"
  c.Snapshot().LookupRangeCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true

[<Test>]
let ``Can filter ordinal virtual frame via LookupRange without ValueAt`` () =
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let c, s2 = InstrumentedOrdinalSource.createSearchableStrings 10_000L words
  let _, s1 = InstrumentedOrdinalSource.createLongs 10_000L
  let f = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
  c.Reset()
  let filtered = f |> Frame.filterRowsBy "S2" "lorem"
  c.Snapshot().LookupRangeCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true

[<Test>]
let ``Can filter ordinal frame via scan when column has no LookupRange`` () =
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let c = AccessCounters()
  let s2 =
    InstrumentedOrdinalSource<string>(100L, (fun i -> words.[int (i % int64 words.Length)]), c, hasMissing=false)
  let _, s1 = InstrumentedOrdinalSource.createLongs 100L
  let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
  c.Reset()
  let filtered = frame |> Frame.filterRowsBy "S2" "lorem"
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> should be (greaterThan 0)
  c.Snapshot().ValueAtCount |> should be (greaterThan 0)

[<Test>]
let ``Can filter virtual frame to empty result when no rows match`` () =
  let _, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame 64L
  let filtered = frame |> Frame.filterRowsBy "S2" "definitely-not-in-vocabulary"
  filtered.RowCount |> shouldEqual 0
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true

[<Test>]
let ``Can match filterRowsBy2 on materialized frame to chained filterRowsBy`` () =
  let df =
    Frame.ofColumns [
      "A" => (series [ 0 => "x"; 1 => "y"; 2 => "x"; 3 => "x" ] :> ISeries<_>)
      "B" => (series [ 0 => 1; 1 => 2; 2 => 1; 3 => 9 ] :> ISeries<_>) ]
  let chained = df |> Frame.filterRowsBy "A" "x" |> Frame.filterRowsBy "B" 1
  let fused = df |> Frame.filterRowsBy2 "A" "x" "B" 1
  fused.RowCount |> shouldEqual chained.RowCount
  fused.RowKeys |> Seq.toList |> shouldEqual (chained.RowKeys |> Seq.toList)
  fused.GetColumn<int>("B").Values |> Seq.toList |> shouldEqual [ 1; 1 ]

[<Test>]
let ``Can filterRowsBy2 with fewer GetSubVector calls than chained filterRowsBy`` () =
  let c, frame, words = InstrumentedOrdinalSource.createOrderedSearchFrame 256L
  let search = words.[0]
  c.Reset()
  frame |> Frame.filterRowsBy2 "S2" search "S2" search |> ignore
  let fusedSubs = c.Snapshot().GetSubVectorCount
  c.Reset()
  frame |> Frame.filterRowsBy "S2" search |> Frame.filterRowsBy "S2" search |> ignore
  let chainedSubs = c.Snapshot().GetSubVectorCount
  fusedSubs |> should be (greaterThan 0)
  fusedSubs |> should be (lessThan chainedSubs)

[<Test>]
let ``Can pctChange virtual frame staying virtual until read`` () =
  let n = 32L
  let c = AccessCounters()
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
  let idx =
    InstrumentedOrdinalSource<DateTimeOffset>
      (n, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
  let vals = InstrumentedOrdinalSource<float>(n, (fun i -> float (i + 1L)), c, hasMissing=false)
  let frame = Virtual.CreateFrame(idx, ["V"], [vals :> IVirtualVectorSource])
  c.Reset()
  let pct = frame |> Frame.pctChange 1
  FrameProbe.rowIndexIsVirtual pct |> shouldEqual true
  c.Snapshot().ValueAtCount |> shouldEqual 0
  // (2-1)/1 = 1.0 at first result row
  pct.GetColumn<float>("V").GetAt(0) |> shouldEqual 1.0

[<Test>]
let ``Can compact ordinal Step filter row keys to 0 .. n-1`` () =
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let n = 1000L
  let _, s2 = InstrumentedOrdinalSource.createSearchableStrings n words
  let _, s1 = InstrumentedOrdinalSource.createLongs n
  let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
  let filtered = frame |> Frame.filterRowsBy "S2" words.[0]
  let expectedCount = int ((n - 1L) / int64 words.Length) + 1
  filtered.RowCount |> shouldEqual expectedCount
  filtered.RowIndex.KeyRange |> shouldEqual (0L, int64 expectedCount - 1L)
  filtered.RowKeys |> Seq.toList |> shouldEqual [ for i in 0L .. int64 expectedCount - 1L -> i ]

[<Test>]
let ``Can filter sliced ordinal frame via Step keeping absolute keys`` () =
  // Non-dense ordinal Ranges (after GetRange) take customInt64KeyRestriction, not compact 0..n-1.
  let words = "lorem ipsum dolor sit amet".Split(' ')
  let n = 500L
  let _, s2 = InstrumentedOrdinalSource.createSearchableStrings n words
  let _, s1 = InstrumentedOrdinalSource.createLongs n
  let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
  let sliced = frame.Rows.[100L .. 399L]
  sliced.RowIndex.KeyRange |> shouldEqual (100L, 399L)
  let filtered = sliced |> Frame.filterRowsBy "S2" words.[0]
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true
  filtered.RowCount |> should be (greaterThan 0)
  // Keys stay in the parent absolute domain (not remapped to 0..count-1).
  let lo, hi = filtered.RowIndex.KeyRange
  lo |> should be (greaterThanOrEqualTo 100L)
  hi |> should be (lessThanOrEqualTo 399L)
  (lo = 0L && hi = int64 filtered.RowCount - 1L) |> shouldEqual false
  filtered.GetColumn<string>("S2").Values |> Seq.distinct |> Seq.toList |> shouldEqual [ words.[0] ]
