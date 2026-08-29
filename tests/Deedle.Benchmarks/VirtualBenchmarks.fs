namespace Deedle.Benchmarks

open System
open System.IO
open BenchmarkDotNet.Attributes
open Deedle
open Deedle.TestData
open Deedle.Virtual
open Deedle.Virtual.Sources
open Deedle.Parquet
open Deedle.Parquet.Virtual.Sources
open Deedle.Vectors.Virtual
open Deedle.Tests.VirtualInstrumentation

/// BenchmarkDotNet suite for synthetic virtual workloads.
/// Pairs with access-count experiments in VirtualOpMatrix / VirtualLookupRange.
[<MemoryDiagnoser>]
[<SimpleJob(warmupCount = 2, iterationCount = 5)>]
type VirtualBenchmarks() =

    let n = 100_000L
    let readKeys = 50
    let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let dataDir = Path.Combine(__SOURCE_DIRECTORY__, "data")
    let csvPath = Path.Combine(dataDir, CsvTestData.defaultDatasetName)
    let parquetPath = Path.Combine(dataDir, ParquetTestData.defaultDatasetName)

    let mutable orderedFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable orderedExactFixedFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable orderedFullFixedFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable ordinalFrame : Frame<int64, string> = Unchecked.defaultof<_>
    let mutable mappedSearchFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable sparseFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable sparseWrongStepFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable series : Series<int64, int64> = Unchecked.defaultof<_>
    let mutable floatSeries : Series<int64, float> = Unchecked.defaultof<_>
    let mutable missingSeries : Series<int64, float> = Unchecked.defaultof<_>
    let mutable joinLeft : Frame<int64, string> = Unchecked.defaultof<_>
    let mutable joinRight : Frame<int64, string> = Unchecked.defaultof<_>
    let mutable scanColumnsFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable counters = Unchecked.defaultof<AccessCounters>
    let mutable searchValue = "lorem"
    let mutable scanFilterValue = 500.0
    let mutable scanLabelValue = "alpha"
    let mutable csvLineIndex : CsvLineIndex = Unchecked.defaultof<_>
    let mutable csvFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable parquetFrame : Frame<DateTimeOffset, string> = Unchecked.defaultof<_>
    let mutable csvValueFilter = 0.0
    let mutable csvLabelFilter = ""

    [<GlobalSetup>]
    member _.Setup() =
        let _, f, _ = InstrumentedOrdinalSource.createOrderedSearchFrame n
        orderedFrame <- f

        let exactLookup =
            LookupRangeExactFixed(fun v ->
                let o = words |> Array.findIndex ((=) v) |> int64
                o, o)
        let _, exactFrame, _ =
            InstrumentedOrdinalSource.createOrderedSearchFrameWith n exactLookup
        orderedExactFixedFrame <- exactFrame

        let _, fullFixedFrame, _ =
            InstrumentedOrdinalSource.createOrderedSearchFrameWith n LookupRangeFullFixed
        orderedFullFixedFrame <- fullFixedFrame

        let _, of_, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame n
        ordinalFrame <- of_

        let _, mapped, _ =
            InstrumentedOrdinalSource.createOrderedMappedSearchFrame n
        mappedSearchFrame <- mapped

        let _, sf, _ = InstrumentedOrdinalSource.createOrderedSearchFrameSparse n 997L 42L
        sparseFrame <- sf

        let _, swf, _ =
            InstrumentedOrdinalSource.createOrderedSearchFrameSparseWrongStep n 997L 42L
        sparseWrongStepFrame <- swf

        let _, s = InstrumentedOrdinalSource.createOrdinalSeries n
        series <- s
        let _, fs = InstrumentedOrdinalSource.createFloatSeries n
        floatSeries <- fs

        let cMiss = AccessCounters()
        let missSrc = InstrumentedOrdinalSource<float>(n, float, cMiss, hasMissing=true)
        missingSeries <- Virtual.CreateOrdinalSeries(missSrc)

        let _, leftCol = InstrumentedOrdinalSource.createFloats n
        let _, rightCol = InstrumentedOrdinalSource.createFloats n
        joinLeft <- Virtual.CreateOrdinalFrame(["A"], [leftCol :> IVirtualVectorSource])
        joinRight <- Virtual.CreateOrdinalFrame(["B"], [rightCol :> IVirtualVectorSource])

        let c, swf, _, floatFilter, labelFilter = InstrumentedOrdinalSource.createOrderedSearchWithScanColumnsFrame n
        counters <- c
        scanColumnsFrame <- swf
        scanFilterValue <- floatFilter
        scanLabelValue <- labelFilter

        if not (Directory.Exists dataDir) then Directory.CreateDirectory dataDir |> ignore
        CsvTestData.ensureSearchCsv csvPath n |> ignore
        ParquetTestData.ensureSearchParquet parquetPath n |> ignore
        csvLineIndex <- CsvLineIndex(csvPath)
        let materialized = Frame.ReadCsv(csvPath, hasHeaders = true)
        csvValueFilter <- materialized.GetColumn<float>("Value").GetAt(0)
        csvLabelFilter <- materialized.GetColumn<string>("Label").GetAt(0)
        csvFrame <-
            Virtual.ReadCsv<DateTimeOffset>(
                csvPath,
                indexColumn = "Timestamp",
                searchColumns =
                  [ VirtualSearchColumn.infer "Category"
                    VirtualSearchColumn.infer "Label" ],
                columnKeys = [ "Id"; "Category"; "Label"; "Value" ])
        parquetFrame <-
            Virtual.ReadParquet(
                parquetPath,
                indexColumn = "Timestamp",
                searchColumns =
                  [ VirtualSearchColumn.infer "Category"
                    VirtualSearchColumn.infer "Label" ],
                columnKeys = [ "Id"; "Category"; "Label"; "Value" ])

    // --- Filter (LookupRange profiles) ----------------------------------------------------------

    /// Ordered index + Step LookupRange — virtual filter, no full scan.
    [<Benchmark(Baseline = true)>]
    member _.FilterRowsBy_OrderedStep() =
        orderedFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// Same ordered data, but LookupRangeExactFixed: virtual but only retains first match.
    [<Benchmark>]
    member _.FilterRowsBy_OrderedExactFixed() =
        orderedExactFixedFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// LookupRangeFullFixed: virtual but does not really filter (naive full range).
    [<Benchmark>]
    member _.FilterRowsBy_OrderedFullFixed() =
        orderedFullFixedFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// Ordinal index + Step LookupRange on search column — same fast path as ordered.
    [<Benchmark>]
    member _.FilterRowsBy_OrdinalStep() =
        ordinalFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// Sparse matches via IndexList LookupRange.
    [<Benchmark>]
    member _.FilterRowsBy_SparseIndexList() =
        sparseFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// Sparse matches but with wrong Step LookupRange: virtual and cheap, but returns incorrect row count.
    [<Benchmark>]
    member _.FilterRowsBy_SparseWrongStep() =
        sparseWrongStepFrame |> Frame.filterRowsBy "S2" searchValue |> ignore

    /// Filter on mapped/projection search column (no reverse lookup): expensive scan at filter time.
    [<Benchmark>]
    member _.FilterRowsBy_MappedColumn_Scan() =
        let searchUpper = searchValue.ToUpperInvariant()
        mappedSearchFrame |> Frame.filterRowsBy "S2" searchUpper |> ignore

    /// Filter on a float column without LookupRange (scan fallback) — same frame as OrderedStep baseline.
    [<Benchmark>]
    member _.FilterRowsBy_NonSearchFloat_Scan() =
        scanColumnsFrame |> Frame.filterRowsBy "S3" scanFilterValue |> ignore

    /// Filter on a string column without LookupRange (scan fallback).
    [<Benchmark>]
    member _.FilterRowsBy_NonSearchString_Scan() =
        scanColumnsFrame |> Frame.filterRowsBy "S4" scanLabelValue |> ignore

    /// Search column (`S2`) with Step LookupRange — ValueAt count should stay near zero.
    [<Benchmark>]
    member _.Synthetic_ValueAtCount_SearchColumn() =
        counters.Reset()
        scanColumnsFrame |> Frame.filterRowsBy "S2" searchValue |> ignore
        counters.ValueAtCount

    /// Non-search float column (`S3`) — scan fallback reads every row.
    [<Benchmark>]
    member _.Synthetic_ValueAtCount_NonSearchFloat() =
        counters.Reset()
        scanColumnsFrame |> Frame.filterRowsBy "S3" scanFilterValue |> ignore
        counters.ValueAtCount

    /// Non-search string column (`S4`) — scan fallback reads every row.
    [<Benchmark>]
    member _.Synthetic_ValueAtCount_NonSearchString() =
        counters.Reset()
        scanColumnsFrame |> Frame.filterRowsBy "S4" scanLabelValue |> ignore
        counters.ValueAtCount

    /// Category filter on configured search column — LookupRange only, no CSV row decode.
    [<Benchmark>]
    member _.Csv_RowDecodeCount_SearchColumn() =
        csvLineIndex.ResetSplitCount()
        csvFrame |> Frame.filterRowsBy "Category" searchValue |> ignore
        csvLineIndex.SplitCount

    /// Value filter on column without LookupRange — one CSV row decode per compared row.
    [<Benchmark>]
    member _.Csv_RowDecodeCount_NonSearchFloat() =
        csvLineIndex.ResetSplitCount()
        csvFrame |> Frame.filterRowsBy "Value" csvValueFilter |> ignore
        csvLineIndex.SplitCount

    /// Label filter on configured search column — LookupRange only, no CSV row decode.
    [<Benchmark>]
    member _.Csv_RowDecodeCount_SearchLabel() =
        csvLineIndex.ResetSplitCount()
        csvFrame |> Frame.filterRowsBy "Label" csvLabelFilter |> ignore
        csvLineIndex.SplitCount

    /// File-backed CSV: non-search float column scan.
    [<Benchmark>]
    member _.Csv_FilterRowsBy_Value_Scan() =
        csvFrame |> Frame.filterRowsBy "Value" csvValueFilter |> ignore

    /// File-backed CSV: configured search column (Label).
    [<Benchmark>]
    member _.Csv_FilterRowsBy_Label() =
        csvFrame |> Frame.filterRowsBy "Label" csvLabelFilter |> ignore

    /// File-backed Parquet: non-search float column scan.
    [<Benchmark>]
    member _.Parquet_FilterRowsBy_Value_Scan() =
        parquetFrame |> Frame.filterRowsBy "Value" csvValueFilter |> ignore

    /// File-backed Parquet: configured search column (Label).
    [<Benchmark>]
    member _.Parquet_FilterRowsBy_Label() =
        parquetFrame |> Frame.filterRowsBy "Label" csvLabelFilter |> ignore

    /// Filter then read first N rows (end-to-end slice of result).
    [<Benchmark>]
    member _.FilterRowsBy_OrderedStep_Read50() =
        let filtered = orderedFrame |> Frame.filterRowsBy "S2" searchValue
        for i in 0 .. readKeys - 1 do
            if int64 i < filtered.RowIndex.KeyCount then
                filtered?S1.GetAt(i) |> ignore

    // --- Series virtual ops (virtualization matrix) ---------------------------------------------

    [<Benchmark>]
    member _.Slice_VirtualSeries() =
        series.[1000L .. 2000L] |> ignore

    [<Benchmark>]
    member _.Lookup_VirtualSeries() =
        series.TryGet(50_000L) |> ignore

    [<Benchmark>]
    member _.MapValues_VirtualSeries() =
        series |> Series.mapValues (fun v -> v + 1L) |> ignore

    [<Benchmark>]
    member _.Materialize_SlicedVirtualSeries() =
        series.[0L .. 999L].Materialize() |> ignore

    [<Benchmark>]
    member _.StatsSum_VirtualSeries() =
        Stats.sum floatSeries |> ignore

    [<Benchmark>]
    member _.Shift_VirtualSeries() =
        floatSeries |> Series.shift 1 |> ignore

    [<Benchmark>]
    member _.Diff_VirtualSeries() =
        floatSeries |> Series.diff 1 |> ignore

    [<Benchmark>]
    member _.WindowSize_VirtualNested() =
        floatSeries.[0L .. 199L]
        |> Series.windowSizeInto (5, Boundary.Skip) DataSegment.data
        |> ignore

    [<Benchmark>]
    member _.FilterRowsBy2_FusedSameColumn() =
        orderedFrame |> Frame.filterRowsBy2 "S2" searchValue "S2" searchValue |> ignore

    [<Benchmark>]
    member _.FilterRowsBy_ChainedSameColumn() =
        orderedFrame
        |> Frame.filterRowsBy "S2" searchValue
        |> Frame.filterRowsBy "S2" searchValue
        |> ignore

    [<Benchmark>]
    member _.SliceThenStatsSum_1000() =
        Stats.sum floatSeries.[0L .. 999L] |> ignore

    [<Benchmark>]
    member _.ZipAlign_IdenticalOrdinal() =
        Series.zipAlign JoinKind.Inner Lookup.Exact series floatSeries |> ignore

    [<Benchmark>]
    member _.SortByKey_AlreadyOrdered() =
        floatSeries |> Series.sortByKey |> ignore

    [<Benchmark>]
    member _.DropMissing_VirtualSeries() =
        missingSeries |> Series.dropMissing |> ignore

    [<Benchmark>]
    member _.Join_IdenticalOrdinalFrames() =
        joinLeft.Join(joinRight, JoinKind.Outer) |> ignore

    /// First mini-batch only — early-stop ML path (.
    [<Benchmark>]
    member _.MaterializeFloatBatches_First1k() =
        Virtual.MaterializeFloatBatches(joinLeft, 1000L, ["A"]) |> Seq.head |> ignore

    /// Drain all batches — full feature matrix, still without one giant allocation.
    [<Benchmark>]
    member _.MaterializeFloatBatches_Full() =
        Virtual.MaterializeFloatBatches(joinLeft, 1000L, ["A"]) |> Seq.length |> ignore
