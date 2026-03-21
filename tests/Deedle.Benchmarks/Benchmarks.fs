namespace Deedle.Benchmarks

open System
open BenchmarkDotNet.Attributes
open Deedle

// ------------------------------------------------------------------------------------------------
// Shared helpers
// ------------------------------------------------------------------------------------------------

[<AutoOpen>]
module private Helpers =

    let generateFrame colKeys rowCount rowOffset =
        let rnd = Random(0)
        [ for c in colKeys do
              let s = series [ for i in 0 .. rowCount - 1 -> rowOffset + i => rnd.NextDouble() ]
              yield c.ToString() => s ]
        |> frame

// ------------------------------------------------------------------------------------------------
// Frame benchmarks
// ------------------------------------------------------------------------------------------------

/// Benchmarks for common frame operations. Input data is created in GlobalSetup
/// so construction cost is not measured as part of the benchmark itself.
[<MemoryDiagnoser>]
type FrameBenchmarks() =

    let mutable frame20x10000 : Frame<int, string> = Unchecked.defaultof<_>
    let mutable frames10x1000 : Frame<int, string> list = []
    let mutable frame1000x1000 : Frame<int, string> = Unchecked.defaultof<_>
    let mutable frame100x10000WithNans : Frame<int, int> = Unchecked.defaultof<_>
    let mutable frameTwoCol1M : Frame<int, string> = Unchecked.defaultof<_>
    let mutable titanic : Frame<int, string> = Unchecked.defaultof<_>

    [<GlobalSetup>]
    member _.Setup() =
        frame20x10000 <- generateFrame (Seq.map string "ABCDEFGHIJKLMNOPQRST") 10000 0
        frames10x1000 <-
            [ for i in 0 .. 9 -> generateFrame (Seq.map string "ABCDEFGHIJ") 1000 (i * 1000) ]
        frame1000x1000 <- generateFrame [ 0 .. 999 ] 1000 0
        frame100x10000WithNans <-
            [ for i in 0 .. 100 ->
                  i
                  => series [ for j in 0 .. 10000 -> j => if j % 10 = 0 then nan else float j ] ]
            |> frame
        let rnd = Random(0)
        frameTwoCol1M <-
            Array.init 100000 (fun i -> (if rnd.Next(2) = 0 then "x" else "y"), float i)
            |> Frame.ofRecords
            |> Frame.indexColsWith [ "Key"; "Value" ]
        titanic <- Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/titanic.csv")

    // -- Converted from Performance.fs ---------------------------------------------------------

    [<Benchmark>]
    member _.NumericalOperatorsOn20x10kFrame() =
        let _add = frame20x10000 + frame20x10000
        let _mul = frame20x10000 * frame20x10000
        let _div = frame20x10000 / frame20x10000
        let _sub = frame20x10000 - frame20x10000
        ()

    [<Benchmark>]
    member _.ShiftFrame20x10kBy10() =
        frame20x10000 |> Frame.shift 10 |> ignore

    [<Benchmark>]
    member _.DiffFrame20x10kBy10() =
        frame20x10000 |> Frame.diff 10 |> ignore

    [<Benchmark>]
    member _.StackValuesOf1000x1000Frame() =
        frame1000x1000 |> Frame.melt |> ignore

    [<Benchmark>]
    member _.Merge10FramesOf1k() =
        frames10x1000 |> Seq.reduce Frame.merge |> ignore

    [<Benchmark>]
    member _.FillForwardMissingIn100x10kFrame() =
        frame100x10000WithNans |> Frame.fillMissing Direction.Forward |> ignore

    [<Benchmark>]
    member _.DropSparseRowsFromLargeFrame() =
        let bigRowSparseFrame =
            frame [ for k in 'A' .. 'Z' ->
                        k
                        => Series.ofValues [ for i in 0 .. 10000 -> if i % 2 = 0 then float i else nan ] ]
        bigRowSparseFrame |> Frame.dropSparseRows |> ignore

    [<Benchmark>]
    member _.DropSparseColsFromLargeFrame() =
        let bigColSparseFrame =
            frame [ for k in 'A' .. 'Z' ->
                        k
                        => Series.ofValues
                               [ for i in 0 .. 10000 -> if (int k) % 3 <> 1 then float i else nan ] ]
        bigColSparseFrame |> Frame.dropSparseCols |> ignore

    [<Benchmark>]
    member _.GetColumnsAndRecreateFrame() =
        frame20x10000.Columns |> Frame.ofColumns |> ignore

    [<Benchmark>]
    member _.TitanicSurvivalRateGroupRowsBy() =
        let bySex = titanic |> Frame.groupRowsByString "Sex"
        let survivedBySex = bySex.Columns.["Survived"].As<bool>()
        let survivals =
            survivedBySex
            |> Series.applyLevel Pair.get1Of2 (fun sr -> sr.Values |> Seq.countBy id |> series)
            |> Frame.ofRows
            |> Frame.indexColsWith [ "Survived"; "Died" ]
        survivals?Total <-
            bySex.Rows
            |> Series.applyLevel Pair.get1Of2 Series.countKeys
        survivals |> ignore

    [<Benchmark>]
    member _.TitanicSurvivalRatePivotTable() =
        titanic
        |> Frame.pivotTable
               (fun _ row -> row.GetAs<string>("Sex"))
               (fun _ row -> row.GetAs<bool>("Survived"))
               (fun df -> df.RowCount)
        |> Frame.indexColsWith [ "Survived"; "Died" ]
        |> ignore

    [<Benchmark>]
    member _.SumColumnUsingUntypedRows() =
        let rows = titanic.Rows
        let mutable c = 0
        for i in fst rows.KeyRange .. snd rows.KeyRange do
            c <- c + titanic.Rows.[i].GetAs<int>("Pclass")
        c

    [<Benchmark>]
    member _.GroupByColumnAndSubtractGroupAverages() =
        let grouped = frameTwoCol1M |> Frame.groupRowsByString "Key"
        grouped.Rows
        |> Series.applyLevel fst (fun r ->
               let df = Frame.ofRows r
               df - Stats.mean df?Value |> Frame.mapRowKeys snd)
        |> Frame.unnest
        |> Frame.mapRowKeys snd
        |> ignore

    // -- Additional benchmarks ------------------------------------------------------------------

    /// Read a CSV file (includes parsing overhead).
    [<Benchmark>]
    member _.ReadCsvTitanic() =
        Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/titanic.csv") |> ignore

    /// Filter rows of a large frame by a predicate on one column.
    [<Benchmark>]
    member _.FilterRowsByColumn() =
        frame20x10000
        |> Frame.filterRowValues (fun row -> row.GetAs<float>("A") > 0.5)
        |> ignore

    /// Map values across all cells in a large frame.
    [<Benchmark>]
    member _.MapValuesOnLargeFrame() =
        frame20x10000 |> Frame.mapValues (fun (v: float) -> v * 2.0) |> ignore

    /// Pivot the titanic frame.
    [<Benchmark>]
    member _.PivotTablePclassAndSurvived() =
        titanic
        |> Frame.pivotTable
               (fun _ row -> row.GetAs<int>("Pclass"))
               (fun _ row -> row.GetAs<bool>("Survived"))
               Frame.countRows
        |> ignore

    /// Inner join of two 20-column, 10k-row frames on row keys.
    [<Benchmark>]
    member _.JoinTwoLargeFrames() =
        let half1 = generateFrame (Seq.map string "ABCDEFGHIJ") 10000 0
        let half2 = generateFrame (Seq.map string "KLMNOPQRST") 10000 0
        Frame.join JoinKind.Inner half1 half2 |> ignore

    /// Compute column means across a 20x10k frame.
    [<Benchmark>]
    member _.ColumnMeansOnLargeFrame() =
        frame20x10000.Columns
        |> Series.mapValues (fun s -> Stats.mean (s.As<float>()))
        |> ignore

    /// Compute column sums across a 20x10k frame.
    [<Benchmark>]
    member _.ColumnSumsOnLargeFrame() =
        frame20x10000.Columns
        |> Series.mapValues (fun s -> Stats.sum (s.As<float>()))
        |> ignore

// ------------------------------------------------------------------------------------------------
// Series benchmarks
// ------------------------------------------------------------------------------------------------

[<MemoryDiagnoser>]
type SeriesBenchmarks() =

    let mutable series1M : Series<int, float> = Unchecked.defaultof<_>
    let mutable series10k : Series<int, float> = Unchecked.defaultof<_>
    let mutable array1M : (int * float) array = [||]
    let mutable s1 : Series<int, float> = Unchecked.defaultof<_>
    let mutable s2 : Series<int, float> = Unchecked.defaultof<_>
    let mutable s3 : Series<int, float> = Unchecked.defaultof<_>
    let mutable s4 : Series<int, float> = Unchecked.defaultof<_>
    let mutable s5 : Series<int, float> = Unchecked.defaultof<_>
    let mutable s6 : Series<int, float> = Unchecked.defaultof<_>

    [<GlobalSetup>]
    member _.Setup() =
        let rnd = Random(0)
        array1M <- Array.init 1000000 (fun i -> i, rnd.NextDouble())
        series1M <- series array1M
        series10k <- series (Array.init 10000 (fun i -> i, rnd.NextDouble()))
        s1 <- series [ for i in 0000001 .. 0300000 -> i => float i ]
        s2 <- series [ for i in 1000001 .. 1300000 -> i => float i ]
        s3 <- series [ for i in 2000001 .. 2300000 -> i => float i ]
        s4 <- series [ for i in 3000001 .. 3300000 -> i => float i ]
        s5 <- series [ for i in 4000001 .. 4300000 -> i => float i ]
        s6 <- series [ for i in 5000001 .. 5300000 -> i => float i ]

    // -- Converted from Performance.fs ---------------------------------------------------------

    [<Benchmark>]
    member _.BuildLarge1MSeriesFromArrays() =
        Series(array1M, array1M) |> ignore

    [<Benchmark>]
    member _.Realign1MSeriesTo1MKeys() =
        let newKeys = [| 1 .. 1000000 |]
        series1M |> Series.realign newKeys |> ignore

    [<Benchmark>]
    member _.Resample1MSeriesUsing10000BlocksForward() =
        let keys = [ for i in 0 .. 100 -> i * 10000 ]
        series1M |> Series.resampleInto keys Direction.Forward (fun _ s -> Stats.mean s) |> ignore

    [<Benchmark>]
    member _.Resample1MSeriesUsing10000BlocksBackward() =
        let keys = [ for i in 0 .. 100 -> i * 10000 ]
        series1M |> Series.resampleInto keys Direction.Backward (fun _ s -> Stats.mean s) |> ignore

    [<Benchmark>]
    member _.Resample1MSeriesUsing100BlocksForward() =
        let keys = [ for i in 0 .. 10000 -> i * 100 ]
        series1M |> Series.resampleInto keys Direction.Forward (fun _ s -> Stats.mean s) |> ignore

    [<Benchmark>]
    member _.Resample1MSeriesUsing100BlocksBackward() =
        let keys = [ for i in 0 .. 10000 -> i * 100 ]
        series1M |> Series.resampleInto keys Direction.Backward (fun _ s -> Stats.mean s) |> ignore

    [<Benchmark>]
    member _.Take500kFrom1MSeries() =
        series1M |> Series.take 500000 |> ignore

    [<Benchmark>]
    member _.Merge3Ordered300kSingleMerge() =
        s1.Merge(s2, s3) |> ignore

    [<Benchmark>]
    member _.Merge6Ordered300kSingleMerge() =
        s1.Merge(s2, s3, s4, s5, s6) |> ignore

    [<Benchmark>]
    member _.Merge3Ordered300kRepeating() =
        s1.Merge(s2).Merge(s3) |> ignore

    [<Benchmark>]
    member _.Merge6Ordered300kRepeating() =
        s1.Merge(s2).Merge(s3).Merge(s4).Merge(s5).Merge(s6) |> ignore

    [<Benchmark>]
    member _.Merge1000Ordered1kSeriesSingle() =
        let series1000of1000 =
            [ for i in 1 .. 1000 ->
                  series [ for j in 1000000 * i + 1 .. 1000000 * i + 1000 -> j => float j ] ]
        Series.mergeAll series1000of1000 |> ignore

    // -- Additional benchmarks ------------------------------------------------------------------

    /// Map values across 1M-element series.
    [<Benchmark>]
    member _.MapValuesOn1MSeries() =
        series1M |> Series.mapValues (fun v -> v * 2.0) |> ignore

    /// Filter values in a 1M-element series.
    [<Benchmark>]
    member _.FilterValuesOn1MSeries() =
        series1M |> Series.filterValues (fun v -> v > 0.5) |> ignore

    /// Sliding window of size 10 over a 10k series.
    [<Benchmark>]
    member _.WindowedMeanOn10kSeries() =
        series10k |> Series.windowInto 10 Stats.mean |> ignore

    /// Point-wise addition of two 1M series.
    [<Benchmark>]
    member _.AddTwo1MSeries() =
        (series1M + series1M) |> ignore

    /// Lookup 1000 random keys in a 1M series.
    [<Benchmark>]
    member _.LookupRandomKeysIn1MSeries() =
        let mutable sum = 0.0
        for i in 0 .. 10 .. 9999 do
            sum <- sum + series1M.[i]
        sum

    /// Compute the mean of a 1M series.
    [<Benchmark>]
    member _.MeanOf1MSeries() =
        Stats.mean series1M

    /// Compute the stdDev of a 1M series.
    [<Benchmark>]
    member _.StdDevOf1MSeries() =
        Stats.stdDev series1M

    /// Create a small (10-element) series 100k times.
    [<Benchmark>]
    member _.CreateSmall10SeriesRepeated() =
        let vs = Array.init 10 (fun i -> i, float i)
        for _ in 1 .. 100000 do
            series vs |> ignore

    /// Create a large (10k-element) series 100 times.
    [<Benchmark>]
    member _.CreateLarge10kSeriesRepeated() =
        let vs = Array.init 10000 (fun i -> i, float i)
        for _ in 1 .. 100 do
            series vs |> ignore
