#if INTERACTIVE
#load "../../src/Deedle/Deedle.fsx"
#r "nuget: NUnit"
#r "nuget: FsUnit"
#else
module Deedle.Arrow.Tests
#endif

open System
open System.IO
open NUnit.Framework
open FsUnit
open Deedle
open Deedle.Arrow

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

let private tmpFile () =
    Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")

let private withTmpFile (f: string -> unit) =
    let p = tmpFile ()
    try f p
    finally if File.Exists(p) then File.Delete(p)

// ------------------------------------------------------------------------------------------------
// Round-trip tests — Arrow IPC file format
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Float column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Value" => Series.ofValues [ 1.0; 2.5; nan; 4.0 ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.["Value"] |> Series.values |> Array.ofSeq
        col2.[0] |> should (equalWithin 1e-10) 1.0
        col2.[1] |> should (equalWithin 1e-10) 2.5
        col2.[2] |> should (equalWithin 1e-10) 4.0
        df2.RowCount |> should equal 4
        // NaN becomes missing in Deedle; the column should have 3 values
        df2.["Value"].ValueCount |> should equal 3)

[<Test>]
let ``Int32 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Count" => Series.ofValues [ 1; 2; 3; 4; 5 ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.["Count"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1; 2; 3; 4; 5 |])

[<Test>]
let ``Int64 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Big" => Series.ofValues [ 1L; Int64.MaxValue; -1L ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.["Big"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1L; Int64.MaxValue; -1L |])

[<Test>]
let ``Bool column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Flag" => Series.ofValues [ true; false; true ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<bool>("Flag") |> Series.values |> Array.ofSeq
        col2 |> should equal [| true; false; true |])

[<Test>]
let ``String column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "Alice"; "Bob"; "Carol" ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq
        col2 |> should equal [| "Alice"; "Bob"; "Carol" |])

[<Test>]
let ``DateTime column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let t1 = DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)
        let t2 = DateTime(2024, 6, 30,  0, 0, 0, DateTimeKind.Utc)
        let df = frame [ "Date" => Series.ofValues [ t1; t2 ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 : DateTime[] = df2.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
        abs((col2.[0] - t1).TotalSeconds) |> should (equalWithin 1.0) 0.0
        abs((col2.[1] - t2).TotalSeconds) |> should (equalWithin 1.0) 0.0)

[<Test>]
let ``Missing values are preserved through Arrow file`` () =
    withTmpFile (fun path ->
        let s = Series.ofValues [ 1.0; nan; 3.0; nan; 5.0 ]
        let df = frame [ "V" => s ]
        writeArrow path df
        let df2 = readArrow path
        df2.["V"].ValueCount |> should equal 3
        df2.RowCount          |> should equal 5)

[<Test>]
let ``Multi-column float frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                         "B" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
        writeArrow path df
        let df2 = readArrow path
        df2.ColumnCount |> should equal 2
        df2.RowCount    |> should equal 3
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        df2.["B"] |> Series.values |> Array.ofSeq |> should equal [| 4.0; 5.0; 6.0 |])

[<Test>]
let ``String column frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "x"; "y"; "z" ] ]
        writeArrow path df
        let df2 = readArrow path
        df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq |> should equal [| "x"; "y"; "z" |])

[<Test>]
let ``Empty frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = Frame.ofColumns ([] : (string * Series<int, float>) list)
        writeArrow path df
        let df2 = readArrow path
        df2.RowCount    |> should equal 0
        df2.ColumnCount |> should equal 0)

// ------------------------------------------------------------------------------------------------
// Round-trip tests — Arrow IPC stream format
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Float frame round-trips through Arrow stream`` () =
    let df = frame [ "X" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                     "Y" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
    use ms = new MemoryStream()
    writeArrowStream ms df
    ms.Position <- 0L
    let df2 = readArrowStream ms
    df2.ColumnCount |> should equal 2
    df2.RowCount    |> should equal 3
    df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]

// ------------------------------------------------------------------------------------------------
// In-memory conversion tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``frameToRecordBatch preserves column count and row count`` () =
    let df = frame [ "A" => Series.ofValues [ 1.0; 2.0 ]
                     "B" => Series.ofValues [ 3.0; 4.0 ] ]
    let batch = frameToRecordBatch df
    batch.ColumnCount |> should equal 2
    batch.Length      |> should equal 2

[<Test>]
let ``recordBatchToFrame preserves column names`` () =
    let df = frame [ "Alpha" => Series.ofValues [ 1.0; 2.0 ]
                     "Beta"  => Series.ofValues [ 3.0; 4.0 ] ]
    let batch  = frameToRecordBatch df
    let df2    = recordBatchToFrame batch
    df2.ColumnKeys |> List.ofSeq |> should equal [ "Alpha"; "Beta" ]

[<Test>]
let ``recordBatchToFrame row keys are 0-based integers`` () =
    let df = frame [ "V" => Series.ofValues [ 10; 20; 30 ] ]
    let batch = frameToRecordBatch df
    let df2   = recordBatchToFrame batch
    df2.RowKeys |> List.ofSeq |> should equal [ 0; 1; 2 ]
