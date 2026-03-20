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
open Apache.Arrow

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

// ------------------------------------------------------------------------------------------------
// Part 2: UInt8/16/32/64 type support
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``UInt8 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Byte" => Series.ofValues [ 0uy; 127uy; 255uy ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<uint8>("Byte") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0uy; 127uy; 255uy |])

[<Test>]
let ``UInt16 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U16" => Series.ofValues [ 0us; 1000us; 65535us ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<uint16>("U16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0us; 1000us; 65535us |])

[<Test>]
let ``UInt32 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U32" => Series.ofValues [ 0u; 100000u; UInt32.MaxValue ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<uint32>("U32") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0u; 100000u; UInt32.MaxValue |])

[<Test>]
let ``UInt64 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U64" => Series.ofValues [ 0UL; 9999999999UL; UInt64.MaxValue ] ]
        writeArrow path df
        let df2 = readArrow path
        let col2 = df2.GetColumn<uint64>("U64") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0UL; 9999999999UL; UInt64.MaxValue |])

[<Test>]
let ``UInt8 missing values are preserved through Arrow file`` () =
    withTmpFile (fun path ->
        let s = Series.ofOptionalObservations [ (0, Some 10uy); (1, None); (2, Some 30uy) ]
        let df = frame [ "B" => s ]
        writeArrow path df
        let df2 = readArrow path
        df2.["B"].ValueCount |> should equal 2
        df2.RowCount          |> should equal 3)

// ------------------------------------------------------------------------------------------------
// Part 2: Date32 / Date64 reading support
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Date32 array is read as DateTime column`` () =
    // Write an Arrow file with a Date32 column using the Apache.Arrow API directly
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let d1 = DateOnly(2024, 1, 15)
        let d2 = DateOnly(2024, 6, 30)
        let arr = Date32Array.Builder().Append(d1).Append(d2).Build()
        let schema = Schema([| Field("EventDate", arr.Data.DataType, nullable = true) |], null)
        let batch = RecordBatch(schema, [| arr :> IArrowArray |], arr.Length)
        // Write in a nested scope so the file is closed before we try to read it
        do
            use stream = File.Create(tmpPath)
            use writer = new Apache.Arrow.Ipc.ArrowFileWriter(stream, schema)
            writer.WriteStart()
            writer.WriteRecordBatch(batch)
            writer.WriteEnd()

        let df = readArrow tmpPath
        df.ColumnCount |> should equal 1
        df.RowCount    |> should equal 2
        let col = df.GetColumn<DateTime>("EventDate") |> Series.values |> Array.ofSeq
        col.[0].Year  |> should equal 2024
        col.[0].Month |> should equal 1
        col.[0].Day   |> should equal 15
        col.[1].Month |> should equal 6
        col.[1].Day   |> should equal 30
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Date64 array is read as DateTime column`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let dt = DateTime(2024, 3, 20, 12, 0, 0, DateTimeKind.Utc)
        let arr = Date64Array.Builder().Append(dt).Build()
        let schema = Schema([| Field("Ts", arr.Data.DataType, nullable = true) |], null)
        let batch = RecordBatch(schema, [| arr :> IArrowArray |], arr.Length)
        do
            use stream = File.Create(tmpPath)
            use writer = new Apache.Arrow.Ipc.ArrowFileWriter(stream, schema)
            writer.WriteStart()
            writer.WriteRecordBatch(batch)
            writer.WriteEnd()

        let df = readArrow tmpPath
        let col = df.GetColumn<DateTime>("Ts") |> Series.values |> Array.ofSeq
        col.[0].Year  |> should equal 2024
        col.[0].Month |> should equal 3
        col.[0].Day   |> should equal 20
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

// ------------------------------------------------------------------------------------------------
// Part 2: seriesToArrowArray / arrowArrayToSeries
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``seriesToArrowArray converts float series to DoubleArray`` () =
    let s = Series.ofValues [ 1.0; 2.5; 3.0 ]
    let arr = seriesToArrowArray s
    arr :? DoubleArray |> should equal true
    arr.Length         |> should equal 3

[<Test>]
let ``seriesToArrowArray preserves missing values`` () =
    let s = Series.ofOptionalObservations [ (0, Some 1.0); (1, None); (2, Some 3.0) ]
    let arr = seriesToArrowArray s
    arr.Length         |> should equal 3
    arr.IsNull(0)      |> should equal false
    arr.IsNull(1)      |> should equal true
    arr.IsNull(2)      |> should equal false

[<Test>]
let ``seriesToArrowArray round-trips int32 series`` () =
    let s = Series.ofValues [ 10; 20; 30 ]
    let arr = seriesToArrowArray s
    arr :? Int32Array |> should equal true
    (arr :?> Int32Array).GetValue(1).Value |> should equal 20

[<Test>]
let ``arrowArrayToSeries returns Series with 0-based int keys`` () =
    let arr = DoubleArray.Builder().Append(1.0).Append(2.0).Append(3.0).Build()
    let s = arrowArrayToSeries (arr :> IArrowArray)
    s.KeyCount   |> should equal 3
    s.Keys |> List.ofSeq |> should equal [ 0; 1; 2 ]

[<Test>]
let ``arrowArrayToSeries preserves missing values`` () =
    let arr = DoubleArray.Builder().Append(1.0).AppendNull().Append(3.0).Build()
    let s = arrowArrayToSeries (arr :> IArrowArray)
    s.ValueCount |> should equal 2
    s.KeyCount   |> should equal 3

[<Test>]
let ``seriesToArrowArray and arrowArrayToSeries round-trip floats`` () =
    let orig = Series.ofValues [ 1.0; 2.5; 4.0 ]
    let arr  = seriesToArrowArray orig
    let back = arrowArrayToSeries arr
    back.Values |> Seq.map (fun v -> v :?> float) |> Seq.toArray
    |> should equal [| 1.0; 2.5; 4.0 |]

// ------------------------------------------------------------------------------------------------
// Part 2: Feather v2 aliases
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``writeFeather and readFeather are aliases for writeArrow and readArrow`` () =
    withTmpFile (fun path ->
        let featherPath = Path.ChangeExtension(path, ".feather")
        try
            let df = frame [ "X" => Series.ofValues [ 1.0; 2.0; 3.0 ] ]
            writeFeather featherPath df
            let df2 = readFeather featherPath
            df2.RowCount    |> should equal 3
            df2.ColumnCount |> should equal 1
            df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        finally
            if File.Exists(featherPath) then File.Delete(featherPath))

// ------------------------------------------------------------------------------------------------
// Part 2: Row-key preservation
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``writeArrowWithIndex preserves string row keys`` () =
    withTmpFile (fun path ->
        let keys = [| "alice"; "bob"; "carol" |]
        let df = frame [ "Score" => Series(keys, [| 90.0; 85.0; 92.0 |]) ]
        writeArrowWithIndex path df
        let df2 = readArrowWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "alice"; "bob"; "carol" ]
        df2.ColumnCount |> should equal 1
        df2.GetColumn<float>("Score") |> Series.values |> Array.ofSeq |> should equal [| 90.0; 85.0; 92.0 |])

[<Test>]
let ``readArrowWithIndex on file without __index__ returns string int keys`` () =
    withTmpFile (fun path ->
        let df = frame [ "V" => Series.ofValues [ 1.0; 2.0 ] ]
        writeArrow path df
        let df2 = readArrowWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "0"; "1" ])

[<Test>]
let ``writeFeatherWithIndex and readFeatherWithIndex round-trip row keys`` () =
    withTmpFile (fun path ->
        let featherPath = Path.ChangeExtension(path, ".feather")
        try
            let keys = [| "x"; "y"; "z" |]
            let df = frame [ "N" => Series(keys, [| 1; 2; 3 |]) ]
            writeFeatherWithIndex featherPath df
            let df2 = readFeatherWithIndex featherPath
            df2.RowKeys |> List.ofSeq |> should equal [ "x"; "y"; "z" ]
        finally
            if File.Exists(featherPath) then File.Delete(featherPath))
