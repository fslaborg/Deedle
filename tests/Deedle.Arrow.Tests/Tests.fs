#if INTERACTIVE
#load "../../src/Deedle/Deedle.fsx"
#r "nuget: NUnit"
#r "nuget: FsUnit"
#r "nuget: FsCheck"
#else
module Deedle.Arrow.Tests
#endif

open System
open System.IO
open NUnit.Framework
open FsUnit
open FsCheck
open Deedle
open Deedle.Arrow
open Apache.Arrow

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

let private dataDir = Path.Combine(__SOURCE_DIRECTORY__, "data")

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
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
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
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.["Count"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1; 2; 3; 4; 5 |])

[<Test>]
let ``Int64 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Big" => Series.ofValues [ 1L; Int64.MaxValue; -1L ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.["Big"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1L; Int64.MaxValue; -1L |])

[<Test>]
let ``Bool column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Flag" => Series.ofValues [ true; false; true ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<bool>("Flag") |> Series.values |> Array.ofSeq
        col2 |> should equal [| true; false; true |])

[<Test>]
let ``String column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "Alice"; "Bob"; "Carol" ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq
        col2 |> should equal [| "Alice"; "Bob"; "Carol" |])

[<Test>]
let ``DateTime column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let t1 = DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)
        let t2 = DateTime(2024, 6, 30,  0, 0, 0, DateTimeKind.Utc)
        let df = frame [ "Date" => Series.ofValues [ t1; t2 ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 : DateTime[] = df2.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
        abs((col2.[0] - t1).TotalSeconds) |> should (equalWithin 1.0) 0.0
        abs((col2.[1] - t2).TotalSeconds) |> should (equalWithin 1.0) 0.0)

[<Test>]
let ``Missing values are preserved through Arrow file`` () =
    withTmpFile (fun path ->
        let s = Series.ofValues [ 1.0; nan; 3.0; nan; 5.0 ]
        let df = frame [ "V" => s ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        df2.["V"].ValueCount |> should equal 3
        df2.RowCount          |> should equal 5)

[<Test>]
let ``Multi-column float frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                         "B" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        df2.ColumnCount |> should equal 2
        df2.RowCount    |> should equal 3
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        df2.["B"] |> Series.values |> Array.ofSeq |> should equal [| 4.0; 5.0; 6.0 |])

[<Test>]
let ``String column frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "x"; "y"; "z" ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq |> should equal [| "x"; "y"; "z" |])

[<Test>]
let ``Empty frame round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = Frame.ofColumns ([] : (string * Series<int, float>) list)
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
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
    Frame.writeArrowStream ms df
    ms.Position <- 0L
    let df2 = Frame.readArrowStream ms
    df2.ColumnCount |> should equal 2
    df2.RowCount    |> should equal 3
    df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]

// ------------------------------------------------------------------------------------------------
// In-memory conversion tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.toRecordBatch preserves column count and row count`` () =
    let df = frame [ "A" => Series.ofValues [ 1.0; 2.0 ]
                     "B" => Series.ofValues [ 3.0; 4.0 ] ]
    let batch = Frame.toRecordBatch df
    batch.ColumnCount |> should equal 2
    batch.Length      |> should equal 2

[<Test>]
let ``Frame.ofRecordBatch preserves column names`` () =
    let df = frame [ "Alpha" => Series.ofValues [ 1.0; 2.0 ]
                     "Beta"  => Series.ofValues [ 3.0; 4.0 ] ]
    let batch  = Frame.toRecordBatch df
    let df2    = Frame.ofRecordBatch batch
    df2.ColumnKeys |> List.ofSeq |> should equal [ "Alpha"; "Beta" ]

[<Test>]
let ``Frame.ofRecordBatch row keys are 0-based integers`` () =
    let df = frame [ "V" => Series.ofValues [ 10; 20; 30 ] ]
    let batch = Frame.toRecordBatch df
    let df2   = Frame.ofRecordBatch batch
    df2.RowKeys |> List.ofSeq |> should equal [ 0; 1; 2 ]

// ------------------------------------------------------------------------------------------------
// Part 2: UInt8/16/32/64 type support
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``UInt8 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Byte" => Series.ofValues [ 0uy; 127uy; 255uy ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<uint8>("Byte") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0uy; 127uy; 255uy |])

[<Test>]
let ``UInt16 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U16" => Series.ofValues [ 0us; 1000us; 65535us ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<uint16>("U16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0us; 1000us; 65535us |])

[<Test>]
let ``UInt32 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U32" => Series.ofValues [ 0u; 100000u; UInt32.MaxValue ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<uint32>("U32") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0u; 100000u; UInt32.MaxValue |])

[<Test>]
let ``UInt64 column round-trips through Arrow file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U64" => Series.ofValues [ 0UL; 9999999999UL; UInt64.MaxValue ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
        let col2 = df2.GetColumn<uint64>("U64") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0UL; 9999999999UL; UInt64.MaxValue |])

[<Test>]
let ``UInt8 missing values are preserved through Arrow file`` () =
    withTmpFile (fun path ->
        let s = Series.ofOptionalObservations [ (0, Some 10uy); (1, None); (2, Some 30uy) ]
        let df = frame [ "B" => s ]
        Frame.writeArrow path df
        let df2 = Frame.readArrow path
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

        let df = Frame.readArrow tmpPath
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

        let df = Frame.readArrow tmpPath
        let col = df.GetColumn<DateTime>("Ts") |> Series.values |> Array.ofSeq
        col.[0].Year  |> should equal 2024
        col.[0].Month |> should equal 3
        col.[0].Day   |> should equal 20
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

// ------------------------------------------------------------------------------------------------
// Part 2: Series.toArrowArray / Series.ofArrowArray
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Series.toArrowArray converts float series to DoubleArray`` () =
    let s = Series.ofValues [ 1.0; 2.5; 3.0 ]
    let arr = Series.toArrowArray s
    arr :? DoubleArray |> should equal true
    arr.Length         |> should equal 3

[<Test>]
let ``Series.toArrowArray preserves missing values`` () =
    let s = Series.ofOptionalObservations [ (0, Some 1.0); (1, None); (2, Some 3.0) ]
    let arr = Series.toArrowArray s
    arr.Length         |> should equal 3
    arr.IsNull(0)      |> should equal false
    arr.IsNull(1)      |> should equal true
    arr.IsNull(2)      |> should equal false

[<Test>]
let ``Series.toArrowArray round-trips int32 series`` () =
    let s = Series.ofValues [ 10; 20; 30 ]
    let arr = Series.toArrowArray s
    arr :? Int32Array |> should equal true
    (arr :?> Int32Array).GetValue(1).Value |> should equal 20

[<Test>]
let ``Series.ofArrowArray returns Series with 0-based int keys`` () =
    let arr = DoubleArray.Builder().Append(1.0).Append(2.0).Append(3.0).Build()
    let s = Series.ofArrowArray (arr :> IArrowArray)
    s.KeyCount   |> should equal 3
    s.Keys |> List.ofSeq |> should equal [ 0; 1; 2 ]

[<Test>]
let ``Series.ofArrowArray preserves missing values`` () =
    let arr = DoubleArray.Builder().Append(1.0).AppendNull().Append(3.0).Build()
    let s = Series.ofArrowArray (arr :> IArrowArray)
    s.ValueCount |> should equal 2
    s.KeyCount   |> should equal 3

[<Test>]
let ``Series.toArrowArray and Series.ofArrowArray round-trip floats`` () =
    let orig = Series.ofValues [ 1.0; 2.5; 4.0 ]
    let arr  = Series.toArrowArray orig
    let back = Series.ofArrowArray arr
    back.Values |> Seq.map (fun v -> v :?> float) |> Seq.toArray
    |> should equal [| 1.0; 2.5; 4.0 |]

// ------------------------------------------------------------------------------------------------
// Part 2: Feather v2 aliases
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.writeFeather and Frame.readFeather are aliases for Frame.writeArrow and readArrow`` () =
    withTmpFile (fun path ->
        let featherPath = Path.ChangeExtension(path, ".feather")
        try
            let df = frame [ "X" => Series.ofValues [ 1.0; 2.0; 3.0 ] ]
            Frame.writeFeather featherPath df
            let df2 = Frame.readFeather featherPath
            df2.RowCount    |> should equal 3
            df2.ColumnCount |> should equal 1
            df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        finally
            if File.Exists(featherPath) then File.Delete(featherPath))

// ------------------------------------------------------------------------------------------------
// Part 2: Row-key preservation
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.writeArrowWithIndex preserves string row keys`` () =
    withTmpFile (fun path ->
        let keys = [| "alice"; "bob"; "carol" |]
        let df = frame [ "Score" => Series(keys, [| 90.0; 85.0; 92.0 |]) ]
        Frame.writeArrowWithIndex path df
        let df2 = Frame.readArrowWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "alice"; "bob"; "carol" ]
        df2.ColumnCount |> should equal 1
        df2.GetColumn<float>("Score") |> Series.values |> Array.ofSeq |> should equal [| 90.0; 85.0; 92.0 |])

[<Test>]
let ``Frame.readArrowWithIndex on file without __index__ returns string int keys`` () =
    withTmpFile (fun path ->
        let df = frame [ "V" => Series.ofValues [ 1.0; 2.0 ] ]
        Frame.writeArrow path df
        let df2 = Frame.readArrowWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "0"; "1" ])

[<Test>]
let ``Frame.writeFeatherWithIndex and Frame.readFeatherWithIndex round-trip row keys`` () =
    withTmpFile (fun path ->
        let featherPath = Path.ChangeExtension(path, ".feather")
        try
            let keys = [| "x"; "y"; "z" |]
            let df = frame [ "N" => Series(keys, [| 1; 2; 3 |]) ]
            Frame.writeFeatherWithIndex featherPath df
            let df2 = Frame.readFeatherWithIndex featherPath
            df2.RowKeys |> List.ofSeq |> should equal [ "x"; "y"; "z" ]
        finally
            if File.Exists(featherPath) then File.Delete(featherPath))

// ------------------------------------------------------------------------------------------------
// Part 3: Frame module API tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.toRecordBatch and Frame.ofRecordBatch round-trip floats`` () =
    let df = frame [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                     "B" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
    let batch = Frame.toRecordBatch df
    let df2   = Frame.ofRecordBatch batch
    df2.ColumnCount |> should equal 2
    df2.RowCount    |> should equal 3
    df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
    df2.["B"] |> Series.values |> Array.ofSeq |> should equal [| 4.0; 5.0; 6.0 |]

[<Test>]
let ``Frame.readArrow and Frame.writeArrow round-trip float frame`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df = frame [ "X" => Series.ofValues [ 10.0; 20.0; 30.0 ] ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.RowCount    |> should equal 3
        df2.ColumnCount |> should equal 1
        df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 10.0; 20.0; 30.0 |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Frame.readArrowStream and Frame.writeArrowStream round-trip`` () =
    let df = frame [ "P" => Series.ofValues [ 1.0; 2.0 ]
                     "Q" => Series.ofValues [ 3.0; 4.0 ] ]
    use ms = new MemoryStream()
    Frame.writeArrowStream ms df
    ms.Position <- 0L
    let df2 = Frame.readArrowStream ms
    df2.ColumnCount |> should equal 2
    df2.RowCount    |> should equal 2
    df2.["P"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0 |]

[<Test>]
let ``Frame.readFeather and Frame.writeFeather are Feather v2 aliases`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".feather")
    try
        let df = frame [ "Val" => Series.ofValues [ 99.0; 88.0 ] ]
        Frame.writeFeather tmpPath df
        let df2 = Frame.readFeather tmpPath
        df2.RowCount    |> should equal 2
        df2.ColumnCount |> should equal 1
        df2.["Val"] |> Series.values |> Array.ofSeq |> should equal [| 99.0; 88.0 |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Frame.writeArrowWithIndex and Frame.readArrowWithIndex preserve string row keys`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let keys = [| "r1"; "r2"; "r3" |]
        let df = frame [ "N" => Series(keys, [| 1.0; 2.0; 3.0 |]) ]
        Frame.writeArrowWithIndex tmpPath df
        let df2 = Frame.readArrowWithIndex tmpPath
        df2.RowKeys |> List.ofSeq |> should equal [ "r1"; "r2"; "r3" ]
        df2.ColumnCount |> should equal 1
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

// ------------------------------------------------------------------------------------------------
// Part 3: Edge case tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Single-row frame round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df =
            frame [ "A" => (Series.ofValues [ 42.0 ]    :> ISeries<int>)
                    "B" => (Series.ofValues [ "hello" ]  :> ISeries<int>) ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.RowCount    |> should equal 1
        df2.ColumnCount |> should equal 2
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 42.0 |]
        df2.GetColumn<string>("B") |> Series.values |> Array.ofSeq |> should equal [| "hello" |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Single-column frame round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df = frame [ "Only" => Series.ofValues [ 1; 2; 3; 4; 5 ] ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.ColumnCount |> should equal 1
        df2.RowCount    |> should equal 5
        df2.GetColumn<int>("Only") |> Series.values |> Array.ofSeq |> should equal [| 1; 2; 3; 4; 5 |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``All-missing column round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let s = Series.ofOptionalObservations [ (0, None); (1, None); (2, None) ] : Series<int, float>
        let df = frame [ "AllMissing" => s ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.RowCount    |> should equal 3
        df2.["AllMissing"].ValueCount |> should equal 0
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Mixed-type frame round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df =
            frame [ "FloatCol" => (Series.ofValues [ 1.0; 2.0; 3.0 ] :> ISeries<int>)
                    "IntCol"   => (Series.ofValues [ 10; 20; 30 ]     :> ISeries<int>)
                    "StrCol"   => (Series.ofValues [ "a"; "b"; "c" ]  :> ISeries<int>) ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.ColumnCount |> should equal 3
        df2.RowCount    |> should equal 3
        df2.["FloatCol"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        df2.GetColumn<int>("IntCol") |> Series.values |> Array.ofSeq |> should equal [| 10; 20; 30 |]
        df2.GetColumn<string>("StrCol") |> Series.values |> Array.ofSeq |> should equal [| "a"; "b"; "c" |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``frame with int64 column round-trips through Arrow stream`` () =
    let df = frame [ "Big" => Series.ofValues [ Int64.MinValue; 0L; Int64.MaxValue ] ]
    use ms = new MemoryStream()
    Frame.writeArrowStream ms df
    ms.Position <- 0L
    let df2 = Frame.readArrowStream ms
    df2.GetColumn<int64>("Big") |> Series.values |> Array.ofSeq
    |> should equal [| Int64.MinValue; 0L; Int64.MaxValue |]

[<Test>]
let ``frame with float32 column round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df = frame [ "F32" => Series.ofValues [ 1.5f; 2.5f; 3.5f ] ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.GetColumn<float32>("F32") |> Series.values |> Array.ofSeq
        |> should equal [| 1.5f; 2.5f; 3.5f |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

[<Test>]
let ``Empty string column round-trips through Arrow file`` () =
    let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
    try
        let df = frame [ "S" => Series.ofValues [ ""; "hello"; "" ] ]
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.GetColumn<string>("S") |> Series.values |> Array.ofSeq
        |> should equal [| ""; "hello"; "" |]
    finally
        if File.Exists(tmpPath) then File.Delete(tmpPath)

// ------------------------------------------------------------------------------------------------
// Part 3: FsCheck property-based tests
// ------------------------------------------------------------------------------------------------

// Non-special float: finite, not NaN
let private isFinite (f: float) = not (Double.IsNaN f) && not (Double.IsInfinity f)

[<Test>]
let ``Property: float frame round-trips through Arrow file (FsCheck)`` () =
    let check (values: float list) =
        let vs = values |> List.filter isFinite
        if vs.IsEmpty then true
        else
            let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
            try
                let df = frame [ "V" => Series.ofValues vs ]
                Frame.writeArrow tmpPath df
                let df2 = Frame.readArrow tmpPath
                let out = df2.["V"] |> Series.values |> Array.ofSeq
                out = Array.ofList vs
            finally
                if File.Exists(tmpPath) then File.Delete(tmpPath)
    Check.QuickThrowOnFailure check

[<Test>]
let ``Property: int32 frame round-trips through Arrow file (FsCheck)`` () =
    let check (values: int list) =
        if values.IsEmpty then true
        else
            let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
            try
                let df = frame [ "I" => Series.ofValues values ]
                Frame.writeArrow tmpPath df
                let df2 = Frame.readArrow tmpPath
                let out = df2.GetColumn<int>("I") |> Series.values |> Array.ofSeq
                out = Array.ofList values
            finally
                if File.Exists(tmpPath) then File.Delete(tmpPath)
    Check.QuickThrowOnFailure check

[<Test>]
let ``Property: string frame round-trips through Arrow file (FsCheck)`` () =
    let check (values: NonNull<string> list) =
        let vs = values |> List.map (fun (NonNull s) -> s)
        if vs.IsEmpty then true
        else
            let tmpPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".arrow")
            try
                let df = frame [ "S" => Series.ofValues vs ]
                Frame.writeArrow tmpPath df
                let df2 = Frame.readArrow tmpPath
                let out = df2.GetColumn<string>("S") |> Series.values |> Array.ofSeq
                out = Array.ofList vs
            finally
                if File.Exists(tmpPath) then File.Delete(tmpPath)
    Check.QuickThrowOnFailure check

[<Test>]
let ``Property: float series round-trips through Arrow array (FsCheck)`` () =
    let check (values: float list) =
        let vs = values |> List.filter isFinite
        if vs.IsEmpty then true
        else
            let orig = Series.ofValues vs
            let arr  = Series.toArrowArray orig
            let back = Series.ofArrowArray arr
            let backVals = back.Values |> Seq.map (fun v -> v :?> float) |> Seq.toList
            backVals = vs
    Check.QuickThrowOnFailure check

[<Test>]
let ``Property: recordBatch round-trip preserves column names (FsCheck)`` () =
    let check (colNames: NonNull<string> list) =
        let names = colNames |> List.map (fun (NonNull s) -> s) |> List.distinct
        if names.IsEmpty then true
        else
            let cols =
                names |> List.map (fun n -> n, Series.ofValues [ 1.0; 2.0 ] :> ISeries<int>)
            let df    = Frame.ofColumns cols
            let batch = Frame.toRecordBatch df
            let df2   = Frame.ofRecordBatch batch
            let outNames = df2.ColumnKeys |> List.ofSeq
            outNames = names
    Check.QuickThrowOnFailure check

// ------------------------------------------------------------------------------------------------
// Sample-file tests — reading checked-in .arrow files
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Read stocks.arrow returns correct columns and row count`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    df.RowCount    |> should equal 5
    df.ColumnCount |> should equal 5
    df.ColumnKeys |> Seq.toList |> should equal ["Ticker"; "Open"; "Close"; "Volume"; "Date"]

[<Test>]
let ``Read stocks.arrow returns correct string column values`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    let tickers = df.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq
    tickers |> should equal [| "MSFT"; "AAPL"; "GOOG"; "AMZN"; "META" |]

[<Test>]
let ``Read stocks.arrow returns correct float column values`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    let opens = df.["Open"] |> Series.values |> Array.ofSeq
    opens.[0] |> should (equalWithin 1e-10) 420.5
    opens.[1] |> should (equalWithin 1e-10) 185.3

[<Test>]
let ``Read stocks.arrow returns correct int column values`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    let vols = df.GetColumn<int>("Volume") |> Series.values |> Array.ofSeq
    vols.[0] |> should equal 28000000
    vols.[4] |> should equal 32000000

[<Test>]
let ``Read stocks.arrow returns DateTime column`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    let dates = df.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
    dates.[0].Year  |> should equal 2024
    dates.[0].Month |> should equal 6
    dates.[0].Day   |> should equal 3

[<Test>]
let ``Read missing.arrow has correct missing value count`` () =
    let path = Path.Combine(dataDir, "missing.arrow")
    let df = Frame.readArrow path
    df.RowCount |> should equal 4
    df.["A"].ValueCount |> should equal 2  // 2 NaN → missing
    df.GetColumn<int>("B") |> Series.values |> Array.ofSeq |> should equal [| 10; 20; 30; 40 |]
    df.GetColumn<string>("C") |> Series.values |> Array.ofSeq |> should equal [| "x"; "y"; "z"; "w" |]

[<Test>]
let ``Read indexed.arrow with Frame.readArrowWithIndex restores row keys`` () =
    let path = Path.Combine(dataDir, "indexed.arrow")
    let df = Frame.readArrowWithIndex path
    df.RowKeys |> List.ofSeq |> should equal [ "Jan"; "Feb"; "Mar"; "Apr" ]
    df.ColumnCount |> should equal 3
    df.ColumnKeys |> Seq.toList |> should equal ["Revenue"; "Cost"; "Profit"]
    df.["Revenue"] |> Series.values |> Array.ofSeq |> should equal [| 1200.0; 1350.0; 1100.0; 1500.0 |]

[<Test>]
let ``stocks.arrow round-trips through write and re-read`` () =
    let path = Path.Combine(dataDir, "stocks.arrow")
    let df = Frame.readArrow path
    withTmpFile (fun tmpPath ->
        Frame.writeArrow tmpPath df
        let df2 = Frame.readArrow tmpPath
        df2.RowCount    |> should equal df.RowCount
        df2.ColumnCount |> should equal df.ColumnCount
        df2.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq
        |> should equal (df.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq))
