module Deedle.Parquet.Tests

open System
open System.IO
open NUnit.Framework
open FsUnit
open FsCheck
open Deedle
open Deedle.Parquet

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

let private tmpFile () =
    Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".parquet")

let private withTmpFile (f: string -> unit) =
    let p = tmpFile ()
    try f p
    finally if File.Exists(p) then File.Delete(p)

// ------------------------------------------------------------------------------------------------
// Round-trip tests — Parquet file format
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Float column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Value" => Series.ofValues [ 1.0; 2.5; nan; 4.0 ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.["Value"] |> Series.values |> Array.ofSeq
        col2.[0] |> should (equalWithin 1e-10) 1.0
        col2.[1] |> should (equalWithin 1e-10) 2.5
        col2.[2] |> should (equalWithin 1e-10) 4.0
        df2.RowCount |> should equal 4
        // NaN becomes missing in Deedle; the column should have 3 values
        df2.["Value"].ValueCount |> should equal 3)

[<Test>]
let ``Int32 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Count" => Series.ofValues [ 1; 2; 3; 4; 5 ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.["Count"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1; 2; 3; 4; 5 |])

[<Test>]
let ``Int64 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Big" => Series.ofValues [ 1L; Int64.MaxValue; -1L ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.["Big"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1L; Int64.MaxValue; -1L |])

[<Test>]
let ``Bool column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Flag" => Series.ofValues [ true; false; true ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<bool>("Flag") |> Series.values |> Array.ofSeq
        col2 |> should equal [| true; false; true |])

[<Test>]
let ``String column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "Alice"; "Bob"; "Carol" ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq
        col2 |> should equal [| "Alice"; "Bob"; "Carol" |])

[<Test>]
let ``DateTime column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let t1 = DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)
        let t2 = DateTime(2024, 6, 30,  0, 0, 0, DateTimeKind.Utc)
        let df = frame [ "Date" => Series.ofValues [ t1; t2 ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 : DateTime[] = df2.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
        abs((col2.[0] - t1).TotalSeconds) |> should (equalWithin 1.0) 0.0
        abs((col2.[1] - t2).TotalSeconds) |> should (equalWithin 1.0) 0.0)

[<Test>]
let ``Missing values are preserved through Parquet file`` () =
    withTmpFile (fun path ->
        let s = Series.ofValues [ 1.0; nan; 3.0; nan; 5.0 ]
        let df = frame [ "V" => s ]
        writeParquet path df
        let df2 = readParquet path
        df2.["V"].ValueCount |> should equal 3
        df2.RowCount          |> should equal 5)

[<Test>]
let ``Multi-column float frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                         "B" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
        writeParquet path df
        let df2 = readParquet path
        df2.ColumnCount |> should equal 2
        df2.RowCount    |> should equal 3
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        df2.["B"] |> Series.values |> Array.ofSeq |> should equal [| 4.0; 5.0; 6.0 |])

[<Test>]
let ``Empty frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = Frame.ofColumns ([] : (string * Series<int, float>) list)
        writeParquet path df
        let df2 = readParquet path
        df2.RowCount    |> should equal 0
        df2.ColumnCount |> should equal 0)

// ------------------------------------------------------------------------------------------------
// Round-trip tests — Parquet stream format
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Float frame round-trips through Parquet stream`` () =
    let df = frame [ "X" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                     "Y" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
    use ms = new MemoryStream()
    writeParquetStream ms df
    ms.Position <- 0L
    let df2 = readParquetStream ms
    df2.ColumnCount |> should equal 2
    df2.RowCount    |> should equal 3
    df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]

// ------------------------------------------------------------------------------------------------
// Unsigned integer type support
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``UInt8 (byte) column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Byte" => Series.ofValues [ 0uy; 127uy; 255uy ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<byte>("Byte") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0uy; 127uy; 255uy |])

[<Test>]
let ``UInt16 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U16" => Series.ofValues [ 0us; 1000us; 65535us ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<uint16>("U16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0us; 1000us; 65535us |])

[<Test>]
let ``UInt32 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U32" => Series.ofValues [ 0u; 100000u; UInt32.MaxValue ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<uint32>("U32") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0u; 100000u; UInt32.MaxValue |])

[<Test>]
let ``UInt64 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U64" => Series.ofValues [ 0UL; 123456789UL; UInt64.MaxValue ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<uint64>("U64") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0UL; 123456789UL; UInt64.MaxValue |])

// ------------------------------------------------------------------------------------------------
// Row-key preservation tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Row keys are preserved through writeParquetWithIndex / readParquetWithIndex`` () =
    withTmpFile (fun path ->
        let df =
            [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ] ]
            |> frame
            |> Frame.indexRowsWith [| "x"; "y"; "z" |]
        writeParquetWithIndex path df
        let df2 = readParquetWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "x"; "y"; "z" ]
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |])

[<Test>]
let ``readParquetWithIndex falls back to integer keys when no __index__ column`` () =
    withTmpFile (fun path ->
        let df = frame [ "V" => Series.ofValues [ 10.0; 20.0 ] ]
        writeParquet path df
        let df2 = readParquetWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "0"; "1" ]
        df2.["V"] |> Series.values |> Array.ofSeq |> should equal [| 10.0; 20.0 |])

// ------------------------------------------------------------------------------------------------
// Frame module API tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.writeParquet and Frame.readParquet work correctly`` () =
    withTmpFile (fun path ->
        let df = frame [ "X" => Series.ofValues [ 1.0; 2.0; 3.0 ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        df2.RowCount |> should equal 3
        df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |])

[<Test>]
let ``Frame.writeParquetWithIndex and Frame.readParquetWithIndex work correctly`` () =
    withTmpFile (fun path ->
        let df =
            [ "V" => Series.ofValues [ 10; 20 ] ]
            |> frame
            |> Frame.indexRowsWith [| "a"; "b" |]
        Frame.writeParquetWithIndex path df
        let df2 = Frame.readParquetWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "a"; "b" ])

// ------------------------------------------------------------------------------------------------
// Edge-case tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Single-row frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "X" => Series.ofValues [ 42.0 ] ]
        writeParquet path df
        let df2 = readParquet path
        df2.RowCount |> should equal 1
        df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 42.0 |])

[<Test>]
let ``Single-column single-row frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Only" => Series.ofValues [ "hello" ] ]
        writeParquet path df
        let df2 = readParquet path
        df2.RowCount    |> should equal 1
        df2.ColumnCount |> should equal 1
        df2.GetColumn<string>("Only") |> Series.values |> Array.ofSeq |> should equal [| "hello" |])

[<Test>]
let ``Mixed-type frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df =
            Frame.ofColumns [
                "Name"  => (Series.ofValues [ "Alice"; "Bob" ] :> ISeries<_>)
                "Count" => (Series.ofValues [ 10; 20 ]         :> ISeries<_>)
                "Score" => (Series.ofValues [ 1.5; 2.5 ]       :> ISeries<_>) ]
        writeParquet path df
        let df2 = readParquet path
        df2.ColumnCount |> should equal 3
        df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq |> should equal [| "Alice"; "Bob" |]
        df2.GetColumn<int>("Count")   |> Series.values |> Array.ofSeq |> should equal [| 10; 20 |]
        df2.["Score"]                 |> Series.values |> Array.ofSeq |> should equal [| 1.5; 2.5 |])

[<Test>]
let ``Float32 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "F32" => Series.ofValues [ 1.0f; 2.5f; 3.0f ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<float32>("F32") |> Series.values |> Array.ofSeq
        col2.[0] |> should (equalWithin 1e-5f) 1.0f
        col2.[1] |> should (equalWithin 1e-5f) 2.5f
        col2.[2] |> should (equalWithin 1e-5f) 3.0f)

[<Test>]
let ``Int16 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "S16" => Series.ofValues [ 0s; -1s; 32767s ] ]
        writeParquet path df
        let df2 = readParquet path
        let col2 = df2.GetColumn<int16>("S16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0s; -1s; 32767s |])

[<Test>]
let ``Empty string values round-trip through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "S" => Series.ofValues [ ""; "a"; "" ] ]
        writeParquet path df
        let df2 = readParquet path
        df2.GetColumn<string>("S") |> Series.values |> Array.ofSeq |> should equal [| ""; "a"; "" |])

// ------------------------------------------------------------------------------------------------
// FsCheck property-based tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``FsCheck: float array round-trips through Parquet`` () =
    let prop (values: float[]) =
        if values.Length = 0 then true
        else
            let path = tmpFile ()
            try
                let df = frame [ "V" => Series.ofValues values ]
                writeParquet path df
                let df2 = readParquet path
                df2.RowCount = values.Length
            finally
                if File.Exists(path) then File.Delete(path)
    Check.QuickThrowOnFailure prop

[<Test>]
let ``FsCheck: int array round-trips through Parquet`` () =
    let prop (values: int[]) =
        if values.Length = 0 then true
        else
            let path = tmpFile ()
            try
                let df = frame [ "V" => Series.ofValues values ]
                writeParquet path df
                let df2 = readParquet path
                let col2 = df2.GetColumn<int>("V") |> Series.values |> Array.ofSeq
                col2 = values
            finally
                if File.Exists(path) then File.Delete(path)
    Check.QuickThrowOnFailure prop

[<Test>]
let ``FsCheck: string array round-trips through Parquet`` () =
    let prop (values: NonNull<string>[]) =
        if values.Length = 0 then true
        else
            let strs = values |> Array.map (fun (NonNull s) -> s)
            let path = tmpFile ()
            try
                let df = frame [ "S" => Series.ofValues strs ]
                writeParquet path df
                let df2 = readParquet path
                let col2 = df2.GetColumn<string>("S") |> Series.values |> Array.ofSeq
                col2 = strs
            finally
                if File.Exists(path) then File.Delete(path)
    Check.QuickThrowOnFailure prop
