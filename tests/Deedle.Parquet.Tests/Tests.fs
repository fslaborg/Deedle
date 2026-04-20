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

let private dataDir = Path.Combine(__SOURCE_DIRECTORY__, "data")

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
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
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
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.["Count"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1; 2; 3; 4; 5 |])

[<Test>]
let ``Int64 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Big" => Series.ofValues [ 1L; Int64.MaxValue; -1L ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.["Big"] |> Series.values |> Array.ofSeq
        col2 |> should equal [| 1L; Int64.MaxValue; -1L |])

[<Test>]
let ``Bool column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Flag" => Series.ofValues [ true; false; true ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<bool>("Flag") |> Series.values |> Array.ofSeq
        col2 |> should equal [| true; false; true |])

[<Test>]
let ``String column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Name" => Series.ofValues [ "Alice"; "Bob"; "Carol" ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq
        col2 |> should equal [| "Alice"; "Bob"; "Carol" |])

[<Test>]
let ``DateTime column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let t1 = DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)
        let t2 = DateTime(2024, 6, 30,  0, 0, 0, DateTimeKind.Utc)
        let df = frame [ "Date" => Series.ofValues [ t1; t2 ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 : DateTime[] = df2.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
        abs((col2.[0] - t1).TotalSeconds) |> should (equalWithin 1.0) 0.0
        abs((col2.[1] - t2).TotalSeconds) |> should (equalWithin 1.0) 0.0)

[<Test>]
let ``Missing values are preserved through Parquet file`` () =
    withTmpFile (fun path ->
        let s = Series.ofValues [ 1.0; nan; 3.0; nan; 5.0 ]
        let df = frame [ "V" => s ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        df2.["V"].ValueCount |> should equal 3
        df2.RowCount          |> should equal 5)

[<Test>]
let ``Multi-column float frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ]
                         "B" => Series.ofValues [ 4.0; 5.0; 6.0 ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        df2.ColumnCount |> should equal 2
        df2.RowCount    |> should equal 3
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |]
        df2.["B"] |> Series.values |> Array.ofSeq |> should equal [| 4.0; 5.0; 6.0 |])

[<Test>]
let ``Empty frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = Frame.ofColumns ([] : (string * Series<int, float>) list)
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
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
    Frame.writeParquetStream ms df
    ms.Position <- 0L
    let df2 = Frame.readParquetStream ms
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
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<byte>("Byte") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0uy; 127uy; 255uy |])

[<Test>]
let ``UInt16 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U16" => Series.ofValues [ 0us; 1000us; 65535us ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<uint16>("U16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0us; 1000us; 65535us |])

[<Test>]
let ``UInt32 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U32" => Series.ofValues [ 0u; 100000u; UInt32.MaxValue ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<uint32>("U32") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0u; 100000u; UInt32.MaxValue |])

[<Test>]
let ``UInt64 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "U64" => Series.ofValues [ 0UL; 123456789UL; UInt64.MaxValue ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<uint64>("U64") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0UL; 123456789UL; UInt64.MaxValue |])

// ------------------------------------------------------------------------------------------------
// Row-key preservation tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Row keys are preserved through Frame.writeParquetWithIndex / readParquetWithIndex`` () =
    withTmpFile (fun path ->
        let df =
            [ "A" => Series.ofValues [ 1.0; 2.0; 3.0 ] ]
            |> frame
            |> Frame.indexRowsWith [| "x"; "y"; "z" |]
        Frame.writeParquetWithIndex path df
        let df2 = Frame.readParquetWithIndex path
        df2.RowKeys |> List.ofSeq |> should equal [ "x"; "y"; "z" ]
        df2.["A"] |> Series.values |> Array.ofSeq |> should equal [| 1.0; 2.0; 3.0 |])

[<Test>]
let ``Frame.readParquetWithIndex falls back to integer keys when no __index__ column`` () =
    withTmpFile (fun path ->
        let df = frame [ "V" => Series.ofValues [ 10.0; 20.0 ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquetWithIndex path
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
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        df2.RowCount |> should equal 1
        df2.["X"] |> Series.values |> Array.ofSeq |> should equal [| 42.0 |])

[<Test>]
let ``Single-column single-row frame round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "Only" => Series.ofValues [ "hello" ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
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
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        df2.ColumnCount |> should equal 3
        df2.GetColumn<string>("Name") |> Series.values |> Array.ofSeq |> should equal [| "Alice"; "Bob" |]
        df2.GetColumn<int>("Count")   |> Series.values |> Array.ofSeq |> should equal [| 10; 20 |]
        df2.["Score"]                 |> Series.values |> Array.ofSeq |> should equal [| 1.5; 2.5 |])

[<Test>]
let ``Float32 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "F32" => Series.ofValues [ 1.0f; 2.5f; 3.0f ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<float32>("F32") |> Series.values |> Array.ofSeq
        col2.[0] |> should (equalWithin 1e-5f) 1.0f
        col2.[1] |> should (equalWithin 1e-5f) 2.5f
        col2.[2] |> should (equalWithin 1e-5f) 3.0f)

[<Test>]
let ``Int16 column round-trips through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "S16" => Series.ofValues [ 0s; -1s; 32767s ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
        let col2 = df2.GetColumn<int16>("S16") |> Series.values |> Array.ofSeq
        col2 |> should equal [| 0s; -1s; 32767s |])

[<Test>]
let ``Empty string values round-trip through Parquet file`` () =
    withTmpFile (fun path ->
        let df = frame [ "S" => Series.ofValues [ ""; "a"; "" ] ]
        Frame.writeParquet path df
        let df2 = Frame.readParquet path
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
                Frame.writeParquet path df
                let df2 = Frame.readParquet path
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
                Frame.writeParquet path df
                let df2 = Frame.readParquet path
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
                Frame.writeParquet path df
                let df2 = Frame.readParquet path
                let col2 = df2.GetColumn<string>("S") |> Series.values |> Array.ofSeq
                col2 = strs
            finally
                if File.Exists(path) then File.Delete(path)
    Check.QuickThrowOnFailure prop

// ------------------------------------------------------------------------------------------------
// Sample-file tests — reading checked-in .parquet files
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Read stocks.parquet returns correct columns and row count`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    df.RowCount    |> should equal 5
    df.ColumnCount |> should equal 5
    df.ColumnKeys |> Seq.toList |> should equal ["Ticker"; "Open"; "Close"; "Volume"; "Date"]

[<Test>]
let ``Read stocks.parquet returns correct string column values`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    let tickers = df.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq
    tickers |> should equal [| "MSFT"; "AAPL"; "GOOG"; "AMZN"; "META" |]

[<Test>]
let ``Read stocks.parquet returns correct float column values`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    let opens = df.["Open"] |> Series.values |> Array.ofSeq
    opens.[0] |> should (equalWithin 1e-10) 420.5
    opens.[1] |> should (equalWithin 1e-10) 185.3

[<Test>]
let ``Read stocks.parquet returns correct int column values`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    let vols = df.GetColumn<int>("Volume") |> Series.values |> Array.ofSeq
    vols.[0] |> should equal 28000000
    vols.[4] |> should equal 32000000

[<Test>]
let ``Read stocks.parquet returns DateTime column`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    let dates = df.GetColumn<DateTime>("Date") |> Series.values |> Array.ofSeq
    dates.[0].Year  |> should equal 2024
    dates.[0].Month |> should equal 6
    dates.[0].Day   |> should equal 3

[<Test>]
let ``Read missing.parquet has correct missing value count`` () =
    let path = Path.Combine(dataDir, "missing.parquet")
    let df = Frame.readParquet path
    df.RowCount |> should equal 4
    df.["A"].ValueCount |> should equal 2  // 2 NaN → missing
    df.GetColumn<int>("B") |> Series.values |> Array.ofSeq |> should equal [| 10; 20; 30; 40 |]
    df.GetColumn<string>("C") |> Series.values |> Array.ofSeq |> should equal [| "x"; "y"; "z"; "w" |]

[<Test>]
let ``Read indexed.parquet with Frame.readParquetWithIndex restores row keys`` () =
    let path = Path.Combine(dataDir, "indexed.parquet")
    let df = Frame.readParquetWithIndex path
    df.RowKeys |> List.ofSeq |> should equal [ "Jan"; "Feb"; "Mar"; "Apr" ]
    df.ColumnCount |> should equal 3
    df.ColumnKeys |> Seq.toList |> should equal ["Revenue"; "Cost"; "Profit"]
    df.["Revenue"] |> Series.values |> Array.ofSeq |> should equal [| 1200.0; 1350.0; 1100.0; 1500.0 |]

[<Test>]
let ``stocks.parquet round-trips through write and re-read`` () =
    let path = Path.Combine(dataDir, "stocks.parquet")
    let df = Frame.readParquet path
    withTmpFile (fun tmpPath ->
        Frame.writeParquet tmpPath df
        let df2 = Frame.readParquet tmpPath
        df2.RowCount    |> should equal df.RowCount
        df2.ColumnCount |> should equal df.ColumnCount
        df2.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq
        |> should equal (df.GetColumn<string>("Ticker") |> Series.values |> Array.ofSeq))

// ------------------------------------------------------------------------------------------------
// Real-world sample file tests — weather / trades / sensors
// Generated by PyArrow to test cross-tool compatibility
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Read weather.parquet returns correct dimensions and types`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    df.RowCount    |> should equal 8
    df.ColumnCount |> should equal 7
    df.ColumnKeys |> Seq.toList |> should equal ["Station"; "Date"; "TempC"; "Humidity"; "WindKmh"; "Precip_mm"; "Overcast"]

[<Test>]
let ``Read weather.parquet Station column is correct`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    let stations = df.GetColumn<string>("Station") |> Series.values |> Array.ofSeq
    stations |> should equal [| "KLAX"; "KJFK"; "KORD"; "KDFW"; "KSEA"; "KMIA"; "KBOS"; "KDEN" |]

[<Test>]
let ``Read weather.parquet TempC float64 values are correct`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    let temps = df.["TempC"] |> Series.values |> Array.ofSeq
    temps.[0] |> should (equalWithin 1e-9) 15.2
    temps.[3] |> should (equalWithin 1e-9) 8.7
    temps.[7] |> should (equalWithin 1e-9) -8.2

[<Test>]
let ``Read weather.parquet Humidity int32 values are correct`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    let humidities = df.GetColumn<int>("Humidity") |> Series.values |> Array.ofSeq
    humidities |> should equal [| 72; 65; 81; 55; 88; 79; 70; 45 |]

[<Test>]
let ``Read weather.parquet Precip_mm has missing values`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    // Precip_mm has 2 nulls (rows 2 and 7)
    df.["Precip_mm"].ValueCount |> should equal 6
    df.RowCount |> should equal 8

[<Test>]
let ``Read weather.parquet Overcast bool column is correct`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    let overcast = df.GetColumn<bool>("Overcast") |> Series.values |> Array.ofSeq
    overcast |> should equal [| false; true; true; false; true; false; true; false |]

[<Test>]
let ``weather.parquet round-trips through write and re-read`` () =
    let path = Path.Combine(dataDir, "weather.parquet")
    let df = Frame.readParquet path
    withTmpFile (fun tmpPath ->
        Frame.writeParquet tmpPath df
        let df2 = Frame.readParquet tmpPath
        df2.RowCount    |> should equal df.RowCount
        df2.ColumnCount |> should equal df.ColumnCount
        df2.GetColumn<string>("Station") |> Series.values |> Array.ofSeq
        |> should equal (df.GetColumn<string>("Station") |> Series.values |> Array.ofSeq)
        df2.["Precip_mm"].ValueCount |> should equal df.["Precip_mm"].ValueCount)

[<Test>]
let ``Read trades.parquet returns correct dimensions`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    df.RowCount    |> should equal 20
    df.ColumnCount |> should equal 7
    df.ColumnKeys |> Seq.toList |> should equal ["TradeId"; "Symbol"; "Price"; "Quantity"; "Side"; "Timestamp"; "Settled"]

[<Test>]
let ``Read trades.parquet Symbol string column is correct`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    let symbols = df.GetColumn<string>("Symbol") |> Series.values |> Array.ofSeq
    symbols.[0]  |> should equal "AAPL"
    symbols.[1]  |> should equal "MSFT"
    symbols.[4]  |> should equal "TSLA"
    symbols.[19] |> should equal "TSLA"

[<Test>]
let ``Read trades.parquet Price float64 values are in range`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    let prices = df.["Price"] |> Series.values |> Array.ofSeq
    prices |> Array.forall (fun p -> p > 100.0 && p < 300.0) |> should equal true

[<Test>]
let ``Read trades.parquet Quantity int64 values are correct`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    let quantities = df.GetColumn<int64>("Quantity") |> Series.values |> Array.ofSeq
    quantities.[0] |> should equal 1000L
    quantities.[4] |> should equal 5000L

[<Test>]
let ``Read trades.parquet Side int16 values are correct`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    let sides = df.GetColumn<int16>("Side") |> Series.values |> Array.ofSeq
    sides.[0] |> should equal 1s    // even index → buy
    sides.[1] |> should equal -1s   // odd index  → sell

[<Test>]
let ``trades.parquet round-trips through write and re-read`` () =
    let path = Path.Combine(dataDir, "trades.parquet")
    let df = Frame.readParquet path
    withTmpFile (fun tmpPath ->
        Frame.writeParquet tmpPath df
        let df2 = Frame.readParquet tmpPath
        df2.RowCount    |> should equal df.RowCount
        df2.ColumnCount |> should equal df.ColumnCount
        df2.GetColumn<int64>("Quantity") |> Series.values |> Array.ofSeq
        |> should equal (df.GetColumn<int64>("Quantity") |> Series.values |> Array.ofSeq))

[<Test>]
let ``Read sensors.parquet returns correct dimensions`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.RowCount    |> should equal 10
    df.ColumnCount |> should equal 7

[<Test>]
let ``Read sensors.parquet float32 ReadingF has missing values`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.["ReadingF"].ValueCount |> should equal 8  // 2 nulls

[<Test>]
let ``Read sensors.parquet float64 ReadingD has missing values`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.["ReadingD"].ValueCount |> should equal 7  // 3 nulls

[<Test>]
let ``Read sensors.parquet int32 Count has missing values`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.GetColumn<int>("Count").ValueCount |> should equal 7  // 3 nulls

[<Test>]
let ``Read sensors.parquet int64 CountBig has missing values`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.GetColumn<int64>("CountBig").ValueCount |> should equal 7  // 3 nulls

[<Test>]
let ``Read sensors.parquet string Label has missing values`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    df.GetColumn<string>("Label").ValueCount |> should equal 8  // 2 nulls

[<Test>]
let ``sensors.parquet round-trips through write and re-read`` () =
    let path = Path.Combine(dataDir, "sensors.parquet")
    let df = Frame.readParquet path
    withTmpFile (fun tmpPath ->
        Frame.writeParquet tmpPath df
        let df2 = Frame.readParquet tmpPath
        df2.RowCount    |> should equal df.RowCount
        df2.ColumnCount |> should equal df.ColumnCount
        df2.["ReadingF"].ValueCount |> should equal df.["ReadingF"].ValueCount
        df2.["ReadingD"].ValueCount |> should equal df.["ReadingD"].ValueCount
        df2.GetColumn<int>("Count").ValueCount |> should equal (df.GetColumn<int>("Count").ValueCount))
