module Deedle.ExcelReader.Tests

open System
open System.IO
open NUnit.Framework
open FsUnit
open Deedle
open Deedle.ExcelReader

let dataDir = Path.Combine(__SOURCE_DIRECTORY__, "data")
let testFile = Path.Combine(dataDir, "test.xlsx")
let missingFile = Path.Combine(dataDir, "missing.xlsx")

// ---------------------------------------------------------------------------
// readExcel – first sheet
// ---------------------------------------------------------------------------

[<Test>]
let ``readExcel returns a frame with correct row count`` () =
    let df = readExcel testFile
    df.RowCount |> should equal 3

[<Test>]
let ``readExcel returns a frame with correct column names`` () =
    let df = readExcel testFile
    df.ColumnKeys |> Seq.toList |> should equal ["Name"; "Age"; "Score"]

[<Test>]
let ``readExcel reads string values correctly`` () =
    let df = readExcel testFile
    let strs = df.GetColumn<obj>("Name") |> Series.values |> Seq.map string |> Seq.toList
    strs |> should equal ["Alice"; "Bob"; "Charlie"]

[<Test>]
let ``readExcel reads numeric values correctly`` () =
    let df = readExcel testFile
    let ages = df.GetColumn<double>("Age") |> Series.values |> Seq.toList
    ages |> should equal [30.0; 25.0; 35.0]

[<Test>]
let ``readExcel reads floating-point values correctly`` () =
    let df = readExcel testFile
    let scores = df.GetColumn<double>("Score") |> Series.values |> Seq.toList
    scores |> should equal [9.5; 8.2; 7.1]

// ---------------------------------------------------------------------------
// sheetNames
// ---------------------------------------------------------------------------

[<Test>]
let ``sheetNames returns all sheet names`` () =
    sheetNames testFile |> should equal ["Sheet1"; "Prices"]

// ---------------------------------------------------------------------------
// readExcelSheet – by name
// ---------------------------------------------------------------------------

[<Test>]
let ``readExcelSheet reads a named sheet`` () =
    let df = readExcelSheet testFile "Prices"
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``readExcelSheet reads first sheet by name`` () =
    let df = readExcelSheet testFile "Sheet1"
    df.RowCount |> should equal 3

[<Test>]
let ``readExcelSheet throws for unknown sheet name`` () =
    (fun () -> readExcelSheet testFile "DoesNotExist" |> ignore)
    |> should throw typeof<Exception>

// ---------------------------------------------------------------------------
// readExcelSheetByIndex
// ---------------------------------------------------------------------------

[<Test>]
let ``readExcelSheetByIndex reads first sheet at index 0`` () =
    let df = readExcelSheetByIndex testFile 0
    df.RowCount |> should equal 3

[<Test>]
let ``readExcelSheetByIndex reads second sheet at index 1`` () =
    let df = readExcelSheetByIndex testFile 1
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``readExcelSheetByIndex throws for out-of-range index`` () =
    (fun () -> readExcelSheetByIndex testFile 99 |> ignore)
    |> should throw typeof<Exception>

// ---------------------------------------------------------------------------
// Missing values
// ---------------------------------------------------------------------------

[<Test>]
let ``readExcel handles missing values as missing entries`` () =
    let df = readExcel missingFile
    df.RowCount |> should equal 3
    // Row 0: A=1, B=missing, C=3
    let a = df.["A"] |> Series.tryGet 0
    let b = df.["B"] |> Series.tryGet 0
    a.IsSome |> should equal true
    b.IsNone |> should equal true
