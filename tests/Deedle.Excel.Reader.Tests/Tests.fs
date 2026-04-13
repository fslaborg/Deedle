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
// Frame.readExcel – first sheet
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.readExcel returns a frame with correct row count`` () =
    let df = Frame.readExcel testFile
    df.RowCount |> should equal 3

[<Test>]
let ``Frame.readExcel returns a frame with correct column names`` () =
    let df = Frame.readExcel testFile
    df.ColumnKeys |> Seq.toList |> should equal ["Name"; "Age"; "Score"]

[<Test>]
let ``Frame.readExcel reads string values correctly`` () =
    let df = Frame.readExcel testFile
    let strs = df.GetColumn<obj>("Name") |> Series.values |> Seq.map string |> Seq.toList
    strs |> should equal ["Alice"; "Bob"; "Charlie"]

[<Test>]
let ``Frame.readExcel reads numeric values correctly`` () =
    let df = Frame.readExcel testFile
    let ages = df.GetColumn<double>("Age") |> Series.values |> Seq.toList
    ages |> should equal [30.0; 25.0; 35.0]

[<Test>]
let ``Frame.readExcel reads floating-point values correctly`` () =
    let df = Frame.readExcel testFile
    let scores = df.GetColumn<double>("Score") |> Series.values |> Seq.toList
    scores |> should equal [9.5; 8.2; 7.1]

// ---------------------------------------------------------------------------
// Frame.sheetNames
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.sheetNames returns all sheet names`` () =
    Frame.sheetNames testFile |> should equal ["Sheet1"; "Prices"]

// ---------------------------------------------------------------------------
// Frame.readExcelSheet – by name
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.readExcelSheet reads a named sheet`` () =
    let df = Frame.readExcelSheet testFile "Prices"
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``Frame.readExcelSheet reads first sheet by name`` () =
    let df = Frame.readExcelSheet testFile "Sheet1"
    df.RowCount |> should equal 3

[<Test>]
let ``Frame.readExcelSheet throws for unknown sheet name`` () =
    (fun () -> Frame.readExcelSheet testFile "DoesNotExist" |> ignore)
    |> should throw typeof<Exception>

// ---------------------------------------------------------------------------
// Frame.readExcelSheetByIndex
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.readExcelSheetByIndex reads first sheet at index 0`` () =
    let df = Frame.readExcelSheetByIndex testFile 0
    df.RowCount |> should equal 3

[<Test>]
let ``Frame.readExcelSheetByIndex reads second sheet at index 1`` () =
    let df = Frame.readExcelSheetByIndex testFile 1
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``Frame.readExcelSheetByIndex throws for out-of-range index`` () =
    (fun () -> Frame.readExcelSheetByIndex testFile 99 |> ignore)
    |> should throw typeof<Exception>

// ---------------------------------------------------------------------------
// Missing values
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.readExcel handles missing values as missing entries`` () =
    let df = Frame.readExcel missingFile
    df.RowCount |> should equal 3
    // Row 0: A=1, B=missing, C=3
    let a = df.["A"] |> Series.tryGet 0
    let b = df.["B"] |> Series.tryGet 0
    a.IsSome |> should equal true
    b.IsNone |> should equal true

// ---------------------------------------------------------------------------
// Frame module API
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.readExcel reads first sheet`` () =
    let df = Frame.readExcel testFile
    df.RowCount |> should equal 3
    df.ColumnKeys |> Seq.toList |> should equal ["Name"; "Age"; "Score"]

[<Test>]
let ``Frame.readExcelSheet reads named sheet`` () =
    let df = Frame.readExcelSheet testFile "Prices"
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``Frame.readExcelSheetByIndex reads sheet at index`` () =
    let df = Frame.readExcelSheetByIndex testFile 1
    df.RowCount |> should equal 2

// ---------------------------------------------------------------------------
// ExcelFrame C# API
// ---------------------------------------------------------------------------

[<Test>]
let ``ExcelFrame.ReadExcel reads first sheet`` () =
    let df = ExcelFrame.ReadExcel(testFile)
    df.RowCount |> should equal 3
    df.ColumnKeys |> Seq.toList |> should equal ["Name"; "Age"; "Score"]

[<Test>]
let ``ExcelFrame.ReadExcelSheet reads named sheet`` () =
    let df = ExcelFrame.ReadExcelSheet(testFile, "Prices")
    df.RowCount |> should equal 2
    df.ColumnKeys |> Seq.toList |> should equal ["Item"; "Price"; "Quantity"]

[<Test>]
let ``ExcelFrame.ReadExcelSheetByIndex reads sheet at index`` () =
    let df = ExcelFrame.ReadExcelSheetByIndex(testFile, 0)
    df.RowCount |> should equal 3

[<Test>]
let ``ExcelFrame.SheetNames returns all sheet names`` () =
    ExcelFrame.SheetNames(testFile) |> should equal ["Sheet1"; "Prices"]
