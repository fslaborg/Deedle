module Deedle.ExcelWriter.Tests

open System
open System.IO
open NUnit.Framework
open FsUnit
open Deedle
open Deedle.ExcelWriter

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tempFile () =
    Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".xlsx")

let cleanup (path: string) =
    if File.Exists(path) then File.Delete(path)

// ---------------------------------------------------------------------------
// Frame.writeExcel – basic round-trip via ExcelDataReader
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.writeExcel creates a file`` () =
    let path = tempFile ()
    try
        let df = frame [ "A" => series [0 => 1.0; 1 => 2.0; 2 => 3.0] ]
        Frame.writeExcel path df
        File.Exists(path) |> should equal true
    finally
        cleanup path

[<Test>]
let ``Frame.writeExcel round-trips numeric data`` () =
    let path = tempFile ()
    try
        let df = frame [ "X" => series [0 => 10.0; 1 => 20.0; 2 => 30.0]
                         "Y" => series [0 => 1.5;  1 => 2.5;  2 => 3.5 ] ]
        Frame.writeExcel path df
        let df2 = Deedle.ExcelReader.Frame.readExcel path
        df2.RowCount |> should equal 3
        df2.ColumnKeys |> Seq.toList |> should equal ["X"; "Y"]
        let xs = df2.GetColumn<float>("X") |> Series.values |> Seq.toList
        xs |> should equal [10.0; 20.0; 30.0]
    finally
        cleanup path

[<Test>]
let ``Frame.writeExcel overwrites existing file`` () =
    let path = tempFile ()
    try
        let df1 = frame [ "A" => series [0 => 1.0] ]
        let df2 = frame [ "B" => series [0 => 9.0; 1 => 8.0] ]
        Frame.writeExcel path df1
        Frame.writeExcel path df2
        let result = Deedle.ExcelReader.Frame.readExcel path
        result.RowCount |> should equal 2
        result.ColumnKeys |> Seq.toList |> should equal ["B"]
    finally
        cleanup path

// ---------------------------------------------------------------------------
// Frame.writeExcelSheet – named sheet
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.writeExcelSheet writes to named sheet`` () =
    let path = tempFile ()
    try
        let df = frame [ "Name"  => series [0 => "Alice"; 1 => "Bob"]
                         "Score" => series [0 => "A+"; 1 => "B"] ]
        Frame.writeExcelSheet path "Results" df
        let result = Deedle.ExcelReader.Frame.readExcelSheet path "Results"
        result.RowCount |> should equal 2
        result.ColumnKeys |> Seq.toList |> should equal ["Name"; "Score"]
    finally
        cleanup path

[<Test>]
let ``Frame.writeExcelSheet numeric data round-trips`` () =
    let path = tempFile ()
    try
        let df = frame [ "Score" => series [0 => 95.0; 1 => 87.0] ]
        Frame.writeExcelSheet path "Data" df
        let result = Deedle.ExcelReader.Frame.readExcelSheet path "Data"
        result.RowCount |> should equal 2
    finally
        cleanup path

// ---------------------------------------------------------------------------
// Frame.writeExcelSheets – multiple sheets
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.writeExcelSheets writes multiple sheets`` () =
    let path = tempFile ()
    try
        let df1 = frame [ "A" => series [0 => 1.0; 1 => 2.0] ]
        let df2 = frame [ "B" => series [0 => 3.0; 1 => 4.0; 2 => 5.0] ]
        Frame.writeExcelSheets path (seq { yield ("Sheet1", df1); yield ("Sheet2", df2) })
        let s1 = Deedle.ExcelReader.Frame.readExcelSheet path "Sheet1"
        let s2 = Deedle.ExcelReader.Frame.readExcelSheet path "Sheet2"
        s1.RowCount |> should equal 2
        s2.RowCount |> should equal 3
    finally
        cleanup path

// ---------------------------------------------------------------------------
// Frame.writeExcelStream – stream output
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.writeExcelStream writes to a MemoryStream`` () =
    let df = frame [ "X" => series [0 => 1.0; 1 => 2.0] ]
    use ms = new MemoryStream()
    Frame.writeExcelStream ms df
    ms.Length |> should be (greaterThan 0L)

// ---------------------------------------------------------------------------
// ExcelFrameWriter C# API
// ---------------------------------------------------------------------------

[<Test>]
let ``ExcelFrameWriter.WriteExcel creates a file`` () =
    let path = tempFile ()
    try
        let df = frame [ "Col" => series [0 => 42.0] ]
        ExcelFrameWriter.WriteExcel(path, df)
        File.Exists(path) |> should equal true
    finally
        cleanup path

[<Test>]
let ``ExcelFrameWriter.WriteExcelSheet writes named sheet`` () =
    let path = tempFile ()
    try
        let df = frame [ "V" => series [0 => 7.0; 1 => 8.0] ]
        ExcelFrameWriter.WriteExcelSheet(path, "MySheet", df)
        let result = Deedle.ExcelReader.Frame.readExcelSheet path "MySheet"
        result.RowCount |> should equal 2
    finally
        cleanup path

// ---------------------------------------------------------------------------
// Empty frame
// ---------------------------------------------------------------------------

[<Test>]
let ``Frame.writeExcel handles empty frame without throwing`` () =
    let path = tempFile ()
    try
        let df : Frame<int, string> = Frame.ofColumns (seq [])
        (fun () -> Frame.writeExcel path df |> ignore) |> should not' (throw typeof<Exception>)
    finally
        cleanup path

