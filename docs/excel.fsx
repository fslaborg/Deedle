(**
---
title: Deedle.Excel.Reader — Excel integration
category: Integrations
categoryindex: 2
index: 1
description: Reading .xls and .xlsx Excel files into Deedle data frames using the ExcelDataReader library
keywords: excel, xls, xlsx, spreadsheet, worksheet, ExcelDataReader
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Excel.Reader.dll"
#r "nuget: ExcelDataReader, 3.8"
#r "nuget: ExcelDataReader.DataSet, 3.8"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#r "nuget: Deedle.Excel.Reader,{{fsdocs-package-version}}"
#r "nuget: ExcelDataReader"
#r "nuget: ExcelDataReader.DataSet"
#endif // FSX
(*** condition: prepare ***)

open System
open System.IO
open Deedle
open Deedle.ExcelReader

(**

# Deedle.Excel.Reader — Excel integration

Deedle provides two separate packages for working with Microsoft Excel files:

| Package | What it does |
|---|---|
| **`Deedle.Excel.Reader`** | Cross-platform reading of `.xlsx` and `.xls` files via [ExcelDataReader](https://github.com/ExcelDataReader/ExcelDataReader) |
| **`Deedle.Excel`** | Live read/write to an open Excel workbook via [NetOffice](https://netoffice.io/) (Windows + Excel required) |

This page covers the cross-platform `Deedle.Excel.Reader`.

---

<a name="setup"></a>

## Package setup

Install with:

```
dotnet add package Deedle.Excel.Reader
```

Then open the namespace:

```fsharp
open Deedle
open Deedle.ExcelReader
```

---

<a name="reading"></a>

## Reading Excel files

### Reading the first worksheet

The simplest function reads the first worksheet of an Excel file and returns a
`Frame<int, string>` whose row keys are zero-based integers:

*)
// Create a sample workbook to demonstrate reading
let samplePath =
    let tmp = Path.GetTempFileName() + ".xlsx"
    // Build a minimal in-memory frame and save via a small helper
    let bytes =
        // Minimal valid OOXML (xlsx) with one sheet containing a 4-column table
        // Generated from the docs/data/sales.xlsx sample file
        File.ReadAllBytes(__SOURCE_DIRECTORY__ + "/data/sales.xlsx")
    File.WriteAllBytes(tmp, bytes)
    tmp

let sales : Frame<int, string> = Frame.readExcel samplePath
(*** include-it ***)

(**
The first row of the worksheet is used as column headers. Subsequent rows become
frame rows; the row keys are contiguous integers starting at 0.

### Reading a specific sheet by name

When a workbook contains multiple worksheets, use `Frame.readExcelSheet` to target one
by name (case-sensitive):
*)
let multiPath =
    File.ReadAllBytes(__SOURCE_DIRECTORY__ + "/data/quarterly.xlsx")
    |> fun b -> let t = Path.GetTempFileName() + ".xlsx" in File.WriteAllBytes(t, b); t

let q1 : Frame<int, string> = Frame.readExcelSheet multiPath "Q1 Sales"
(*** include-it ***)

(**
### Reading a specific sheet by index

Use `Frame.readExcelSheetByIndex` with a zero-based index when you know the position
but not the name:
*)
let q2 : Frame<int, string> = Frame.readExcelSheetByIndex multiPath 1
(*** include-it ***)

(**
---

<a name="sheet-names"></a>

## Listing worksheets

`Frame.sheetNames` returns the names of all worksheets in a workbook in order:
*)
let sheets : string list = Frame.sheetNames multiPath
(*** include-it ***)

(**
---

<a name="columns"></a>

## Working with the resulting frame

The frame returned by all `read*` functions has `string` column keys (taken from
the header row) and `int` row keys. You can use all standard Deedle operations
on it.

### Accessing columns

Use `frame.GetColumn<'T>` to retrieve a typed series:
*)
// Get the Revenue column as a float series
let revenue : Series<int, float> = q1.GetColumn<float>("Revenue")
(*** include-it ***)

(**
Column values are returned as `obj` by default; use `GetColumn<'T>` to coerce to
a concrete type, or `Frame.mapCols`/`Frame.map` for bulk conversion.

### Selecting and renaming columns

You can slice, rename or re-index the frame exactly as you would any other Deedle
frame:
*)
// Keep only the Revenue column and use Month as the row index
let q1Indexed =
    q1
    |> Frame.indexRowsString "Month"
    |> Frame.sliceCols ["Revenue"]
(*** include-it ***)

(**
### Combining sheets

Reading multiple sheets into a single frame is straightforward with `Frame.merge`
or `Frame.append`:
*)
let allRevenue =
    [q1; q2]
    |> List.map (fun f -> f |> Frame.indexRowsString "Month")
    |> List.reduce Frame.merge
(*** include-it ***)

(**
---

<a name="type-coercion"></a>

## Type coercion and missing values

ExcelDataReader returns cell values as `obj`. Deedle stores them in `object`
columns. When you call `GetColumn<float>`, Deedle uses `Convert.ToDouble`
internally; when you call `GetColumn<string>`, it calls `.ToString()`.

Empty cells become missing values in the resulting series:

```fsharp
// Columns with empty cells are read as missing values
let withGaps : Series<int, float> = frame.GetColumn<float>("SomeColumn")
withGaps |> Series.dropMissing
```

For fine-grained control over the conversion, use `Frame.mapValues`:

```fsharp
let asFloat (v: obj) =
    match v with
    | :? double as d -> d
    | :? int    as i -> float i
    | _              -> nan

frame
|> Frame.mapValues asFloat
```

---

<a name="error-handling"></a>

## Error handling

`Frame.readExcelSheet` raises `System.Exception` if the named sheet does not exist,
with a message that lists available sheet names. `Frame.readExcelSheetByIndex` raises
if the index is out of range.

Both are recoverable with a standard `try/with`:

```fsharp
let tryReadSheet path name =
    try
        Frame.readExcelSheet path name |> Some
    with _ -> None
```

---

<a name="excel-writer"></a>

## Writing to Excel (Windows only)

The `Deedle.Excel` package exposes a live read/write API that works by
communicating with a running Excel instance via [NetOffice](https://netoffice.io/).
Because it requires an active COM connection it is **Windows-only** and suitable
for interactive / scripting scenarios (e.g. F# Interactive with Excel open).

Install with:

```
dotnet add package Deedle.Excel
```

Basic usage in F# Interactive:

```fsharp
open Deedle
open Deedle.Excel

// Push a frame to the active Excel workbook
xl?A1 <- myFrame

// Keep Excel in sync as the frame changes
xl.KeepInSync <- true
xl?A1 <- myFrame
```

For cross-platform programmatic xlsx generation, consider combining
`Deedle.Excel.Reader` with a library such as
[ClosedXML](https://github.com/ClosedXML/ClosedXML) or
[EPPlus](https://www.epplussoftware.com/) for the write side.
*)
