# Excel integration — ExcelReader &amp; ExcelWriter

Deedle provides two packages for working with Microsoft Excel files:

Package | What it does
--- | ---
**`Deedle.ExcelReader`** | Cross-platform reading of `.xlsx` and `.xls` files via [ExcelDataReader](https://github.com/ExcelDataReader/ExcelDataReader)
**`Deedle.ExcelWriter`** | Cross-platform writing of `.xlsx` files via [MiniExcel](https://github.com/mini-software/MiniExcel)


> **Note:** the Windows-only `Deedle.Excel` package (live COM/NetOffice integration) is deprecated
and no longer maintained. Use `Deedle.ExcelReader` and `Deedle.ExcelWriter` instead.
> 

-----------------------

<a name="setup"></a>
## Package setup

Install with:

```fsharp
dotnet add package Deedle.ExcelReader
dotnet add package Deedle.ExcelWriter

```

Then open the namespaces:

```fsharp
open Deedle
open Deedle.ExcelReader  // for reading
open Deedle.ExcelWriter  // for writing

```

-----------------------

<a name="reading"></a>
## Reading Excel files

### Reading the first worksheet

The simplest function reads the first worksheet of an Excel file and returns a
`Frame<int, string>` whose row keys are zero-based integers:

```fsharp
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
```

```
val samplePath: string = "/tmp/tmpU1yiYt.tmp.xlsx"
val sales: Frame<int,string> =
  
     Date       Product  Quantity Price 
0 -> 2024-01-15 Widget A 100      9.99  
1 -> 2024-01-15 Widget B 50       19.99 
2 -> 2024-01-22 Widget A 150      9.99  
3 -> 2024-01-22 Widget C 75       14.99 
4 -> 2024-02-01 Widget B 200      19.99
```

The first row of the worksheet is used as column headers. Subsequent rows become
frame rows; the row keys are contiguous integers starting at 0.

You can also use the `Frame.ReadExcel` static member (a type extension, like `Frame.ReadCsv`):

```fsharp
let sales2 = Frame.ReadExcel(samplePath)                 // first sheet
let q1b    = Frame.ReadExcel(multiPath, "Q1 Sales")      // by name
let q2b    = Frame.ReadExcel(multiPath, 1)               // by zero-based index

```

### Reading a specific sheet by name

When a workbook contains multiple worksheets, use `Frame.readExcelSheet` to target one
by name (case-sensitive):

```fsharp
let multiPath =
    File.ReadAllBytes(__SOURCE_DIRECTORY__ + "/data/quarterly.xlsx")
    |> fun b -> let t = Path.GetTempFileName() + ".xlsx" in File.WriteAllBytes(t, b); t

let q1 : Frame<int, string> = Frame.readExcelSheet multiPath "Q1 Sales"
```

```
val multiPath: string = "/tmp/tmpjMEdeX.tmp.xlsx"
val q1: Frame<int,string> =
  
     Month Revenue 
0 -> Jan   15000   
1 -> Feb   18000   
2 -> Mar   22000
```

### Reading a specific sheet by index

Use `Frame.readExcelSheetByIndex` with a zero-based index when you know the position
but not the name:

```fsharp
let q2 : Frame<int, string> = Frame.readExcelSheetByIndex multiPath 1
```

```
val q2: Frame<int,string> =
  
     Month Revenue 
0 -> Apr   25000   
1 -> May   21000   
2 -> Jun   28000
```

-----------------------

<a name="sheet-names"></a>
## Listing worksheets

`Frame.sheetNames` returns the names of all worksheets in a workbook in order:

```fsharp
let sheets : string list = Frame.sheetNames multiPath
```

```
val sheets: string list = ["Q1 Sales"; "Q2 Sales"]
```

-----------------------

<a name="columns"></a>
## Working with the resulting frame

The frame returned by all `read*` functions has `string` column keys (taken from
the header row) and `int` row keys. You can use all standard Deedle operations
on it.

### Accessing columns

Use `frame.GetColumn<'T>` to retrieve a typed series:

```fsharp
// Get the Revenue column as a float series
let revenue : Series<int, float> = q1.GetColumn<float>("Revenue")
```

```
val revenue: Series<int,float> = 
0 -> 15000 
1 -> 18000 
2 -> 22000
```

Column values are returned as `obj` by default; use `GetColumn<'T>` to coerce to
a concrete type, or `Frame.mapCols`/`Frame.map` for bulk conversion.

### Selecting and renaming columns

You can slice, rename or re-index the frame exactly as you would any other Deedle
frame:

```fsharp
// Keep only the Revenue column and use Month as the row index
let q1Indexed =
    q1
    |> Frame.indexRowsString "Month"
    |> Frame.sliceCols ["Revenue"]
```

```
val q1Indexed: Frame<string,string> =
  
       Revenue 
Jan -> 15000   
Feb -> 18000   
Mar -> 22000
```

### Combining sheets

Reading multiple sheets into a single frame is straightforward with `Frame.merge`
or `Frame.append`:

```fsharp
let allRevenue =
    [q1; q2]
    |> List.map (fun f -> f |> Frame.indexRowsString "Month")
    |> List.reduce Frame.merge
```

```
val allRevenue: Frame<string,string> =
  
       Revenue 
Jan -> 15000   
Feb -> 18000   
Mar -> 22000   
Apr -> 25000   
May -> 21000   
Jun -> 28000
```

-----------------------

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

-----------------------

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

-----------------------

<a name="writing"></a>
## Writing Excel files

The `Deedle.ExcelWriter` package provides cross-platform `.xlsx` writing using
[MiniExcel](https://github.com/mini-software/MiniExcel) — a high-performance,
low-memory library with no dependency on COM or Windows.

### Writing a single sheet

```fsharp
open Deedle
open Deedle.ExcelWriter

let df =
    frame [ "Name"  => series [0 => "Alice"; 1 => "Bob"; 2 => "Charlie"]
            "Score" => series [0 => 95.0; 1 => 87.0; 2 => 91.0] ]

// Write to "Sheet1" (default sheet name)
Frame.writeExcel "/tmp/results.xlsx" df

// Write to a named sheet
Frame.writeExcelSheet "/tmp/results.xlsx" "Scores" df

```

Any existing file at the target path is overwritten.

### Writing multiple sheets

Provide a sequence of `(sheetName, frame)` pairs:

```fsharp
let q1 = frame [ "Month" => series [0 => "Jan"; 1 => "Feb"]
                 "Revenue" => series [0 => 1000.0; 1 => 1200.0] ]
let q2 = frame [ "Month" => series [0 => "Apr"; 1 => "May"]
                 "Revenue" => series [0 => 1300.0; 1 => 1100.0] ]

Frame.writeExcelSheets "/tmp/quarterly.xlsx"
    (seq { yield ("Q1", q1); yield ("Q2", q2) })

```

### Writing to a stream

For in-memory scenarios or HTTP responses, write to any `System.IO.Stream`:

```fsharp
use ms = new System.IO.MemoryStream()
Frame.writeExcelStream ms df
// ms now contains a valid .xlsx binary

```

### Round-trip example

`Deedle.ExcelReader` and `Deedle.ExcelWriter` compose naturally for ETL pipelines:

```fsharp
open Deedle
open Deedle.ExcelReader
open Deedle.ExcelWriter

let input  = Frame.readExcel "input.xlsx"
let output = input |> Frame.filterRows (fun _ row -> row.GetAs<float>("Score") > 90.0)
Frame.writeExcel "output.xlsx" output

```
