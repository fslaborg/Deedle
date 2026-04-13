---
title: Deedle in C# — Cookbook
category: Documentation
categoryindex: 1
index: 13
description: Using Deedle data frames and series from C# with extension methods, LINQ, and static API
keywords: C#, cookbook, extension methods, LINQ, interop
---

# Deedle in C# — Cookbook

Deedle is an F# library, but it exposes a fully usable C# API through .NET standard
interfaces and extension methods. This page is a practical reference for C# developers.

**Installation** — add the NuGet package:

```
dotnet add package Deedle
```

Then add the using directive:

```csharp
using Deedle;
```

---

## Creating a Series

`Series<TKey, TValue>` is an ordered mapping from keys to values (with optional missing values).

### SeriesBuilder — collection-initialiser syntax

```csharp
// Integer keys, double values
var prices = new SeriesBuilder<int, double>
{
    { 0, 100.0 },
    { 1, 102.5 },
    { 2, 101.8 }
}.Series;

// DateTime keys
var ts = new SeriesBuilder<DateTime, double>
{
    { new DateTime(2024, 1, 1), 10.0 },
    { new DateTime(2024, 1, 2), 11.5 },
    { new DateTime(2024, 1, 3), 9.8 }
}.Series;
```

### From IEnumerable

```csharp
using System.Linq;

// KeyValuePair sequence → series
var series = Enumerable.Range(0, 5)
    .Select(i => KeyValue.Create(i, i * i))
    .ToSeries();

// Plain value sequence → ordinal (0-based) series
var ordinal = new[] { 1.0, 2.0, 3.0 }.ToOrdinalSeries();
```

---

## Creating a Frame

`Frame<TRowKey, TColumnKey>` is a table of series that all share the same row index.

### From CSV

```csharp
// Load from file (types are inferred)
var frame = Frame.ReadCsv("data.csv");

// Limit type inference to first 100 rows (faster for large files)
var frame2 = Frame.ReadCsv("data.csv", inferRows: 100);

// Load from an open stream (e.g., from HttpClient)
using var stream = File.OpenRead("data.csv");
var frame3 = Frame.ReadCsv(stream);
```

### From anonymous objects / records

```csharp
var df = Frame.FromRecords(new[]
{
    new { Date = new DateTime(2024, 1, 1), Open = 100.0, Close = 102.0 },
    new { Date = new DateTime(2024, 1, 2), Open = 102.0, Close = 101.5 },
});
// Column keys are taken from property names: "Date", "Open", "Close"
```

### From a dictionary of series

```csharp
var df = Frame.FromColumns(new Dictionary<string, Series<int, double>>
{
    { "Open",  new SeriesBuilder<int, double> { { 0, 100.0 }, { 1, 102.0 } }.Series },
    { "Close", new SeriesBuilder<int, double> { { 0, 102.0 }, { 1, 101.5 } }.Series },
});
```

### Using FrameBuilder (preserves column order)

`FrameBuilder.Columns` preserves insertion order of columns, unlike some dictionary-based
approaches.

```csharp
var builder = new FrameBuilder.Columns<int, string>();
builder.Add("Open",   new[] { 100.0, 102.0, 101.5 }.ToOrdinalSeries());
builder.Add("High",   new[] { 103.0, 104.5, 102.0 }.ToOrdinalSeries());
builder.Add("Close",  new[] { 102.0, 101.5, 100.8 }.ToOrdinalSeries());
var ohlc = builder.Frame;
```

---

## Accessing Columns

### Typed column access — `GetColumn<T>`

```csharp
Series<int, double> opens = df.GetColumn<double>("Open");
double firstOpen = opens[0];
```

### Slice multiple columns — `frame.Columns[...]`

`frame.Columns` is a series of columns. Use the indexer to select one or more columns
by key. This returns a new frame containing only those columns.

```csharp
// Single column → returns a Frame with one column
var oneCol = df.Columns[new[] { "Open" }];

// Multiple columns → new frame with a subset of columns
var subset = df.Columns[new[] { "Open", "Close" }];
```

> **Tip**: `frame.Columns[listOfKeys]` is the idiomatic way to dice a frame by columns,
> analogous to `frame.Rows[listOfKeys]` for rows. The operation lives on `Columns` (a
> property of the frame), not directly on the frame itself — that's the key mental model.

### Dynamic column access

Cast the frame to `dynamic` to access columns by name as properties:

```csharp
dynamic dfd = df;
Series<int, double> closeSeries = dfd.Close;
double price = dfd.Close[1];   // get value at row key 1

// Add a new column dynamically
dfd.Spread = dfd.High - dfd.Low;
```

---

## Accessing Rows

`frame.Rows` is a series of rows. Each row is itself an `ObjectSeries<TColumnKey>`.

### Single row by key

```csharp
ObjectSeries<string> row0 = df.Rows[0];

// Get a typed value from the row
double openValue = row0.GetAs<double>("Open");
```

### Multiple rows by key list

```csharp
// Returns a new Frame containing only the requested rows
var subset = df.Rows[new int[] { 0, 1, 2 }];
Console.WriteLine(subset.RowCount); // 3
```

### Slicing rows by range (ordered index required)

When the row index is ordered, you can slice with a range using `Between`:

```csharp
// On a DateTime-indexed frame, get rows in January 2024
var jan = df.Rows.Between(new DateTime(2024, 1, 1), new DateTime(2024, 1, 31));
```

---

## Adding and Modifying Columns

`Frame` is mostly immutable; `AddColumn` returns a new frame. The only mutating
operation is adding a column via `dynamic` assignment or `AddColumn`.

```csharp
// AddColumn returns void — it mutates the frame in-place
df.AddColumn("Spread", df.GetColumn<double>("High") - df.GetColumn<double>("Low"));

// Equivalent using dynamic
dynamic dfd = df;
dfd.Spread = dfd.High - dfd.Low;
```

---

## Missing Values

Deedle uses `OptionalValue<T>` (a discriminated union) to represent missing values
rather than `null` or `NaN`. When a `double.NaN` is encountered in CSV or during
arithmetic, it is stored as a missing value.

```csharp
// OptionalValue.Missing represents a missing entry
var withMissing = new SeriesBuilder<int, double>
{
    { 0, 1.0 },
    { 2, 3.0 }   // key 1 is absent → missing when aligned with another series
}.Series;

// Check for missing values
foreach (var obs in withMissing.Observations)
    Console.WriteLine($"{obs.Key} → {obs.Value}");

// Fill missing values
var filled = withMissing.FillMissing(0.0);   // constant fill
var filled2 = withMissing.FillMissing(Direction.Forward);  // forward-fill

// Drop rows where ALL column values are missing
var clean = FrameModule.DropSparseRows(df);
```

---

## Statistics

`Series` exposes convenient extension methods, and `Stats` provides richer analysis.

```csharp
var s = new[] { 1.0, 2.0, 3.0, 4.0, 5.0 }.ToOrdinalSeries();

double total  = s.Sum();          // 15.0
double mean   = s.Mean();         // 3.0
double min    = s.Min();          // 1.0
double max    = s.Max();          // 5.0

// Moving window statistics
Series<int, double> movMax3 = Stats.movingMax(3, s);
Series<int, double> movMin3 = Stats.movingMin(3, s);

// Expanding (cumulative) statistics
Series<int, double> expMax = Stats.expandingMax(s);
Series<int, double> expMin = Stats.expandingMin(s);
```

### Column-level statistics on a frame

```csharp
// Sum of each column — returns Series<TColumnKey, double>
var colSums = df.Sum();

// Mean of each column
var colMeans = df.Mean();
```

---

## Row-wise Aggregation

Use `MapRows` to transform each row into a scalar value:

```csharp
// Compute the mid-price for each row
var midPrices = df.MapRows((key, row) =>
    (row.GetAs<double>("Open") + row.GetAs<double>("Close")) / 2.0);
// midPrices is Series<TRowKey, double>
```

Use `MapCols` to transform each column (returns a new frame with the same structure):

```csharp
// Normalise each column to [0,1]
var normalised = df.MapCols((colKey, col) =>
{
    var doubles = col.As<double>();
    var mn = doubles.Min();
    var mx = doubles.Max();
    return (ObjectSeries<int>)(object)(doubles - mn) / (mx - mn);
});
```

### Row-wise mean with missing values

Missing values are automatically skipped during aggregation:

```csharp
// Average across columns for each row, ignoring missing values
var rowMeans = df.Rows.Select(kvp => kvp.Value.As<double>().Mean());
```

---

## Joining and Merging Frames

```csharp
// Inner join (keeps only rows present in both frames)
var inner = left.Join(right, JoinKind.Inner);

// Outer join (keeps all rows, fills missing where absent)
var outer = left.Join(right, JoinKind.Outer);

// Left join
var leftJoin = left.Join(right, JoinKind.Left);
```

When both frames have columns with the same name, rename them first to avoid key conflicts:

```csharp
// Rename columns before joining
var leftRen  = FrameModule.IndexColsWith(left,  new[] { "L_Open", "L_Close" });
var rightRen = FrameModule.IndexColsWith(right, new[] { "R_Open", "R_Close" });
var joined   = leftRen.Join(rightRen, JoinKind.Inner);
```

---

## Time Series — Windowing and Chunking

### WindowWhile — sliding windows defined by a key predicate

```csharp
// Build a time-stamped series
var ts = new SeriesBuilder<DateTime, double>
{
    { new DateTime(2024, 1,  1), 100.0 },
    { new DateTime(2024, 1,  2), 101.0 },
    { new DateTime(2024, 1, 10), 102.0 },
    { new DateTime(2024, 1, 11), 103.0 },
}.Series;

// A window spans keys within 3 days of the start key
var windows = ts.WindowWhile((k1, k2) => (k2 - k1).TotalDays < 3);
// windows[new DateTime(2024,1,1)] → series with Jan-1 and Jan-2

// WindowWhileInto — aggregate each window immediately
var rollingMean = ts.WindowWhileInto(
    (k1, k2) => (k2 - k1).TotalDays < 3,
    window => window.Mean());
```

### ChunkWhile — non-overlapping partitions

```csharp
// Partition the series into contiguous chunks where each step < 5 days
var chunks = ts.ChunkWhile((k1, k2) => (k2 - k1).TotalDays < 5);
// chunks[new DateTime(2024,1,1)] → series with Jan-1, Jan-2
// chunks[new DateTime(2024,1,10)] → series with Jan-10, Jan-11

// ChunkWhileInto — aggregate each chunk immediately
var chunkSums = ts.ChunkWhileInto(
    (k1, k2) => (k2 - k1).TotalDays < 5,
    chunk => chunk.Sum());
```

### PairwiseWith — combine consecutive pairs

```csharp
// Day-over-day changes
var changes = ts.PairwiseWith((key, prev, curr) => curr - prev);
```

---

## Projections and Filtering

```csharp
// Filter to rows where Open > 101
var highOpen = df.Where(kvp => kvp.Value.GetAs<double>("Open") > 101.0);

// Select/transform values in a typed series
var opens = df.GetColumn<double>("Open");
var scaled = opens.Select(kvp => kvp.Value * 100.0);
```

---

## Mapping Frame Rows to Typed Objects — `GetRowsAs<T>`

When you want to work with strongly-typed row objects, define a C# interface whose
property names match the frame's column names:

```csharp
public interface IPrice
{
    double Open  { get; }
    double High  { get; }
    double Low   { get; }
    double Close { get; }
}

var rows = ohlcFrame.GetRowsAs<IPrice>();
double firstOpen = rows[0].Open;
```

`GetRowsAs<T>` creates a proxy that reads from the underlying frame row. Only
`get`-only properties are bound; properties with setters are ignored on the read path.

---

## Converting to Arrays and Collections

```csharp
// All present (non-missing) values
IEnumerable<double> vals = df.GetColumn<double>("Open").GetAllValues()
                              .Select(opt => opt.Value);

// Round-trip to/from a 2-D array (missing values fill with the default)
double[,] arr = df.ToArray2D<double>();
var df2 = Frame.FromArray2D(arr);

// Convert to a list of KeyValuePair observations
var obs = df.GetColumn<double>("Open").Observations.ToList();
```

---

## Practical Example — Titanic Survival Rate

```csharp
var titanic = Frame.ReadCsv("titanic.csv");

// How many passengers in each ticket class survived?
var survived = titanic.GetColumn<bool>("Survived");
var pclass   = titanic.GetColumn<int>("Pclass");

// Group survived flags by ticket class
var byClass = survived.Zip<bool, int, string>(pclass,
    (s, c) => $"Class {c}: {(s ? "survived" : "died")}");

// Print distinct outcomes and their counts
foreach (var g in byClass.GetAllValues().GroupBy(v => v.Value))
    Console.WriteLine($"{g.Key}: {g.Count()}");
```

---

## Tips

* **Frame is mostly immutable** — operations like `Join`, `Where`, and `Select` return
  new frames. `AddColumn` (and dynamic column assignment) are the only in-place mutations.

* **`frame.Rows[...]` and `frame.Columns[...]`** accept a single key, a `IList<TKey>`,
  or (for ordered indices) a range from `.Between(lo, hi)`. The operation lives on the
  `Rows` / `Columns` property, not directly on the frame — which can be surprising at first.

* **Type inference for CSV** — `Frame.ReadCsv` infers column types from the data. Pass
  `inferRows:` to limit how many rows are scanned. For very large files, specifying
  `schema:` explicitly is faster and avoids ambiguity.

* **Missing values and `double.NaN`** — when reading CSVs, empty cells and `NaN` literals
  become missing values. Use `FillMissing` to replace them before arithmetic to avoid
  `NaN` propagation.

* **Dynamic frame access** — casting a frame to `dynamic` lets you use `dfd.ColumnName`
  syntax. This is convenient for interactive code; prefer `GetColumn<T>` in production
  code for type safety.

---

## Reading Excel files

Add the `Deedle.Excel.Reader` package:

```
dotnet add package Deedle.Excel.Reader
```

```csharp
using Deedle;
using Deedle.ExcelReader;
```

### Read the first worksheet

```csharp
var df = ExcelFrame.ReadExcel("data.xlsx");
// Row keys are 0-based ints, column keys are header strings
Console.WriteLine($"Rows: {df.RowCount}, Cols: {df.ColumnCount}");
```

### Read a specific sheet by name or index

```csharp
var q1 = ExcelFrame.ReadExcelSheet("sales.xlsx", "Q1 Sales");
var q2 = ExcelFrame.ReadExcelSheetByIndex("sales.xlsx", 1);
```

### List all worksheet names

```csharp
var names = ExcelFrame.SheetNames("sales.xlsx");
foreach (var name in names)
    Console.WriteLine(name);
```

---

## Apache Arrow and Feather I/O

Add the `Deedle.Arrow` package:

```
dotnet add package Deedle.Arrow
```

```csharp
using Deedle;
using Deedle.Arrow;
```

### Read and write Arrow / Feather files

```csharp
// Write
df.WriteArrow("data.arrow");
df.WriteFeather("data.feather");   // Feather v2 = Arrow IPC format

// Read
var df1 = ArrowFrame.ReadArrow("data.arrow");
var df2 = ArrowFrame.ReadFeather("data.feather");
```

### Preserve row keys across round-trips

```csharp
// Write with row index stored in __index__ column
df.WriteArrowWithIndex("indexed.arrow");

// Read back with original row keys
Frame<string, string> df3 = ArrowFrame.ReadArrowWithIndex("indexed.arrow");
```

### Stream I/O

```csharp
using var ms = new MemoryStream();
df.WriteArrowStream(ms);
ms.Position = 0;
var df4 = ArrowFrame.ReadArrowStream(ms);
```

### Convert to/from Apache Arrow RecordBatch

```csharp
using Apache.Arrow;

RecordBatch batch = df.ToRecordBatch();
Console.WriteLine($"Columns: {batch.ColumnCount}, Rows: {batch.Length}");
```

---

## Apache Parquet I/O

Add the `Deedle.Parquet` package:

```
dotnet add package Deedle.Parquet
```

```csharp
using Deedle;
using Deedle.Parquet;
```

### Read and write Parquet files

```csharp
// Write
df.WriteParquet("data.parquet");

// Read
var df1 = ParquetFrame.ReadParquet("data.parquet");
```

### Preserve row keys across round-trips

```csharp
df.WriteParquetWithIndex("indexed.parquet");
Frame<string, string> df2 = ParquetFrame.ReadParquetWithIndex("indexed.parquet");
```

### Stream I/O

```csharp
using var ms = new MemoryStream();
df.WriteParquetStream(ms);
ms.Position = 0;
var df3 = ParquetFrame.ReadParquetStream(ms);
```

---

## Further Reading

* [Data frame features (F#)](frame.html) — the full API reference with F# examples,
  most of which translate directly to C#.
* [Series features (F#)](series.html) — windowing, chunking, resampling.
* [Statistics](stats.html) — summary statistics, moving and expanding windows.
* [Handling missing values](missing.html) — all fill strategies and propagation rules.
* [Joining and merging frames](joining.html) — inner, outer, left, and right joins.
* [Excel integration (F#)](excel.html) — reading `.xls` and `.xlsx` files.
* [Arrow and Feather integration (F#)](arrow.html) — Arrow IPC file and stream I/O, RecordBatch conversions.
* [Parquet integration (F#)](parquet.html) — Parquet file and stream I/O.
* [C# test suite](https://github.com/fslaborg/Deedle/tree/master/tests/Deedle.CSharp.Tests) —
  executable examples covering every major C# API surface.
