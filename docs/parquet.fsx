(**
---
title: Deedle.Parquet — Apache Parquet integration
category: Integrations
categoryindex: 2
index: 3
description: Reading and writing Apache Parquet files for efficient columnar storage and cross-platform data exchange
keywords: parquet, columnar storage, compression, interop, pandas, spark
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Parquet.dll"
#r "../packages/Parquet.Net/lib/net10.0/Parquet.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#r "nuget: Deedle.Parquet,{{fsdocs-package-version}}"
#endif // FSX
(*** condition: prepare ***)

open System
open System.IO
open Deedle
open Deedle.Parquet

(**

# Deedle.Parquet — Apache Parquet Integration

The `Deedle.Parquet` package adds support for [Apache Parquet](https://parquet.apache.org/) —
a columnar storage format optimised for analytics workloads. Parquet files are widely used by
Spark, Pandas, DuckDB, BigQuery, and many other data tools for efficient on-disk storage
with built-in compression.

Install it alongside Deedle:

```
dotnet add package Deedle.Parquet
```

Then open both namespaces:

```fsharp
open Deedle
open Deedle.Parquet
```

After opening `Deedle.Parquet` you gain access to a `Frame` module that extends the usual
Deedle API with Parquet-specific functions (e.g. `Frame.readParquet`, `Frame.writeParquet`).

<a name="file-io"></a>

## Reading and writing Parquet files

### Writing a frame to disk

*)

// Build a small sample frame
let prices =
    frame [ "Open"  => Series.ofValues [ 100.0; 102.5; 101.0; 103.0 ]
            "Close" => Series.ofValues [ 101.5; 101.0; 103.5; 104.0 ]
            "Vol"   => Series.ofValues [ 12000; 15000; 11000; 14000 ] ]

(*** include-value: prices ***)

// Write as Parquet file
Frame.writeParquet "/tmp/prices.parquet" prices

(**

### Reading a frame from disk

*)

let prices2 = Frame.readParquet "/tmp/prices.parquet"
(*** include-value: prices2 ***)

(**

Row keys after reading are always 0-based integers. See the section on
[row-key preservation](#index) below if you need to round-trip named rows.

<a name="stream-io"></a>

## Stream-based I/O

Parquet files can also be read from and written to any seekable .NET `Stream`.
This is useful when working with in-memory buffers or cloud storage SDKs that
expose streams rather than file paths.

*)

let streamExample () =
    // Write to an in-memory stream
    use ms = new MemoryStream()
    Frame.writeParquetStream ms prices

    // Rewind and read back
    ms.Position <- 0L
    let prices3 = Frame.readParquetStream ms
    prices3

let prices3 = streamExample ()
(*** include-value: prices3 ***)

(**

> **Note:** Parquet streams must be seekable (`stream.CanSeek = true`). Unlike
> Arrow IPC streams, network-only streams are not supported.

<a name="index"></a>

## Preserving row keys (round-trip with named rows)

By default, row keys are dropped when writing Parquet files. Use
`Frame.writeParquetWithIndex` to serialise row keys into a special `__index__` column,
then `Frame.readParquetWithIndex` to restore them.

*)

// Frame with string row keys
let monthly =
    let keys = [| "Jan"; "Feb"; "Mar"; "Apr" |]
    Frame.ofColumns [
        "Revenue", Series(keys, [| 1200.0; 1350.0; 1100.0; 1500.0 |])
        "Cost",    Series(keys, [| 800.0;  900.0;  750.0;  1000.0 |])
    ]
(*** include-value: monthly ***)

Frame.writeParquetWithIndex "/tmp/monthly.parquet" monthly

// Read back, restoring original string row keys
let monthly2 : Frame<string, string> = Frame.readParquetWithIndex "/tmp/monthly.parquet"
(*** include-value: monthly2 ***)

(**

<a name="missing"></a>

## Missing values

Deedle missing values are encoded as Parquet nulls via nullable column types.
Value-type columns (e.g. `float`, `int`) are stored as `Nullable<T>` in Parquet;
string columns use native Parquet null support. Missing values survive
round-trips with zero data loss:

*)

let withGaps =
    frame [ "Score" => Series.ofValues [ 1.0; nan; 3.0; nan; 5.0 ] ]

Frame.writeParquet "/tmp/gaps.parquet" withGaps

let restored = Frame.readParquet "/tmp/gaps.parquet"
// The column has 5 rows but only 3 non-missing values
printfn "Rows: %d, Values: %d" restored.RowCount (restored.["Score"].ValueCount)

(**

<a name="types"></a>

## Type mapping

The following .NET/Deedle types are mapped natively to Parquet types:

| .NET type | Parquet type | Notes |
|---|---|---|
| `float` / `double` | `DOUBLE` | |
| `float32` | `FLOAT` | |
| `int` / `int32` | `INT32` | |
| `int64` | `INT64` | |
| `int16` | `INT16` | |
| `uint8` / `byte` | `BYTE` | |
| `uint16` | `UINT16` | |
| `uint32` | `UINT32` | |
| `uint64` | `UINT64` | |
| `bool` | `BOOLEAN` | |
| `string` | `STRING` | |
| `DateTime` | `DATETIMEOFFSET` | |
| Other | `STRING` | Via `ToString()` |

All value types are stored as nullable (`Nullable<T>`) to support Deedle missing
values on round-trip.

<a name="csharp"></a>

## C# API

The package also provides a C# friendly API:

```csharp
using Deedle;
using Deedle.Parquet;

// Static factory
var df = ParquetFrame.ReadParquet("prices.parquet");
ParquetFrame.WriteParquet("prices.parquet", df);

// Extension methods
df.WriteParquet("output.parquet");
df.WriteParquetWithIndex("output.parquet");
var df2 = ParquetFrame.ReadParquetWithIndex<string>("indexed.parquet");
```

<a name="python"></a>

## Interoperability with Python / pandas

Deedle.Parquet files are fully compatible with Python's `pyarrow` and `pandas`
libraries.

**Write from Deedle, read in Python:**

```fsharp
// F# side
let df = Frame.ReadCsv("data.csv")
Frame.writeParquet "/tmp/data.parquet" df
```

```python
# Python side
import pandas as pd
df = pd.read_parquet("/tmp/data.parquet")
print(df.head())
```

**Write from Python, read in Deedle:**

```python
# Python side
import pandas as pd
df = pd.DataFrame({"A": [1.0, 2.0, 3.0], "B": ["x", "y", "z"]})
df.to_parquet("/tmp/from_python.parquet")
```

```fsharp
// F# side
let df = Frame.readParquet "/tmp/from_python.parquet"
printfn "%A" df
```

<a name="vs-arrow"></a>

## Parquet vs Arrow

Both Apache Parquet and Apache Arrow are columnar data formats, but they serve
different purposes:

| | **Parquet** | **Arrow** |
|---|---|---|
| **Primary use** | On-disk storage & interchange | In-memory processing & IPC |
| **Compression** | Built-in (Snappy, LZ4, Zstd, Gzip) | None (raw bytes) |
| **File size** | Compact | Larger |
| **Read speed** | Requires decompression | Zero-copy / memory-mapped |
| **Ecosystem** | Spark, BigQuery, Pandas, DuckDB | Pandas, DuckDB, DataFusion |
| **Deedle package** | `Deedle.Parquet` | `Deedle.Arrow` |

Use **Parquet** when you want compact, portable files for storage or exchange
with other tools. Use **Arrow** when you need fast in-memory interop or IPC
between processes.

<a name="nuget"></a>

## NuGet package information

| Package | NuGet |
|---|---|
| `Deedle` | Core library |
| `Deedle.Parquet` | Apache Parquet integration |

`Deedle.Parquet` depends on [Parquet.Net](https://github.com/aloneguid/parquet-dotnet)
(MIT licence), a pure .NET implementation of the Apache Parquet format with no native
dependencies.

*)

// Clean up temp files
[ "/tmp/prices.parquet"; "/tmp/monthly.parquet"; "/tmp/gaps.parquet" ]
|> List.iter (fun p -> if File.Exists(p) then File.Delete(p))
