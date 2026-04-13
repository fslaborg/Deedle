(**
---
title: Deedle.Arrow — Apache Arrow and Feather integration
category: Integrations
categoryindex: 2
index: 2
description: Reading and writing Arrow IPC and Feather v2 files, converting between Deedle frames and Arrow RecordBatches
keywords: arrow, feather, IPC, RecordBatch, columnar, interop, pyarrow, pandas
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Arrow.dll"
#r "../packages/Apache.Arrow/lib/net8.0/Apache.Arrow.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#r "nuget: Deedle.Arrow,{{fsdocs-package-version}}"
#endif // FSX
(*** condition: prepare ***)

open System
open System.IO
open Deedle
open Deedle.Arrow

(**

# Deedle.Arrow — Apache Arrow and Feather Integration

The `Deedle.Arrow` package adds first-class support for [Apache Arrow](https://arrow.apache.org/) —
the industry-standard columnar in-memory format used by Python (`pyarrow`/`pandas`), R (`arrow`),
Spark, DuckDB, and many other data tools.

Install it alongside Deedle:

```
dotnet add package Deedle.Arrow
```

Then open both namespaces:

```fsharp
open Deedle
open Deedle.Arrow
```

After opening `Deedle.Arrow` you gain access to a `Frame` module and a `Series` module that extend
the usual Deedle API with Arrow-specific functions (e.g. `Frame.readArrow`, `Frame.toRecordBatch`).

<a name="file-io"></a>

## Reading and writing Arrow / Feather files

The Arrow IPC *file* format (extension `.arrow`) is also the basis of **Feather v2** files
(extension `.feather`), so both are handled by the same functions.

### Writing a frame to disk

*)

// Build a small sample frame
let prices =
    frame [ "Open"  => Series.ofValues [ 100.0; 102.5; 101.0; 103.0 ]
            "Close" => Series.ofValues [ 101.5; 101.0; 103.5; 104.0 ]
            "Vol"   => Series.ofValues [ 12000; 15000; 11000; 14000 ] ]

(*** include-fsi-merged-output ***)

// Write as Arrow IPC file
Frame.writeArrow "/tmp/prices.arrow" prices

// Write as Feather v2 (identical format, different extension)
Frame.writeFeather "/tmp/prices.feather" prices

(**

### Reading a frame from disk

*)

let prices2 = Frame.readArrow "/tmp/prices.arrow"
(*** include-fsi-merged-output ***)

let prices3 = Frame.readFeather "/tmp/prices.feather"
(*** include-fsi-merged-output ***)

(**

Row keys after reading are always 0-based integers. See the section on
[row-key preservation](#index) below if you need to round-trip named rows.

<a name="stream-io"></a>

## Arrow IPC stream format

The Arrow IPC *stream* format (extension `.arrows`) is designed for network transport and
streaming pipelines. Unlike the file format, it does not require seekable storage.

*)

let arrowStreamExample () =
    // Write to an in-memory stream
    use ms = new MemoryStream()
    Frame.writeArrowStream ms prices

    // Rewind and read back
    ms.Position <- 0L
    let prices4 = Frame.readArrowStream ms
    prices4

let prices4 = arrowStreamExample ()
(*** include-fsi-merged-output ***)

(**

<a name="recordbatch"></a>

## Converting to and from RecordBatch

You can work directly with Apache Arrow `RecordBatch` objects — useful when integrating
with other Arrow-based libraries such as DuckDB or DataFusion.

*)

open Apache.Arrow

// Convert a frame to an Arrow RecordBatch
let batch : RecordBatch = Frame.toRecordBatch prices
printfn "Columns: %d, Rows: %d" batch.ColumnCount batch.Length

// Convert a RecordBatch back to a Deedle frame
let prices5 : Frame<int, string> = Frame.ofRecordBatch batch
(*** include-fsi-merged-output ***)

(**

<a name="series"></a>

## Converting Series to Arrow arrays

The `Series` module provides single-column Arrow conversions, useful for building
Arrow arrays from Deedle series without going through a full frame.

*)

let vols = Series.ofValues [ 12000; 15000; 11000; 14000 ]

// Convert to Arrow IArrowArray
let arrowArr : IArrowArray = Series.toArrowArray vols
printfn "Arrow array type: %s, length: %d" (arrowArr.GetType().Name) arrowArr.Length

// Convert back to a Series<int, obj>
let volsBack : Series<int, obj> = Series.ofArrowArray arrowArr
(*** include-fsi-merged-output ***)

(**

<a name="index"></a>

## Preserving row keys (round-trip with named rows)

By default, row keys are dropped when writing Arrow files. Use `Frame.writeArrowWithIndex`
to serialise row keys into a special `__index__` column, then `Frame.readArrowWithIndex`
to restore them.

*)

// Frame with string row keys
let monthly =
    let keys = [| "Jan"; "Feb"; "Mar"; "Apr" |]
    Frame.ofColumns [
        "Revenue", Series(keys, [| 1200.0; 1350.0; 1100.0; 1500.0 |])
        "Cost",    Series(keys, [| 800.0;  900.0;  750.0;  1000.0 |])
    ]
(*** include-fsi-merged-output ***)

Frame.writeArrowWithIndex "/tmp/monthly.arrow" monthly

// Read back, restoring original string row keys
let monthly2 : Frame<string, string> = Frame.readArrowWithIndex "/tmp/monthly.arrow"
(*** include-fsi-merged-output ***)

(**

The same `WithIndex` variants exist for Feather files:

```fsharp
Frame.writeFeatherWithIndex "/tmp/monthly.feather" monthly
let monthly3 = Frame.readFeatherWithIndex "/tmp/monthly.feather"
```

<a name="types"></a>

## Type mapping

The following .NET/Deedle types are mapped natively to Arrow array types:

| .NET type | Arrow type | Notes |
|---|---|---|
| `float` / `double` | `Float64` | |
| `float32` | `Float32` | |
| `int` / `int32` | `Int32` | |
| `int64` | `Int64` | |
| `int16` | `Int16` | |
| `uint8` / `byte` | `UInt8` | |
| `uint16` | `UInt16` | |
| `uint32` | `UInt32` | |
| `uint64` | `UInt64` | |
| `bool` | `Boolean` | |
| `string` | `Utf8` | |
| `DateTime` | `Timestamp(Microsecond, UTC)` | Stored as UTC |
| `DateTimeOffset` | `Timestamp(Microsecond, UTC)` | Stored as UTC |
| Other | `Utf8` | Via `ToString()` |

Deedle missing values are encoded as Arrow validity-bitmap nulls, so they survive
round-trips with zero data loss.

When *reading* Arrow files written by other tools (Python, R, etc.) the following
additional Arrow types are supported:

| Arrow type | .NET type in Deedle |
|---|---|
| `Date32` | `DateTime` (date-only, midnight UTC) |
| `Date64` | `DateTime` (milliseconds since epoch, UTC) |
| Any unsigned integer | Corresponding .NET unsigned type |

<a name="python"></a>

## Interoperability with Python / pyarrow

Deedle.Arrow files are fully compatible with Python's `pyarrow` library.

**Write from Deedle, read in Python:**

```fsharp
// F# side
let df = Frame.ReadCsv("data.csv")
Frame.writeFeather "/tmp/data.feather" df
```

```python
# Python side
import pyarrow.feather as feather
df = feather.read_table("/tmp/data.feather").to_pandas()
print(df.head())
```

**Write from Python, read in Deedle:**

```python
# Python side
import pandas as pd
import pyarrow.feather as feather

df = pd.DataFrame({"A": [1.0, 2.0, 3.0], "B": ["x", "y", "z"]})
feather.write_feather(df, "/tmp/from_python.feather")
```

```fsharp
// F# side
let df = Frame.readFeather "/tmp/from_python.feather"
printfn "%A" df
```

<a name="nuget"></a>

## NuGet package information

| Package | NuGet |
|---|---|
| `Deedle` | Core library |
| `Deedle.Arrow` | Apache Arrow / Feather v2 integration |

`Deedle.Arrow` depends on the official `Apache.Arrow` NuGet package (Apache Software
Foundation, Apache 2.0 licence), which is the reference .NET implementation of the
Apache Arrow spec.

*)

// Clean up temp files
[ "/tmp/prices.arrow"; "/tmp/prices.feather"; "/tmp/monthly.arrow" ]
|> List.iter (fun p -> if File.Exists(p) then File.Delete(p))
