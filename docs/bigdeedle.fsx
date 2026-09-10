(**
---
title: Big Deedle — virtual frames
category: Guides
categoryindex: 1
index: 10
description: Load large CSV and Parquet as virtual frames, filter without full materialize, and know what stays lazy
keywords: big deedle, virtual frame, LookupRange, Virtual.ReadCsv, Virtual.ReadParquet, out-of-core
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#endif // FSX
(*** condition: prepare ***)

open System
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual

let root = __SOURCE_DIRECTORY__ + "/data/"
let path = root + "bigdeedle-prices.csv"

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    "\n" + (fmt.Invoke(o, [||]) :?> string)
  else null)

(**

# Big Deedle — virtual frames

Big Deedle opens large CSV or Parquet files as **virtual** frames: metadata and filters stay
cheap, and cells are decoded when you read them. The typical workflow is **load → filter →
slice → project → materialize a subset → analytics / export**.

<a name="load"></a>

## 1. Creating a virtual frame

`Virtual.ReadCsv` loads a CSV without materializing the full table. The sample below uses
`data/bigdeedle-prices.csv` with inferred `LookupRange` on `Category` and `Cycle`.
*)

let prices =
  Virtual.ReadCsv(
    path,
    searchColumns =
      [ VirtualSearchColumn.infer "Category"
        VirtualSearchColumn.infer "Cycle" ],
    columnKeys = [ "Category"; "Open"; "Close"; "Volume"; "Cycle" ])

Virtual.Describe prices
(*** include-fsi-output ***)

prices.ColumnKeys |> Seq.toList
(*** include-fsi-output ***)

(**
ReadCsv parameters:

Path, IndexColumn, SearchColumn, ColumnKeys, ByteOffsetIndex

IndexColumn if passed - checks if the column is ascending and unique, if not, falls back to default - an ordinal index.

**When to use ordinal vs ordered row index**

- **Ordinal** (`0 .. N-1`, default): row keys are positions in file order. Use when you mostly filter by column values (`filterRowsBy`), scan subsets, or export batches — you do not need to slice with a business key. `Virtual.ReadCsvDirectory` always uses ordinal.
- **Ordered** (`indexColumn` when strictly increasing and unique): row keys come from this column (timestamps, IDs). Use for time-series key-range slices (`frame.Rows.[t1 .. t2]`), nearest-key lookup and aligning on real keys. Pass an explicit type argument for `DateTime` / `DateTimeOffset` columns.

If you pass a DateTime column as indexColumn, you have to use explicit type argument. If the column is not ordered or has duplicate values, the index will fall back to ordinal.

Empty/`NA` cells become missing values

SearchColumns -

You can choose columns for quick and virtual FilterByRows. For each column a VirtualLookupRange will be created. You can choose to either pass VirtualSearchColumn.infer, for Deedle to pick best LookupRange, or select an explicit mode so the full scan isn't performed. Be careful, if you pass a Step, and the data isn't actually cyclical, the results for operations on that column won't be correct - Deedle doesn't check correctness of explicitly passed VirtualSearchColumn mode.

columnKeys:

List of columns to be included in the Virtual Frame. If ommited, all columns will be used.
Deedle takes the first row of the data as the column keys.
If you have data without Labels - use hasHeaders = false. In that case columns will be named Column1, Column2 etc, same as in normal Frame. At this point schema is not supported in Virtual Frames.

<a name="lookup"></a>

## 2. VirtualLookupRange

To pass an explicit LookupRange, you have to know the type of data in the column (eg. VirtualSearchColumn.withString), the type of LookupRange (eg.VirtualLookupRange.forRepeatingCycle) and with cycle columns, the specific values - or a range (if for example there are 20 consecutive numbers that are repeating)
*)

let explicitCycle =
  Virtual.ReadCsv(
    path,
    searchColumns =
      [ VirtualSearchColumn.withString "Category"
          (VirtualLookupRange.forRepeatingCycle [| "tech"; "energy"; "retail" |])
        VirtualSearchColumn.withInt64 "Cycle"
          (VirtualLookupRange.forRepeatingCycle [| 1L..3L |]) ],
    columnKeys = [ "Category"; "Open"; "Close"; "Volume"; "Cycle" ])

(**

If you don't want to list all of the values, you can just use `VirtualSearchColumn.infer`. In this case there will be a full scan performedat the creation of the Frame.

Options for explicit LookupRanges:

| Data shape | Helper |
|------------|--------|
| Repeating cycle | `VirtualLookupRange.forRepeatingCycle words` |
| Known categorical levels | `VirtualLookupRange.forCategorical map` |
| Build map once at construction | `VirtualLookupRange.forCategoricalScan length valueAt` |
| Irregular / high cardinality | `VirtualLookupRange.scan length valueAt` (correct, O(N) per filter) |
| Low-cardinality CSV/Parquet string | `VirtualSearchColumn.infer "ColumnName"` at load |

<a name="explore"></a>

## 3. Explore without materializing

You can inspect structure and filter rows without pulling every cell.

Row count and filter by column value:
*)

prices.RowCount
(*** include-fsi-output ***)

let tech = prices |> Frame.filterRowsBy "Category" "tech"
tech.RowCount
(*** include-fsi-output ***)

Virtual.IsVirtualRowIndex tech
(*** include-fsi-output ***)

(**

Two predicates — `filterRowsBy2` intersects both LookupRanges in one pass when the row index is ordered. On ordinal frames it falls back to two chained `filterRowsBy` calls (still virtual, still correct):
*)

let techCycle1 =
  prices
  |> Frame.filterRowsBy2 "Category" "tech" "Cycle" 1L

techCycle1.RowCount
(*** include-fsi-output ***)

let techCycle1Chain =
  prices
  |> Frame.filterRowsBy "Category" "tech"
  |> Frame.filterRowsBy "Cycle" 1L

techCycle1.RowCount = techCycle1Chain.RowCount
(*** include-fsi-output ***)

(**

Peek one value — a single decode, not a full-column pull:
*)

let firstTech = tech.RowKeys |> Seq.head
tech.GetColumn<float>("Close").[firstTech]
(*** include-fsi-output ***)

(**

Other ways to explore without loading everything into memory:

- `Virtual.Describe frame` — row count, column types, virtual index kind
- `Virtual.TryGetLookupRange(frame, "Column")` — `LookupRange` mode for a search column
- `Virtual.IsVirtualRowIndex frame` — whether the row index is still virtual
- `frame.ColumnKeys`, `frame.RowCount`, `frame.GetRowAt(0)` — metadata / one row
- Row/column slices (`frame.Rows.[..]`, `Frame.sliceCols`) — stay virtual; narrow before heavy work

<a name="operations"></a>

## 4. Operations on virtual frames

Most `Frame` / `Series` APIs work. The distinction is whether the **result stays virtual**
or **pulls data into memory**.

**Stays virtual (prep pipeline):**

- `filterRowsBy`, `filterRowsBy2`, `dropMissing`
- Row/column slice, `map`, `fillMissing`, `Series.shift` / `diff` / `pctChange`
- `Frame.sliceCols`, adding/replacing columns with aligned virtual series
- Identical-ordinal zip / join

**Materializes (use on a filtered slice, not the full file):**

- Full-series `Stats.*` — O(N) read of kept rows
- `groupBy`, window aggregates, `sortRows` / `sortRowsBy` (by value)
- Mismatched-key join, `joinOn`

Example prep that remains virtual:
*)

let prepared =
  tech
  |> Frame.sliceCols [ "Open"; "Close" ]
  |> fun f -> f.Rows.[f.RowKeys |> Seq.head .. f.RowKeys |> Seq.skip 4 |> Seq.head]

let closeShifted = prepared.GetColumn<float>("Close") |> Series.shift 1
closeShifted
(*** include-fsi-output ***)

(**

`Stats.sum` on a column reads every row in the subset (materialize pull over those rows only):
*)

prepared.GetColumn<float>("Close") |> Stats.sum
(*** include-fsi-output ***)

(**

<a name="matrix"></a>

## 5. What stays virtual vs what materializes

| Operation | Result |
|-----------|--------|
| `Virtual.ReadCsv` / `ReadCsvDirectory` / `ReadParquet`, metadata, `Describe` | **VIRTUAL** |
| `filterRowsBy` / `filterRowsBy2` (with LookupRange) | **VIRTUAL** |
| Slice / map / fill / shift / diff / pctChange | **VIRTUAL** |
| Nested `windowSize` / `chunkSize` (identity) | **VIRTUAL** nested slices |
| Identical-ordinal zip / join | **VIRTUAL** |
| `dropMissing` | **VIRTUAL** (presence scan + sub-vector) |
| Full-series `Stats.*` | **MATERIALIZE** pull (O(N)); prefer **slice first** |
| Window **aggregates**, `groupBy`, value `sortBy` | **MATERIALIZE** |
| Mismatched-key join, `joinOn`, nearest lookup | **MATERIALIZE** |
| `Virtual.MaterializeFloatBatches` | Explicit **subset** pull for ML |

<a name="ml"></a>

## 6. ML export with `MaterializeFloatBatches`

MaterializeFloatBatches yields data one batch of a given size at a time, without performing a full scan.
You can choose which columns will be used, and set a labels column, that will be returned separately. Each batch is produced by slicing the frame and reading only those rows × columns.
Set order to `FloatBatchOrder.Shuffled` / `ShuffledWithSeed` if you want rows order to berandomized once per enumeration (each row appears in exactly one batch).

| Parameter | Description |
|-----------|-------------|
| `frame` | Source frame (virtual or in-memory) |
| `batchSize` | Rows per batch (last batch may be smaller) |
| `columns` | Column keys to materialize (`float` or `int64`) |
| `missingPolicy` | Missing cells (default `FloatMissingPolicy.NaN`) |
| `includeRowKeys` | Copy row keys for each batch |
| `labelsColumn` | Optional label column (`float` or `int64`) |
| `layout` | Row-major (default) or column-major flat layout |
| `includeMissingMask` | `FloatBatch.MissingMask` for feature cells |
| `maxRows` | Cap total rows exported across all batches |
| `order` | `FloatBatchOrder.Sequential` (default) or shuffled variants |
*)

let batches =
  Virtual.MaterializeFloatBatches(
    tech,
    batchSize = 4L,
    columns = [ "Open"; "Close" ],
    order = FloatBatchOrder.ShuffledWithSeed 42,
    missingPolicy = FloatMissingPolicy.NaN)

let firstBatch = batches |> Seq.head
firstBatch.Rows, firstBatch.Cols
(*** include-fsi-output ***)

firstBatch.FeaturesFlat.[0..1]
(*** include-fsi-output ***)

(**

<a name="delayed"></a>

## 7. DelayedSeries vs virtual vs `ReadCsv`

| Model | When to use |
|-------|-------------|
| **`Frame.ReadCsv`** | Small/medium data (as in the [tutorial](tutorial.html)); full API in RAM |
| **`Virtual.ReadCsv`** | Single CSV; ordinal `0..N-1` by default, or ordered index when `indexColumn` is valid |
| **`Virtual.ReadCsvDirectory`** | Multiple same-schema CSVs concatenated as ordinal `0..N-1` |
| **`Virtual.ReadParquet`** | Columnar files; same LookupRange story after `open Deedle.Parquet` |
| **`DelayedSeries`** | Lazy **range loaders** (DB/API); see [Delay-loaded series](lazysource.html) |

Virtual frames are **source-first** (`IVirtualVectorSource`), not a full custom builder
rewrite. Design background: [Design notes](design.html#bigdeedle).

<a name="custom"></a>

## 8. Custom `IVirtualVectorSource`

For backends other than CSV/Parquet, implement `IVirtualVectorSource<'T>` (`Length`, `ValueAt`,
`GetSubVector`, and preferably `LookupRange` on searchable columns), then wrap with
`Virtual.CreateOrdinalFrame` or `Virtual.CreateFrame`. All columns must share the same
addressing scheme id.
*)

let n = 20L
let cats = [| "tech"; "energy"; "retail" |]
let scheme = "demo-ordinal"

let catSource =
  OrdinalVirtualSource<string>(
    n,
    (fun i -> OptionalValue(cats.[int (i % int64 cats.Length)])),
    scheme,
    lookupRange = VirtualLookupRange.forRepeatingCycle cats)

let closeSource =
  OrdinalVirtualSource<float>(
    n,
    (fun i -> OptionalValue(40.0 + float i)),
    scheme)

let demo =
  Virtual.CreateOrdinalFrame(
    [ "Category"; "Close" ],
    [ catSource :> IVirtualVectorSource; closeSource :> IVirtualVectorSource ])

Virtual.Describe demo
(*** include-fsi-output ***)

(demo |> Frame.filterRowsBy "Category" "tech").RowCount
(*** include-fsi-output ***)

