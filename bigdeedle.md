# Big Deedle — virtual frames

Big Deedle opens large CSV or Parquet files as **virtual** frames: metadata and filters stay
cheap, and cells are decoded when you read them. The typical workflow is **load → filter →
slice → project → materialize a subset → analytics / export**.

<a name="load"></a>
## 1. Creating a virtual frame

`Virtual.ReadCsv` loads a CSV without materializing the full table. The sample below uses
`data/bigdeedle-prices.csv` with inferred `LookupRange` on `Category` and `Cycle`.

```fsharp
let prices =
  Virtual.ReadCsv(
    path,
    searchColumns =
      [ VirtualSearchColumn.infer "Category"
        VirtualSearchColumn.infer "Cycle" ],
    columnKeys = [ "Category"; "Open"; "Close"; "Volume"; "Cycle" ])

Virtual.Describe prices
```

```
val prices: Frame<int64,string> =
  
      Category Open  Close Volume  Cycle 
0  -> tech     37.5  37.8  1200000 1     
1  -> energy   38.1  37.9  980000  2     
2  -> tech     37.95 38.4  1100000 3     
3  -> retail   38.5  38.2  870000  1     
4  -> tech     38.25 38.9  1350000 2     
5  -> energy   39    38.7  920000  3     
6  -> tech     38.8  39.2  1010000 1     
7  -> retail   39.4  39.1  760000  2     
8  -> tech     39.15 39.6  1180000 3     
9  -> energy   39.7  39.4  890000  1     
10 -> tech     39.5  40    1420000 2     
11 -> retail   40.1  39.8  810000  3     
12 -> tech     39.9  40.3  1250000 1     
13 -> energy   40.4  40.1  950000  2     
14 -> tech     40.2  40.7  1300000 3     
15 -> retail   40.8  40.5  780000  1     
16 -> tech     40.6  41    1150000 2     
17 -> energy   41.1  40.8  900000  3     
18 -> tech     40.9  41.4  1280000 1     
19 -> retail   41.5  41.2  830000  2     
20 -> tech     41.3  41.8  1400000 3     
21 -> energy   41.9  41.6  910000  1     
22 -> tech     41.7  42.1  1220000 2     
23 -> retail   42.2  41.9  790000  3     

val it: string = "rows=24, rowIndex=ordinal virtual (0..N-1), columns=5"
```

```fsharp
prices.ColumnKeys |> Seq.toList
```

```
val it: string list = ["Category"; "Open"; "Close"; "Volume"; "Cycle"]
```

ReadCsv parameters:

Path, IndexColumn, SearchColumn, ColumnKeys, ByteOffsetIndex

IndexColumn if passed - checks if the column is ascending and unique, if not, falls back to default - an ordinal index.

**When to use ordinal vs ordered row index**

* **Ordinal** (`0 .. N-1`, default): row keys are positions in file order. Use when you mostly filter by column values (`filterRowsBy`), scan subsets, or export batches — you do not need to slice with a business key. `Virtual.ReadCsvDirectory` always uses ordinal.

* **Ordered** (`indexColumn` when strictly increasing and unique): row keys come from this column (timestamps, IDs). Use for time-series key-range slices (`frame.Rows.[t1 .. t2]`), nearest-key lookup and aligning on real keys. Pass an explicit type argument for `DateTime` / `DateTimeOffset` columns.

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

```fsharp
let explicitCycle =
  Virtual.ReadCsv(
    path,
    searchColumns =
      [ VirtualSearchColumn.withString "Category"
          (VirtualLookupRange.forRepeatingCycle [| "tech"; "energy"; "retail" |])
        VirtualSearchColumn.withInt64 "Cycle"
          (VirtualLookupRange.forRepeatingCycle [| 1L..3L |]) ],
    columnKeys = [ "Category"; "Open"; "Close"; "Volume"; "Cycle" ])
```

If you don't want to list all of the values, you can just use `VirtualSearchColumn.infer`. In this case there will be a full scan performedat the creation of the Frame.

Options for explicit LookupRanges:

Data shape | Helper
--- | ---
Repeating cycle | `VirtualLookupRange.forRepeatingCycle words`
Known categorical levels | `VirtualLookupRange.forCategorical map`
Build map once at construction | `VirtualLookupRange.forCategoricalScan length valueAt`
Irregular / high cardinality | `VirtualLookupRange.scan length valueAt` (correct, O(N) per filter)
Low-cardinality CSV/Parquet string | `VirtualSearchColumn.infer "ColumnName"` at load


<a name="explore"></a>
## 3. Explore without materializing

You can inspect structure and filter rows without pulling every cell.

Row count and filter by column value:

```fsharp
prices.RowCount
```

```
val it: int = 24
```

```fsharp
let tech = prices |> Frame.filterRowsBy "Category" "tech"
tech.RowCount
```

```
val tech: Frame<int64,string> =
  
      Category Open  Close Volume  Cycle 
0  -> tech     37.5  37.8  1200000 1     
2  -> tech     37.95 38.4  1100000 3     
4  -> tech     38.25 38.9  1350000 2     
6  -> tech     38.8  39.2  1010000 1     
8  -> tech     39.15 39.6  1180000 3     
10 -> tech     39.5  40    1420000 2     
12 -> tech     39.9  40.3  1250000 1     
14 -> tech     40.2  40.7  1300000 3     
16 -> tech     40.6  41    1150000 2     
18 -> tech     40.9  41.4  1280000 1     
20 -> tech     41.3  41.8  1400000 3     
22 -> tech     41.7  42.1  1220000 2     

val it: int = 12
```

```fsharp
Virtual.IsVirtualRowIndex tech
```

```
val it: bool = true
```

Two predicates — `filterRowsBy2` intersects both LookupRanges in one pass when the row index is ordered. On ordinal frames it falls back to two chained `filterRowsBy` calls (still virtual, still correct):

```fsharp
let techCycle1 =
  prices
  |> Frame.filterRowsBy2 "Category" "tech" "Cycle" 1L

techCycle1.RowCount
```

```
val techCycle1: Frame<int64,string> =
  
      Category Open Close Volume  Cycle 
0  -> tech     37.5 37.8  1200000 1     
6  -> tech     38.8 39.2  1010000 1     
12 -> tech     39.9 40.3  1250000 1     
18 -> tech     40.9 41.4  1280000 1     

val it: int = 4
```

```fsharp
let techCycle1Chain =
  prices
  |> Frame.filterRowsBy "Category" "tech"
  |> Frame.filterRowsBy "Cycle" 1L

techCycle1.RowCount = techCycle1Chain.RowCount
```

```
val techCycle1Chain: Frame<int64,string> =
  
      Category Open Close Volume  Cycle 
0  -> tech     37.5 37.8  1200000 1     
6  -> tech     38.8 39.2  1010000 1     
12 -> tech     39.9 40.3  1250000 1     
18 -> tech     40.9 41.4  1280000 1     

val it: bool = true
```

Peek one value — a single decode, not a full-column pull:

```fsharp
let firstTech = tech.RowKeys |> Seq.head
tech.GetColumn<float>("Close").[firstTech]
```

```
val firstTech: int64 = 0L
val it: float = 37.8
```

Other ways to explore without loading everything into memory:

* `Virtual.Describe frame` — row count, column types, virtual index kind

* `Virtual.TryGetLookupRange(frame, "Column")` — `LookupRange` mode for a search column

* `Virtual.IsVirtualRowIndex frame` — whether the row index is still virtual

* `frame.ColumnKeys`, `frame.RowCount`, `frame.GetRowAt(0)` — metadata / one row

* Row/column slices (`frame.Rows.[..]`, `Frame.sliceCols`) — stay virtual; narrow before heavy work

<a name="operations"></a>
## 4. Operations on virtual frames

Most `Frame` / `Series` APIs work. The distinction is whether the **result stays virtual**
or **pulls data into memory**.

**Stays virtual (prep pipeline):**

* `filterRowsBy`, `filterRowsBy2`, `dropMissing`

* Row/column slice, `map`, `fillMissing`, `Series.shift` / `diff` / `pctChange`

* `Frame.sliceCols`, adding/replacing columns with aligned virtual series

* Identical-ordinal zip / join

**Materializes (use on a filtered slice, not the full file):**

* Full-series `Stats.*` — O(N) read of kept rows

* `groupBy`, window aggregates, `sortRows` / `sortRowsBy` (by value)

* Mismatched-key join, `joinOn`

Example prep that remains virtual:

```fsharp
let prepared =
  tech
  |> Frame.sliceCols [ "Open"; "Close" ]
  |> fun f -> f.Rows.[f.RowKeys |> Seq.head .. f.RowKeys |> Seq.skip 4 |> Seq.head]

let closeShifted = prepared.GetColumn<float>("Close") |> Series.shift 1
closeShifted
```

```
val prepared: Frame<int64,string> =
  
     Open  Close 
0 -> 37.5  37.8  
2 -> 37.95 38.4  
4 -> 38.25 38.9  
6 -> 38.8  39.2  
8 -> 39.15 39.6  

val closeShifted: Series<int64,float> =
  
2 -> 37.8 
4 -> 38.4 
6 -> 38.9 
8 -> 39.2 

val it: Series<int64,float> = 
2 -> 37.8 
4 -> 38.4 
6 -> 38.9 
8 -> 39.2
```

`Stats.sum` on a column reads every row in the subset (materialize pull over those rows only):

```fsharp
prepared.GetColumn<float>("Close") |> Stats.sum
```

```
val it: float = 193.9
```

<a name="matrix"></a>
## 5. What stays virtual vs what materializes

Operation | Result
--- | ---
`Virtual.ReadCsv` / `ReadCsvDirectory` / `ReadParquet`, metadata, `Describe` | **VIRTUAL**
`filterRowsBy` / `filterRowsBy2` (with LookupRange) | **VIRTUAL**
Slice / map / fill / shift / diff / pctChange | **VIRTUAL**
Nested `windowSize` / `chunkSize` (identity) | **VIRTUAL** nested slices
Identical-ordinal zip / join | **VIRTUAL**
`dropMissing` | **VIRTUAL** (presence scan + sub-vector)
Full-series `Stats.*` | **MATERIALIZE** pull (O(N)); prefer **slice first**
Window **aggregates**, `groupBy`, value `sortBy` | **MATERIALIZE**
Mismatched-key join, `joinOn`, nearest lookup | **MATERIALIZE**
`Virtual.MaterializeFloatBatches` | Explicit **subset** pull for ML


<a name="ml"></a>
## 6. ML export with `MaterializeFloatBatches`

MaterializeFloatBatches yields data one batch of a given size at a time, without performing a full scan.
You can choose which columns will be used, and set a labels column, that will be returned separately. Each batch is produced by slicing the frame and reading only those rows × columns.
Set order to `FloatBatchOrder.Shuffled` / `ShuffledWithSeed` if you want rows order to berandomized once per enumeration (each row appears in exactly one batch).

Parameter | Description
--- | ---
`frame` | Source frame (virtual or in-memory)
`batchSize` | Rows per batch (last batch may be smaller)
`columns` | Column keys to materialize (`float` or `int64`)
`missingPolicy` | Missing cells (default `FloatMissingPolicy.NaN`)
`includeRowKeys` | Copy row keys for each batch
`labelsColumn` | Optional label column (`float` or `int64`)
`layout` | Row-major (default) or column-major flat layout
`includeMissingMask` | `FloatBatch.MissingMask` for feature cells
`maxRows` | Cap total rows exported across all batches
`order` | `FloatBatchOrder.Sequential` (default) or shuffled variants


```fsharp
let batches =
  Virtual.MaterializeFloatBatches(
    tech,
    batchSize = 4L,
    columns = [ "Open"; "Close" ],
    order = FloatBatchOrder.ShuffledWithSeed 42,
    missingPolicy = FloatMissingPolicy.NaN)

let firstBatch = batches |> Seq.head
firstBatch.Rows, firstBatch.Cols
```

```
val batches: FloatBatch<int64> seq
val firstBatch: FloatBatch<int64> =
  { Rows = 4
    Cols = 2
    Layout = RowMajor
    FeaturesFlat = [|39.9; 40.3; 38.8; 39.2; 39.5; 40.0; 37.5; 37.8|]
    Labels = None
    MissingMask = None
    RowKeys = None }
val it: int * int = (4, 2)
```

```fsharp
firstBatch.FeaturesFlat.[0..1]
```

```
val it: float array = [|39.9; 40.3|]
```

<a name="delayed"></a>
## 7. DelayedSeries vs virtual vs `ReadCsv`

Model | When to use
--- | ---
**`Frame.ReadCsv`** | Small/medium data (as in the [tutorial](tutorial.html)); full API in RAM
**`Virtual.ReadCsv`** | Single CSV; ordinal `0..N-1` by default, or ordered index when `indexColumn` is valid
**`Virtual.ReadCsvDirectory`** | Multiple same-schema CSVs concatenated as ordinal `0..N-1`
**`Virtual.ReadParquet`** | Columnar files; same LookupRange story after `open Deedle.Parquet`
**`DelayedSeries`** | Lazy **range loaders** (DB/API); see [Delay-loaded series](lazysource.html)


Virtual frames are **source-first** (`IVirtualVectorSource`), not a full custom builder
rewrite. Design background: [Design notes](design.html#bigdeedle).

<a name="custom"></a>
## 8. Custom `IVirtualVectorSource`

For backends other than CSV/Parquet, implement `IVirtualVectorSource<'T>` (`Length`, `ValueAt`,
`GetSubVector`, and preferably `LookupRange` on searchable columns), then wrap with
`Virtual.CreateOrdinalFrame` or `Virtual.CreateFrame`. All columns must share the same
addressing scheme id.

```fsharp
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
```

```
val n: int64 = 20L
val cats: string array = [|"tech"; "energy"; "retail"|]
val scheme: string = "demo-ordinal"
val catSource: OrdinalVirtualSource<string>
val closeSource: OrdinalVirtualSource<float>
val demo: Frame<int64,string> =
  
      Category Close 
0  -> tech     40    
1  -> energy   41    
2  -> retail   42    
3  -> tech     43    
4  -> energy   44    
5  -> retail   45    
6  -> tech     46    
7  -> energy   47    
8  -> retail   48    
9  -> tech     49    
10 -> energy   50    
11 -> retail   51    
12 -> tech     52    
13 -> energy   53    
14 -> retail   54    
15 -> tech     55    
16 -> energy   56    
17 -> retail   57    
18 -> tech     58    
19 -> energy   59    

val it: string = "rows=20, rowIndex=ordinal virtual (0..N-1), columns=2"
```

```fsharp
(demo |> Frame.filterRowsBy "Category" "tech").RowCount
```

```
val it: int = 7
```
