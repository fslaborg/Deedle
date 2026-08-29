#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualParquetSource
#endif

open System
open System.IO
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Parquet
open Deedle.Parquet.Virtual.Sources
open Deedle.TestData
open Deedle.Virtual.Sources
open Deedle.Tests.VirtualInstrumentation

// ------------------------------------------------------------------------------------------------
// Virtual.ReadParquet (src/Deedle.Parquet/VirtualParquetSource.fs)
// ------------------------------------------------------------------------------------------------

module private SearchDataset =
  let nLarge = 100_000L
  let dataDir = Path.Combine(__SOURCE_DIRECTORY__, "data")
  let csvPath = Path.Combine(dataDir, CsvTestData.defaultDatasetName)
  let parquetPath = Path.Combine(dataDir, ParquetTestData.defaultDatasetName)
  let gate = obj()

  let ensureParquet () =
    lock gate (fun () ->
      Directory.CreateDirectory dataDir |> ignore
      CsvTestData.ensureSearchCsv csvPath nLarge |> ignore
      ParquetTestData.ensureSearchParquet parquetPath nLarge |> ignore)

[<Test; NonParallelizable>]
let ``Can read Parquet search dataset matching CSV value sum`` () =
  SearchDataset.ensureParquet()
  let expected = CsvTestData.readMeta(SearchDataset.csvPath).ValueSum
  let csvSum = Stats.sum (CsvTestData.createFloatValueSeries SearchDataset.csvPath)
  Assert.That(abs (csvSum - expected), Is.LessThan(1.0))
  let materializedSum =
    Stats.sum ((Frame.readParquet SearchDataset.parquetPath).GetColumn<float>("Value"))
  Assert.That(abs (materializedSum - expected), Is.LessThan(1.0))
  let virtualSum = Stats.sum (ParquetTestData.createFloatValueSeries SearchDataset.parquetPath)
  Assert.That(abs (virtualSum - expected), Is.LessThan(1.0))
  Assert.That(abs (virtualSum - materializedSum), Is.LessThan(0.01))

[<Test; NonParallelizable>]
let ``Can infer Step LookupRange on Parquet Category column`` () =
  SearchDataset.ensureParquet()
  let frame =
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "Timestamp",
      searchColumns = [ VirtualSearchColumn.infer "Category" ],
      columnKeys = [ "Id"; "Category" ])
  Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step CsvTestData.words8.Length))
  Virtual.IsVirtualColumn(frame, "Category") |> shouldEqual true
  let filtered = frame |> Frame.filterRowsBy "Category" "lorem"
  Virtual.GetRowIndexKind filtered |> shouldEqual VirtualRowIndexKind.OrderedVirtual
  filtered.RowCount |> shouldEqual 12_500
  FrameProbe.rowIndexIsVirtual filtered |> shouldEqual true

[<Test; NonParallelizable>]
let ``ReadParquet can filterRowsBy on non-search columns via scan fallback`` () =
  SearchDataset.ensureParquet()
  let frame =
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "Timestamp",
      columnKeys = [ "Id"; "Category" ])
  let materialized = Frame.readParquet SearchDataset.parquetPath
  let sample = materialized.GetColumn<string>("Category").GetAt(0)
  let expected = materialized |> Frame.filterRowsBy "Category" sample
  let filtered = frame |> Frame.filterRowsBy "Category" sample
  Virtual.IsVirtualRowIndex filtered |> shouldEqual true
  filtered.RowCount |> shouldEqual expected.RowCount

[<Test>]
let ``ReadParquet can filterRowsBy on high-cardinality search column via scan fallback`` () =
  let path = Path.Combine(Path.GetTempPath(), sprintf "deedle-parquet-hicard-%d.parquet" Environment.TickCount)
  try
    let n = 100
    let schema = Parquet.Schema.ParquetSchema([|
      Parquet.Schema.DataField("Timestamp", typeof<Nullable<DateTime>>) :> Parquet.Schema.Field
      Parquet.Schema.DataField("Category", typeof<string>) :> Parquet.Schema.Field |])
    let fields = schema.GetDataFields()
    let start = DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)
    let ts = [| for i in 0 .. n - 1 -> Nullable(start.AddDays(float i)) |]
    let cats = [| for i in 0 .. n - 1 -> sprintf "unique-category-%d" i |]
    do
      use stream = File.Create path
      use writer = global.Parquet.ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
      use rg = writer.CreateRowGroup()
      rg.WriteColumnAsync(Parquet.Data.DataColumn(fields.[0], ts)).GetAwaiter().GetResult()
      rg.WriteColumnAsync(Parquet.Data.DataColumn(fields.[1], cats)).GetAwaiter().GetResult()
    let frame = Virtual.ReadParquet(path, indexColumn = "Timestamp", searchColumns = [ VirtualSearchColumn.infer "Category" ], columnKeys = [ "Category" ])
    Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some VirtualColumnLookupRange.Scan)
    let filtered = frame |> Frame.filterRowsBy "Category" cats.[0]
    Virtual.IsVirtualRowIndex filtered |> shouldEqual true
    filtered.RowCount |> shouldEqual 1
  finally
    GC.Collect()
    GC.WaitForPendingFinalizers()
    try File.Delete path with :? IOException -> ()

[<Test; NonParallelizable>]
let ``Can filter ReadParquet frame with explicit LookupRange`` () =
  SearchDataset.ensureParquet()
  let frame =
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "Timestamp",
      searchColumns = [ VirtualSearchColumn.withString "Category" (VirtualLookupRange.forRepeatingCycle CsvTestData.words8) ],
      columnKeys = [ "Id"; "Category" ])
  Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step CsvTestData.words8.Length))
  frame.RowCount |> shouldEqual (int SearchDataset.nLarge)
  (frame |> Frame.filterRowsBy "Category" "lorem").RowCount |> shouldEqual 12_500

[<Test; NonParallelizable>]
let ``Can filterRowsBy2 on ReadParquet with same count as single filter`` () =
  SearchDataset.ensureParquet()
  let frame =
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "Timestamp",
      searchColumns = [ VirtualSearchColumn.withString "Category" (VirtualLookupRange.forRepeatingCycle CsvTestData.words8) ],
      columnKeys = [ "Id"; "Category" ])
  Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step CsvTestData.words8.Length))
  let searchValue = "lorem"
  let single = frame |> Frame.filterRowsBy "Category" searchValue
  let fused = frame |> Frame.filterRowsBy2 "Category" searchValue "Category" searchValue
  FrameProbe.rowIndexIsVirtual fused |> shouldEqual true
  fused.RowCount |> shouldEqual single.RowCount

[<Test>]
let ``Can read Parquet null floats as missing values`` () =
  let path = Path.Combine(Path.GetTempPath(), sprintf "deedle-parquet-nulls-%d.parquet" Environment.TickCount)
  try
    let schema = Parquet.Schema.ParquetSchema([|
      Parquet.Schema.DataField("Value", typeof<Nullable<float>>) :> Parquet.Schema.Field |])
    let fields = schema.GetDataFields()
    do
      use stream = File.Create path
      use writer = Parquet.ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
      use rg = writer.CreateRowGroup()
      let data = [| Nullable(1.0); Nullable(); Nullable(3.0) |]
      rg.WriteColumnAsync(Parquet.Data.DataColumn(fields.[0], data)).GetAwaiter().GetResult()
    let values =
      use idx = new ParquetFileIndex(path)
      idx.ReadFloatColumn "Value"
    values.[1].HasValue |> shouldEqual false
    let series =
      Virtual.CreateOrdinalSeries(
        OrdinalVirtualSource(int64 values.Length, (fun i -> values.[int i]), "parquet-file")
        :> IVirtualVectorSource<float>)
    series.Values |> Seq.toList |> shouldEqual [ 1.0; 3.0 ]
    Stats.sum series |> shouldEqual 4.0
  finally
    if File.Exists path then File.Delete path

[<Test>]
let ``Can read all Parquet column CLR types through Virtual ReadParquet`` () =
  let path = Path.Combine(Path.GetTempPath(), sprintf "deedle-parquet-alltypes-%d.parquet" Environment.TickCount)
  try
    let t0 = DateTime(2020, 1, 1, 12, 0, 0, DateTimeKind.Utc)
    let t1 = DateTime(2020, 1, 2, 12, 0, 0, DateTimeKind.Utc)
    let df =
      Frame.ofColumns [
        "Timestamp" => (Series.ofValues [ t0; t1 ] :> ISeries<_>)
        "F64"  => (Series.ofOptionalObservations [ (0, Some 1.5); (1, None) ] :> ISeries<_>)
        "F32"  => (Series.ofValues [ 1.0f; 2.5f ] :> ISeries<_>)
        "I32"  => (Series.ofValues [ 10; 20 ] :> ISeries<_>)
        "I64"  => (Series.ofValues [ 100L; 200L ] :> ISeries<_>)
        "I16"  => (Series.ofValues [ 1s; -2s ] :> ISeries<_>)
        "U8"   => (Series.ofValues [ 1uy; 255uy ] :> ISeries<_>)
        "U16"  => (Series.ofValues [ 1us; 1000us ] :> ISeries<_>)
        "U32"  => (Series.ofValues [ 1u; 100000u ] :> ISeries<_>)
        "U64"  => (Series.ofValues [ 1UL; 123456789UL ] :> ISeries<_>)
        "Flag" => (Series.ofValues [ true; false ] :> ISeries<_>)
        "Name" => (Series.ofValues [ "alpha"; "beta" ] :> ISeries<_>)
        "When" => (Series.ofValues [ t0; t1 ] :> ISeries<_>) ]
    Frame.writeParquet path df
    let keys = [ "F64"; "F32"; "I32"; "I64"; "I16"; "U8"; "U16"; "U32"; "U64"; "Flag"; "Name"; "When" ]
    let frame = Virtual.ReadParquet(path, indexColumn = "Timestamp", columnKeys = keys)
    frame.RowCount |> shouldEqual 2
    frame.GetColumn<float>("F64").TryGetAt(0).Value |> shouldEqual 1.5
    frame.GetColumn<float>("F64").TryGetAt(1).HasValue |> shouldEqual false
    frame.GetColumn<string>("Name").Values |> Seq.toList |> shouldEqual [ "alpha"; "beta" ]
  finally
    if File.Exists path then try File.Delete path with _ -> ()

[<Test>]
let ``Can throw when ReadParquet file is missing`` () =
  (fun () -> Virtual.ReadParquet(Path.Combine(Path.GetTempPath(), "deedle-parquet-missing.parquet")) |> ignore)
  |> should throw typeof<System.Exception>

[<Test; NonParallelizable>]
let ``Can throw when ReadParquet column name is unknown`` () =
  SearchDataset.ensureParquet()
  (fun () ->
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "Timestamp",
      columnKeys = [ "NotAColumn" ])
    |> ignore)
  |> should throw typeof<System.Exception>

[<Test; NonParallelizable>]
let ``Can auto-detect Timestamp index column when reading Parquet virtually`` () =
  SearchDataset.ensureParquet()
  let frame = Virtual.ReadParquet(SearchDataset.parquetPath, columnKeys = [ "Id"; "Category" ])
  Virtual.GetRowIndexKind frame |> shouldEqual VirtualRowIndexKind.OrderedVirtual
  FrameProbe.rowIndexIsVirtual frame |> shouldEqual true
  frame.RowCount |> shouldEqual (int SearchDataset.nLarge)
  frame.RowKeys |> Seq.head |> fun (k: DateTimeOffset) -> k.Year |> shouldEqual 2000

[<Test; NonParallelizable>]
let ``Can infer remaining Parquet columns when columnKeys omitted`` () =
  SearchDataset.ensureParquet()
  let frame = Virtual.ReadParquet(SearchDataset.parquetPath, indexColumn = "Timestamp")
  let keys = frame.ColumnKeys |> Seq.toList
  keys |> List.contains "Id" |> shouldEqual true
  keys |> List.contains "Category" |> shouldEqual true
  keys |> List.contains "Value" |> shouldEqual true
  keys |> List.contains "Timestamp" |> shouldEqual false

[<Test; NonParallelizable>]
let ``Can throw when ReadParquet index column is unknown`` () =
  SearchDataset.ensureParquet()
  (fun () ->
    Virtual.ReadParquet(
      SearchDataset.parquetPath,
      indexColumn = "NotAnIndex",
      columnKeys = [ "Id" ])
    |> ignore)
  |> should throw typeof<System.Exception>

[<Test>]
let ``Can throw when ReadParquet file has no data rows`` () =
  let path = Path.Combine(Path.GetTempPath(), sprintf "deedle-parquet-empty-%d.parquet" Environment.TickCount64)
  try
    let schema = Parquet.Schema.ParquetSchema([|
      Parquet.Schema.DataField("Timestamp", typeof<Nullable<DateTime>>) :> Parquet.Schema.Field
      Parquet.Schema.DataField("Value", typeof<Nullable<float>>) :> Parquet.Schema.Field |])
    let fields = schema.GetDataFields()
    do
      use stream = File.Create path
      use writer = global.Parquet.ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
      use rg = writer.CreateRowGroup()
      rg.WriteColumnAsync(Parquet.Data.DataColumn(fields.[0], Array.empty<Nullable<DateTime>>)).GetAwaiter().GetResult()
      rg.WriteColumnAsync(Parquet.Data.DataColumn(fields.[1], Array.empty<Nullable<float>>)).GetAwaiter().GetResult()
    // Confirm the fixture is truly empty before exercising ReadParquet.
    do
      use idx = new ParquetFileIndex(path)
      idx.Length |> shouldEqual 0L
    (fun () -> Virtual.ReadParquet(path, indexColumn = "Timestamp", columnKeys = [ "Value" ]) |> ignore)
    |> should throw typeof<ArgumentException>
  finally
    GC.Collect()
    GC.WaitForPendingFinalizers()
    try File.Delete path with :? IOException -> ()
