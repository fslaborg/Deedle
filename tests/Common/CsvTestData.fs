module Deedle.TestData.CsvTestData

open System
open System.Globalization
open System.IO
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Virtual.Sources

/// Generators and fixtures for CSV virtual / RealSource benchmarks (not part of the library API).
let words8 =
  "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')

/// Second string column (not the configured search column) for scan-fallback benchmarks.
let words4 =
  "alpha beta gamma delta".Split(' ')

let defaultDatasetName = "b6-search-100k-random.csv"
let defaultSeed = 42
let profileVersion = "random-v2"

type CsvDatasetMeta =
  { Version: string
    Seed: int
    RowCount: int64
    ValueSum: float }

let metaPath (csvPath: string) = csvPath + ".meta"

let private writeMeta (csvPath: string) (meta: CsvDatasetMeta) =
  use writer = new StreamWriter(metaPath csvPath, false)
  writer.WriteLine(sprintf "version=%s" meta.Version)
  writer.WriteLine(sprintf "seed=%d" meta.Seed)
  writer.WriteLine(sprintf "rows=%d" meta.RowCount)
  writer.WriteLine(sprintf "valueSum=%s" (meta.ValueSum.ToString("R", CultureInfo.InvariantCulture)))

let readMeta (csvPath: string) =
  let lines = File.ReadAllLines(metaPath csvPath)
  let lookup key =
    lines
    |> Array.tryFind (fun line -> line.StartsWith(key + "=", StringComparison.Ordinal))
    |> Option.map (fun line -> line.Substring(key.Length + 1))
    |> Option.defaultWith (fun () -> invalidOp (sprintf "CsvTestData meta missing key '%s'" key))
  { Version = lookup "version"
    Seed = Int32.Parse(lookup "seed", CultureInfo.InvariantCulture)
    RowCount = Int64.Parse(lookup "rows", CultureInfo.InvariantCulture)
    ValueSum = Double.Parse(lookup "valueSum", CultureInfo.InvariantCulture) }

let private shuffleInPlace (rng: Random) (items: int[]) =
  for i in items.Length - 1 .. -1 .. 0 do
    let j = rng.Next(i + 1)
    let tmp = items.[i]
    items.[i] <- items.[j]
    items.[j] <- tmp

let generateSearchCsv (path: string) (rowCount: int64) (seed: int) =
  let dir = Path.GetDirectoryName(path)
  if not (String.IsNullOrEmpty dir) && not (Directory.Exists dir) then
    Directory.CreateDirectory dir |> ignore
  let rng = Random(seed)
  let ids = Array.init (int rowCount) id
  shuffleInPlace rng ids
  use writer = new StreamWriter(path, false)
  writer.WriteLine("Id,Timestamp,Category,Label,Value")
  let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.Zero)
  let valueSum =
    seq { 0L .. rowCount - 1L }
    |> Seq.fold (fun acc i ->
        let id = ids.[int i]
        let cat = words8.[int (i % int64 words8.Length)]
        let label = words4.[int (i % int64 words4.Length)]
        let ts = start.AddSeconds(float i).ToString("o", CultureInfo.InvariantCulture)
        let value = rng.NextDouble() * 10000.0
        let valueStr = value.ToString("F4", CultureInfo.InvariantCulture)
        writer.WriteLine(sprintf "%d,%s,%s,%s,%s" id ts cat label valueStr)
        acc + Double.Parse(valueStr, CultureInfo.InvariantCulture)
      ) 0.0
  writeMeta path
    { Version = profileVersion
      Seed = seed
      RowCount = rowCount
      ValueSum = valueSum }
  path

let ensureSearchCsvWithSeed (path: string) (rowCount: int64) (seed: int) =
  let valid =
    File.Exists path &&
    File.Exists (metaPath path) &&
    try
      let meta = readMeta path
      meta.Version = profileVersion &&
      meta.Seed = seed &&
      meta.RowCount = rowCount &&
      let idx = CsvLineIndex(path)
      idx.Length = rowCount && idx.ReadFields(0L).Length >= 5
    with _ -> false
  if valid then path
  else
    if File.Exists path then File.Delete path
    let metaFile = metaPath path
    if File.Exists metaFile then File.Delete metaFile
    generateSearchCsv path rowCount seed

let ensureSearchCsv (path: string) (rowCount: int64) =
  ensureSearchCsvWithSeed path rowCount defaultSeed

/// Search-dataset frame: Timestamp index, Id + searchable Category (8-word cycle Step LookupRange).
let createSearchDatasetFrame (csvPath: string) =
  let options =
    { VirtualReadCsvOptions.Default with
        IndexColumn = Some "Timestamp"
        SearchColumns =
          [ VirtualSearchColumn.infer "Category"
            VirtualSearchColumn.infer "Label" ]
        ColumnKeys = Some [ "Id"; "Category" ] }
  VirtualCsvSource.createFrame csvPath options |> unbox<Frame<DateTimeOffset, string>>, words8

let createFloatValueSeries (csvPath: string) =
  let lineIndex = CsvLineIndex(csvPath)
  let src =
    VirtualCsvSource.createColumnSource lineIndex "Value" None
    :?> IVirtualVectorSource<float>
  Virtual.CreateOrdinalSeries(src)
