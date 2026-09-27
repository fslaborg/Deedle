module Deedle.TestData.ParquetTestData

open System
open System.Globalization
open System.IO
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Virtual.Sources
open Deedle.TestData
open Deedle.Parquet
open Deedle.Parquet.Virtual.Sources
open Parquet.Schema
open Parquet.Data

let defaultDatasetName = "b6-search-100k-random.parquet"

let createFloatValueSeries (parquetPath: string) =
  // Keep index alive via the value closure (same lifetime rule as Virtual.ReadParquet).
  let fileIndex = new ParquetFileIndex(parquetPath)
  let data = fileIndex.ReadFloatColumn "Value"
  let src =
    OrdinalVirtualSource(
      fileIndex.Length,
      (fun row ->
        GC.KeepAlive(fileIndex)
        data.[int row]),
      "parquet-file")
    :> IVirtualVectorSource<float>
  Virtual.CreateOrdinalSeries(src)

let private parquetValueSumMatches (parquetPath: string) (expectedSum: float) (rowCount: int64) =
  try
    use idx = new ParquetFileIndex(parquetPath)
    if idx.Length <> rowCount then false
    else
      let actual =
        idx.ReadFloatColumn "Value"
        |> Array.fold (fun acc ov ->
          match ov with
          | OptionalValue.Present value when not (Double.IsNaN value) -> acc + value
          | _ -> acc) 0.0
      abs (actual - expectedSum) < 1.0
  with _ -> false

let private parseRow (lineIndex: CsvLineIndex) row =
  let parts = lineIndex.ReadFields(row)
  let dto =
    DateTimeOffset.Parse(parts.[1], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)

  Nullable(Int64.Parse(parts.[0], CultureInfo.InvariantCulture)),
  Nullable(dto.UtcDateTime),
  parts.[2],
  parts.[3],
  Nullable(Double.Parse(parts.[4], CultureInfo.InvariantCulture))

let private writeTypedSearchParquet (parquetPath: string) (csvPath: string) =
  // Stream CSV fields into typed Parquet columns (schema CLR types drive Virtual.ReadParquet).
  // Parquet.Net rejects DateTimeOffset fields — store UTC DateTime and convert on read.
  // Use CsvLineIndex so quoted commas match Virtual.ReadCsv parsing.
  let lineIndex = CsvLineIndex(csvPath)
  let rows = [| for row in 0L .. lineIndex.Length - 1L -> parseRow lineIndex row |]
  let schema = ParquetSchema([|
    DataField("Id", typeof<Nullable<int64>>) :> Field
    DataField("Timestamp", typeof<Nullable<DateTime>>) :> Field
    DataField("Category", typeof<string>) :> Field
    DataField("Label", typeof<string>) :> Field
    DataField("Value", typeof<Nullable<float>>) :> Field |])
  let dataFields = schema.GetDataFields()
  let ids = rows |> Array.map (fun (id, _, _, _, _) -> id)
  let timestamps = rows |> Array.map (fun (_, timestamp, _, _, _) -> timestamp)
  let categories = rows |> Array.map (fun (_, _, category, _, _) -> category)
  let labels = rows |> Array.map (fun (_, _, _, label, _) -> label)
  let values = rows |> Array.map (fun (_, _, _, _, value) -> value)
  if File.Exists parquetPath then File.Delete parquetPath
  use stream = File.Create parquetPath
  use writer = global.Parquet.ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
  use rg = writer.CreateRowGroup()
  rg.WriteColumnAsync(DataColumn(dataFields.[0], ids)).GetAwaiter().GetResult()
  rg.WriteColumnAsync(DataColumn(dataFields.[1], timestamps)).GetAwaiter().GetResult()
  rg.WriteColumnAsync(DataColumn(dataFields.[2], categories)).GetAwaiter().GetResult()
  rg.WriteColumnAsync(DataColumn(dataFields.[3], labels)).GetAwaiter().GetResult()
  rg.WriteColumnAsync(DataColumn(dataFields.[4], values)).GetAwaiter().GetResult()

let ensureSearchParquet (parquetPath: string) (rowCount: int64) =
  let csvPath = Path.ChangeExtension(parquetPath, ".csv")
  CsvTestData.ensureSearchCsv csvPath rowCount |> ignore
  let expectedSum = CsvTestData.readMeta(csvPath).ValueSum
  if parquetValueSumMatches parquetPath expectedSum rowCount then parquetPath
  else
    writeTypedSearchParquet parquetPath csvPath
    if not (parquetValueSumMatches parquetPath expectedSum rowCount) then
      failwithf
        "ParquetTestData: regenerated '%s' but Value sum still mismatches CSV meta (expected ~%g)"
        parquetPath expectedSum
    parquetPath
