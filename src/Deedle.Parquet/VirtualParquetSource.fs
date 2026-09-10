namespace Deedle.Parquet.Virtual.Sources

open System
open System.Collections.Concurrent
open System.IO
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Parquet.Schema

/// Options for [`Virtual.ReadParquet`].
type VirtualReadParquetOptions =
  { IndexColumn: string option
    SearchColumns: VirtualSearchColumn list
    ColumnKeys: string list option }

  static member Default =
    { IndexColumn = None
      SearchColumns = []
      ColumnKeys = None }

open Deedle.Parquet

/// Column CLR kinds aligned with [`Implementation.netTypeToDataField`] / `readColumn`.
[<RequireQualifiedAccess>]
type internal ParquetColumnKind =
  | Float | Float32 | Int | Int64 | Int16 | Byte
  | UInt16 | UInt32 | UInt64 | Bool | String | DateTime | DateTimeOffset

module private OptionalArrays =
  let sumPresent (values: OptionalValue<float>[]) =
    values
    |> Array.fold (fun acc ov ->
      match ov with
      | OptionalValue.Present value when not (Double.IsNaN value) -> acc + value
      | _ -> acc) 0.0

  let mapPresent (f: obj -> 'T) (values: OptionalValue<obj>[]) : OptionalValue<'T>[] =
    values |> Array.map (fun ov ->
      match ov with
      | OptionalValue.Present value -> OptionalValue(f value)
      | _ -> OptionalValue.Missing)

/// Shared Parquet file handle: schema, row count, and lazily loaded column arrays.
/// Column sources capture this instance so the file stays open for the virtual frame lifetime;
/// dispose explicitly only for short-lived validation helpers.
type ParquetFileIndex(path: string) =
  let stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
  let reader = global.Parquet.ParquetReader.CreateAsync(stream).GetAwaiter().GetResult()
  let dataFields = reader.Schema.GetDataFields()
  let mutable disposed = false
  // Prefer metadata NumRows — never ReadEntireRowGroup just to count.
  let rowCount =
    match reader.Metadata <> null && reader.Metadata.NumRows > 0L, reader.RowGroupCount with
    | true, _ -> reader.Metadata.NumRows
    | false, 0 -> 0L
    | false, _ ->
      [| 0 .. reader.RowGroupCount - 1 |]
      |> Array.sumBy (fun rgIdx ->
        use rgReader = reader.OpenRowGroupReader(rgIdx)
        int64 rgReader.RowCount)
  let columnCache = ConcurrentDictionary<string, obj>()

  member _.Path = path
  member _.Length = rowCount
  member _.DataFields = dataFields

  member _.FieldIndex(name: string) =
    match dataFields |> Array.tryFindIndex (fun f -> String.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase)) with
    | Some idx -> idx
    | None -> failwithf "VirtualParquetSource: column '%s' not found" name

  /// Read only the named column from each row group (not the entire row group).
  member private this.ReadColumn (name: string) =
    if disposed then invalidOp "ParquetFileIndex: disposed"
    let field = dataFields.[this.FieldIndex name]
    [| for rgIdx in 0 .. reader.RowGroupCount - 1 do
         use rgReader = reader.OpenRowGroupReader(rgIdx)
         yield rgReader.ReadColumnAsync(field).GetAwaiter().GetResult() |]

  /// Boxed optional cells via [`Implementation.readColumn`] + `IVector.ObjectSequence`.
  member private this.ReadColumnValues (name: string) =
    let values = ResizeArray<OptionalValue<obj>>()
    for col in this.ReadColumn name do
      let (_, vec) = Implementation.readColumn col
      for ov in vec.ObjectSequence do
        values.Add(ov)
    values.ToArray()

  /// Cache typed column arrays. `cacheKey` distinguishes conversions of the same field
  /// (e.g. DateTime vs DateTimeOffset for the index).
  member private this.Materialize (cacheKey: string) (build: unit -> obj) =
    columnCache.GetOrAdd(cacheKey, fun _ -> build())

  /// Exact CLR type from `readColumn` (no widening/narrowing).
  member this.ReadTypedColumn<'T>(name: string) =
    this.Materialize (name + "#:" + typeof<'T>.FullName) (fun () ->
      box (OptionalArrays.mapPresent unbox<'T> (this.ReadColumnValues name)))
    :?> OptionalValue<'T>[]

  /// Float column (nulls → Missing). Kept as a named alias for benchmarks/tests.
  member this.ReadFloatColumn(name: string) =
    this.ReadTypedColumn<float>(name)

  /// Index helper: accepts DateTime or DateTimeOffset cells.
  member this.ReadDateTimeOffsetColumn(name: string) =
    this.Materialize (name + "#:DateTimeOffset") (fun () ->
      box (OptionalArrays.mapPresent (fun v ->
        match v with
        | :? DateTimeOffset as dto -> dto
        | :? DateTime as dt -> DateTimeOffset(dt)
        | _ -> unbox<DateTimeOffset> v) (this.ReadColumnValues name)))
    :?> OptionalValue<DateTimeOffset>[]

  interface IDisposable with
    member _.Dispose() =
      if not disposed then
        disposed <- true
        (reader :> IDisposable).Dispose()
        stream.Dispose()

module internal ParquetColumnSource =
  /// Capture `index` in the value closure so the file handle outlives frame construction.
  let private optionalSource
      (index: ParquetFileIndex)
      (data: OptionalValue<'T>[])
      (asLong: ('T -> int64) option)
      (lookupRange: LookupRangeMode<'T>)
      (searchColumnConfigured: bool) =
    OrdinalVirtualSource(
      index.Length,
      (fun row ->
        GC.KeepAlive(index)
        data.[int row]),
      "parquet-file",
      ?asLong=asLong,
      lookupRange=lookupRange,
      searchColumnConfigured=searchColumnConfigured)
    :> IVirtualVectorSource

  let private typedResolved<'T> (index: ParquetFileIndex) (name: string) (asLong: ('T -> int64) option) (resolved: ResolvedColumnSearch) (pick: ResolvedColumnSearch -> LookupRangeMode<'T> option) =
    optionalSource index (index.ReadTypedColumn<'T>(name)) asLong (defaultArg (pick resolved) LookupRangeUnsupported) resolved.Configured

  let private typedPlain<'T> (index: ParquetFileIndex) (name: string) (asLong: ('T -> int64) option) (mode: LookupRangeMode<'T>) =
    optionalSource index (index.ReadTypedColumn<'T>(name)) asLong mode false

  let createFloat (index: ParquetFileIndex) (name: string) (resolved: ResolvedColumnSearch) =
    typedResolved<float> index name None resolved (fun r -> r.Float)

  let createFloat32 (index: ParquetFileIndex) (name: string) =
    typedPlain<float32> index name None LookupRangeUnsupported

  let createInt (index: ParquetFileIndex) (name: string) =
    typedPlain<int> index name (Some int64) LookupRangeUnsupported

  let createInt64 (index: ParquetFileIndex) (name: string) (resolved: ResolvedColumnSearch) =
    typedResolved<int64> index name (Some id) resolved (fun r -> r.Int64)

  let createInt16 (index: ParquetFileIndex) (name: string) =
    typedPlain<int16> index name (Some int64) LookupRangeUnsupported

  let createByte (index: ParquetFileIndex) (name: string) =
    typedPlain<byte> index name (Some int64) LookupRangeUnsupported

  let createUInt16 (index: ParquetFileIndex) (name: string) =
    typedPlain<uint16> index name (Some int64) LookupRangeUnsupported

  let createUInt32 (index: ParquetFileIndex) (name: string) =
    typedPlain<uint32> index name (Some int64) LookupRangeUnsupported

  let createUInt64 (index: ParquetFileIndex) (name: string) =
    // Full uint64 range does not fit int64; LookupValue is unsupported for this column.
    typedPlain<uint64> index name None LookupRangeUnsupported

  let createBool (index: ParquetFileIndex) (name: string) =
    typedPlain<bool> index name None LookupRangeUnsupported

  let createString (index: ParquetFileIndex) (name: string) (resolved: ResolvedColumnSearch) =
    typedResolved<string> index name None resolved (fun r -> r.String)

  let createDateTime (index: ParquetFileIndex) (name: string) =
    typedPlain<DateTime> index name (Some (fun (dt: DateTime) -> DateTimeOffset(dt).UtcTicks)) LookupRangeUnsupported

  let createDateTimeOffset (index: ParquetFileIndex) (name: string) =
    optionalSource index (index.ReadDateTimeOffsetColumn name) (Some (fun dto -> dto.UtcTicks)) LookupRangeUnsupported false

  let resolveIndexColumn (fields: DataField[]) (options: VirtualReadParquetOptions) =
    match options.IndexColumn with
    | Some name ->
      match fields |> Array.tryFindIndex (fun f -> String.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase)) with
      | Some idx -> idx
      | None -> failwithf "VirtualParquetSource: index column '%s' not found" name
    | None ->
      let preferred =
        fields
        |> Array.tryFindIndex (fun f ->
          String.Equals(f.Name, "Timestamp", StringComparison.OrdinalIgnoreCase)
          || String.Equals(f.Name, "DateTime", StringComparison.OrdinalIgnoreCase)
          || f.Name.EndsWith("Time", StringComparison.OrdinalIgnoreCase))
      match preferred with
      | Some idx -> idx
      | None -> 0

  let columnKind (field: DataField) =
    let clrType = field.ClrType
    let baseType =
      match Nullable.GetUnderlyingType clrType with
      | null -> clrType
      | ut -> ut
    match baseType with
    | t when t = typeof<float> || t = typeof<double> -> ParquetColumnKind.Float
    | t when t = typeof<float32> -> ParquetColumnKind.Float32
    | t when t = typeof<int> -> ParquetColumnKind.Int
    | t when t = typeof<int64> -> ParquetColumnKind.Int64
    | t when t = typeof<int16> -> ParquetColumnKind.Int16
    | t when t = typeof<byte> -> ParquetColumnKind.Byte
    | t when t = typeof<uint16> -> ParquetColumnKind.UInt16
    | t when t = typeof<uint32> -> ParquetColumnKind.UInt32
    | t when t = typeof<uint64> -> ParquetColumnKind.UInt64
    | t when t = typeof<bool> -> ParquetColumnKind.Bool
    | t when t = typeof<string> -> ParquetColumnKind.String
    | t when t = typeof<DateTime> -> ParquetColumnKind.DateTime
    | t when t = typeof<DateTimeOffset> -> ParquetColumnKind.DateTimeOffset
    | _ -> ParquetColumnKind.String

module VirtualParquetSource =
  open ParquetColumnSource

  let private parquetKindName kind =
    match kind with
    | ParquetColumnKind.String -> "string"
    | ParquetColumnKind.Int64 -> "int64"
    | ParquetColumnKind.Float | ParquetColumnKind.Float32 -> "float"
    | _ -> "other"

  let private createTypedColumn (index: ParquetFileIndex) (name: string) (kind: ParquetColumnKind) (resolved: ResolvedColumnSearch) =
    match kind with
    | ParquetColumnKind.Float -> createFloat index name resolved
    | ParquetColumnKind.Float32 -> createFloat32 index name
    | ParquetColumnKind.Int -> createInt index name
    | ParquetColumnKind.Int64 -> createInt64 index name resolved
    | ParquetColumnKind.Int16 -> createInt16 index name
    | ParquetColumnKind.Byte -> createByte index name
    | ParquetColumnKind.UInt16 -> createUInt16 index name
    | ParquetColumnKind.UInt32 -> createUInt32 index name
    | ParquetColumnKind.UInt64 -> createUInt64 index name
    | ParquetColumnKind.Bool -> createBool index name
    | ParquetColumnKind.String -> createString index name resolved
    | ParquetColumnKind.DateTime -> createDateTime index name
    | ParquetColumnKind.DateTimeOffset -> createDateTimeOffset index name

  let createFrame (parquetPath: string) (options: VirtualReadParquetOptions) =
    if not (File.Exists parquetPath) then failwithf "VirtualParquetSource: file not found '%s'" parquetPath
    // Do not dispose: column sources keep `fileIndex` alive for the frame lifetime.
    let fileIndex = new ParquetFileIndex(parquetPath)
    if fileIndex.Length = 0L then invalidArg "parquetPath" "Parquet file has no data rows"
    let fields = fileIndex.DataFields
    if fields.Length = 0 then invalidArg "parquetPath" "Parquet file has no columns"
    let indexCol = resolveIndexColumn fields options
    let indexName = fields.[indexCol].Name
    let indexSource = createDateTimeOffset fileIndex indexName :?> IVirtualVectorSource<DateTimeOffset>
    let valueColumnNames =
      fields
      |> Array.mapi (fun i f -> i, f.Name)
      |> Array.filter (fun (i, _) -> i <> indexCol)
      |> Array.toList
    let keys =
      match options.ColumnKeys with
      | Some ks -> ks
      | None -> valueColumnNames |> List.map snd
    let resolveSearchForColumn (name: string) (kind: ParquetColumnKind) =
      let kindName = parquetKindName kind
      VirtualLookupRange.resolveSearchColumnsLookupRange
        "Deedle.Virtual.ReadParquet"
        options.SearchColumns
        name
        kindName
        (fun () ->
          let data = fileIndex.ReadTypedColumn<string>(name)
          let valueAt row =
            match data.[int row] with
            | OptionalValue.Present value -> value
            | _ -> ""
          VirtualLookupRange.tryInferStringLookupRange fileIndex.Length valueAt)
        (fun () ->
          let data = fileIndex.ReadTypedColumn<int64>(name)
          VirtualLookupRange.tryInferInt64LookupRange fileIndex.Length (fun row ->
            match data.[int row] with
            | OptionalValue.Present value -> Some value
            | _ -> None))
        (fun () ->
          let data = fileIndex.ReadTypedColumn<float>(name)
          VirtualLookupRange.tryInferFloatLookupRange fileIndex.Length (fun row ->
            match data.[int row] with
            | OptionalValue.Present value -> Some value
            | _ -> None))
    let sources =
      keys
      |> List.map (fun name ->
          let colIdx = fileIndex.FieldIndex name
          let kind = columnKind fields.[colIdx]
          createTypedColumn fileIndex name kind (resolveSearchForColumn name kind))
    Virtual.CreateFrame(indexSource, keys, sources)

[<AutoOpen>]
module VirtualParquetExtensions =
  type Deedle.Virtual.Virtual with
    /// Load a Parquet file as a virtual frame with an ordered row index.
    /// Requested columns are read into memory and cached; the underlying file handle
    /// stays reachable for the lifetime of the returned frame.
    /// Column CLR types match [`Frame.readParquet`] / `Implementation.readColumn`.
    static member ReadParquet(path: string, ?indexColumn: string, ?searchColumns: VirtualSearchColumn list, ?columnKeys: string list) =
      let options : VirtualReadParquetOptions =
        { IndexColumn = indexColumn
          SearchColumns = defaultArg searchColumns []
          ColumnKeys = columnKeys }
      VirtualParquetSource.createFrame path options
