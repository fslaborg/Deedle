namespace Deedle.Virtual.Sources

open System
open System.Globalization
open System.IO
open System.Text
open FSharp.Data
open Deedle
open Deedle.Vectors.Virtual
open Deedle.Virtual

module internal CsvParsing =
  /// RFC4180-ish CSV field split (quotes, escaped quotes). Does not handle embedded newlines.
  let splitCsvLine (line: string) =
    let acc = ResizeArray<string>()
    let sb = StringBuilder()
    let mutable i = 0
    let mutable inQuotes = false
    while i < line.Length do
      let c = line.[i]
      if inQuotes then
        if c = '"' then
          if i + 1 < line.Length && line.[i + 1] = '"' then
            sb.Append('"') |> ignore
            i <- i + 2
          else
            inQuotes <- false
            i <- i + 1
        else
          sb.Append(c) |> ignore
          i <- i + 1
      else
        match c with
        | '"' ->
            inQuotes <- true
            i <- i + 1
        | ',' ->
            acc.Add(sb.ToString())
            sb.Clear() |> ignore
            i <- i + 1
        | _ ->
            sb.Append(c) |> ignore
            i <- i + 1
    acc.Add(sb.ToString().TrimEnd('\r', '\n'))
    acc.ToArray()

  /// Scan a UTF-8 CSV for physical line starts (CRLF/LF). Does not treat quoted embedded newlines as one record.
  let indexPhysicalLineOffsets (path: string) (skipHeader: bool) =
    use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
    if fs.Length >= 3L then
      let bom = Array.zeroCreate 3
      fs.Read(bom, 0, 3) |> ignore
      if bom.[0] <> 0xEFuy || bom.[1] <> 0xBBuy || bom.[2] <> 0xBFuy then
        fs.Seek(0L, SeekOrigin.Begin) |> ignore
    let offs = ResizeArray<int64>()
    let mutable skip = skipHeader
    let mutable lineStart = fs.Position
    let rec consume () =
      let b = fs.ReadByte()
      if b < 0 then
        if not skip && fs.Position > lineStart then offs.Add(lineStart)
      elif b = 10 then
        if not skip then offs.Add(lineStart)
        skip <- false
        lineStart <- fs.Position
        consume ()
      else consume ()
    consume ()
    offs.ToArray()

  let readPhysicalLineAt (path: string) (offset: int64) =
    use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
    fs.Seek(offset, SeekOrigin.Begin) |> ignore
    let buf = ResizeArray<byte>()
    let mutable b = fs.ReadByte()
    while b >= 0 && b <> 10 do
      if b <> 13 then buf.Add(byte b)
      b <- fs.ReadByte()
    Encoding.UTF8.GetString(buf.ToArray())

  let field (fields: string[]) (columnIndex: int) =
    if columnIndex >= fields.Length then
      invalidOp (sprintf "VirtualCsvSource: column %d missing (fields=%d)" columnIndex fields.Length)
    fields.[columnIndex].TrimEnd('\r', '\n')

  let isMissingCell (s: string) =
    let t = s.Trim()
    String.IsNullOrEmpty t ||
    Array.exists (fun m -> String.Equals(t, m, StringComparison.OrdinalIgnoreCase)) TextConversions.DefaultMissingValues

  let tryParseInt64 (s: string) =
    match Int64.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture) with
    | true, v -> Some v
    | false, _ -> None

  let tryParseFloat (s: string) =
    match Double.TryParse(s.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture) with
    | true, v -> Some v
    | false, _ -> None

  let tryParseDateTime (s: string) =
    match DateTimeOffset.TryParse(s.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) with
    | true, dto -> Some dto
    | false, _ -> None

  let parseDateTimeStrict (s: string) =
    DateTimeOffset.Parse(s.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)

  let columnIndex (header: string[]) (name: string) =
    match header |> Array.tryFindIndex (fun h -> String.Equals(h, name, StringComparison.OrdinalIgnoreCase)) with
    | Some idx -> idx
    | None -> invalidArg "name" (sprintf "VirtualCsvSource: column '%s' not found in header" name)

/// Shared row index for one CSV file (built once, reused by column sources).
/// By default stores physical line start offsets and seeks on read. Pass [`byteOffset=false`] to cache every line string in RAM.
type CsvLineIndex(path: string, ?hasHeaders: bool, ?byteOffset: bool) =
  let hasHeaders = defaultArg hasHeaders true
  let byteOffset = defaultArg byteOffset true
  let lines, offsets =
    if byteOffset then
      [||], CsvParsing.indexPhysicalLineOffsets path hasHeaders
    else
      use reader = new StreamReader(path)
      if hasHeaders then reader.ReadLine() |> ignore
      let acc = ResizeArray<string>()
      while not reader.EndOfStream do
        acc.Add(reader.ReadLine())
      acc.ToArray(), [||]
  let rowCount = if byteOffset then offsets.Length else lines.Length
  let fieldCache : string[][] = Array.create rowCount null
  let cacheLock = obj()
  let mutable splitCount = 0

  member _.Path = path
  member _.Length = int64 rowCount
  member _.IsByteOffset = byteOffset
  /// Number of CSV rows split since construction or last [`ResetSplitCount`].
  member _.SplitCount = splitCount
  /// Reset [`SplitCount`] (for tests and diagnostics).
  member _.ResetSplitCount() = splitCount <- 0

  member _.ReadFields(row: int64) =
    let i = int row
    if i < 0 || i >= fieldCache.Length then
      invalidArg "row" (sprintf "CsvLineIndex: row %d out of range [0, %d)" row fieldCache.Length)
    match fieldCache.[i] with
    | null ->
        lock cacheLock (fun () ->
          match fieldCache.[i] with
          | null ->
              splitCount <- splitCount + 1
              let raw = if byteOffset then CsvParsing.readPhysicalLineAt path offsets.[i] else lines.[i]
              let fields = CsvParsing.splitCsvLine raw
              fieldCache.[i] <- fields
              fields
          | fields -> fields)
    | fields -> fields

  member _.HeaderColumns =
    if hasHeaders then
      use reader = new StreamReader(path)
      match reader.ReadLine() with
      | null -> [||]
      | line -> CsvParsing.splitCsvLine line |> Array.map (fun s -> s.Trim())
    elif rowCount = 0 then
      [||]
    else
      let firstLine =
        if byteOffset then CsvParsing.readPhysicalLineAt path offsets.[0]
        else lines.[0]
      let columnCount = CsvParsing.splitCsvLine firstLine |> Array.length
      [| for i in 1 .. columnCount -> sprintf "Column%d" i |]

module VirtualCsvSource =
  open CsvParsing

  let private parseInt64Strict (s: string) =
    Int64.Parse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture)

  let private parseFloatStrict (s: string) =
    Double.Parse(s.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture)

  let private parseStringStrict (s: string) =
    let t = s.TrimEnd('\r', '\n')
    if isMissingCell t then invalidArg "index" "VirtualCsvSource: index column value is missing"
    t

  type private VirtualCsvFrameHelper =
    static member CreateOrdered<'T when 'T : equality>(index: IVirtualVectorSource<'T>, keys, sources) =
      box (Virtual.CreateFrame(index, keys, sources))

  let private looksLikeDateTime (s: string) =
    s.IndexOf('-') >= 0 || s.IndexOf('/') >= 0 || s.IndexOf('T') >= 0

  let private inferColumnKind (index: CsvLineIndex) (columnIndex: int) (sampleRows: int) =
    if index.Length = 0L then "string"
    else
      let sampleCount = min sampleRows (int index.Length)
      let samples =
        [ for row in 0 .. sampleCount - 1 ->
            field (index.ReadFields(int64 row)) columnIndex ]
        |> List.filter (not << isMissingCell)
      match samples with
      | [] -> "string"
      // Prefer numerics over DateTimeOffset.TryParse, which accepts bare integers like "1".
      | _ when List.forall (tryParseInt64 >> Option.isSome) samples -> "int64"
      | _ when List.forall (tryParseFloat >> Option.isSome) samples -> "float"
      | _ when List.forall (fun s -> Option.isSome (tryParseDateTime s) && looksLikeDateTime s) samples -> "datetime"
      | _ -> "string"

  let private createOptionalColumn (lineIndex: CsvLineIndex) columnIndex (tryParse: string -> 'T option)
      (asLong: ('T -> int64) option) (lookupRange: LookupRangeMode<'T> option) (searchColumnConfigured: bool) =
    let valueAt row =
      let s = field (lineIndex.ReadFields row) columnIndex
      if isMissingCell s then OptionalValue.Missing
      else
        match tryParse s with
        | Some v -> OptionalValue(v)
        | None -> OptionalValue.Missing
    match lookupRange with
    | Some mode ->
        OrdinalVirtualSource(lineIndex.Length, valueAt, "csv-file", ?asLong=asLong, lookupRange=mode, searchColumnConfigured=searchColumnConfigured)
        :> IVirtualVectorSource
    | None ->
        OrdinalVirtualSource(lineIndex.Length, valueAt, "csv-file", ?asLong=asLong, searchColumnConfigured=searchColumnConfigured)
        :> IVirtualVectorSource

  /// Index columns stay strict: empty/invalid index cells throw (row keys must be present).
  let private createStrictColumn (lineIndex: CsvLineIndex) columnIndex (parse: string -> 'T)
      (asLong: ('T -> int64) option) =
    let valueAt row =
      let s = field (lineIndex.ReadFields row) columnIndex
      OptionalValue(parse s)
    OrdinalVirtualSource(lineIndex.Length, valueAt, "csv-file", ?asLong=asLong) :> IVirtualVectorSource

  let private stringValueAt (lineIndex: CsvLineIndex) (columnIndex: int) =
    fun (row: int64) ->
      let s = field (lineIndex.ReadFields row) columnIndex
      if isMissingCell s then "" else s.Trim()

  let private optionalInt64At (lineIndex: CsvLineIndex) (columnIndex: int) (row: int64) =
    let s = field (lineIndex.ReadFields row) columnIndex
    if isMissingCell s then None else tryParseInt64 s

  let private optionalFloatAt (lineIndex: CsvLineIndex) (columnIndex: int) (row: int64) =
    let s = field (lineIndex.ReadFields row) columnIndex
    if isMissingCell s then None else tryParseFloat s

  let private resolveSearchLookupRanges (lineIndex: CsvLineIndex) (colIdx: int) (name: string) (kind: string) (options: VirtualReadCsvOptions) =
    VirtualLookupRange.resolveSearchColumnsLookupRange
      "Deedle.Virtual.ReadCsv"
      options.SearchColumns
      name
      kind
      (fun () -> VirtualLookupRange.tryInferStringLookupRange lineIndex.Length (stringValueAt lineIndex colIdx))
      (fun () -> VirtualLookupRange.tryInferInt64LookupRange lineIndex.Length (optionalInt64At lineIndex colIdx))
      (fun () -> VirtualLookupRange.tryInferFloatLookupRange lineIndex.Length (optionalFloatAt lineIndex colIdx))

  let private createTypedColumn (lineIndex: CsvLineIndex) (columnIndex: int) (kind: string) (resolved: ResolvedColumnSearch) =
    let configured = resolved.Configured
    match kind with
    | "datetime" ->
      createOptionalColumn lineIndex columnIndex tryParseDateTime (Some (fun dto -> dto.UtcTicks)) None false
    | "int64" ->
      createOptionalColumn lineIndex columnIndex tryParseInt64 (Some id) resolved.Int64 configured
    | "float" ->
      createOptionalColumn lineIndex columnIndex tryParseFloat None resolved.Float configured
    | _ ->
      createOptionalColumn lineIndex columnIndex (fun s -> Some s) None resolved.String configured

  let private warnOrdinalIndex (apiName: string) (columnName: string) (reason: string) =
    System.Diagnostics.Trace.TraceWarning(
      sprintf "%s: column '%s' %s; using ordinal row index 0..N-1 instead." apiName columnName reason)

  /// Strict ascending and unique in file row order (O(N) scan at load).
  let private isOrderedUniqueIndex (lineIndex: CsvLineIndex) (columnIndex: int) (kind: string) =
    if lineIndex.Length <= 1L then true
    else
      let mutable ok = true
      let mutable prevInt = 0L
      let mutable prevDt = DateTimeOffset.MinValue
      let mutable prevFl = 0.0
      let mutable prevStr = ""
      let mutable hasPrev = false
      for row in 0L .. lineIndex.Length - 1L do
        if not ok then ()
        else
          let s = field (lineIndex.ReadFields row) columnIndex
          if isMissingCell s then ok <- false
          else
            match kind with
            | "int64" ->
                match tryParseInt64 s with
                | None -> ok <- false
                | Some v ->
                    if hasPrev && v <= prevInt then ok <- false
                    else prevInt <- v; hasPrev <- true
            | "datetime" ->
                match tryParseDateTime s with
                | None -> ok <- false
                | Some v ->
                    if hasPrev && v.UtcTicks <= prevDt.UtcTicks then ok <- false
                    else prevDt <- v; hasPrev <- true
            | "float" ->
                match tryParseFloat s with
                | None -> ok <- false
                | Some v ->
                    if hasPrev && v <= prevFl then ok <- false
                    else prevFl <- v; hasPrev <- true
            | _ ->
                let v = s.TrimEnd('\r', '\n')
                if hasPrev && String.Compare(v, prevStr, StringComparison.Ordinal) <= 0 then ok <- false
                else prevStr <- v; hasPrev <- true
      ok

  let private createStrictIndexSource (lineIndex: CsvLineIndex) (columnIndex: int) (kind: string) =
    match kind with
    | "int64" ->
      createStrictColumn lineIndex columnIndex parseInt64Strict (Some id)
    | "datetime" ->
      createStrictColumn lineIndex columnIndex parseDateTimeStrict (Some (fun dto -> dto.UtcTicks))
    | "float" ->
      createStrictColumn lineIndex columnIndex parseFloatStrict (Some BitConverter.DoubleToInt64Bits)
    | _ ->
      createStrictColumn lineIndex columnIndex parseStringStrict None

  let private valueColumnNames (header: string[]) (indexCol: int option) (columnKeys: string list option) =
    match columnKeys with
    | Some ks -> ks
    | None ->
        header
        |> Array.mapi (fun i name -> i, name)
        |> Array.filter (fun (i, _) -> match indexCol with None -> true | Some idx -> i <> idx)
        |> Array.map snd
        |> Array.toList

  let private columnSources (lineIndex: CsvLineIndex) (header: string[]) (keys: string list) (options: VirtualReadCsvOptions) =
    keys
    |> List.map (fun name ->
        let colIdx = columnIndex header name
        let kind = inferColumnKind lineIndex colIdx 100
        let resolved = resolveSearchLookupRanges lineIndex colIdx name kind options
        createTypedColumn lineIndex colIdx kind resolved)

  let private resolveHeader (lineIndex: CsvLineIndex) (options: VirtualReadCsvOptions) =
    let header = lineIndex.HeaderColumns
    if options.HasHeaders && header.Length = 0 then
      invalidArg "csvPath" "CSV has no header row"
    header

  let private createOrdinalFrameFromLineIndex (lineIndex: CsvLineIndex) (options: VirtualReadCsvOptions) =
    let header = resolveHeader lineIndex options
    let keys = valueColumnNames header None options.ColumnKeys
    let sources = columnSources lineIndex header keys options
    Virtual.CreateOrdinalFrame(keys, sources)

  let private createOrderedFrameFromLineIndex (lineIndex: CsvLineIndex) (indexCol: int) (_indexName: string) (options: VirtualReadCsvOptions) =
    let header = resolveHeader lineIndex options
    let kind = inferColumnKind lineIndex indexCol 100
    let indexSource = createStrictIndexSource lineIndex indexCol kind
    let keys = valueColumnNames header (Some indexCol) options.ColumnKeys
    let sources = columnSources lineIndex header keys options
    match kind with
    | "int64" ->
        VirtualCsvFrameHelper.CreateOrdered<int64>(indexSource :?> IVirtualVectorSource<int64>, keys, sources)
    | "datetime" ->
        VirtualCsvFrameHelper.CreateOrdered<DateTimeOffset>(indexSource :?> IVirtualVectorSource<DateTimeOffset>, keys, sources)
    | "float" ->
        VirtualCsvFrameHelper.CreateOrdered<float>(indexSource :?> IVirtualVectorSource<float>, keys, sources)
    | _ ->
        VirtualCsvFrameHelper.CreateOrdered<string>(indexSource :?> IVirtualVectorSource<string>, keys, sources)

  /// Build a virtual frame with ordinal row index `0 .. N-1`.
  let createOrdinalFrame (csvPath: string) (options: VirtualReadCsvOptions) =
    if not (File.Exists csvPath) then raise (FileNotFoundException(sprintf "VirtualCsvSource: file not found '%s'" csvPath, csvPath))
    let lineIndex = CsvLineIndex(csvPath, hasHeaders=options.HasHeaders, byteOffset=options.ByteOffsetIndex)
    if lineIndex.Length = 0L then invalidArg "csvPath" "CSV has no data rows"
    createOrdinalFrameFromLineIndex lineIndex options

  /// Build a virtual frame from a CSV file (boxed; unbox at the API boundary).
  /// With `IndexColumn = None`, uses ordinal rows `0 .. N-1`.
  /// With `IndexColumn = Some name`, uses that column when strictly increasing and unique in file order; otherwise ordinal with a trace warning.
  let createFrame (csvPath: string) (options: VirtualReadCsvOptions) =
    if not (File.Exists csvPath) then raise (FileNotFoundException(sprintf "VirtualCsvSource: file not found '%s'" csvPath, csvPath))
    let lineIndex = CsvLineIndex(csvPath, hasHeaders=options.HasHeaders, byteOffset=options.ByteOffsetIndex)
    if lineIndex.Length = 0L then invalidArg "csvPath" "CSV has no data rows"
    let header = resolveHeader lineIndex options
    let frame =
      match options.IndexColumn with
      | None ->
          box (createOrdinalFrameFromLineIndex lineIndex options)
      | Some indexName ->
          let indexCol = columnIndex header indexName
          let kind = inferColumnKind lineIndex indexCol 100
          if isOrderedUniqueIndex lineIndex indexCol kind then
            createOrderedFrameFromLineIndex lineIndex indexCol indexName options
          else
            warnOrdinalIndex "Deedle.Virtual.ReadCsv" indexName "is not strictly increasing and unique in file order"
            box (createOrdinalFrameFromLineIndex lineIndex options)
    frame

  let createIndexSource (lineIndex: CsvLineIndex) (columnName: string) =
    let colIdx = columnIndex lineIndex.HeaderColumns columnName
    let kind = inferColumnKind lineIndex colIdx 100
    createStrictIndexSource lineIndex colIdx kind

  /// Create a value column source for a CSV file (type inferred from sample rows).
  let createColumnSource (lineIndex: CsvLineIndex) (columnName: string) (stringLookupRange: LookupRangeMode<string> option) =
    let colIdx = columnIndex lineIndex.HeaderColumns columnName
    let kind = inferColumnKind lineIndex colIdx 100
    let resolved =
      match stringLookupRange with
      | None -> ResolvedColumnSearch.Empty
      | Some mode ->
        { ResolvedColumnSearch.Empty with String = Some mode; Configured = true }
    createTypedColumn lineIndex colIdx kind resolved

  /// Resolve the configured index column name, if any.
  let resolveIndexColumnName (header: string[]) (options: VirtualReadCsvOptions) =
    match options.IndexColumn with
    | None -> invalidArg "options" "VirtualCsvSource: no index column configured"
    | Some name -> name

  /// Map a global ordinal row to (part index, row-in-part).
  let private locatePartRow (partSizes: int[]) (i: int64) =
    let rec loop part acc =
      let n = int64 partSizes.[part]
      if i < acc + n then part, i - acc
      else loop (part + 1) (acc + n)
    loop 0 0L

  /// Concatenate CSV files as one ordinal virtual frame (sorted paths). Shared schema required.
  /// Uses linear 0..N-1 addressing so existing [`OrdinalVirtualSource`] LookupRange applies as-is.
  let createConcatenatedFrame (csvPaths: string[]) (options: VirtualReadCsvOptions) =
    if csvPaths.Length = 0 then invalidArg "csvPaths" "At least one CSV file is required"
    let indexes =
      csvPaths |> Array.map (fun p ->
        if not (File.Exists p) then raise (FileNotFoundException(sprintf "VirtualCsvSource: file not found '%s'" p, p))
        CsvLineIndex(p, hasHeaders=options.HasHeaders, byteOffset=options.ByteOffsetIndex))
    if indexes |> Array.exists (fun i -> i.Length = 0L) then invalidArg "csvPaths" "CSV part has no data rows"
    let header = resolveHeader indexes.[0] options
    for i in 1 .. indexes.Length - 1 do
      if indexes.[i].HeaderColumns <> header then
        invalidArg "csvPaths" (sprintf "VirtualCsvSource: schema mismatch in '%s'" csvPaths.[i])
    let partSizes = indexes |> Array.map (fun i -> int i.Length)
    let total = partSizes |> Array.sumBy int64
    let keys =
      match options.ColumnKeys with
      | Some ks -> ks
      | None -> Array.toList header
    let makeColumn name =
      let colIdx = columnIndex header name
      let kind = inferColumnKind indexes.[0] colIdx 100
      let resolved = resolveSearchLookupRanges indexes.[0] colIdx name kind options
      let cell i =
        let part, row = locatePartRow partSizes i
        field (indexes.[part].ReadFields row) colIdx
      let sourceOf parse asLong lookupRange configured =
        let valueAt i =
          let s = cell i
          if isMissingCell s then OptionalValue.Missing
          else match parse s with Some v -> OptionalValue(v) | None -> OptionalValue.Missing
        match lookupRange with
        | Some mode ->
            OrdinalVirtualSource(total, valueAt, "csv-file", ?asLong=asLong, lookupRange=mode, searchColumnConfigured=configured)
            :> IVirtualVectorSource
        | None ->
            OrdinalVirtualSource(total, valueAt, "csv-file", ?asLong=asLong, searchColumnConfigured=configured)
            :> IVirtualVectorSource
      match kind with
      | "datetime" -> sourceOf tryParseDateTime (Some (fun dto -> dto.UtcTicks)) None false
      | "int64" -> sourceOf tryParseInt64 (Some id) resolved.Int64 resolved.Configured
      | "float" -> sourceOf tryParseFloat None resolved.Float resolved.Configured
      | _ -> sourceOf (fun s -> Some s) None resolved.String resolved.Configured
    Virtual.CreateOrdinalFrame(keys, keys |> List.map makeColumn)

namespace Deedle.Virtual

open System.IO
open Deedle
open Deedle.Virtual.Sources

[<AutoOpen>]
module VirtualCsvExtensions =
  let private buildReadCsvOptions indexColumn searchColumns columnKeys byteOffsetIndex hasHeaders =
    { IndexColumn = indexColumn
      SearchColumns = defaultArg searchColumns []
      ColumnKeys = columnKeys
      ByteOffsetIndex = defaultArg byteOffsetIndex true
      HasHeaders = defaultArg hasHeaders true }

  type Virtual with
    /// Load a CSV file as a virtual frame with ordinal row index `0 .. N-1`.
    static member ReadCsv(path: string, ?searchColumns: VirtualSearchColumn list, ?columnKeys: string list, ?byteOffsetIndex: bool, ?hasHeaders: bool) : Frame<int64, string> =
      VirtualCsvSource.createOrdinalFrame path (buildReadCsvOptions None searchColumns columnKeys byteOffsetIndex hasHeaders)

    /// Load a CSV file as a virtual frame.
    /// When `indexColumn` is set, that column becomes the row index only if it is strictly increasing and unique in file order; otherwise an ordinal index is used (trace warning).
    /// Specify the row key type explicitly (for example `DateTimeOffset` for a timestamp column).
    [<RequiresExplicitTypeArguments>]
    static member ReadCsv<'R when 'R : equality>(path: string, indexColumn: string, ?searchColumns: VirtualSearchColumn list, ?columnKeys: string list, ?byteOffsetIndex: bool, ?hasHeaders: bool) : Frame<'R, string> =
      VirtualCsvSource.createFrame path (buildReadCsvOptions (Some indexColumn) searchColumns columnKeys byteOffsetIndex hasHeaders)
      |> unbox<Frame<'R, string>>

    /// Load matching CSV files in a directory as one ordinal virtual frame (files sorted by name).
    /// Rows are addressed 0 .. N-1 across files; all files must share the first file's header.
    static member ReadCsvDirectory
        ( directory: string,
          ?searchPattern: string,
          ?searchColumns: VirtualSearchColumn list,
          ?columnKeys: string list,
          ?byteOffsetIndex: bool,
          ?hasHeaders: bool ) =
      if not (Directory.Exists directory) then
        raise (DirectoryNotFoundException(sprintf "VirtualCsvSource: directory not found '%s'" directory))
      let pattern = defaultArg searchPattern "*.csv"
      let files = Directory.GetFiles(directory, pattern) |> Array.sort
      if files.Length = 0 then invalidArg "directory" (sprintf "No files matching '%s' in '%s'" pattern directory)
      VirtualCsvSource.createConcatenatedFrame files (buildReadCsvOptions None searchColumns columnKeys byteOffsetIndex hasHeaders)
