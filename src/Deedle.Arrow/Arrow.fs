/// <summary>
/// Provides conversions between Deedle Frames/Series and Apache Arrow RecordBatches,
/// and functions for reading and writing the Arrow IPC file and stream formats.
/// </summary>
///
/// <category>Arrow integration</category>
module Deedle.Arrow

open System
open System.IO
open System.Threading
open Deedle
open Apache.Arrow
open Apache.Arrow.Types
open Apache.Arrow.Ipc

// ------------------------------------------------------------------------------------------------
// Internal helpers
// ------------------------------------------------------------------------------------------------

/// Map a .NET type to the corresponding Arrow IArrowType.
/// Unmapped types fall back to Utf8 (values are serialised via ToString).
let private netTypeToArrowType (t: Type) : IArrowType =
    if   t = typeof<float>          then DoubleType.Default    :> IArrowType
    elif t = typeof<float32>        then FloatType.Default     :> IArrowType
    elif t = typeof<int>            then Int32Type.Default     :> IArrowType
    elif t = typeof<int64>          then Int64Type.Default     :> IArrowType
    elif t = typeof<int16>          then Int16Type.Default     :> IArrowType
    elif t = typeof<uint8>          then UInt8Type.Default     :> IArrowType
    elif t = typeof<uint16>         then UInt16Type.Default    :> IArrowType
    elif t = typeof<uint32>         then UInt32Type.Default    :> IArrowType
    elif t = typeof<uint64>         then UInt64Type.Default    :> IArrowType
    elif t = typeof<bool>           then BooleanType.Default   :> IArrowType
    elif t = typeof<string>         then StringType.Default    :> IArrowType
    elif t = typeof<DateTime>       then TimestampType(TimeUnit.Microsecond, "UTC") :> IArrowType
    elif t = typeof<DateTimeOffset> then TimestampType(TimeUnit.Microsecond, "UTC") :> IArrowType
    else                                 StringType.Default    :> IArrowType

let private epoch = DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
let private epochDto = DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)
// 1 microsecond = 10 ticks (1 tick = 100 nanoseconds)
let private ticksPerMicrosecond = 10L

/// Convert a boxed IVector<obj> column to an Arrow IArrowArray.
let private columnToArrowArray (typ: Type) (vec: IVector<obj>) : IArrowArray =
    let data = vec.DataSequence |> Array.ofSeq
    if typ = typeof<float> then
        let b = DoubleArray.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<float> v)   |> ignore
            | OptionalValue.Missing   -> b.AppendNull()              |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<float32> then
        let b = FloatArray.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<float32> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()              |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<int> then
        let b = Int32Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<int> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()          |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<int64> then
        let b = Int64Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<int64> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()            |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<int16> then
        let b = Int16Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<int16> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()            |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<uint8> then
        let b = UInt8Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<uint8> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()            |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<uint16> then
        let b = UInt16Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<uint16> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()             |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<uint32> then
        let b = UInt32Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<uint32> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()             |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<uint64> then
        let b = UInt64Array.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<uint64> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()             |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<bool> then
        let b = BooleanArray.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(unbox<bool> v) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()           |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<DateTime> then
        let b = new TimestampArray.Builder(TimeUnit.Microsecond, "UTC")
        for ov in data do
            match ov with
            | OptionalValue.Present v ->
                let dto = DateTimeOffset(unbox<DateTime> v)
                b.Append(dto) |> ignore
            | OptionalValue.Missing -> b.AppendNull() |> ignore
        b.Build() :> IArrowArray
    elif typ = typeof<DateTimeOffset> then
        let b = new TimestampArray.Builder(TimeUnit.Microsecond, "UTC")
        for ov in data do
            match ov with
            | OptionalValue.Present v ->
                b.Append(unbox<DateTimeOffset> v) |> ignore
            | OptionalValue.Missing -> b.AppendNull() |> ignore
        b.Build() :> IArrowArray
    else
        // Fallback: serialise values as UTF-8 strings via ToString
        let b = StringArray.Builder()
        for ov in data do
            match ov with
            | OptionalValue.Present v -> b.Append(v.ToString()) |> ignore
            | OptionalValue.Missing   -> b.AppendNull()          |> ignore
        b.Build() :> IArrowArray

/// Convert an Arrow IArrowArray to a typed IVector and its .NET element type.
let private arrowArrayToVector (arr: IArrowArray) : Type * IVector =
    let n = arr.Length
    match arr with
    | :? DoubleArray as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<float>, Vector.ofOptionalValues vals :> IVector
    | :? FloatArray as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<float32>, Vector.ofOptionalValues vals :> IVector
    | :? Int32Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<int>, Vector.ofOptionalValues vals :> IVector
    | :? Int64Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<int64>, Vector.ofOptionalValues vals :> IVector
    | :? Int16Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<int16>, Vector.ofOptionalValues vals :> IVector
    | :? UInt8Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<uint8>, Vector.ofOptionalValues vals :> IVector
    | :? UInt16Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<uint16>, Vector.ofOptionalValues vals :> IVector
    | :? UInt32Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<uint32>, Vector.ofOptionalValues vals :> IVector
    | :? UInt64Array as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<uint64>, Vector.ofOptionalValues vals :> IVector
    | :? Date32Array as a ->
        // Date32 stores days since Unix epoch as int32
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(epoch.AddDays(float (a.GetValue(i).Value))))
        typeof<DateTime>, Vector.ofOptionalValues vals :> IVector
    | :? Date64Array as a ->
        // Date64 stores milliseconds since Unix epoch as int64
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(epoch.AddMilliseconds(float (a.GetValue(i).Value))))
        typeof<DateTime>, Vector.ofOptionalValues vals :> IVector
    | :? BooleanArray as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetValue(i).Value))
        typeof<bool>, Vector.ofOptionalValues vals :> IVector
    | :? StringArray as a ->
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else OptionalValue(a.GetString(i)))
        typeof<string>, Vector.ofOptionalValues vals :> IVector
    | :? TimestampArray as a ->
        let tsType = a.Data.DataType :?> TimestampType
        let scale =
            match tsType.Unit with
            | TimeUnit.Second      -> TimeSpan.TicksPerSecond
            | TimeUnit.Millisecond -> TimeSpan.TicksPerMillisecond
            | TimeUnit.Microsecond -> ticksPerMicrosecond
            | TimeUnit.Nanosecond  -> 1L  // nanoseconds truncated to 100-ns ticks
            | _                    -> ticksPerMicrosecond
        let vals = Array.init n (fun i ->
            if a.IsNull(i) then OptionalValue.Missing
            else
                let ticks = a.GetValue(i).Value * scale
                OptionalValue(epoch.AddTicks(ticks)))
        typeof<DateTime>, Vector.ofOptionalValues vals :> IVector
    | _ ->
        // Unrecognised array type: return a column of missing values
        let vals = Array.create n OptionalValue<obj>.Missing
        typeof<obj>, Vector.ofOptionalValues vals :> IVector

/// Build a Frame<int,string> from an array of RecordBatches that all share the same schema.
/// Batches are concatenated vertically; row keys are 0-based integers.
let private batchesToFrame (batches: RecordBatch[]) : Frame<int, string> =
    if batches.Length = 0 then
        Frame<int, string>(
            Index.ofKeys [||],
            Index.ofKeys [||],
            Vector.ofValues [||],
            IndexBuilder.Instance,
            VectorBuilder.Instance)
    else
        let schema    = batches.[0].Schema
        let nCols     = schema.FieldsList.Count
        let totalRows = batches |> Array.sumBy (fun b -> b.Length)
        let colNames  = [| for i in 0 .. nCols - 1 -> schema.GetFieldByIndex(i).Name |]

        let columns =
            [| for colIdx in 0 .. nCols - 1 ->
                let allArrays = batches |> Array.map (fun b -> b.Column(colIdx))
                // Determine element type from the first non-empty array
                let firstArr = allArrays |> Array.tryFind (fun a -> a.Length > 0)

                match firstArr with
                | None ->
                    let vals = Array.create totalRows OptionalValue<obj>.Missing
                    Vector.ofOptionalValues vals :> IVector

                | Some sample ->
                    match sample with
                    | :? DoubleArray ->
                        let vals = Array.zeroCreate<OptionalValue<float>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> DoubleArray
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? FloatArray ->
                        let vals = Array.zeroCreate<OptionalValue<float32>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> FloatArray
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? Int32Array ->
                        let vals = Array.zeroCreate<OptionalValue<int>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> Int32Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? Int64Array ->
                        let vals = Array.zeroCreate<OptionalValue<int64>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> Int64Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? Int16Array ->
                        let vals = Array.zeroCreate<OptionalValue<int16>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> Int16Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? UInt8Array ->
                        let vals = Array.zeroCreate<OptionalValue<uint8>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> UInt8Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? UInt16Array ->
                        let vals = Array.zeroCreate<OptionalValue<uint16>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> UInt16Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? UInt32Array ->
                        let vals = Array.zeroCreate<OptionalValue<uint32>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> UInt32Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? UInt64Array ->
                        let vals = Array.zeroCreate<OptionalValue<uint64>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> UInt64Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? Date32Array ->
                        // Date32 stores days since Unix epoch as int32
                        let vals = Array.zeroCreate<OptionalValue<DateTime>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> Date32Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <-
                                    if a.IsNull(i) then OptionalValue.Missing
                                    else OptionalValue(epoch.AddDays(float (a.GetValue(i).Value)))
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? Date64Array ->
                        // Date64 stores milliseconds since Unix epoch as int64
                        let vals = Array.zeroCreate<OptionalValue<DateTime>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> Date64Array
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <-
                                    if a.IsNull(i) then OptionalValue.Missing
                                    else OptionalValue(epoch.AddMilliseconds(float (a.GetValue(i).Value)))
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? BooleanArray ->
                        let vals = Array.zeroCreate<OptionalValue<bool>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> BooleanArray
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetValue(i).Value)
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? StringArray ->
                        let vals = Array.zeroCreate<OptionalValue<string>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> StringArray
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <- if a.IsNull(i) then OptionalValue.Missing else OptionalValue(a.GetString(i))
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | :? TimestampArray ->
                        let tsType = (allArrays.[0] :?> TimestampArray).Data.DataType :?> TimestampType
                        let scale =
                            match tsType.Unit with
                            | TimeUnit.Second      -> TimeSpan.TicksPerSecond
                            | TimeUnit.Millisecond -> TimeSpan.TicksPerMillisecond
                            | TimeUnit.Microsecond -> ticksPerMicrosecond
                            | TimeUnit.Nanosecond  -> 1L
                            | _                    -> ticksPerMicrosecond
                        let vals = Array.zeroCreate<OptionalValue<DateTime>> totalRows
                        let mutable row = 0
                        for arr in allArrays do
                            let a = arr :?> TimestampArray
                            for i in 0 .. a.Length - 1 do
                                vals.[row] <-
                                    if a.IsNull(i) then OptionalValue.Missing
                                    else OptionalValue(epoch.AddTicks(a.GetValue(i).Value * scale))
                                row <- row + 1
                        Vector.ofOptionalValues vals :> IVector
                    | _ ->
                        let vals = Array.create totalRows OptionalValue<obj>.Missing
                        Vector.ofOptionalValues vals :> IVector
            |]

        let frameData = columns |> Vector.ofValues
        let rowIndex  = Index.ofKeys [| 0 .. totalRows - 1 |]
        let colIndex  = Index.ofKeys colNames
        Frame<int, string>(rowIndex, colIndex, frameData, IndexBuilder.Instance, VectorBuilder.Instance)

// ------------------------------------------------------------------------------------------------
// Public API — in-memory conversions
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Convert a Deedle <c>Frame</c> to an Apache Arrow <c>RecordBatch</c>.
/// Column keys are converted to strings via <c>ToString()</c>.
/// Row keys are not included in the batch; use an explicit column for round-tripping.
/// Missing values are encoded as Arrow validity-bitmap nulls.
/// </summary>
let frameToRecordBatch (frame: Frame<'R,'C>) : RecordBatch =
    let data     = frame.GetFrameData()
    let colNames = data.ColumnKeys |> Seq.map (fun k -> k.[0].ToString()) |> Array.ofSeq
    let cols     = data.Columns    |> Array.ofSeq
    let fields   =
        cols
        |> Array.mapi (fun i (typ, _) ->
            new Field(colNames.[i], netTypeToArrowType typ, nullable = true))
    let schema = new Schema(fields, null)
    let arrays = cols |> Array.map (fun (typ, vec) -> columnToArrowArray typ vec)
    new RecordBatch(schema, arrays, frame.RowCount)

/// <summary>
/// Convert an Apache Arrow <c>RecordBatch</c> to a Deedle <c>Frame&lt;int,string&gt;</c>.
/// Row keys are 0-based integers. Arrow null values become Deedle missing values.
/// </summary>
let recordBatchToFrame (batch: RecordBatch) : Frame<int, string> =
    batchesToFrame [| batch |]

/// <summary>
/// Convert a Deedle <c>Series</c> to an Apache Arrow <c>IArrowArray</c>.
/// The series element type determines the Arrow array type (same mapping as <c>frameToRecordBatch</c>).
/// Missing values are encoded as Arrow validity-bitmap nulls.
/// </summary>
let seriesToArrowArray (series: Series<'K, 'V>) : IArrowArray =
    let typ = typeof<'V>
    let data =
        series.ObservationsAll
        |> Seq.map (fun kvp ->
            if kvp.Value.HasValue then OptionalValue(box kvp.Value.Value : obj)
            else OptionalValue<obj>.Missing)
        |> Array.ofSeq
    let vec : IVector<obj> = Vector.ofOptionalValues data
    columnToArrowArray typ vec

/// <summary>
/// Convert an Apache Arrow <c>IArrowArray</c> to a Deedle <c>Series&lt;int,obj&gt;</c>.
/// Row keys are 0-based integers. Arrow null values become Deedle missing values.
/// The values are boxed as <c>obj</c>; use <c>Series.map</c> or <c>Frame.GetColumn&lt;'T&gt;</c>
/// on a framed version to obtain a typed series.
/// </summary>
let arrowArrayToSeries (arr: IArrowArray) : Series<int, obj> =
    let (_, vec) = arrowArrayToVector arr
    let n = int vec.Length
    let keys = Array.init n id
    let optVals =
        vec.ObjectSequence
        |> Seq.map (fun ov -> if ov.HasValue then Some ov.Value else None)
        |> Array.ofSeq
    Series.ofOptionalObservations (Array.zip keys optVals)

// ------------------------------------------------------------------------------------------------
// Public API — Arrow IPC file format  (.arrow / .feather v2)
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Write a Deedle <c>Frame</c> to an Arrow IPC file (the standard <c>.arrow</c> format,
/// also compatible with Feather v2 <c>.feather</c> files).
/// </summary>
let writeArrow (path: string) (frame: Frame<'R, string>) : unit =
    let batch  = frameToRecordBatch frame
    use stream = File.Create(path)
    use writer = new ArrowFileWriter(stream, batch.Schema)
    writer.WriteStart()
    writer.WriteRecordBatch(batch)
    writer.WriteEnd()

/// <summary>
/// Read an Arrow IPC file (<c>.arrow</c> or Feather v2 <c>.feather</c>) into a
/// Deedle <c>Frame&lt;int,string&gt;</c>.
/// Files with multiple record batches are concatenated into a single frame.
/// </summary>
let readArrow (path: string) : Frame<int, string> =
    use stream = File.OpenRead(path)
    use reader = new ArrowFileReader(stream)
    let countVt : System.Threading.Tasks.ValueTask<int> = reader.RecordBatchCountAsync()
    let count  = countVt.AsTask().GetAwaiter().GetResult()
    let batches =
        [| for i in 0 .. count - 1 ->
            let batchVt : System.Threading.Tasks.ValueTask<RecordBatch> =
                reader.ReadRecordBatchAsync(i, CancellationToken.None)
            batchVt.AsTask().GetAwaiter().GetResult() |]
    batchesToFrame batches

// ------------------------------------------------------------------------------------------------
// Public API — Arrow IPC stream format  (.arrows)
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Write a Deedle <c>Frame</c> to an Arrow IPC stream (suitable for network transport
/// or streaming pipelines). The stream is left open after writing.
/// </summary>
let writeArrowStream (stream: Stream) (frame: Frame<'R, string>) : unit =
    let batch  = frameToRecordBatch frame
    use writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen = true)
    writer.WriteStart()
    writer.WriteRecordBatch(batch)
    writer.WriteEnd()

/// <summary>
/// Read an Arrow IPC stream into a Deedle <c>Frame&lt;int,string&gt;</c>.
/// All record batches in the stream are concatenated into a single frame.
/// </summary>
let readArrowStream (stream: Stream) : Frame<int, string> =
    use reader = new ArrowStreamReader(stream)
    let batches = ResizeArray<RecordBatch>()
    let mutable batch = reader.ReadNextRecordBatch()
    while batch <> null do
        batches.Add(batch)
        batch <- reader.ReadNextRecordBatch()
    batchesToFrame (batches.ToArray())

// ------------------------------------------------------------------------------------------------
// Public API — Feather v2 aliases
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Write a Deedle <c>Frame</c> to a Feather v2 file (<c>.feather</c>).
/// Feather v2 is the Arrow IPC file format — this is an alias for <c>writeArrow</c>.
/// </summary>
let writeFeather (path: string) (frame: Frame<'R, string>) : unit = writeArrow path frame

/// <summary>
/// Read a Feather v2 file (<c>.feather</c>) into a Deedle <c>Frame&lt;int,string&gt;</c>.
/// Feather v2 is the Arrow IPC file format — this is an alias for <c>readArrow</c>.
/// </summary>
let readFeather (path: string) : Frame<int, string> = readArrow path

// ------------------------------------------------------------------------------------------------
// Public API — Row-key preservation
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Write a Deedle <c>Frame</c> to an Arrow IPC file, storing row keys in a special
/// <c>__index__</c> column so they can be restored on read.
/// Row keys are serialised via <c>ToString()</c>.
/// </summary>
let writeArrowWithIndex (path: string) (frame: Frame<'R, string>) : unit =
    let rowKeys = frame.RowKeys |> Array.ofSeq
    let indexVals = rowKeys |> Array.map (fun k -> k.ToString())
    let indexSeries = Series(rowKeys, indexVals)
    let frameWithIndex = frame |> Frame.addCol "__index__" indexSeries
    writeArrow path frameWithIndex

/// <summary>
/// Read an Arrow IPC file that was written with <c>writeArrowWithIndex</c>, restoring
/// the original string row keys from the <c>__index__</c> column.
/// If no <c>__index__</c> column is present, the frame is returned with 0-based integer
/// row keys converted to strings.
/// </summary>
let readArrowWithIndex (path: string) : Frame<string, string> =
    let frame = readArrow path
    if frame.ColumnKeys |> Seq.contains "__index__" then
        let indexArr = frame.GetColumn<string>("__index__") |> Series.values |> Array.ofSeq
        frame |> Frame.dropCol "__index__" |> Frame.indexRowsWith indexArr
    else
        frame |> Frame.indexRowsWith (frame.RowKeys |> Seq.map string |> Array.ofSeq)

/// <summary>
/// Write a Deedle <c>Frame</c> to a Feather v2 file, storing row keys in a special
/// <c>__index__</c> column so they can be restored on read.
/// </summary>
let writeFeatherWithIndex (path: string) (frame: Frame<'R, string>) : unit =
    writeArrowWithIndex path frame

/// <summary>
/// Read a Feather v2 file that was written with <c>writeFeatherWithIndex</c>, restoring
/// the original string row keys from the <c>__index__</c> column.
/// </summary>
let readFeatherWithIndex (path: string) : Frame<string, string> =
    readArrowWithIndex path

// ------------------------------------------------------------------------------------------------
// F# module API — Frame module
// After `open Deedle.Arrow`, these are accessible as `Frame.readArrow`, `Frame.writeArrow`, etc.
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Arrow-specific functions on Deedle <c>Frame</c> values.
/// Open <c>Deedle.Arrow</c> and then call these as <c>Frame.readArrow</c>,
/// <c>Frame.writeArrow</c>, <c>Frame.toRecordBatch</c>, etc.
/// </summary>
module Frame =

    /// <summary>
    /// Convert a Deedle <c>Frame</c> to an Apache Arrow <c>RecordBatch</c>.
    /// Column keys are converted to strings via <c>ToString()</c>.
    /// Row keys are <em>not</em> included; use <c>Frame.writeArrow</c> / <c>Frame.readArrow</c>
    /// for full round-trip serialisation.
    /// </summary>
    let toRecordBatch (frame: Frame<'R,'C>) : RecordBatch =
        frameToRecordBatch frame

    /// <summary>
    /// Convert an Apache Arrow <c>RecordBatch</c> to a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// Row keys are 0-based integers. Arrow null values become Deedle missing values.
    /// </summary>
    let ofRecordBatch (batch: RecordBatch) : Frame<int, string> =
        recordBatchToFrame batch

    /// <summary>
    /// Read an Arrow IPC file (<c>.arrow</c> or Feather v2 <c>.feather</c>) into a
    /// Deedle <c>Frame&lt;int,string&gt;</c>.
    /// Files with multiple record batches are concatenated into a single frame.
    /// </summary>
    let readArrow (path: string) : Frame<int, string> =
        readArrow path

    /// <summary>
    /// Write a Deedle <c>Frame</c> to an Arrow IPC file (the standard <c>.arrow</c> format,
    /// also compatible with Feather v2 <c>.feather</c> files).
    /// </summary>
    let writeArrow (path: string) (frame: Frame<'R, string>) : unit =
        writeArrow path frame

    /// <summary>
    /// Read an Arrow IPC stream into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// All record batches in the stream are concatenated into a single frame.
    /// </summary>
    let readArrowStream (stream: System.IO.Stream) : Frame<int, string> =
        readArrowStream stream

    /// <summary>
    /// Write a Deedle <c>Frame</c> to an Arrow IPC stream (suitable for network transport
    /// or streaming pipelines). The stream is left open after writing.
    /// </summary>
    let writeArrowStream (stream: System.IO.Stream) (frame: Frame<'R, string>) : unit =
        writeArrowStream stream frame

    /// <summary>
    /// Read a Feather v2 file (<c>.feather</c>) into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// Feather v2 is the Arrow IPC file format — this is an alias for <c>Frame.readArrow</c>.
    /// </summary>
    let readFeather (path: string) : Frame<int, string> =
        readFeather path

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Feather v2 file (<c>.feather</c>).
    /// Feather v2 is the Arrow IPC file format — this is an alias for <c>Frame.writeArrow</c>.
    /// </summary>
    let writeFeather (path: string) (frame: Frame<'R, string>) : unit =
        writeFeather path frame

    /// <summary>
    /// Write a Deedle <c>Frame</c> to an Arrow IPC file, storing row keys in a special
    /// <c>__index__</c> column so they can be restored on read.
    /// Row keys are serialised via <c>ToString()</c>.
    /// </summary>
    let writeArrowWithIndex (path: string) (frame: Frame<'R, string>) : unit =
        writeArrowWithIndex path frame

    /// <summary>
    /// Read an Arrow IPC file that was written with <c>Frame.writeArrowWithIndex</c>,
    /// restoring the original string row keys.
    /// If no <c>__index__</c> column is present, returns a frame with 0-based integer
    /// row keys converted to strings.
    /// </summary>
    let readArrowWithIndex (path: string) : Frame<string, string> =
        readArrowWithIndex path

// ------------------------------------------------------------------------------------------------
// F# module API — Series module
// After `open Deedle.Arrow`, these are accessible as `Series.toArrowArray`, etc.
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Arrow-specific functions on Deedle <c>Series</c> values.
/// Open <c>Deedle.Arrow</c> and then call these as <c>Series.toArrowArray</c>,
/// <c>Series.ofArrowArray</c>, etc.
/// </summary>
module Series =

    /// <summary>
    /// Convert a Deedle <c>Series</c> to an Apache Arrow <c>IArrowArray</c>.
    /// The series element type determines the Arrow array type (same mapping as
    /// <c>Frame.toRecordBatch</c>). Missing values are encoded as Arrow validity-bitmap nulls.
    /// </summary>
    let toArrowArray (series: Series<'K, 'V>) : IArrowArray =
        seriesToArrowArray series

    /// <summary>
    /// Convert an Apache Arrow <c>IArrowArray</c> to a Deedle <c>Series&lt;int,obj&gt;</c>.
    /// Row keys are 0-based integers. Arrow null values become Deedle missing values.
    /// The values are boxed as <c>obj</c>; use <c>Series.map</c> to obtain a typed series.
    /// </summary>
    let ofArrowArray (arr: IArrowArray) : Series<int, obj> =
        arrowArrayToSeries arr

// ------------------------------------------------------------------------------------------------
// C#-friendly API: static factory methods and extension methods
// ------------------------------------------------------------------------------------------------

open System.Runtime.CompilerServices

/// <summary>
/// Static factory methods for reading Arrow / Feather files, accessible from C# as
/// <c>ArrowFrame.ReadArrow(path)</c> etc.
/// </summary>
type ArrowFrame =

    /// <summary>Read an Arrow IPC file into a <c>Frame&lt;int,string&gt;</c>.</summary>
    static member ReadArrow(path: string) : Frame<int, string> =
        readArrow path

    /// <summary>Read a Feather v2 file into a <c>Frame&lt;int,string&gt;</c>.</summary>
    static member ReadFeather(path: string) : Frame<int, string> =
        readFeather path

    /// <summary>
    /// Read an Arrow IPC file written with <c>WriteArrowWithIndex</c>,
    /// restoring string row keys.
    /// </summary>
    static member ReadArrowWithIndex(path: string) : Frame<string, string> =
        readArrowWithIndex path

    /// <summary>
    /// Read a Feather v2 file written with <c>WriteFeatherWithIndex</c>,
    /// restoring string row keys.
    /// </summary>
    static member ReadFeatherWithIndex(path: string) : Frame<string, string> =
        readFeatherWithIndex path

    /// <summary>Read an Arrow IPC stream into a <c>Frame&lt;int,string&gt;</c>.</summary>
    static member ReadArrowStream(stream: Stream) : Frame<int, string> =
        readArrowStream stream


/// <summary>
/// C# extension methods on <c>Frame&lt;'R, string&gt;</c> for Arrow and Feather I/O.
/// </summary>
[<Extension>]
type FrameArrowExtensions =

    /// <summary>Write this frame to an Arrow IPC file (<c>.arrow</c> / Feather v2).</summary>
    [<Extension>]
    static member WriteArrow(frame: Frame<'R, string>, path: string) : unit =
        writeArrow path frame

    /// <summary>Write this frame to a Feather v2 file (<c>.feather</c>).</summary>
    [<Extension>]
    static member WriteFeather(frame: Frame<'R, string>, path: string) : unit =
        writeFeather path frame

    /// <summary>
    /// Write this frame to an Arrow IPC file, preserving row keys in
    /// an <c>__index__</c> column for round-tripping.
    /// </summary>
    [<Extension>]
    static member WriteArrowWithIndex(frame: Frame<'R, string>, path: string) : unit =
        writeArrowWithIndex path frame

    /// <summary>
    /// Write this frame to a Feather v2 file, preserving row keys in
    /// an <c>__index__</c> column for round-tripping.
    /// </summary>
    [<Extension>]
    static member WriteFeatherWithIndex(frame: Frame<'R, string>, path: string) : unit =
        writeFeatherWithIndex path frame

    /// <summary>Write this frame to an Arrow IPC stream.</summary>
    [<Extension>]
    static member WriteArrowStream(frame: Frame<'R, string>, stream: Stream) : unit =
        writeArrowStream stream frame

    /// <summary>Convert this frame to an Apache Arrow <c>RecordBatch</c>.</summary>
    [<Extension>]
    static member ToRecordBatch(frame: Frame<'R, 'C>) : RecordBatch =
        frameToRecordBatch frame
