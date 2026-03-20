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
