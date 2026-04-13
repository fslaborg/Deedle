/// <summary>
/// Provides functions for reading and writing Deedle Frames in Apache Parquet format
/// using the Parquet.Net library.
/// </summary>
///
/// <category>Parquet integration</category>
namespace Deedle.Parquet

open System
open System.IO
open Deedle
open Parquet
open Parquet.Schema
open Parquet.Data

[<AutoOpen>]
module private Implementation =

    // ------------------------------------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------------------------------------

    /// Get the untyped Data array from a DataColumn, with an explicit Array type.
    let getData (col: DataColumn) : Array = col.Data

    /// Get the value count from a DataColumn.
    let getNumValues (col: DataColumn) : int = int col.NumValues

    /// Map a .NET type to a nullable Parquet DataField.
    /// Value types are wrapped in Nullable<T> to support missing values.
    /// Unmapped types fall back to string (values are serialised via ToString).
    let netTypeToDataField (name: string) (t: Type) : DataField =
        if   t = typeof<float>          then DataField(name, typeof<Nullable<float>>)
        elif t = typeof<float32>        then DataField(name, typeof<Nullable<float32>>)
        elif t = typeof<int>            then DataField(name, typeof<Nullable<int>>)
        elif t = typeof<int64>          then DataField(name, typeof<Nullable<int64>>)
        elif t = typeof<int16>          then DataField(name, typeof<Nullable<int16>>)
        elif t = typeof<byte>           then DataField(name, typeof<Nullable<byte>>)
        elif t = typeof<uint16>         then DataField(name, typeof<Nullable<uint16>>)
        elif t = typeof<uint32>         then DataField(name, typeof<Nullable<uint32>>)
        elif t = typeof<uint64>         then DataField(name, typeof<Nullable<uint64>>)
        elif t = typeof<bool>           then DataField(name, typeof<Nullable<bool>>)
        elif t = typeof<string>         then DataField(name, typeof<string>)
        elif t = typeof<DateTime>       then DataField(name, typeof<Nullable<DateTime>>)
        elif t = typeof<DateTimeOffset> then DataField(name, typeof<Nullable<DateTimeOffset>>)
        else                                 DataField(name, typeof<string>)

    /// Convert a boxed IVector<obj> column to a Parquet DataColumn.
    let columnToDataColumn (field: DataField) (typ: Type) (vec: IVector<obj>) : DataColumn =
        let data = vec.DataSequence |> Array.ofSeq
        if typ = typeof<float> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<float> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<float32> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<float32> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<int> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<int> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<int64> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<int64> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<int16> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<int16> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<byte> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<byte> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<uint16> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<uint16> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<uint32> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<uint32> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<uint64> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<uint64> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<bool> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<bool> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<string> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> unbox<string> v
                | OptionalValue.Missing   -> null)
            DataColumn(field, arr)
        elif typ = typeof<DateTime> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<DateTime> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        elif typ = typeof<DateTimeOffset> then
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> Nullable(unbox<DateTimeOffset> v)
                | OptionalValue.Missing   -> Nullable())
            DataColumn(field, arr)
        else
            // Fallback: serialise values as strings via ToString
            let arr = data |> Array.map (fun ov ->
                match ov with
                | OptionalValue.Present v -> v.ToString()
                | OptionalValue.Missing   -> null)
            DataColumn(field, arr)

    /// Read a single DataColumn into a Deedle IVector (Type * IVector).
    let readColumn (col: DataColumn) : Type * IVector =
        let clrType = col.Field.ClrType
        let baseType =
            let ut = Nullable.GetUnderlyingType(clrType)
            if ut <> null then ut else clrType
        let n = getNumValues col
        let data = getData col
        // Check the actual data array element type, not the field's ClrType,
        // because Parquet.Net may return Nullable<T>[] even when the field says T
        let elemType = data.GetType().GetElementType()
        let isNullable =
            Nullable.GetUnderlyingType(elemType) <> null || not elemType.IsValueType

        if baseType = typeof<float> then
            if isNullable then
                let arr = data :?> Nullable<float>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<float>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> float[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<float>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<float32> then
            if isNullable then
                let arr = data :?> Nullable<float32>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<float32>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> float32[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<float32>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<int> then
            if isNullable then
                let arr = data :?> Nullable<int>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<int>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> int[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<int>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<int64> then
            if isNullable then
                let arr = data :?> Nullable<int64>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<int64>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> int64[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<int64>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<int16> then
            if isNullable then
                let arr = data :?> Nullable<int16>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<int16>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> int16[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<int16>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<byte> then
            if isNullable then
                let arr = data :?> Nullable<byte>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<byte>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> byte[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<byte>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<uint16> then
            if isNullable then
                let arr = data :?> Nullable<uint16>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<uint16>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> uint16[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<uint16>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<uint32> then
            if isNullable then
                let arr = data :?> Nullable<uint32>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<uint32>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> uint32[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<uint32>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<uint64> then
            if isNullable then
                let arr = data :?> Nullable<uint64>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<uint64>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> uint64[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<uint64>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<bool> then
            if isNullable then
                let arr = data :?> Nullable<bool>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<bool>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> bool[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<bool>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<string> then
            let arr = data :?> string[]
            let vals = Array.init n (fun i ->
                if arr.[i] <> null then OptionalValue(arr.[i]) else OptionalValue.Missing)
            typeof<string>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<DateTime> then
            if isNullable then
                let arr = data :?> Nullable<DateTime>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<DateTime>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> DateTime[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<DateTime>, Vector.ofOptionalValues vals :> IVector

        elif baseType = typeof<DateTimeOffset> then
            if isNullable then
                let arr = data :?> Nullable<DateTimeOffset>[]
                let vals = Array.init n (fun i ->
                    if arr.[i].HasValue then OptionalValue(arr.[i].Value) else OptionalValue.Missing)
                typeof<DateTimeOffset>, Vector.ofOptionalValues vals :> IVector
            else
                let arr = data :?> DateTimeOffset[]
                let vals = Array.init n (fun i -> OptionalValue(arr.[i]))
                typeof<DateTimeOffset>, Vector.ofOptionalValues vals :> IVector

        else
            // Unrecognised type: return a column of missing values
            let vals = Array.create n OptionalValue<obj>.Missing
            typeof<obj>, Vector.ofOptionalValues vals :> IVector

    /// Build a Frame<int,string> from row-group DataColumn arrays.
    /// Row groups are concatenated vertically; row keys are 0-based integers.
    let rowGroupsToFrame (dataFields: DataField[]) (rowGroupColumns: DataColumn[][]) : Frame<int, string> =
        let nCols = dataFields.Length
        if nCols = 0 then
            Frame<int, string>(
                Index.ofKeys [||],
                Index.ofKeys [||],
                Vector.ofValues [||],
                IndexBuilder.Instance,
                VectorBuilder.Instance)
        else
            let colNames = dataFields |> Array.map (fun f -> f.Name)

            // For single row group, use direct conversion
            if rowGroupColumns.Length = 1 then
                let rg = rowGroupColumns.[0]
                let totalRows = getNumValues rg.[0]
                let columns =
                    [| for colIdx in 0 .. nCols - 1 ->
                        let (_, vec) = readColumn rg.[colIdx]
                        vec |]
                let frameData = columns |> Vector.ofValues
                let rowIndex  = Index.ofKeys [| 0 .. totalRows - 1 |]
                let colIndex  = Index.ofKeys colNames
                Frame<int, string>(rowIndex, colIndex, frameData, IndexBuilder.Instance, VectorBuilder.Instance)
            else
                // Multiple row groups: read each, convert, concatenate via ObjectSequence
                let totalRows =
                    rowGroupColumns |> Array.sumBy (fun rg ->
                        if rg.Length > 0 then getNumValues rg.[0] else 0)
                let columns =
                    [| for colIdx in 0 .. nCols - 1 ->
                        // Read each row-group's column independently
                        let parts =
                            rowGroupColumns |> Array.map (fun rg -> readColumn rg.[colIdx])
                        // Determine the element type from the first non-empty part
                        let elemType = parts |> Array.tryPick (fun (t, _) -> if t <> typeof<obj> then Some t else None)
                                        |> Option.defaultValue typeof<obj>
                        // Concatenate by collecting all OptionalValue<obj> sequences
                        let allVals = Array.zeroCreate<OptionalValue<obj>> totalRows
                        let mutable row = 0
                        for (_, vec) in parts do
                            for ov in vec.ObjectSequence do
                                allVals.[row] <- ov
                                row <- row + 1
                        Vector.ofOptionalValues allVals :> IVector
                    |]
                let frameData = columns |> Vector.ofValues
                let rowIndex  = Index.ofKeys [| 0 .. totalRows - 1 |]
                let colIndex  = Index.ofKeys colNames
                Frame<int, string>(rowIndex, colIndex, frameData, IndexBuilder.Instance, VectorBuilder.Instance)

    // ------------------------------------------------------------------------------------------------
    // Public API — Parquet file format
    // ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet file.
    /// Column keys are converted to strings via <c>ToString()</c>.
    /// Row keys are not included in the file; use <c>writeParquetWithIndex</c> for round-tripping.
    /// Missing values are encoded as Parquet nulls.
    /// </summary>
    let writeParquet (path: string) (frame: Frame<'R, string>) : unit =
        let data     = frame.GetFrameData()
        let colNames = data.ColumnKeys |> Seq.map (fun k -> k.[0].ToString()) |> Array.ofSeq
        let cols     = data.Columns    |> Array.ofSeq
        if cols.Length = 0 then
            // Parquet requires at least one field; write an empty marker file
            File.WriteAllBytes(path, [||])
        else
            let fields   = cols |> Array.mapi (fun i (typ, _) -> netTypeToDataField colNames.[i] typ :> Field)
            let schema   = ParquetSchema(fields)
            let dataFields = schema.GetDataFields()
            use stream   = File.Create(path)
            use writer   = ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
            if frame.RowCount > 0 then
                let dataColumns = cols |> Array.mapi (fun i (typ, vec) -> columnToDataColumn dataFields.[i] typ vec)
                let rg = writer.CreateRowGroup()
                for col in dataColumns do
                    rg.WriteColumnAsync(col).GetAwaiter().GetResult()

    /// <summary>
    /// Read a Parquet file into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// Row keys are 0-based integers. Parquet null values become Deedle missing values.
    /// Files with multiple row groups are concatenated into a single frame.
    /// </summary>
    let readParquet (path: string) : Frame<int, string> =
        let fi = FileInfo(path)
        if fi.Length = 0L then
            // Empty marker file — return an empty frame
            Frame<int, string>(
                Index.ofKeys [||],
                Index.ofKeys [||],
                Vector.ofValues [||],
                IndexBuilder.Instance,
                VectorBuilder.Instance)
        else
            use stream = File.OpenRead(path)
            use reader = ParquetReader.CreateAsync(stream).GetAwaiter().GetResult()
            if reader.RowGroupCount = 0 then
                Frame<int, string>(
                    Index.ofKeys [||],
                    Index.ofKeys [||],
                    Vector.ofValues [||],
                    IndexBuilder.Instance,
                    VectorBuilder.Instance)
            else
                let dataFields = reader.Schema.GetDataFields()
                let rowGroupColumns =
                    [| for i in 0 .. reader.RowGroupCount - 1 ->
                        reader.ReadEntireRowGroupAsync(i).GetAwaiter().GetResult() |]
                rowGroupsToFrame dataFields rowGroupColumns

    // ------------------------------------------------------------------------------------------------
    // Public API — Parquet stream I/O
    // ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet stream.
    /// The stream must be seekable. The stream is left open after writing.
    /// </summary>
    let writeParquetStream (stream: Stream) (frame: Frame<'R, string>) : unit =
        let data     = frame.GetFrameData()
        let colNames = data.ColumnKeys |> Seq.map (fun k -> k.[0].ToString()) |> Array.ofSeq
        let cols     = data.Columns    |> Array.ofSeq
        if cols.Length = 0 then
            () // Parquet requires at least one field; skip writing for empty frames
        else
            let fields   = cols |> Array.mapi (fun i (typ, _) -> netTypeToDataField colNames.[i] typ :> Field)
            let schema   = ParquetSchema(fields)
            let dataFields = schema.GetDataFields()
            use writer   = ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult()
            if frame.RowCount > 0 then
                let dataColumns = cols |> Array.mapi (fun i (typ, vec) -> columnToDataColumn dataFields.[i] typ vec)
                let rg = writer.CreateRowGroup()
                for col in dataColumns do
                    rg.WriteColumnAsync(col).GetAwaiter().GetResult()

    /// <summary>
    /// Read a Parquet stream into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// The stream must be seekable. All row groups are concatenated into a single frame.
    /// </summary>
    let readParquetStream (stream: Stream) : Frame<int, string> =
        use reader = ParquetReader.CreateAsync(stream, leaveStreamOpen = true).GetAwaiter().GetResult()
        if reader.RowGroupCount = 0 then
            Frame<int, string>(
                Index.ofKeys [||],
                Index.ofKeys [||],
                Vector.ofValues [||],
                IndexBuilder.Instance,
                VectorBuilder.Instance)
        else
            let dataFields = reader.Schema.GetDataFields()
            let rowGroupColumns =
                [| for i in 0 .. reader.RowGroupCount - 1 ->
                    reader.ReadEntireRowGroupAsync(i).GetAwaiter().GetResult() |]
            rowGroupsToFrame dataFields rowGroupColumns

    // ------------------------------------------------------------------------------------------------
    // Public API — Row-key preservation
    // ------------------------------------------------------------------------------------------------

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet file, storing row keys in a special
    /// <c>__index__</c> column so they can be restored on read.
    /// Row keys are serialised via <c>ToString()</c>.
    /// </summary>
    let writeParquetWithIndex (path: string) (frame: Frame<'R, string>) : unit =
        let rowKeys = frame.RowKeys |> Array.ofSeq
        let indexVals = rowKeys |> Array.map (fun k -> k.ToString())
        let indexSeries = Series(rowKeys, indexVals)
        let frameWithIndex = frame |> Frame.addCol "__index__" indexSeries
        writeParquet path frameWithIndex

    /// <summary>
    /// Read a Parquet file that was written with <c>writeParquetWithIndex</c>, restoring
    /// the original string row keys from the <c>__index__</c> column.
    /// If no <c>__index__</c> column is present, the frame is returned with 0-based integer
    /// row keys converted to strings.
    /// </summary>
    let readParquetWithIndex (path: string) : Frame<string, string> =
        let frame = readParquet path
        if frame.ColumnKeys |> Seq.contains "__index__" then
            let indexArr = frame.GetColumn<string>("__index__") |> Series.values |> Array.ofSeq
            frame |> Frame.dropCol "__index__" |> Frame.indexRowsWith indexArr
        else
            frame |> Frame.indexRowsWith (frame.RowKeys |> Seq.map string |> Array.ofSeq)

// ------------------------------------------------------------------------------------------------
// F# module API — Frame module
// After `open Deedle.Parquet`, these are accessible as `Frame.readParquet`, etc.
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Parquet-specific functions on Deedle <c>Frame</c> values.
/// Open <c>Deedle.Parquet</c> and then call these as <c>Frame.readParquet</c>,
/// <c>Frame.writeParquet</c>, etc.
/// </summary>
module Frame =

    /// <summary>
    /// Read a Parquet file into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// Row keys are 0-based integers. Parquet null values become Deedle missing values.
    /// Files with multiple row groups are concatenated into a single frame.
    /// </summary>
    let readParquet (path: string) : Frame<int, string> =
        Implementation.readParquet path

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet file.
    /// </summary>
    let writeParquet (path: string) (frame: Frame<'R, string>) : unit =
        Implementation.writeParquet path frame

    /// <summary>
    /// Read a Parquet stream into a Deedle <c>Frame&lt;int,string&gt;</c>.
    /// The stream must be seekable. All row groups are concatenated into a single frame.
    /// </summary>
    let readParquetStream (stream: System.IO.Stream) : Frame<int, string> =
        Implementation.readParquetStream stream

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet stream.
    /// The stream must be seekable. The stream is left open after writing.
    /// </summary>
    let writeParquetStream (stream: System.IO.Stream) (frame: Frame<'R, string>) : unit =
        Implementation.writeParquetStream stream frame

    /// <summary>
    /// Write a Deedle <c>Frame</c> to a Parquet file, storing row keys in a special
    /// <c>__index__</c> column so they can be restored on read.
    /// Row keys are serialised via <c>ToString()</c>.
    /// </summary>
    let writeParquetWithIndex (path: string) (frame: Frame<'R, string>) : unit =
        Implementation.writeParquetWithIndex path frame

    /// <summary>
    /// Read a Parquet file that was written with <c>Frame.writeParquetWithIndex</c>,
    /// restoring the original string row keys.
    /// If no <c>__index__</c> column is present, returns a frame with 0-based integer
    /// row keys converted to strings.
    /// </summary>
    let readParquetWithIndex (path: string) : Frame<string, string> =
        Implementation.readParquetWithIndex path

// ------------------------------------------------------------------------------------------------
// C#-friendly API: static factory methods and extension methods
// ------------------------------------------------------------------------------------------------

open System.Runtime.CompilerServices

/// <summary>
/// Static factory methods for reading Parquet files, accessible from C# as
/// <c>ParquetFrame.ReadParquet(path)</c> etc.
/// </summary>
type ParquetFrame =

    /// <summary>Read a Parquet file into a <c>Frame&lt;int,string&gt;</c>.</summary>
    static member ReadParquet(path: string) : Frame<int, string> =
        readParquet path

    /// <summary>
    /// Read a Parquet file written with <c>WriteParquetWithIndex</c>,
    /// restoring string row keys.
    /// </summary>
    static member ReadParquetWithIndex(path: string) : Frame<string, string> =
        readParquetWithIndex path

    /// <summary>Read a Parquet stream into a <c>Frame&lt;int,string&gt;</c>.</summary>
    static member ReadParquetStream(stream: Stream) : Frame<int, string> =
        readParquetStream stream


/// <summary>
/// C# extension methods on <c>Frame&lt;'R, string&gt;</c> for Parquet I/O.
/// </summary>
[<Extension>]
type FrameParquetExtensions =

    /// <summary>Write this frame to a Parquet file.</summary>
    [<Extension>]
    static member WriteParquet(frame: Frame<'R, string>, path: string) : unit =
        writeParquet path frame

    /// <summary>
    /// Write this frame to a Parquet file, preserving row keys in
    /// an <c>__index__</c> column for round-tripping.
    /// </summary>
    [<Extension>]
    static member WriteParquetWithIndex(frame: Frame<'R, string>, path: string) : unit =
        writeParquetWithIndex path frame

    /// <summary>Write this frame to a Parquet stream.</summary>
    [<Extension>]
    static member WriteParquetStream(frame: Frame<'R, string>, stream: Stream) : unit =
        writeParquetStream stream frame
