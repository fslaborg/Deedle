/// Conversion between Deedle Frame/Series and ML.NET IDataView.
///
/// After opening <c>Deedle.MicrosoftML</c>, the <c>Frame.toDataView</c> and
/// <c>Frame.ofDataView</c> helpers are available:
///
/// <code>
/// open Deedle
/// open Deedle.MicrosoftML
///
/// let dv  : IDataView           = Frame.toDataView myFrame
/// let df  : Frame&lt;int, string&gt; = Frame.ofDataView dv
/// </code>
namespace Deedle.MicrosoftML

open System
open Microsoft.ML          // DataViewSchema, DataViewRowCursor, ValueGetter<T>, IDataView
open Microsoft.ML.Data     // NumberDataViewType, BooleanDataViewType, TextDataViewType, DataViewRowId, VectorDataViewType, VBuffer
open Deedle

// ── Internal column storage ──────────────────────────────────────────────────

/// Materialised column data – one case per supported CLR type.
[<NoComparison; NoEquality>]
type internal ColData =
    | DoubleCol      of float    array        // ML.NET NumberDataViewType.Double
    | FloatCol       of float32  array        // ML.NET NumberDataViewType.Single
    | Int32Col       of int32    array        // ML.NET NumberDataViewType.Int32
    | Int64Col       of int64    array        // ML.NET NumberDataViewType.Int64
    | BoolCol        of bool     array        // ML.NET BooleanDataViewType
    | StringCol      of string   array        // ML.NET TextDataViewType (ReadOnlyMemory<char>)
    | FloatVectorCol of float32 array array   // ML.NET VectorDataViewType(Single); fixed-length vectors

module internal ColData =
    let viewType : ColData -> DataViewType = function
        | DoubleCol _           -> upcast NumberDataViewType.Double
        | FloatCol _            -> upcast NumberDataViewType.Single
        | Int32Col _            -> upcast NumberDataViewType.Int32
        | Int64Col _            -> upcast NumberDataViewType.Int64
        | BoolCol _             -> upcast BooleanDataViewType.Instance
        | StringCol _           -> upcast TextDataViewType.Instance
        | FloatVectorCol rows   ->
            // Derive vector size from the first row; 0-length arrays → unknown size (0).
            let size = if rows.Length > 0 then rows.[0].Length else 0
            upcast VectorDataViewType(NumberDataViewType.Single, size)

    let length (cd: ColData) =
        match cd with
        | DoubleCol      a -> a.Length
        | FloatCol       a -> a.Length
        | Int32Col       a -> a.Length
        | Int64Col       a -> a.Length
        | BoolCol        a -> a.Length
        | StringCol      a -> a.Length
        | FloatVectorCol a -> a.Length

// ── Frame → column extraction ────────────────────────────────────────────────

module internal Extraction =

    /// Extract a typed array from a Deedle series, substituting <paramref name="missing"/>
    /// for any missing values.
    let extractSeries
            (series  : Series<'R, 'T>)
            (rowKeys : 'R array)
            (missing : 'T) : 'T array =
        rowKeys |> Array.map (fun rk ->
            let v = series.TryGet(rk)
            if v.HasValue then v.Value else missing)

    /// Try to read a frame column as type 'T; returns None on any conversion failure.
    /// Must be a module-level binding so F# permits the explicit type parameter.
    let tryGetCol<'R, 'T when 'R : equality>
            (frame  : Frame<'R, string>)
            (colKey : string) : Series<'R, 'T> option =
        try Some (frame.GetColumn<'T>(colKey))
        with _ -> None

    /// Detect the column's element type by trying typed conversions in order,
    /// then materialise the column into a <c>ColData</c>.
    let extractColData
            (frame   : Frame<'R, string>)
            (colKey  : string)
            (rowKeys : 'R array) : ColData =

        // float first: most Deedle numeric frames store float columns.
        // String columns will throw on GetColumn<float>, falling through.
        match tryGetCol<_, float> frame colKey with
        | Some s -> DoubleCol (extractSeries s rowKeys Double.NaN)
        | None ->

        match tryGetCol<_, string> frame colKey with
        | Some s -> StringCol (extractSeries s rowKeys "")
        | None ->

        match tryGetCol<_, bool> frame colKey with
        | Some s -> BoolCol (extractSeries s rowKeys false)
        | None ->

        match tryGetCol<_, int32> frame colKey with
        | Some s -> Int32Col (extractSeries s rowKeys 0)
        | None ->

        match tryGetCol<_, int64> frame colKey with
        | Some s -> Int64Col (extractSeries s rowKeys 0L)
        | None ->

        match tryGetCol<_, float32> frame colKey with
        | Some s -> FloatCol (extractSeries s rowKeys Single.NaN)
        | None ->

        // float32 array → fixed-length vector column (most common ML feature-vector type)
        match tryGetCol<_, float32 array> frame colKey with
        | Some s ->
            let empty : float32 array = [||]
            FloatVectorCol (extractSeries s rowKeys empty)
        | None ->

        // Final fallback: NaN doubles.
        DoubleCol (Array.create rowKeys.Length Double.NaN)

// ── IDataView row cursor ─────────────────────────────────────────────────────

/// DataViewRowCursor over a fixed set of materialised column arrays.
type private FrameDataViewCursor
        (schema   : DataViewSchema,
         columns  : ColData array,
         rowCount : int) =
    inherit DataViewRowCursor()

    let mutable position = -1L

    // ── abstract property overrides ──────────────────────────────────────────
    override _.Schema   = schema
    override _.Position = position
    // Batch is always 0 for a simple sequential cursor.
    override _.Batch    = 0L

    // ── abstract method overrides ────────────────────────────────────────────
    override _.MoveNext() =
        position <- position + 1L
        int position < rowCount

    /// All columns are active; we materialise everything unconditionally.
    override _.IsColumnActive(_col) = true

    override _.GetGetter<'TValue>(col : DataViewSchema.Column) : ValueGetter<'TValue> =
        let data = columns.[col.Index]
        // Each arm creates a ValueGetter<T> for the concrete array type, then
        // box/unbox it to produce ValueGetter<'TValue>.  The when-guard ensures
        // the requested type matches the declared column type.
        match data with
        | DoubleCol arr when typeof<'TValue> = typeof<float> ->
            box (ValueGetter<float>(fun (v : float byref) ->
                v <- arr.[int position])) :?> ValueGetter<'TValue>
        | FloatCol arr when typeof<'TValue> = typeof<float32> ->
            box (ValueGetter<float32>(fun (v : float32 byref) ->
                v <- arr.[int position])) :?> ValueGetter<'TValue>
        | Int32Col arr when typeof<'TValue> = typeof<int32> ->
            box (ValueGetter<int32>(fun (v : int32 byref) ->
                v <- arr.[int position])) :?> ValueGetter<'TValue>
        | Int64Col arr when typeof<'TValue> = typeof<int64> ->
            box (ValueGetter<int64>(fun (v : int64 byref) ->
                v <- arr.[int position])) :?> ValueGetter<'TValue>
        | BoolCol arr when typeof<'TValue> = typeof<bool> ->
            box (ValueGetter<bool>(fun (v : bool byref) ->
                v <- arr.[int position])) :?> ValueGetter<'TValue>
        | StringCol arr when typeof<'TValue> = typeof<ReadOnlyMemory<char>> ->
            box (ValueGetter<ReadOnlyMemory<char>>(fun (v : ReadOnlyMemory<char> byref) ->
                v <- arr.[int position].AsMemory())) :?> ValueGetter<'TValue>
        | FloatVectorCol arr when typeof<'TValue> = typeof<VBuffer<float32>> ->
            box (ValueGetter<VBuffer<float32>>(fun (v : VBuffer<float32> byref) ->
                let row = arr.[int position]
                v <- VBuffer<float32>(row.Length, row |> Array.copy))) :?> ValueGetter<'TValue>
        | _ ->
            invalidOp (sprintf
                "Column '%s': cannot produce a getter for type %s (column type is %s)"
                col.Name typeof<'TValue>.Name (col.Type.ToString()))

    override _.GetIdGetter() : ValueGetter<DataViewRowId> =
        ValueGetter<DataViewRowId>(fun (v : DataViewRowId byref) ->
            v <- DataViewRowId(uint64 position, 0UL))

// ── IDataView implementation ─────────────────────────────────────────────────

/// IDataView that wraps materialised Deedle column arrays.
/// Constructed via <c>Frame.toDataView</c>.
[<Sealed>]
type FrameDataView internal (schema   : DataViewSchema,
                              columns  : ColData array,
                              rowCount : int) =

    /// The ML.NET schema describing all columns.
    member _.Schema = schema

    /// The number of rows.
    member _.RowCount = rowCount

    interface IDataView with
        member _.Schema     = schema
        member _.CanShuffle = true
        member _.GetRowCount() = Nullable<int64>(int64 rowCount)

        member _.GetRowCursor(_columnsNeeded, _rand) =
            new FrameDataViewCursor(schema, columns, rowCount) :> DataViewRowCursor

        member _.GetRowCursorSet(_columnsNeeded, _n, _rand) =
            [| new FrameDataViewCursor(schema, columns, rowCount) :> DataViewRowCursor |]

// ── IDataView → Frame ────────────────────────────────────────────────────────

module private FromDataView =
    /// Build a value-reader closure for one column, using the cursor's typed getter.
    /// The closure returns <c>obj</c> so heterogeneous columns can be collected together.
    let makeReader (cursor : DataViewRowCursor) (col : DataViewSchema.Column) : unit -> obj =
        match col.Type with
        | :? NumberDataViewType as t when t = NumberDataViewType.Double ->
            let g = cursor.GetGetter<float>(col)
            fun () ->
                let mutable v = Double.NaN
                g.Invoke(&v)
                box v
        | :? NumberDataViewType as t when t = NumberDataViewType.Single ->
            let g = cursor.GetGetter<float32>(col)
            fun () ->
                let mutable v = Single.NaN
                g.Invoke(&v)
                box v
        | :? NumberDataViewType as t when t = NumberDataViewType.Int32 ->
            let g = cursor.GetGetter<int32>(col)
            fun () ->
                let mutable v = 0
                g.Invoke(&v)
                box v
        | :? NumberDataViewType as t when t = NumberDataViewType.Int64 ->
            let g = cursor.GetGetter<int64>(col)
            fun () ->
                let mutable v = 0L
                g.Invoke(&v)
                box v
        | :? BooleanDataViewType ->
            let g = cursor.GetGetter<bool>(col)
            fun () ->
                let mutable v = false
                g.Invoke(&v)
                box v
        | :? TextDataViewType ->
            let g = cursor.GetGetter<ReadOnlyMemory<char>>(col)
            fun () ->
                let mutable v = ReadOnlyMemory<char>()
                g.Invoke(&v)
                box (v.ToString())
        | :? VectorDataViewType as vt when vt.ItemType :? NumberDataViewType &&
                                           (vt.ItemType :?> NumberDataViewType) = NumberDataViewType.Single ->
            // Vector column → read as float32 array per row.
            let g = cursor.GetGetter<VBuffer<float32>>(col)
            fun () ->
                let mutable buf = VBuffer<float32>()
                g.Invoke(&buf)
                box (buf.DenseValues() |> Array.ofSeq)
        | :? VectorDataViewType as vt when vt.ItemType :? NumberDataViewType &&
                                           (vt.ItemType :?> NumberDataViewType) = NumberDataViewType.Double ->
            // Double vector column → read as float array per row.
            let g = cursor.GetGetter<VBuffer<float>>(col)
            fun () ->
                let mutable buf = VBuffer<float>()
                g.Invoke(&buf)
                box (buf.DenseValues() |> Array.ofSeq)
        | _ ->
            // Unknown type; emit null for every row.
            fun () -> box null

// ── Public API ───────────────────────────────────────────────────────────────

/// Static helpers that extend the <c>Frame</c> concept with ML.NET conversions.
/// Open <c>Deedle.MicrosoftML</c> to bring these into scope alongside the
/// standard Deedle <c>Frame</c> module.
///
/// <category>ML.NET IDataView integration</category>
type Frame =

    /// <summary>
    /// Convert a Deedle Frame to an ML.NET <see cref="IDataView"/>.
    /// </summary>
    /// <remarks>
    /// Column types are inferred from the stored values:
    /// <list type="bullet">
    ///   <item><c>float</c> columns → <c>Double</c></item>
    ///   <item><c>string</c> columns → <c>Text</c> (<c>ReadOnlyMemory&lt;char&gt;</c>)</item>
    ///   <item><c>bool</c> columns → <c>Boolean</c></item>
    ///   <item><c>int32</c> / <c>int64</c> / <c>float32</c> → their ML.NET equivalents</item>
    ///   <item><c>float32 array</c> columns → fixed-length <c>VectorDataViewType(Single)</c></item>
    /// </list>
    /// Missing values become <c>NaN</c> for numeric columns and empty string for text.
    /// </remarks>
    static member toDataView (frame : Frame<'R, string>) : IDataView =
        let rowKeys  = frame.RowKeys |> Array.ofSeq
        let rowCount = rowKeys.Length

        let colDatas =
            frame.ColumnKeys
            |> Seq.map (fun colKey ->
                colKey, Extraction.extractColData frame colKey rowKeys)
            |> Array.ofSeq

        let schema =
            let b = DataViewSchema.Builder()
            for name, data in colDatas do
                b.AddColumn(name, ColData.viewType data)
            b.ToSchema()

        let cols = colDatas |> Array.map snd
        new FrameDataView(schema, cols, rowCount) :> IDataView

    /// <summary>
    /// Convert an ML.NET <see cref="IDataView"/> to a Deedle Frame with integer (0-based) row keys.
    /// </summary>
    /// <remarks>
    /// Column types are mapped as:
    /// <list type="bullet">
    ///   <item><c>Double</c> → <c>float</c></item>
    ///   <item><c>Single</c> → <c>float32</c></item>
    ///   <item><c>Int32</c> → <c>int</c></item>
    ///   <item><c>Int64</c> → <c>int64</c></item>
    ///   <item><c>Boolean</c> → <c>bool</c></item>
    ///   <item><c>Text</c> → <c>string</c></item>
    ///   <item><c>Vector(Single)</c> → <c>float32 array</c> (one array per row)</item>
    ///   <item><c>Vector(Double)</c> → <c>float array</c> (one array per row)</item>
    ///   <item>Other composite types → <c>null</c></item>
    /// </list>
    /// </remarks>
    static member ofDataView (dataView : IDataView) : Frame<int, string> =
        let schema    = dataView.Schema
        let schemaArr = schema |> Seq.toArray

        use cursor = dataView.GetRowCursor(schemaArr, null)

        // Getters must be created once before the iteration loop starts.
        let readers = schemaArr |> Array.map (FromDataView.makeReader cursor)

        let colBuffers = Array.init schemaArr.Length (fun _ -> ResizeArray<obj>())

        while cursor.MoveNext() do
            for i in 0 .. readers.Length - 1 do
                colBuffers.[i].Add(readers.[i]())

        let n       = if colBuffers.Length = 0 then 0 else colBuffers.[0].Count
        let rowKeys = Array.init n id

        let colSeries =
            Array.init schemaArr.Length (fun i ->
                let col  = schemaArr.[i]
                let vals = colBuffers.[i].ToArray()
                col.Name, Series(rowKeys, vals))

        Deedle.Frame.ofColumns colSeries
