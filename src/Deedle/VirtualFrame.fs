namespace Deedle.Virtual

// ------------------------------------------------------------------------------------------------
// Helpers that can be used when implementing Lookup in your own Deedle sources
// ------------------------------------------------------------------------------------------------

module IndexUtilsModule =
  open Deedle
  open System

  /// Binary search in range [ 0L .. count ]. The function is generic in ^T and
  /// is 'inline' so that the comparison on ^T is optimized.
  ///
  ///  - `count` specifies the upper bound for the binary search
  ///  - `valueAt` is a function that returns value ^T at the specified location
  ///  - `value` is the ^T value that we are looking for
  ///  - `lookup` is the lookup semantics as used in Deedle
  ///  - `check` is a function that tests whether we want a given location
  ///    (if no, we scan - this can be used to find the first available value in a series)
  ///
  let inline binarySearch count (valueAt:Func<int64, ^T>) value (lookup:Lookup) (check:Func<_, _>) =

    /// Binary search the 'asOfTicks' series, looking for the
    /// specified 'asOf' (the invariant is that: lo <= res < hi)
    /// The result is index 'idx' such that: 'asOfAt idx <= asOf && asOf (idx+1) > asOf'
    let rec binarySearch lo hi =
      let mid = (lo + hi) / 2L
      if lo + 1L = hi then lo
      else
        if valueAt.Invoke mid > value then binarySearch lo mid
        else binarySearch mid hi

    /// Scan the series, looking for first value that passes 'check'
    let rec scan next idx =
      if idx < 0L || idx >= count then OptionalValue.Missing
      elif check.Invoke idx then OptionalValue(idx)
      else scan next (next idx)

    if count = 0L then OptionalValue.Missing
    else
      let found = binarySearch 0L count
      match lookup with
      | Lookup.Exact ->
          // We're looking for an exact value, if it's not the one at 'idx' then Nothing
          if valueAt.Invoke found = value && check.Invoke found then OptionalValue(found)
          else OptionalValue.Missing
      | Lookup.ExactOrGreater | Lookup.ExactOrSmaller when valueAt.Invoke found = value && check.Invoke found ->
          // We found an exact match and we the lookup behaviour permits that
          OptionalValue(found)
      | Lookup.Greater | Lookup.ExactOrGreater ->
          // Otherwise we need to scan (because the found value does not work or is not allowed)
          scan ((+) 1L) (if valueAt.Invoke found <= value then found + 1L else found)
      | Lookup.Smaller | Lookup.ExactOrSmaller ->
          scan ((-) 1L) (if valueAt.Invoke found >= value then found - 1L else found)
      | _ -> invalidArg "lookup" "Unexpected Lookup behaviour"

/// Helpers that can be used when implementing Lookup
type IndexUtils =
  /// See the comment for `IndexUtilsModule.binarySearch`
  static member BinarySearch(count, valueAt, (value:int64), lookup, check) =
    IndexUtilsModule.binarySearch count valueAt value lookup check


// ------------------------------------------------------------------------------------------------
// Public API for creating virtual frames and series
// ------------------------------------------------------------------------------------------------

open Deedle
open Deedle.Ranges
open Deedle.Internal
open Deedle.Addressing
open Deedle.Vectors
open Deedle.Vectors.Virtual
open Deedle.Indices.Virtual
open System

module Address = LinearAddress

/// <exclude />
///
/// Helper that is invoked via Reflection to create generic virtual vectors.
type VirtualVectorHelper =
  static member Create<'T>(source:IVirtualVectorSource<'T>) =
    VirtualVector<'T>(source)

  static member GetSource<'T>(vec: VirtualVector<'T>) =
    vec.Source :> IVirtualVectorSource

/// Options for [`Virtual.ReadCsv`].
type VirtualReadCsvOptions =
  { /// Column used as ordered row index when strictly increasing and unique in file order. When `None`, rows are `0 .. N-1`.
    IndexColumn: string option
    /// Searchable columns and their LookupRange modes (`Infer` when omitted at the API).
    SearchColumns: VirtualSearchColumn list
    /// Explicit column keys (defaults to all CSV columns except index column).
    ColumnKeys: string list option
    /// When true (default), index rows by file byte offset and read lines on demand. Pass `false` to cache every line string in RAM.
    ByteOffsetIndex: bool
    /// When false, the first row is data and columns are named `Column1`, `Column2`, … (same as `Frame.ReadCsv`).
    HasHeaders: bool }

  static member Default =
    { IndexColumn = None
      SearchColumns = []
      ColumnKeys = None
      ByteOffsetIndex = true
      HasHeaders = true }

/// Provides static methods for creating virtual series and virtual frames.
/// Those provide necessary wrapping around `IVirtualVectorSource` values
type Virtual private () =
  static let createMi = typeof<VirtualVectorHelper>.GetMethod("Create")

  static let createFrame rowIndex columnIndex (sources:seq<IVirtualVectorSource>) =
    let data =
      sources
      |> Seq.map (fun source ->
          createMi.MakeGenericMethod(source.ElementType).Invoke(null, [| source |]) :?> IVector)
      |> Vector.ofValues
    Frame<_, _>(rowIndex, columnIndex, data, VirtualIndexBuilder.Instance, VirtualVectorBuilder.Instance)

  /// Creates a virtual series with ordinal index. The parameter is `IVirtualVectorSource`
  /// that specifies how to access values in the series (and is also used to determine the size
  /// of the series index)
  static member CreateOrdinalSeries(source) =
    let vector = VirtualVector(source)
    let index = VirtualOrdinalIndex(Ranges.inlineCreate (+) [ 0L, source.Length-1L ], source)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)


  /// Create a virtual series with an index and values specified by two `IVirtualVectorSource` values.
  /// The index source should support lookup (which is used for series lookup, slicing etc.)
  /// The value source does not need to implement lookup - mainly `ValueAt`, merging and getting sub-source
  static member CreateSeries(indexSource:IVirtualVectorSource<_>, valueSource:IVirtualVectorSource<_>) =
    let vector = VirtualVector(valueSource)
    let index = VirtualOrderedIndex(indexSource)
    Series(index, vector, VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance)

  /// Create a frame with ordinal index, containing the specified sources as columns.
  static member CreateOrdinalFrame(keys:seq<_>, sources:seq<IVirtualVectorSource>) =
    let count = sources |> Seq.fold (fun st src ->
      match st with
      | None -> Some(src.Length)
      | Some n when n = src.Length -> Some(n)
      | _ -> invalidArg "sources" "Sources should have the same length!" ) None
    let count =
      match count with
      | Some n -> n
      | None -> invalidArg "sources" "At least one column is required"
    let source = sources |> Seq.head
    createFrame (VirtualOrdinalIndex(Ranges.inlineCreate (+) [0L, count-1L], source)) (Index.ofKeys (ReadOnlyCollection.ofSeq keys)) sources

  /// Create a frame with ordinal index, containing the specified sources as columns.
  /// The index source should support lookup (which is used for series lookup, slicing etc.)
  /// The value source does not need to implement lookup - mainly `ValueAt`, merging and getting sub-source
  static member CreateFrame(indexSource:IVirtualVectorSource<_>, keys, sources:seq<IVirtualVectorSource>) =
    createFrame (VirtualOrderedIndex indexSource) (Index.ofKeys (ReadOnlyCollection.ofSeq keys)) sources

/// Row order when splitting a frame into mini-batches.
type FloatBatchOrder =
  /// Contiguous row ranges in frame order (default).
  | Sequential
  /// Uniform random permutation of rows. Each row appears in exactly one batch.
  /// A row-index permutation (`O(n)` int64 memory) is built when enumeration starts.
  | Shuffled
  /// Like <see cref="Shuffled"/>, with a fixed seed for reproducibility.
  | ShuffledWithSeed of seed: int

/// Missing-value handling used by [`Virtual.MaterializeFloatBatches`].
type FloatMissingPolicy =
  /// Map missing cells to `Double.NaN`.
  | NaN
  /// Map missing cells to a caller-supplied float.
  | Value of float

/// Layout of [`FloatBatch.FeaturesFlat`].
type FloatBatchLayout =
  /// `index = row * cols + col`
  | RowMajor
  /// `index = col * rows + row`
  | ColumnMajor

/// One mini-batch from [`Virtual.MaterializeFloatBatches`].
type FloatBatch<'TRowKey> =
  { Rows : int
    Cols : int
    Layout : FloatBatchLayout
    /// Contiguous feature matrix (`Rows * Cols` elements).
    FeaturesFlat : float[]
    /// Optional label column (`Rows` elements), same missing policy as features.
    Labels : float[] option
    /// When requested, `true` where the source cell was missing (`Rows * Cols`, same layout as features).
    MissingMask : bool[] option
    RowKeys : 'TRowKey[] option }

  /// Row-major jagged view of the batch features (built from the flat buffer).
  member this.Features =
    if this.Cols = 0 then
      Array.init this.Rows (fun _ -> [||])
    else
      let index row col =
        match this.Layout with
        | FloatBatchLayout.RowMajor -> row * this.Cols + col
        | FloatBatchLayout.ColumnMajor -> col * this.Rows + row
      Array.init this.Rows (fun row ->
        Array.init this.Cols (fun col -> this.FeaturesFlat.[index row col]))

module private FloatBatchMaterialize =
  open Deedle

  let shuffleInPlace (rng: System.Random) (items: int64[]) =
    for i in items.Length - 1 .. -1 .. 0 do
      let j = rng.Next(i + 1)
      let tmp = items.[i]
      items.[i] <- items.[j]
      items.[j] <- tmp

  let buildPermutation total order =
    match order with
    | FloatBatchOrder.Sequential -> None
    | FloatBatchOrder.Shuffled | FloatBatchOrder.ShuffledWithSeed _ ->
        let arr = Array.init (int total) int64
        let rng =
          match order with
          | FloatBatchOrder.ShuffledWithSeed seed -> System.Random(seed)
          | FloatBatchOrder.Shuffled -> System.Random()
          | FloatBatchOrder.Sequential -> failwith "unreachable"
        shuffleInPlace rng arr
        Some arr

  let readNumericValueAtOrdinal (frame: Frame<_, _>) (series: Series<_, 'T>) ordinal (convert: 'T -> float) =
    let addr = frame.RowIndex.AddressAt ordinal
    let cell = series.GetAddressRange(RangeRestriction.Fixed(addr, addr))
    match cell.Vector.DataSequence |> Seq.head with
    | OptionalValue.Present v -> convert v, false
    | OptionalValue.Missing -> Double.NaN, true

  let readNumericColumnAtOrdinals (frame: Frame<_, _>) (colKey: 'TColumnKey) (ordinals: int64[]) =
    match frame.TryGetColumn<float>(colKey, Lookup.Exact) with
    | OptionalValue.Present s ->
      ordinals |> Array.map (fun i -> readNumericValueAtOrdinal frame s i id)
    | OptionalValue.Missing ->
      try
        let s = frame.GetColumn<int64>(colKey)
        ordinals |> Array.map (fun i -> readNumericValueAtOrdinal frame s i float)
      with _ ->
        invalidArg "columns" (sprintf "Column '%A' is not numeric (float or int64)" colKey)

  let layoutIndex rows cols layout row col =
    match layout with
    | FloatBatchLayout.RowMajor -> row * cols + col
    | FloatBatchLayout.ColumnMajor -> col * rows + row

  let writeBatchAtOrdinals
    (frame: Frame<'TRowKey, 'TColumnKey>)
    (columns: 'TColumnKey[])
    (ordinals: int64[])
    (layout: FloatBatchLayout)
    (missingValue: float)
    (useNan: bool)
    (includeMissingMask: bool)
    (labelsColumn: 'TColumnKey option)
    =
    let rowCount = ordinals.Length
    let colCount = columns.Length
    let cellCount = rowCount * colCount

    let resolveMissing (value: float) (isMissing: bool) =
      if isMissing then
        let resolved = if useNan then Double.NaN else missingValue
        resolved, true
      else
        value, false

    let featuresFlat = Array.zeroCreate<float> cellCount
    let missingMask =
      if includeMissingMask then Some(Array.zeroCreate cellCount) else None

    for colIdx in 0 .. colCount - 1 do
      let values = readNumericColumnAtOrdinals frame columns.[colIdx] ordinals
      for rowIdx in 0 .. rowCount - 1 do
        let rawValue, rawMissing = values.[rowIdx]
        let value, isMissing = resolveMissing rawValue rawMissing
        let flatIdx = layoutIndex rowCount colCount layout rowIdx colIdx
        featuresFlat.[flatIdx] <- value
        match missingMask with
        | Some mask -> mask.[flatIdx] <- isMissing
        | None -> ()

    let labels =
      match labelsColumn with
      | None -> None
      | Some colKey ->
        Some(
          readNumericColumnAtOrdinals frame colKey ordinals
          |> Array.map (fun (v, m) ->
            let resolved, _ = resolveMissing v m
            resolved))

    featuresFlat, labels, missingMask

  let readNumericColumn (frame: Frame<_, _>) (colKey: 'TColumnKey) =
    let mapSeq (series: Series<_, 'T>) (convert: 'T -> float) =
      series.Vector.DataSequence
      |> Seq.map (function
        | OptionalValue.Present v -> convert v, false
        | OptionalValue.Missing -> Double.NaN, true)
      |> Seq.toArray

    match frame.TryGetColumn<float>(colKey, Lookup.Exact) with
    | OptionalValue.Present s -> mapSeq s id
    | OptionalValue.Missing ->
      try mapSeq (frame.GetColumn<int64>(colKey)) float
      with _ ->
        invalidArg "columns" (sprintf "Column '%A' is not numeric (float or int64)" colKey)

  let writeBatch
    (batchFrame: Frame<'TRowKey, 'TColumnKey>)
    (columns: 'TColumnKey[])
    (layout: FloatBatchLayout)
    (missingValue: float)
    (useNan: bool)
    (includeMissingMask: bool)
    (labelsColumn: 'TColumnKey option)
    =
    let rowCount = int batchFrame.RowIndex.KeyCount
    let colCount = columns.Length
    let cellCount = rowCount * colCount

    let resolveMissing (value: float) (isMissing: bool) =
      if isMissing then
        let resolved = if useNan then Double.NaN else missingValue
        resolved, true
      else
        value, false

    let featuresFlat = Array.zeroCreate<float> cellCount
    let missingMask =
      if includeMissingMask then Some(Array.zeroCreate cellCount) else None

    for colIdx in 0 .. colCount - 1 do
      let values = readNumericColumn batchFrame columns.[colIdx]
      for rowIdx in 0 .. rowCount - 1 do
        let rawValue, rawMissing = values.[rowIdx]
        let value, isMissing = resolveMissing rawValue rawMissing
        let flatIdx = layoutIndex rowCount colCount layout rowIdx colIdx
        featuresFlat.[flatIdx] <- value
        match missingMask with
        | Some mask -> mask.[flatIdx] <- isMissing
        | None -> ()

    let labels =
      match labelsColumn with
      | None -> None
      | Some colKey ->
        Some(
          readNumericColumn batchFrame colKey
          |> Array.map (fun (v, m) ->
            let resolved, _ = resolveMissing v m
            resolved))

    featuresFlat, labels, missingMask

type Virtual with
  /// <summary>
  /// Materialize selected numeric columns as a lazy sequence of mini-batches.
  /// Each batch is produced by slicing the frame and reading only those rows × columns.
  /// With <see cref="FloatBatchOrder.Sequential"/> (default), batches are contiguous
  /// address ranges. With random order, rows are permuted once when enumeration starts
  /// and each row appears in exactly one batch. Enumerating later batches does not
  /// re-read earlier rows or allocate a full-frame feature matrix.
  /// </summary>
  /// <param name="frame">Source frame (virtual or in-memory).</param>
  /// <param name="batchSize">Positive number of rows per batch (last batch may be smaller).</param>
  /// <param name="columns">Column keys to materialize (`float` or `int64`).</param>
  /// <param name="missingPolicy">How to replace missing cells (default <see cref="FloatMissingPolicy.NaN"/>).</param>
  /// <param name="includeRowKeys">When set, also copy this batch's row keys (may touch the key source).</param>
  /// <param name="labelsColumn">Optional single label/target column (`float` or `int64`, same missing policy).</param>
  /// <param name="layout">Row-major (default) or column-major flat layout.</param>
  /// <param name="includeMissingMask">When true, set <see cref="FloatBatch.MissingMask"/> for feature cells.</param>
  /// <param name="maxRows">Optional cap on total rows exported across all batches.</param>
  /// <param name="order">Row order within batches (default sequential). <see cref="FloatBatchOrder.Shuffled"/> shuffles rows once per enumeration.</param>
  static member MaterializeFloatBatches
    (frame:Frame<'TRowKey, 'TColumnKey>,
     batchSize:int64,
     columns:'TColumnKey list,
     ?missingPolicy:FloatMissingPolicy,
     ?includeRowKeys:bool,
     ?labelsColumn:'TColumnKey,
     ?layout:FloatBatchLayout,
     ?includeMissingMask:bool,
     ?maxRows:int64,
     ?order:FloatBatchOrder)
      : seq<FloatBatch<'TRowKey>> =
    if batchSize <= 0L then invalidArg "batchSize" "Must be positive"

    let missingPolicy = defaultArg missingPolicy FloatMissingPolicy.NaN
    let missingValue =
      match missingPolicy with
      | FloatMissingPolicy.NaN -> Double.NaN
      | FloatMissingPolicy.Value v -> v
    let useNan =
      match missingPolicy with
      | FloatMissingPolicy.NaN -> true
      | _ -> false

    let includeRowKeys = defaultArg includeRowKeys false
    let layout = defaultArg layout FloatBatchLayout.RowMajor
    let includeMissingMask = defaultArg includeMissingMask false
    let order = defaultArg order FloatBatchOrder.Sequential
    let columnsArr = columns |> List.toArray
    let total =
      match maxRows with
      | Some cap -> min frame.RowIndex.KeyCount cap
      | None -> frame.RowIndex.KeyCount

    let permutation = FloatBatchMaterialize.buildPermutation total order

    Seq.unfold (fun batchStart ->
      if batchStart >= total then None
      else
        let batchLen = min batchSize (total - batchStart)
        let rowCount = int batchLen
        let colCount = columnsArr.Length

        let featuresFlat, labels, missingMask, rowKeysOpt =
          match permutation with
          | None ->
              let last = batchStart + batchLen - 1L
              let loAddr = frame.RowIndex.AddressAt(batchStart)
              let hiAddr = frame.RowIndex.AddressAt(last)
              let batchFrame = frame.GetAddressRange(RangeRestriction.Fixed(loAddr, hiAddr))
              let featuresFlat, labels, missingMask =
                FloatBatchMaterialize.writeBatch
                  batchFrame columnsArr layout missingValue useNan includeMissingMask labelsColumn
              let rowKeysOpt =
                if includeRowKeys then
                  Some(batchFrame.RowIndex.KeySequence |> Seq.toArray)
                else
                  None
              featuresFlat, labels, missingMask, rowKeysOpt
          | Some perm ->
              let ordinals =
                Array.init (int batchLen) (fun j -> perm.[int batchStart + j])
              let featuresFlat, labels, missingMask =
                FloatBatchMaterialize.writeBatchAtOrdinals
                  frame columnsArr ordinals layout missingValue useNan includeMissingMask labelsColumn
              let rowKeysOpt =
                if includeRowKeys then
                  Some(
                    ordinals
                    |> Array.map (fun i -> frame.RowIndex.KeyAt(frame.RowIndex.AddressAt i)))
                else
                  None
              featuresFlat, labels, missingMask, rowKeysOpt

        Some(
          { Rows = rowCount
            Cols = colCount
            Layout = layout
            FeaturesFlat = featuresFlat
            Labels = labels
            MissingMask = missingMask
            RowKeys = rowKeysOpt },
          batchStart + batchLen)
    ) 0L

/// Diagnostic access to LookupRange configuration stored on a virtual column source.
type IVirtualVectorSourceLookupDiagnostics =
  abstract TryGetLookupRange : unit -> VirtualColumnLookupRange option

/// Ordinal pull-on-read virtual source with optional LookupRange semantics.
type OrdinalVirtualSource<'T>
    ( length: int64,
      valueAt: int64 -> OptionalValue<'T>,
      schemeId: string,
      ?asLong: 'T -> int64,
      ?lookupRange: LookupRangeMode<'T>,
      ?searchColumnConfigured: bool ) =

  let lookupRangeMode = defaultArg lookupRange LookupRangeUnsupported
  let searchColumnConfigured = defaultArg searchColumnConfigured false
  let addressing = Indices.Linear.LinearAddressOperations(0L, length - 1L) :> IAddressOperations
  let context = sprintf "OrdinalVirtualSource<%s>" (typeof<'T>.Name)

  let valueAtLoc (loc: IVectorLocation) =
    valueAt (Address.asInt64 loc.Address)

  let rec createFromSpec (spec: LookupRangeExecutor.SubVectorSpec<'T>) =
    let subValueAt i = valueAt (spec.MapRow i)
    OrdinalVirtualSource<'T>(spec.Length, subValueAt, schemeId, ?asLong=spec.AsLong, lookupRange=spec.LookupRange, searchColumnConfigured=searchColumnConfigured) :> IVirtualVectorSource<'T>

  interface IVirtualVectorSource with
    member this.Length = length
    member this.AddressingSchemeID = schemeId
    member this.ElementType = typeof<'T>
    member this.AddressOperations = addressing
    member this.Invoke(op) = op.Invoke(this :> IVirtualVectorSource<'T>)

  interface IVirtualVectorSource<'T> with
    member _.MergeWith(sources) =
      let parts =
        (length, valueAt)
        :: [ for s in sources ->
               match s with
               | :? OrdinalVirtualSource<'T> as src -> src.Length, src.RawValueAt
               | _ -> invalidOp "MergeWith: expected OrdinalVirtualSource" ]
      let total = parts |> List.sumBy fst
      let rec valueAtMerged i = function
        | [] -> invalidOp (sprintf "MergeWith: index %d out of range (len=%d)" i total)
        | (len, vat)::rest ->
            if i < len then vat i
            else valueAtMerged (i - len) rest
      let mergedValueAt i = valueAtMerged i parts
      OrdinalVirtualSource<'T>(total, mergedValueAt, schemeId, ?asLong=asLong, lookupRange=lookupRangeMode, searchColumnConfigured=searchColumnConfigured) :> _

    member _.LookupRange(v) =
      match lookupRangeMode with
      | LookupRangeUnsupported ->
          VirtualVectorSource.scanLookupRange addressing valueAtLoc v
      | mode ->
          LookupRangeExecutor.lookupRange length mode v context

    member _.LookupValue(k, l, check) =
      let asLongFn =
        match asLong with
        | Some g -> g
        | None -> invalidOp "LookupValue: asLong not configured"
      let longAt i =
        match valueAt i with
        | OptionalValue.Present v -> asLongFn v
        | OptionalValue.Missing -> Int64.MinValue
      let c = Func<int64, bool>(fun i ->
        match valueAt i with
        | OptionalValue.Present _ -> check.Invoke(Address.ofInt64 i)
        | OptionalValue.Missing -> false)
      IndexUtilsModule.binarySearch length (Func<_, _>(fun i -> longAt i)) (asLongFn k) l c
      |> OptionalValue.bind (fun i ->
          match valueAt i with
          | OptionalValue.Present v -> OptionalValue((v, Address.ofInt64 i))
          | OptionalValue.Missing -> OptionalValue.Missing)

    member _.ValueAt(loc) =
      valueAt (Address.asInt64 loc.Address)

    member _.GetSubVector(range) =
      match LookupRangeExecutor.getSubVector length lookupRangeMode asLong range with
      | Choice1Of2 spec -> createFromSpec spec
      | Choice2Of2 _ -> invalidOp "GetSubVector: unexpected result"

  interface IVirtualVectorSourceLookupDiagnostics with
    member _.TryGetLookupRange() =
      if searchColumnConfigured then Some(VirtualLookupRange.classifyLookupRange lookupRangeMode)
      else None

  member _.Length = length
  member _.RawValueAt(i: int64) = valueAt i
  member _.TryGetLookupRange() =
    if searchColumnConfigured then Some(VirtualLookupRange.classifyLookupRange lookupRangeMode)
    else None

// ------------------------------------------------------------------------------------------------
// Diagnostics for virtual frames
// ------------------------------------------------------------------------------------------------

/// Describes how the row index of a virtual frame is stored.
type VirtualRowIndexKind =
  | OrderedVirtual
  | OrdinalVirtual
  | LinearOrOther


module private VirtualFrameDiag =
  open Deedle.Vectors.Virtual
  open Deedle.VectorHelpers

  /// True when the vector uses a virtual addressing scheme (unwrap not needed; wrappers preserve scheme).
  let isVirtualVector (vec: IVector) =
    match vec.AddressingScheme with
    | :? VirtualAddressingScheme -> true
    | _ -> false

  let private getSourceMi = typeof<VirtualVectorHelper>.GetMethod("GetSource")

  let private tryGetSourceFromVector (vec: IVector) =
    let rec unwrap (v: IVector) =
      match v with
      | :? IWrappedVector<obj> as wrapped -> unwrap (wrapped.UnwrapVector() :> IVector)
      | _ -> v
    let v = unwrap vec
    if not (isVirtualVector v) then None
    else
      Some (getSourceMi.MakeGenericMethod(v.ElementType).Invoke(null, [| v |]) :?> IVirtualVectorSource)

  let rec tryFindLookupDiagnostics (source: IVirtualVectorSource) =
    match source with
    | :? IVirtualVectorSourceLookupDiagnostics as d -> Some d
    | :? VirtualVectorSource.ILinearAddressedSource<'T> as wrapped -> tryFindLookupDiagnostics wrapped.Source
    | :? VirtualVectorSource.IBoxedVectorSource<'T> as wrapped -> tryFindLookupDiagnostics wrapped.Source
    | :? VirtualVectorSource.IMappedVectorSource<_, _> as wrapped -> tryFindLookupDiagnostics wrapped.Source
    | _ -> None

  let tryGetColumnSource (frame: Frame<_, _>) (column: 'C when 'C : equality) =
    match frame.ColumnIndex.Lookup(column, Lookup.Exact, fun _ -> true) with
    | OptionalValue.Present(_, addr) ->
        match frame.Data.GetValue addr with
        | OptionalValue.Present vec -> tryGetSourceFromVector vec
        | _ -> None
    | OptionalValue.Missing -> None

type Virtual with
  /// Classify how the frame's row index is stored (ordered/ordinal virtual vs linear).
  static member GetRowIndexKind(frame: Frame<'R, 'C>) =
    match frame.RowIndex with
    | :? VirtualOrdinalIndex -> VirtualRowIndexKind.OrdinalVirtual
    | :? VirtualOrderedIndex<'R> -> VirtualRowIndexKind.OrderedVirtual
    | _ -> VirtualRowIndexKind.LinearOrOther

  /// True when the row index is ordered or ordinal virtual.
  static member IsVirtualRowIndex(frame: Frame<'R, 'C>) =
    match Virtual.GetRowIndexKind frame with
    | VirtualRowIndexKind.LinearOrOther -> false
    | OrderedVirtual | OrdinalVirtual -> true

  /// True when the named column uses a virtual addressing scheme.
  static member IsVirtualColumn(frame: Frame<'R, 'C>, column: 'C when 'C : equality) =
    match frame.ColumnIndex.Lookup(column, Lookup.Exact, fun _ -> true) with
    | OptionalValue.Present(_, addr) ->
        match frame.Data.GetValue addr with
        | OptionalValue.Present vec -> VirtualFrameDiag.isVirtualVector vec
        | OptionalValue.Missing -> false
    | OptionalValue.Missing -> false

  /// Short human-readable summary of row count, row-index kind, and column count.
  static member Describe(frame: Frame<'R, 'C>) =
    let kind =
      match Virtual.GetRowIndexKind frame with
      | OrderedVirtual -> "ordered virtual"
      | OrdinalVirtual -> "ordinal virtual (0..N-1)"
      | LinearOrOther -> "linear / materialized"
    sprintf "rows=%d, rowIndex=%s, columns=%d" frame.RowCount kind frame.ColumnCount

  /// Scheme id from the virtual row-index source, when present (e.g. `"csv-file"`, instrumented test ids).
  static member TryGetRowIndexSchemeId(frame: Frame<'R, 'C>) =
    match frame.RowIndex with
    | :? VirtualOrderedIndex<'R> as idx -> Some idx.Source.AddressingSchemeID
    | :? VirtualOrdinalIndex as idx -> Some idx.Source.AddressingSchemeID
    | _ -> None

  /// LookupRange kind for a column listed in `searchColumns` at load (inferred or explicit).
  /// `None` when the column was not configured as searchable or is not virtual.
  static member TryGetLookupRange(frame: Frame<'R, 'C>, column: 'C when 'C : equality) =
    match VirtualFrameDiag.tryGetColumnSource frame column with
    | None -> None
    | Some source ->
        match VirtualFrameDiag.tryFindLookupDiagnostics source with
        | None -> None
        | Some diag -> diag.TryGetLookupRange()
