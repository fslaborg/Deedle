#nowarn "10001"
namespace Deedle

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

open Deedle
open Deedle.Keys
open Deedle.Addressing
open Deedle.Internal
open Deedle.Indices
open Deedle.Vectors
open Deedle.JoinHelpers

open System
open System.ComponentModel
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Runtime.InteropServices
open System.Collections.Specialized
open Microsoft.FSharp.Quotations
open VectorHelpers

/// Represents the underlying (raw) data of the frame in a format that can
/// be used for exporting data frame to other formats etc. (DataTable, CSV, Excel)
///
/// [category:Core frame and series types]
type FrameData =
  { /// A sequence of keys for all column. Individual key is an array
    /// which contains multiple values for hierarchical indices
    ColumnKeys : seq<obj[]>
    
    /// A sequence of keys for all rows. Individual key is an array
    /// which contains multiple values for hierarchical indices
    RowKeys : seq<obj[]>

    /// Represents the data of the frame as a sequence of columns containing
    /// type and array with column values. `OptionalValue.Missing` is used to
    /// represent missing data.
    Columns : seq<Type * IVector<obj>> }  

/// An empty interface that is implemented by `Frame<'R, 'C>`. The purpose of the
/// interface is to allow writing code that works on arbitrary data frames (you 
/// need to provide an implementation of the `IFrameOperation<'V>` which contains
/// a generic method `Invoke` that will be called with the typed data frame).
///
/// [category:Specialized frame and series types]
type IFrame = 
  /// Calls the `Invoke` method of the specified interface `IFrameOperation<'V>`
  /// with the typed data frame as an argument
  abstract Apply : IFrameOperation<'V> -> 'V

/// Represents an operation that can be invoked on `Frame<'R, 'C>`. The operation
/// is generic in the type of row and column keys.
///
/// [category:Specialized frame and series types]
and IFrameOperation<'V> =
  abstract Invoke : Frame<'R, 'C> -> 'V

// --------------------------------------------------------------------------------------
// Data frame
// --------------------------------------------------------------------------------------

/// A frame is the key Deedle data structure (together with series). It represents a 
/// data table (think spreadsheet or CSV file) with multiple rows and columns. The frame 
/// consists of row index, column index and data. The indices are used for efficient 
/// lookup when accessing data by the row key `'TRowKey` or by the column key 
/// `'TColumnKey`. Deedle frames are optimized for the scenario when all values in a given 
/// column are of the same type (but types of different columns can differ).
///
/// ## Joining, zipping and appending
/// More info
///
///
/// [category:Core frame and series types]
and Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>
    ( rowIndex:IIndex<'TRowKey>, columnIndex:IIndex<'TColumnKey>, 
      data:IVector<IVector>, indexBuilder:IIndexBuilder, vectorBuilder:IVectorBuilder) =

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  // Check that the addressing schemes match
  do for v in data.DataSequence do
       if rowIndex.AddressingScheme <> v.Value.AddressingScheme then
         invalidOp "Row index and vectors of a frame should share addressing scheme!"
     if columnIndex.AddressingScheme <> data.AddressingScheme then
       invalidOp "Column index and data vector of a frame should share addressing scheme!"

  let mutable isEmpty = rowIndex.IsEmpty && columnIndex.IsEmpty

  // TODO: Perhaps assert that the 'data' vector has all things required by column index
  // (to simplify various handling below)

  // NOTE: Row index is only mutated when adding the first series to a data
  // frame that has been created as empty, otherwise it is immutable
  let mutable rowIndex = rowIndex       
  let mutable columnIndex = columnIndex
  let mutable data = data

  let frameColumnsChanged = new DelegateEvent<NotifyCollectionChangedEventHandler>()
  
  let safeGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then 
      invalidArg "column" (sprintf "Column with a key '%O' does not exist in the data frame" column)
    let columnVector = data.GetValue (snd columnIndex.Value)
    if not columnVector.HasValue then
      invalidOp "column" (sprintf "Column with a key '%O' is present, but does not contain a value" column)
    columnVector.Value

  let tryGetColVector column = 
    let columnIndex = columnIndex.Lookup(column)
    if not columnIndex.HasValue then OptionalValue.Missing else
    data.GetValue (snd columnIndex.Value)

  /// Create frame from a series of columns. This is used inside Frame and so we have to have it
  /// as a static member here. The function is optimised for the case when all series share the
  /// same index (by checking object reference equality)
  static let fromColumnsNonGeneric indexBuilder vectorBuilder (seriesConv:'S -> ISeries<_>) (nested:Series<_, 'S>) = 
    let columns = Series.observations nested |> Array.ofSeq
    let rowIndex = Seq.headOrNone columns |> Option.map (fun (_, s) -> (seriesConv s).Index)
    match rowIndex with 
    | Some rowIndex when (columns |> Seq.forall (fun (_, s) -> Object.ReferenceEquals((seriesConv s).Index, rowIndex))) ->
        // OPTIMIZATION: If all series have the same index (same object), then no join is needed 
        // (This is particularly valuable for things like +, *, /, - operators on Frame)
        let vector = columns |> Seq.map (fun (_, s) -> 
          // When the nested series data is in 'IBoxedVector', get the unboxed representation
          unboxVector (seriesConv s).Vector) |> Vector.ofValues
        Frame<_, _>(rowIndex, Index.ofKeys (Array.map fst columns), vector, indexBuilder, vectorBuilder)

    | _ ->
        // Create new row index by unioning all keys
        let rowKeys = columns |> Seq.collect (fun (_, s) -> (seriesConv s).Index.Keys) |> Seq.distinct |> Array.ofSeq
        // If all source series were sorted, the resulting frame should also be sorted
        let sorted = 
          if nested.ValueCount > 0 && columns |> Seq.forall (fun (_, s) -> (seriesConv s).Index.IsOrdered) then
            let comparer = columns |> Seq.pick (fun (_, s) -> Some((seriesConv s).Index.Comparer))
            Array.sortInPlaceWith (fun a b -> comparer.Compare(a, b)) rowKeys
            Some true
          else None
        let rowIndex = nested.IndexBuilder.Create(rowKeys, sorted)

        // Create column index by taking all column keys
        let colKeys = nested |> Series.observationsAll |> Seq.map fst |> Array.ofSeq
        let colIndex = nested.IndexBuilder.Create(colKeys, None)
        // Build the data 
        let data = 
          nested |> Series.observationsAll |> Seq.map (fun (_, s) ->
            let series = match s with Some s -> seriesConv s | _ -> Series([], []) :> ISeries<_>
            let cmd = nested.IndexBuilder.Reindex(series.Index, rowIndex, Lookup.Exact, Vectors.Return 0, fun _ -> true)
            // When the nested series data is in 'IBoxedVector', get the unboxed representation
            VectorHelpers.transformColumn nested.VectorBuilder 
              rowIndex.AddressingScheme cmd (unboxVector series.Vector) )
          |> Vector.ofValues
        Frame<_, _>(rowIndex, colIndex, data, indexBuilder, vectorBuilder)

  static member internal FromColumnsNonGeneric indexBuilder vectorBuilder seriesConv nested = 
    fromColumnsNonGeneric indexBuilder vectorBuilder seriesConv nested

  member private x.setColumnIndex newColumnIndex = 
    columnIndex <- newColumnIndex
    let args = NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset)
    frameColumnsChanged.Trigger([| x; args |])

  member internal x.IndexBuilder = indexBuilder
  member internal x.VectorBuilder = vectorBuilder
  member internal frame.Data = data

  member frame.RowIndex = rowIndex
  member frame.ColumnIndex = columnIndex

  member frame.RowCount = frame.RowIndex.KeyCount |> int
  member frame.ColumnCount = frame.ColumnIndex.KeyCount |> int

  // ----------------------------------------------------------------------------------------------
  // Joining, zipping and appending
  // ----------------------------------------------------------------------------------------------

  // Note - this has to be actual member and not an extension so that C# callers can specify
  // generic type arguments using `df.Zip<double, double, double>(...)` (doesn't work for extensions)

  /// Aligns two data frames using both column index and row index and apply the specified operation
  /// on values of a specified type that are available in both data frames. The parameters `columnKind`,
  /// and `rowKind` can be specified to determine how the alginment works (similarly to `Join`).
  /// Column keys are always matched using `Lookup.Exact`, but `lookup` determines lookup for rows.
  ///
  /// Once aligned, the call `df1.Zip<T>(df2, f)` applies the specifed function `f` on all `T` values
  /// that are available in corresponding locations in both frames. For values of other types, the 
  /// value from `df1` is returned.
  ///
  /// ## Parameters
  ///  - `otherFrame` - Other frame to be aligned and zipped with the current instance
  ///  - `columnKind` - Specifies how to align columns (inner, outer, left or right join)
  ///  - `rowKind` - Specifies how to align rows (inner, outer, left or right join)
  ///  - `lookup` - Specifies how to find matching value for a row (when using left or right join on rows)
  ///    Supported values are `Lookup.Exact`, `Lookup.ExactOrSmaller` and `Lookup.ExactOrGreater`.
  ///  - `op` - A function that is applied to aligned values. The `Zip` operation is generic
  ///    in the type of this function and the type of function is used to determine which 
  ///    values in the frames are zipped and which are left unchanged.
  ///
  /// [category:Joining, zipping and appending]
  member frame1.Zip<'V1, 'V2, 'V3>(otherFrame:Frame<'TRowKey, 'TColumnKey>, columnKind, rowKind, lookup, op:Func<'V1, 'V2, 'V3>) =
    
    // Create transformations to join the rows (using the same logic as Join)
    // and make functions that transform vectors (when they are only available in first/second frame)
    let rowIndex, f1cmd, f2cmd = 
      createJoinTransformation indexBuilder otherFrame.IndexBuilder rowKind lookup rowIndex 
        otherFrame.RowIndex (Vectors.Return 0) (Vectors.Return 1)
    let f1trans = VectorHelpers.transformColumn vectorBuilder rowIndex.AddressingScheme f1cmd
    let f2trans = VectorHelpers.transformColumn vectorBuilder rowIndex.AddressingScheme (VectorHelpers.substitute (1, Vectors.Return 0) f2cmd)

    // To join columns using 'Series.join', we create series containing raw "IVector" data 
    // (so that we do not convert each series to objects series)
    let s1 = Series(frame1.ColumnIndex, frame1.Data, frame1.VectorBuilder, frame1.IndexBuilder)
    let s2 = Series(otherFrame.ColumnIndex, otherFrame.Data, otherFrame.VectorBuilder, otherFrame.IndexBuilder)

    // Operations that try converting vectors to the required types for 'op'
    let asV1 = VectorHelpers.tryConvertType<'V1> ConversionKind.Flexible
    let asV2 = VectorHelpers.tryConvertType<'V2> ConversionKind.Flexible
    let (|TryConvert|_|) f inp = OptionalValue.asOption (f inp)

    let newColumns = 
      s1.Zip(s2, columnKind).Select(fun (KeyValue(_, (l, r))) -> 
        match l, r with
        | OptionalValue.Present (TryConvert asV1 lv), OptionalValue.Present (TryConvert asV2 rv) ->
            let lvVect : IVector<Choice<'V1, 'V2, 'V3>> = lv.Select(Choice1Of3)
            let lrVect : IVector<Choice<'V1, 'V2, 'V3>> = rv.Select(Choice2Of3)
            let res = Vectors.Combine(lazy rowIndex.KeyCount, [f1cmd; f2cmd], BinaryTransform.CreateLifted (fun l r ->
              match l, r with
              | Choice1Of3 l, Choice2Of3 r -> op.Invoke(l, r) |> Choice3Of3
              | _ -> failwith "Zip: Got invalid vector while zipping" ))
            frame1.VectorBuilder.Build(rowIndex.AddressingScheme, res, [| lvVect; lrVect |]).
              Select(function Choice3Of3 v -> v | _ -> failwith "Zip: Produced invalid vector") :> IVector
        | OptionalValue.Present v, _ -> f1trans v
        | _, OptionalValue.Present v -> f2trans v
        | _ -> failwith "zipAlignInto: join failed." )
    Frame<_, _>(rowIndex, newColumns.Index, newColumns.Vector, indexBuilder, vectorBuilder)

  /// Aligns two data frames using both column index and row index and apply the specified operation
  /// on values of a specified type that are available in both data frames. This overload uses
  /// `JoinKind.Outer` for both columns and rows.
  ///
  /// Once aligned, the call `df1.Zip<T>(df2, f)` applies the specifed function `f` on all `T` values
  /// that are available in corresponding locations in both frames. For values of other types, the 
  /// value from `df1` is returned.
  ///
  /// ## Parameters
  ///  - `otherFrame` - Other frame to be aligned and zipped with the current instance
  ///  - `op` - A function that is applied to aligned values. The `Zip` operation is generic
  ///    in the type of this function and the type of function is used to determine which 
  ///    values in the frames are zipped and which are left unchanged.
  ///
  /// [category:Joining, zipping and appending]
  member frame1.Zip<'V1, 'V2, 'V3>(otherFrame:Frame<'TRowKey, 'TColumnKey>, op:Func<'V1, 'V2, 'V3>) =
    frame1.Zip<'V1, 'V2, 'V3>(otherFrame, JoinKind.Outer, JoinKind.Outer, Lookup.Exact, op)

  /// Join two data frames. The columns of the joined frames must not overlap and their
  /// rows are aligned and transformed according to the specified join kind.
  /// When the index of both frames is ordered, it is possible to specify `lookup` 
  /// in order to align indices from other frame to the indices of the main frame
  /// (typically, to find the nearest key with available value for a key).
  ///
  /// ## Parameters
  ///  - `otherFrame` - Other frame (right) to be joined with the current instance (left)
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///  - `lookup` - When `kind` is `Left` or `Right` and the two frames have ordered row index,
  ///    this parameter can be used to specify how to find value for a key when there is no
  ///    exactly matching key or when there are missing values.
  ///    Supported values are `Lookup.Exact`, `Lookup.ExactOrSmaller` and `Lookup.ExactOrGreater`.
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>, kind, lookup) =    
    // Union/intersect/align row indices and get transformations to apply to left/right vectors
    let newRowIndex, thisRowCmd, otherRowCmd = 
      createJoinTransformation indexBuilder otherFrame.IndexBuilder kind lookup rowIndex 
        otherFrame.RowIndex (Vectors.Return 0) (Vectors.Return 0)
    // Append the column indices and get transformation to combine them
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newColumnIndex, colCmd = 
      indexBuilder.Merge( [(columnIndex, Vectors.Return 0); (otherFrame.ColumnIndex, Vectors.Return 1) ], BinaryTransform.AtMostOne)
    // Apply transformation to both data vectors
    let newThisData = data.Select(transformColumn vectorBuilder newRowIndex.AddressingScheme thisRowCmd)
    let newOtherData = otherFrame.Data.Select(transformColumn otherFrame.VectorBuilder newRowIndex.AddressingScheme otherRowCmd)
    // Combine column vectors a single vector & return results
    let newData = vectorBuilder.Build(newColumnIndex.AddressingScheme, colCmd, [| newThisData; newOtherData |])
    Frame(newRowIndex, newColumnIndex, newData, indexBuilder, vectorBuilder)

  /// Join two data frames. The columns of the joined frames must not overlap and their
  /// rows are aligned and transformed according to the specified join kind.
  /// For more alignment options on ordered frames, see overload taking `lookup`.
  ///
  /// ## Parameters
  ///  - `otherFrame` - Other frame (right) to be joined with the current instance (left)
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>, kind) =    
    frame.Join(otherFrame, kind, Lookup.Exact)

  /// Performs outer join on two data frames. The columns of the joined frames must not 
  /// overlap and their rows are aligned. The unavailable values are marked as missing.
  ///
  /// ## Parameters
  ///  - `otherFrame` - Other frame (right) to be joined with the current instance (left)
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>) =    
    frame.Join(otherFrame, JoinKind.Outer, Lookup.Exact)

  /// Join data frame and a series. The column key for the joined series must not occur in the 
  /// current data frame. The rows are aligned and transformed according to the specified join kind.
  /// When the index of both objects is ordered, it is possible to specify `lookup` 
  /// in order to align indices from other frame to the indices of the main frame
  /// (typically, to find the nearest key with available value for a key).
  ///
  /// ## Parameters
  ///  - `colKey` - Column key to be used for the joined series
  ///  - `series` - Series to be joined with the current data frame
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///  - `lookup` - When `kind` is `Left` or `Right` and the two frames have ordered row index,
  ///    this parameter can be used to specify how to find value for a key when there is no
  ///    exactly matching key or when there are missing values.
  ///    Supported values are `Lookup.Exact`, `Lookup.ExactOrSmaller` and `Lookup.ExactOrGreater`.
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join<'V>(colKey, series:Series<'TRowKey, 'V>, kind, lookup) =    
    let otherFrame = Frame([colKey], [series])
    frame.Join(otherFrame, kind, lookup)

  /// Join data frame and a series. The column key for the joined series must not occur in the 
  /// current data frame. The rows are aligned and transformed according to the specified join kind.
  ///
  /// ## Parameters
  ///  - `colKey` - Column key to be used for the joined series
  ///  - `series` - Series to be joined with the current data frame
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join<'V>(colKey, series:Series<'TRowKey, 'V>, kind) =    
    frame.Join(colKey, series, kind, Lookup.Exact)

  /// Performs outer join on data frame and a series. The column key for the joined
  /// series must not occur in the current data frame. The rows are automatically aligned
  /// and unavailable values are marked as missing.
  ///
  /// ## Parameters
  ///  - `colKey` - Column key to be used for the joined series
  ///  - `series` - Series to be joined with the current data frame
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join<'V>(colKey, series:Series<'TRowKey, 'V>) =    
    frame.Join(colKey, series, JoinKind.Outer, Lookup.Exact)


  /// Merge two data frames with non-overlapping values. The operation takes the union of columns
  /// and rows of the source data frames and then unions the values. An exception is thrown when 
  /// both data frames define value for a column/row location, but the operation succeeds if one
  /// frame has a missing value at the location.
  ///
  /// Note that the rows are *not* automatically reindexed to avoid overlaps. This means that when
  /// a frame has rows indexed with ordinal numbers, you may need to explicitly reindex the row
  /// keys before calling append.
  ///
  /// ## Parameters
  ///  - `otherFrame` - The other frame to be appended (combined) with the current instance
  ///
  /// [category:Joining, zipping and appending]
  member frame.Merge(otherFrame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Merge([| otherFrame |])

  /// Merge multiple data frames with non-overlapping values. The operation takes the union of columns
  /// and rows of the source data frames and then unions the values. An exception is thrown when 
  /// both data frames define value for a column/row location, but the operation succeeds if one
  /// frame has a missing value at the location.
  ///
  /// Note that the rows are *not* automatically reindexed to avoid overlaps. This means that when
  /// a frame has rows indexed with ordinal numbers, you may need to explicitly reindex the row
  /// keys before calling append.
  ///
  /// ## Parameters
  ///  - `otherFrames` - A collection containing other data frame to be appended 
  ///    (combined) with the current instance
  ///
  /// [category:Joining, zipping and appending]
  member frame.Merge(otherFrames:seq<Frame<'TRowKey, 'TColumnKey>>) = 
    frame.Merge(Array.ofSeq otherFrames)

  /// Merge multiple data frames with non-overlapping values. The operation takes the union of columns
  /// and rows of the source data frames and then unions the values. An exception is thrown when 
  /// both data frames define value for a column/row location, but the operation succeeds if one
  /// frame has a missing value at the location.
  ///
  /// Note that the rows are *not* automatically reindexed to avoid overlaps. This means that when
  /// a frame has rows indexed with ordinal numbers, you may need to explicitly reindex the row
  /// keys before calling append.
  ///
  /// ## Parameters
  ///  - `otherFrames` - A collection containing other data frame to be appended 
  ///    (combined) with the current instance
  ///
  /// [category:Joining, zipping and appending]
  member frame.Merge([<ParamArray>] otherFrames:Frame<'TRowKey, 'TColumnKey>[]) = 
    
    // Get an array with all frames we're merging
    let frames = Array.append [| frame |] otherFrames

    // Merge the row indices and get a transformation that combines N vectors
    // (AtMostOne - specifies that when the vectors are merged, there should be no overlap)
    let constrs = frames |> Seq.mapi (fun i f -> f.RowIndex, Vectors.Return(i)) |> List.ofSeq
    let newRowIndex, rowCmd = indexBuilder.Merge(constrs, NaryTransform.AtMostOne)

    // Define a function to construct result vector via the above row command, which will dynamically
    // dispatch to a type-specific generic implementation
    //
    // This uses the specified 'witnessVec' to invoke 'VectorCallSite' with the runtime
    // type of the witness vector as a static type argument, then it converts all vectors
    // to this specified type and appends them.
    let convertAndAppendVectors (witnessVec:IVector) (vectors:IVector list) =
      { new VectorCallSite<IVector> with
          override x.Invoke<'T>(_:IVector<'T>) =
            let typed = vectors |> Seq.map (VectorHelpers.convertType<'T> ConversionKind.Flexible) |> Array.ofSeq
            vectorBuilder.Build(newRowIndex.AddressingScheme, rowCmd, typed) :> IVector }
      |> witnessVec.Invoke

    // define the transformation itself, piecing components together
    let append = NaryTransform.Create(fun (lst: OptionalValue<IVector> list) ->
      let witnessVec = 
        match lst |> Seq.tryFind (fun v -> v.HasValue) with
        | Some(v) -> v.Value
        | _       -> invalidOp "Logic error: could not find non-empty IVector ??"
      let empty = Vector.ofValues [] :> IVector
      let input = lst |> List.map (OptionalValue.defaultArg empty)
      input |> convertAndAppendVectors witnessVec |> OptionalValue.Create )

    // build the union of all column keys
    let colConstrs = frames |> Seq.mapi (fun i f -> f.ColumnIndex, Vectors.Return(i)) |> List.ofSeq
    let newColIndex, frameCmd = indexBuilder.Merge(colConstrs, append)

    let frameData = frames |> Array.map (fun f -> f.Data)
    let newData = vectorBuilder.Build(newColIndex.AddressingScheme, frameCmd, frameData)
    Frame(newRowIndex, newColIndex, newData, indexBuilder, vectorBuilder)

  // ----------------------------------------------------------------------------------------------
  // Frame accessors
  // ----------------------------------------------------------------------------------------------

  /// [category:Accessors and slicing]
  member frame.IsEmpty = 
    rowIndex.Mappings |> Seq.isEmpty

  /// [category:Accessors and slicing]
  member frame.RowKeys = rowIndex.Keys :> seq<_>
  /// [category:Accessors and slicing]
  member frame.ColumnKeys = columnIndex.Keys :> seq<_>

  /// [category:Accessors and slicing]
  member frame.ColumnTypes = 
    seq { for kvp in columnIndex.Mappings -> data.GetValue(kvp.Value).Value.ElementType }

  /// [category:Accessors and slicing]
  member frame.Columns = 
    let newData = data.Select(fun vect -> ObjectSeries<_>(rowIndex, boxVector vect, vectorBuilder, indexBuilder))
    ColumnSeries(Series(columnIndex, newData, vectorBuilder, indexBuilder))

  /// [category:Accessors and slicing]
  member frame.ColumnsDense = 
    let newData = data.Select(fun _ vect -> 
      // Assuming that the data has all values - which should be an invariant...
      let all = rowIndex.Mappings |> Seq.forall (fun (KeyValue(key, addr)) -> vect.Value.GetObject(addr).HasValue)
      if all then OptionalValue(ObjectSeries(rowIndex, boxVector vect.Value, vectorBuilder, indexBuilder))
      else OptionalValue.Missing )
    ColumnSeries(Series(columnIndex, newData, vectorBuilder, indexBuilder))

  /// [category:Accessors and slicing]
  member frame.RowsDense : RowSeries<'TRowKey, 'TColumnKey> = 
    // Create an in-memory series that contains `ObjectSeries` for each
    // row that has a value for each column. This returns an in-memory
    // series because we do not know how many rows we'll need to return.
    let emptySeries = Series<_, _>(rowIndex, Vector.ofValues [], vectorBuilder, indexBuilder)
    let res = emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Locate(row.Key)
      let rowVec = createObjRowReader data vectorBuilder rowAddress columnIndex.AddressAt
      let all = columnIndex.Mappings |> Seq.forall (fun (KeyValue(key, addr)) -> rowVec.GetValue(addr).HasValue)
      if all then OptionalValue(ObjectSeries(columnIndex, rowVec, vectorBuilder, indexBuilder))
      else OptionalValue.Missing )

    // Create a row series from the filtered series
    RowSeries<_, _>.FromSeries(Series.dropMissing res) 

  /// [category:Accessors and slicing]
  member frame.Rows : RowSeries<'TRowKey, 'TColumnKey> =    
    // This operation needs to work on virtualized frames (by returning a
    // virtualized series) and it also needs to be efficient for in-memory frames.
    // We do this by creating `Combine([...], NaryTransform.RowReader)` command - 
    // the `RowReader` case is then detected by the ArrayVectorBuilder and 
    // rather than actually creating the vectors, it returns a lazy vector of
    // `IVector<obj>` values created using `createRowReader`.
    let vector =
      data |> createRowVector vectorBuilder rowIndex.AddressingScheme (lazy rowIndex.KeyCount) 
        columnIndex.KeyCount columnIndex.AddressAt (fun rowReader ->
          ObjectSeries(columnIndex, rowReader, vectorBuilder, indexBuilder) )

    // The following delegates slicing to the frame by calling 
    // `frame.GetSubrange` which is more efficient than re-creating from rows
    //
    // NOTE: Why do we use `RowSeries<_, _>.FromSeries` above and inline object
    // expression here? Due to some odd type inference in recursive type definitions,
    // this is the only way to make it work... :-/
    { new RowSeries<'TRowKey, 'TColumnKey>(rowIndex, vector, frame.VectorBuilder, frame.IndexBuilder) with
        override x.GetSlice(lo, hi) = 
          let inclusive v = v |> Option.map (fun v -> v, BoundaryBehavior.Inclusive)
          frame.GetSubrange(inclusive lo, inclusive hi) }

  /// [category:Accessors and slicing]
  member frame.GetRowsAs<'TRow>() : Series<'TRowKey, 'TRow> =    
    if typeof<'TColumnKey> <> typeof<string> then 
      failwith "The GetRows operation can only be used when column key is a string."

    let keys = columnIndex.Keys |> Seq.map unbox<string> |> List.ofSeq 
    let rowBuilder = VectorHelpers.createTypedRowReader<'TRow> keys (fun column ->
      let address = columnIndex.Locate(unbox<'TColumnKey> column)
      if address = Address.invalid then
        failwithf "The interface member '%s' does not exist in the column index." column
      address )

    let vector = rowBuilder rowIndex.KeyCount rowIndex.AddressAt data
    Series<'TRowKey, 'TRow>(rowIndex, vector, vectorBuilder, indexBuilder)

  /// [category:Accessors and slicing]
  member frame.Item 
    with get(column:'TColumnKey, row:'TRowKey) = frame.Columns.[column].[row]
 
  // ----------------------------------------------------------------------------------------------
  // Accessing frame rows
  // ----------------------------------------------------------------------------------------------

  /// Returns the row key that is located at the specified int offset.
  /// If the index is invalid, `ArgumentOutOfRangeException` is thrown. 
  /// You can get the corresponding row using `GetRowAt`.
  ///
  /// ## Parameters
  ///  - `index` - Offset (integer) of the row key to be returned
  ///
  /// [category:Accessors and slicing]
  member frame.GetRowKeyAt(index) = 
    frame.RowIndex.KeyAt(frame.RowIndex.AddressAt index)

  /// Returns a row of the data frame that is located at the specified int offset.
  /// This does not use the row key and directly accesses the frame data. This method
  /// is generic and returns the result as a series containing values of the specified type. 
  /// To get heterogeneous series of type `ObjectSeries<'TCol>`, use the `frame.Rows` property.
  /// If the index is invalid, `ArgumentOutOfRangeException` is thrown. You can get the 
  /// matching key at a specified index using `GetRowKeyAt`.
  ///
  /// ## Parameters
  ///  - `index` - Offset (integer) of the row to be returned
  ///
  /// [category:Accessors and slicing]
  member frame.GetRowAt<'T>(index) : Series<_, 'T> = 
    if index < 0 || int64 index >= rowIndex.KeyCount then
      raise (new ArgumentOutOfRangeException("index", "Index must be positive and smaller than the number of rows."))
    let rowAddress = rowIndex.AddressAt <| int64 index
    let vector = createRowReader data vectorBuilder rowAddress columnIndex.AddressAt
    Series(columnIndex, vector, vectorBuilder, indexBuilder)

  /// Returns a row with the specieifed key wrapped in `OptionalValue`. When the specified key 
  /// is not found, the result is `OptionalValue.Missing`. This method is generic and returns the result 
  /// as a series containing values of the specified type. To get heterogeneous series of 
  /// type `ObjectSeries<'TCol>`, use the `frame.Rows` property.
  ///
  /// ## Parameters
  ///  - `rowKey` - Specifies the key of the row to be returned
  ///
  /// [category:Accessors and slicing]
  member frame.TryGetRow<'T>(rowKey) : OptionalValue<Series<_, 'T>> =
    let rowAddress = rowIndex.Locate(rowKey)
    if rowAddress = Address.invalid then OptionalValue.Missing
    else 
      let vector = createRowReader data vectorBuilder rowAddress columnIndex.AddressAt
      OptionalValue(Series(columnIndex, vector, vectorBuilder, indexBuilder))

  /// Returns a row with the specieifed key wrapped in `OptionalValue`. When the specified key 
  /// is not found, the result is `OptionalValue.Missing`. This method is generic and returns the result 
  /// as a series containing values of the specified type. To get heterogeneous series of 
  /// type `ObjectSeries<'TCol>`, use the `frame.Rows` property.
  ///
  /// ## Parameters
  ///  - `rowKey` - Specifies the key of the row to be returned
  ///  - `lookup` - Specifies how to find value in a frame with ordered rows when the key does 
  ///               not exactly match (look for nearest available value with the smaller/greater key).
  ///
  /// [category:Accessors and slicing]
  member frame.TryGetRow<'T>(rowKey, lookup) : OptionalValue<Series<_, 'T>> =
    let rowAddress = rowIndex.Lookup(rowKey, lookup, fun _ -> true)
    if not rowAddress.HasValue then OptionalValue.Missing
    else 
      let vector = createRowReader data vectorBuilder (snd rowAddress.Value) columnIndex.AddressAt
      OptionalValue(Series(columnIndex, vector, vectorBuilder, indexBuilder))

  /// Returns a row with the specieifed key. This method is generic and returns the result 
  /// as a series containing values of the specified type. To get heterogeneous series of 
  /// type `ObjectSeries<'TCol>`, use the `frame.Rows` property.
  ///
  /// ## Parameters
  ///  - `rowKey` - Specifies the key of the row to be returned
  ///
  /// [category:Accessors and slicing]
  member frame.GetRow<'T>(rowKey) : Series<_, 'T> = frame.TryGetRow(rowKey).Value

  /// Returns a row with the specieifed key. This method is generic and returns the result 
  /// as a series containing values of the specified type. To get heterogeneous series of 
  /// type `ObjectSeries<'TCol>`, use the `frame.Rows` property.
  ///
  /// ## Parameters
  ///  - `rowKey` - Specifies the key of the row to be returned
  ///  - `lookup` - Specifies how to find value in a frame with ordered rows when the key does 
  ///               not exactly match (look for nearest available value with the smaller/greater key).
  ///
  /// [category:Accessors and slicing]
  member frame.GetRow<'T>(rowKey, lookup) : Series<_, 'T> =  frame.TryGetRow(rowKey, lookup).Value

  /// Try to find a row with the specified row key, or using the specified `lookup` parameter,
  /// and return the found row together with its actual key in case `lookup` was used. In case the
  /// row is not found, `OptionalValue.Missing` is returned.
  ///
  /// ## Parameters
  ///  - `rowKey` - Specifies the key of the row to be returned
  ///  - `lookup` - Specifies how to find value in a frame with ordered rows when the key does 
  ///               not exactly match (look for nearest available value with the smaller/greater key).
  ///
  /// [category:Accessors and slicing]
  member frame.TryGetRowObservation<'T>(rowKey, lookup) : OptionalValue<KeyValuePair<_, Series<_, 'T>>> =
    let rowAddress = rowIndex.Lookup(rowKey, lookup, fun _ -> true)
    if not rowAddress.HasValue then OptionalValue.Missing
    else 
      let vector = createRowReader data vectorBuilder (snd rowAddress.Value) columnIndex.AddressAt
      let row = Series(columnIndex, vector, vectorBuilder, indexBuilder)
      OptionalValue(KeyValuePair(fst rowAddress.Value, row))
  
  // ----------------------------------------------------------------------------------------------
  // Fancy accessors for frame columns and frame data
  // ----------------------------------------------------------------------------------------------

  /// [category:Fancy accessors]
  member frame.GetColumns<'R>() = 
    frame.Columns.SelectOptional(fun (KeyValue(k, vopt)) ->
      vopt |> OptionalValue.bind (fun ser -> ser.TryAs<'R>(ConversionKind.Safe)))

  /// [category:Fancy accessors]
  member frame.GetRows<'R>() = 
    frame.Rows.SelectOptional(fun (KeyValue(k, vopt)) ->
      vopt |> OptionalValue.bind (fun ser -> ser.TryAs<'R>(ConversionKind.Safe)))

  /// [category:Fancy accessors]
  member frame.GetAllValues<'R>() = frame.GetAllValues<'R>(ConversionKind.Safe)

  /// [category:Fancy accessors]
  member frame.GetAllValues<'R>(strict) =
    seq { for (KeyValue(_, v)) in frame.GetAllColumns<'R>() do yield! v |> Series.values }

  /// Returns data of the data frame as a 2D array. The method attempts to convert
  /// all values to the specified type 'R. When a value is missing, the specified 
  /// `defaultValue` is used.
  ///
  /// ## Parameters
  ///  - `defaultValue` - Default value used to fill all missing values
  ///
  /// [category:Fancy accessors]
  member frame.ToArray2D<'R>(defaultValue) =
    toArray2D<'R> frame.RowCount frame.ColumnCount frame.Data (lazy defaultValue)

  /// Returns data of the data frame as a 2D array. The method attempts to convert
  /// all values to the specified type 'R. If the specified type is 'float' or 'double'
  /// then the method automatically uses NaN. For other values, the default value has to
  /// be explicitly specified using another overload.
  ///
  /// [category:Fancy accessors]
  member frame.ToArray2D<'R>() =
    let defaultValue = 
      if typeof<'R> = typeof<Double> then lazy unbox<'R> Double.NaN
      elif typeof<'R> = typeof<Single> then lazy unbox<'R> Single.NaN
      else lazy (invalidOp "ToArray2D: Frame contains missing data, but default value was not provided.")
    toArray2D frame.RowCount frame.ColumnCount frame.Data defaultValue
    
  // ----------------------------------------------------------------------------------------------
  // Series related operations - add, drop, get, ?, ?<-, etc.
  // ----------------------------------------------------------------------------------------------

  /// Mutates the data frame by adding an additional data series
  /// as a new column with the specified column key. The sequence is
  /// aligned to the data frame based on ordering. If it is longer, it is
  /// trimmed and if it is shorter, missing values will be added.
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the newly added column
  ///  - `series` - A sequence of values to be added
  ///
  /// [category:Series operations]
  member frame.AddColumn(column:'TColumnKey, series:seq<_>) = 
    frame.AddColumn(column, series, Lookup.Exact)

  /// Mutates the data frame by adding an additional data series
  /// as a new column with the specified column key. The operation 
  /// uses left join and aligns new series to the existing frame keys.
  ///
  /// ## Parameters
  ///  - `series` - A data series to be added (the row key type has to match)
  ///  - `column` - A key (or name) for the newly added column
  ///
  /// [category:Series operations]
  member frame.AddColumn(column:'TColumnKey, series:ISeries<_>) = 
    frame.AddColumn(column, series, Lookup.Exact)

  /// Mutates the data frame by adding an additional data series
  /// as a new column with the specified column key. The sequence is
  /// aligned to the data frame based on ordering. If it is longer, it is
  /// trimmed and if it is shorter, missing values will be added.
  /// A parameter `lookup` can be used to specify how to find a value in the
  /// added series (if the sequence contains invalid values like `null` or `NaN`). 
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the newly added column
  ///  - `series` - A sequence of values to be added
  ///  - `lookup` - Specify how to find value in the added series (look for 
  ///    nearest available value with the smaller/greater key).
  ///
  /// [category:Series operations]
  member frame.AddColumn(column:'TColumnKey, series:seq<'V>, lookup) = 
    if isEmpty then
      if typeof<'TRowKey> = typeof<int> then
        let series = unbox<Series<'TRowKey, 'V>> (Series.ofValues series)
        frame.AddColumn(column, series, lookup)
      else
        invalidOp "Adding data sequence to an empty frame with non-integer columns is not supported."
    else
      let count = Seq.length series
      let rowCount = Seq.length frame.RowIndex.Keys
      // Pad with missing values, if there is not enough, or trim if there is more
      let vector = 
        if count >= rowCount then 
          Vector.ofValues (Seq.take count series)
        else
          let nulls = seq { for i in Seq.range 1 (rowCount - count) -> None }
          Vector.ofOptionalValues (Seq.append (Seq.map Some series) nulls)

      let series = Series(frame.RowIndex, vector, vectorBuilder, indexBuilder)
      frame.AddColumn(column, series, lookup)

  /// Mutates the data frame by adding an additional data series
  /// as a new column with the specified column key. The operation 
  /// uses left join and aligns new series to the existing frame keys.
  /// A parameter `lookup` can be used to specify how to find a value in the
  /// added series (if an exact key is not available). The `lookup` parameter
  /// can only be used with ordered indices.
  ///
  /// ## Parameters
  ///  - `series` - A data series to be added (the row key type has to match)
  ///  - `column` - A key (or name) for the newly added column
  ///  - `lookup` - Specify how to find value in the added series (look for 
  ///    nearest available value with the smaller/greater key).
  ///
  /// [category:Series operations]
  member frame.AddColumn<'V>(column:'TColumnKey, series:ISeries<'TRowKey>, lookup) = 
    if isEmpty then
      // If the frame was empty, then initialize both indices
      rowIndex <- series.Index
      isEmpty <- false
      data <- Vector.ofValues [series.Vector]
      frame.setColumnIndex (Index.ofKeys [| column |])
    else
      let other = 
        Frame( series.Index, Index.ofUnorderedKeys [| column |], 
               Vector.ofValues [series.Vector], series.Index.Builder, series.VectorBuilder )
      let joined = frame.Join(other, JoinKind.Left, lookup)
      data <- joined.Data
      frame.setColumnIndex joined.ColumnIndex

  /// Mutates the data frame by removing the specified series from the 
  /// frame columns. The operation throws if the column key is not found.
  ///
  /// ## Parameters
  ///  - `column` - The key (or name) to be dropped from the frame
  ///  - `frame` - Source data frame (which is not mutated by the operation)
  ///
  /// [category:Series operations]
  member frame.DropColumn(column:'TColumnKey) = 
    let newColumnIndex, colCmd = indexBuilder.DropItem( (columnIndex, Vectors.Return 0), column)
    data <- vectorBuilder.Build(newColumnIndex.AddressingScheme, colCmd, [| data |])
    frame.setColumnIndex newColumnIndex

  /// Mutates the data frame by replacing the specified series with
  /// a new series. (If the series does not exist, only the new series is added.) 
  /// When adding a series, the specified `lookup` parameter is used for matching 
  /// keys. The parameter can only be used for frame with ordered indices.
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A data series to be used (the row key type has to match)
  ///  - `lookup` - Specify how to find value in the added series (look for 
  ///    nearest available value with the smaller/greater key).
  ///
  /// [category:Series operations]
  member frame.ReplaceColumn(column:'TColumnKey, series:ISeries<_>, lookup) = 
    if columnIndex.Lookup(column, Lookup.Exact, fun _ -> true).HasValue then
      frame.DropColumn(column)
    frame.AddColumn(column, series, lookup)

  /// Mutates the data frame by replacing the specified series with
  /// a new data sequence. (If the series does not exist, only the new series is added.) 
  /// When adding a series, the specified `lookup` parameter is used for filling
  /// missing values (e.g. `null` or `NaN`). The parameter can only be used for 
  /// frame with ordered indices.
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A data series to be used (the row key type has to match)
  ///  - `lookup` - Specify how to find value in the added series (look for 
  ///    nearest available value with the smaller/greater key).
  ///
  /// [category:Series operations]
  member frame.ReplaceColumn(column, data:seq<'V>, lookup) = 
    let newSeries = Series(frame.RowIndex, Vector.ofValues data, vectorBuilder, indexBuilder)
    frame.ReplaceColumn(column, newSeries, lookup)

  /// Mutates the data frame by replacing the specified series with
  /// a new series. (If the series does not exist, only the new
  /// series is added.)
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A data series to be used (the row key type has to match)
  ///
  /// [category:Series operations]
  member frame.ReplaceColumn(column:'TColumnKey, series:ISeries<_>) = 
    frame.ReplaceColumn(column, series, Lookup.Exact)

  /// Mutates the data frame by replacing the specified series with
  /// a new data sequence . (If the series does not exist, only the new
  /// series is added.)
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A sequence of values to be added
  ///
  /// [category:Series operations]
  member frame.ReplaceColumn(column, data:seq<'V>) = 
    frame.ReplaceColumn(column, data, Lookup.Exact)

  /// [category:Series operations]
  member frame.Item 
    with get(column:'TColumnKey) = frame.GetColumn<float>(column)
    and set(column:'TColumnKey) (series:Series<'TRowKey, float>) = frame.ReplaceColumn(column, series)

  /// [category:Projection and filtering]
  member frame.ColumnApply<'T>(f:Func<Series<'TRowKey, 'T>, ISeries<_>>) = frame.ColumnApply<'T>(ConversionKind.Safe, f)

  /// [category:Projection and filtering]
  member frame.ColumnApply<'T>(conversionKind:ConversionKind, f:Func<Series<'TRowKey, 'T>, ISeries<_>>) = 
    frame.Columns |> Series.mapValues (fun os ->
      match os.TryAs<'T>(conversionKind) with
      | OptionalValue.Present s -> f.Invoke s
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric indexBuilder vectorBuilder id

  /// [category:Projection and filtering]
  member frame.Select<'T1, 'T2>(f:System.Func<'TRowKey, 'TColumnKey, 'T1, 'T2>) = 
    frame.Columns |> Series.map (fun c os ->
      match os.TryAs<'T1>(ConversionKind.Safe) with
      | OptionalValue.Present s -> Series.map (fun r v -> f.Invoke(r, c, v)) s :> ISeries<_>
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric indexBuilder vectorBuilder id 

  /// [category:Projection and filtering]
  member frame.SelectValues<'T1, 'T2>(f:System.Func<'T1, 'T2>) = 
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<_,'T1>) -> s.SelectValues(f) :> ISeries<_>)

  /// Custom operator that can be used for applying fuction to all elements of 
  /// a frame. This provides a nicer syntactic sugar for the `Frame.mapValues` 
  /// function.
  static member ($) (f, frame: Frame<'TRowKey,'TColumnKey>) = 
    frame.SelectValues(Func<_,_>(f))

  /// [category:Series operations]
  member frame.GetColumn<'R>(column:'TColumnKey, lookup) : Series<'TRowKey, 'R> = 
    match unboxVector (safeGetColVector(column, lookup, fun _ -> true)) with
    | :? IVector<'R> as vec -> 
        Series(rowIndex, vec, vectorBuilder, indexBuilder)
    | colVector ->
        Series(rowIndex, convertType ConversionKind.Flexible colVector, vectorBuilder, indexBuilder)

  /// [category:Series operations]
  member frame.GetColumnAt<'R>(index:int) : Series<'TRowKey, 'R> = 
    frame.Columns.GetAt(index).As<'R>()

  /// [category:Series operations]
  member frame.GetColumn<'R>(column:'TColumnKey) : Series<'TRowKey, 'R> = 
    frame.GetColumn(column, Lookup.Exact)

  /// [category:Series operations]
  member frame.GetAllColumns<'R>() = frame.GetAllColumns<'R>(ConversionKind.Flexible)

  /// [category:Series operations]
  member frame.TryGetColumn<'R>(column:'TColumnKey, lookup) =
    tryGetColVector(column, lookup, fun _ -> true) 
    |> OptionalValue.map (fun v -> Series(rowIndex, convertType<'R> ConversionKind.Flexible v, vectorBuilder, indexBuilder))

  /// [category:Series operations]
  member frame.TryGetColumnObservation<'R>(column:'TColumnKey, lookup) =
    let columnIndex = columnIndex.Lookup(column, lookup, fun _ -> true)
    if not columnIndex.HasValue then 
      OptionalValue.Missing 
    else
      data.GetValue (snd columnIndex.Value) 
      |> OptionalValue.map (fun vec ->
        let ser = Series(rowIndex, convertType<'R> ConversionKind.Flexible vec, vectorBuilder, indexBuilder)
        KeyValuePair(fst columnIndex.Value, ser) )

  /// [category:Series operations]
  member frame.GetAllColumns<'R>(conversionKind:ConversionKind) =
    frame.Columns.Observations |> Seq.choose (fun os -> 
      match os.Value.TryAs<'R>(conversionKind) with
      | OptionalValue.Present s -> Some (KeyValuePair(os.Key, s))
      | _ -> None)

  /// [category:Series operations]
  member frame.RenameColumns(columnKeys) =
    if Seq.length columnIndex.Keys <> Seq.length columnKeys then 
      invalidArg "columnKeys" "The number of new column keys does not match with the number of columns"
    frame.setColumnIndex (Index.ofKeys (ReadOnlyCollection.ofSeq columnKeys))

  /// [category:Series operations]
  member frame.RenameColumn(oldKey, newKey) =
    let newKeys = columnIndex.Keys |> Seq.map (fun k -> if k = oldKey then newKey else k)
    frame.setColumnIndex (Index.ofKeys (ReadOnlyCollection.ofSeq newKeys))

  /// [category:Series operations]
  member frame.RenameColumns(mapping:Func<_, _>) =
    frame.setColumnIndex (Index.ofKeys (ReadOnlyCollection.map mapping.Invoke columnIndex.Keys))

  /// [category:Series operations]
  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceColumn(column, series)

  /// [category:Series operations]
  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceColumn(column, data)

  /// [category:Series operations]
  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetColumn<float>(column)

  // ----------------------------------------------------------------------------------------------
  // Some operators
  // ----------------------------------------------------------------------------------------------

  // Apply operation 'op' with 'series' on the right to all columns convertible to 'T
  static member inline private PointwiseFrameSeriesR<'T>(frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, 'T>, op:'T -> 'T -> 'T) : Frame<'TRowKey, 'TColumnKey> =
    // Join the row index of the frame & the index of the series
    // (so that we can apply the transformation repeatedly on columns)
    let newIndex, frameCmd, seriesCmd = 
      createJoinTransformation frame.IndexBuilder series.IndexBuilder JoinKind.Outer Lookup.Exact frame.RowIndex 
        series.Index (Vectors.Return 0) (Vectors.Return 1)
    let opCmd = Vectors.Combine(lazy newIndex.KeyCount, [frameCmd; seriesCmd], BinaryTransform.CreateLifted(op))

    // Apply the transformation on all columns that can be converted to 'T
    let newData = frame.Data.Select(fun vector ->
      match VectorHelpers.tryConvertType ConversionKind.Safe vector with
      | OptionalValue.Present(tyvec) ->
          frame.VectorBuilder.Build(newIndex.AddressingScheme, opCmd, [| tyvec; series.Vector |]) :> IVector
      | _ -> 
          { new VectorCallSite<_> with
              member x.Invoke(tyvec) =
                frame.VectorBuilder.Build(newIndex.AddressingScheme, frameCmd, [| tyvec |]) :> IVector }
          |> vector.Invoke ) 
    Frame(newIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  // Apply operation 'op' to all columns that exist in both frames and are convertible to 'T
  static member inline internal PointwiseFrameFrame<'T>(frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>, op:'T -> 'T -> 'T) =
    frame1.Zip<'T, 'T, 'T>(frame2, JoinKind.Outer, JoinKind.Outer, Lookup.Exact, fun a b -> op a b)

  // Apply operation 'op' with 'scalar' on the right to all columns convertible to 'T
  static member inline private ScalarOperationR<'T>(frame:Frame<'TRowKey, 'TColumnKey>, scalar:'T, op:'T -> 'T -> 'T) : Frame<'TRowKey, 'TColumnKey> =
    frame.Columns |> Series.mapValues (fun os -> 
      match os.TryAs<'T>(ConversionKind.Safe) with
      | OptionalValue.Present s -> (Series.mapValues (fun v -> op v scalar) s) :> ISeries<_>
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric frame.IndexBuilder frame.VectorBuilder id

  // Apply operation 'op' with 'series' on the left to all columns convertible to 'T
  static member inline private PointwiseFrameSeriesL<'T>(frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, 'T>, op:'T -> 'T -> 'T) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<'T>(frame, series, fun a b -> op b a)
  // Apply operation 'op' with 'scalar' on the left to all columns convertible to 'T
  static member inline private ScalarOperationL<'T>(frame:Frame<'TRowKey, 'TColumnKey>, scalar:'T, op:'T -> 'T -> 'T) : Frame<'TRowKey, 'TColumnKey> =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<'T>(frame, scalar, fun a b -> op b a)
  // Apply operation 'op' to all values in all columns convertible to 'T
  static member inline internal UnaryOperation<'T>(frame:Frame<'TRowKey, 'TColumnKey>, op : 'T -> 'T) = 
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<'TRowKey, 'T>) -> (Series.mapValues op s) :> ISeries<_>)
  // Apply operation 'op' to all values in all columns convertible to 'T1 (the operation returns different type!)
  static member inline internal UnaryGenericOperation<'T1, 'T2>(frame:Frame<'TRowKey, 'TColumnKey>, op : 'T1 -> 'T2) =
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<'TRowKey, 'T1>) -> (Series.mapValues op s) :> ISeries<_>)

  // Unary numerical operators (just minus)

  /// [category:Operators]
  static member (~-) (frame) = Frame<'TRowKey, 'TColumnKey>.UnaryGenericOperation<float, _>(frame, (~-))

  // Pointwise binary operations applied to two frames

  /// [category:Operators]
  static member (+) (frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameFrame<float>(frame1, frame2, (+))
  /// [category:Operators]
  static member (-) (frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameFrame<float>(frame1, frame2, (-))
  /// [category:Operators]
  static member (*) (frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameFrame<float>(frame1, frame2, (*))
  /// [category:Operators]
  static member (/) (frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameFrame<float>(frame1, frame2, (/))

  // Binary operators taking float/int series on the left/right (int is converted to float)

  /// [category:Operators]
  static member (+) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, float>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, series, (+))
  /// [category:Operators]
  static member (-) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, float>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, series, (-))
  /// [category:Operators]
  static member (*) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, float>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, series, (*))
  /// [category:Operators]
  static member (/) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, float>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, series, (/))
  /// [category:Operators]
  static member (+) (series:Series<'TRowKey, float>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, series, (+))
  /// [category:Operators]
  static member (-) (series:Series<'TRowKey, float>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, series, (-))
  /// [category:Operators]
  static member (*) (series:Series<'TRowKey, float>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, series, (*))
  /// [category:Operators]
  static member (/) (series:Series<'TRowKey, float>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, series, (/))

  /// [category:Operators]
  static member (+) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, int>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, Series.mapValues float series, (+))
  /// [category:Operators]
  static member (-) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, int>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, Series.mapValues float series, (-))
  /// [category:Operators]
  static member (*) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, int>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, Series.mapValues float series, (*))
  /// [category:Operators]
  static member (/) (frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, int>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<float>(frame, Series.mapValues float series, (/))
  /// [category:Operators]
  static member (+) (series:Series<'TRowKey, int>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, Series.mapValues float series, (+))
  /// [category:Operators]
  static member (-) (series:Series<'TRowKey, int>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, Series.mapValues float series, (-))
  /// [category:Operators]
  static member (*) (series:Series<'TRowKey, int>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, Series.mapValues float series, (*))
  /// [category:Operators]
  static member (/) (series:Series<'TRowKey, int>, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesL<float>(frame, Series.mapValues float series, (/))

    
  // Binary operators taking float/int scalar on the left/right (int is converted to float)

  /// [category:Operators]
  static member (+) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, (+))
  /// [category:Operators]
  static member (+) (scalar:float, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, scalar, (+))
  /// [category:Operators]
  static member (-) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, (-))
  /// [category:Operators]
  static member (-) (scalar:float, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, scalar, (-))
  /// [category:Operators]
  static member (*) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, (*))
  /// [category:Operators]
  static member (*) (scalar:float, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, scalar, (*))
  /// [category:Operators]
  static member (/) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, (/))
  /// [category:Operators]
  static member (/) (scalar:float, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, scalar, (/))
  /// [category:Operators]
  static member Pow (frame:Frame<'TRowKey, 'TColumnKey>, scalar:float) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, scalar, ( ** ))
  /// [category:Operators]
  static member Pow (scalar:float, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, scalar, ( ** ))

  /// [category:Operators]
  static member (+) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:int) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, float scalar, (+))
  /// [category:Operators]
  static member (+) (scalar:int, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, float scalar, (+))
  /// [category:Operators]
  static member (-) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:int) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, float scalar, (-))
  /// [category:Operators]
  static member (-) (scalar:int, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, float scalar, (-))
  /// [category:Operators]
  static member (*) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:int) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, float scalar, (*))
  /// [category:Operators]
  static member (*) (scalar:int, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, float scalar, (*))
  /// [category:Operators]
  static member (/) (frame:Frame<'TRowKey, 'TColumnKey>, scalar:int) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<float>(frame, float scalar, (/))
  /// [category:Operators]
  static member (/) (scalar:int, frame:Frame<'TRowKey, 'TColumnKey>) =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationL<float>(frame, float scalar, (/))

  // Trigonometric
  
  /// [category:Operators]
  static member Acos(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, acos)
  /// [category:Operators]
  static member Asin(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, asin)
  /// [category:Operators]
  static member Atan(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, atan)
  /// [category:Operators]
  static member Sin(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, sin)
  /// [category:Operators]
  static member Sinh(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, sinh)
  /// [category:Operators]
  static member Cos(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, cos)
  /// [category:Operators]
  static member Cosh(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, cosh)
  /// [category:Operators]
  static member Tan(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, tan)
  /// [category:Operators]
  static member Tanh(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, tanh)

  // Actually useful

  /// [category:Operators]
  static member Abs(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, abs)
  /// [category:Operators]
  static member Ceiling(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, ceil)
  /// [category:Operators]
  static member Exp(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, exp)
  /// [category:Operators]
  static member Floor(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, floor)
  /// [category:Operators]
  static member Truncate(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, truncate)
  /// [category:Operators]
  static member Log(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, log)
  /// [category:Operators]
  static member Log10(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, log10)
  /// [category:Operators]
  static member Round(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryOperation<float>(frame, round)
  /// [category:Operators]
  static member Sign(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryGenericOperation<float, _>(frame, sign)
  /// [category:Operators]
  static member Sqrt(frame) = Frame<'TRowKey, 'TColumnKey>.UnaryGenericOperation<float, _>(frame, sqrt)

  // ----------------------------------------------------------------------------------------------
  // Constructor
  // ----------------------------------------------------------------------------------------------

  new(names:seq<'TColumnKey>, columns:seq<ISeries<'TRowKey>>) =
    let df = Frame(Index.ofKeys [], Index.ofKeys [], Vector.ofValues [], IndexBuilder.Instance, VectorBuilder.Instance)
    let df = (df, Seq.zip names columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.ofUnorderedKeys [colKey], Vector.ofValues [colData.Vector], IndexBuilder.Instance, VectorBuilder.Instance)
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data, IndexBuilder.Instance, VectorBuilder.Instance)

  member frame.Clone() =
    Frame<_, _>(rowIndex, columnIndex, data, indexBuilder, vectorBuilder)

  // ----------------------------------------------------------------------------------------------
  // Interfaces and overrides
  // ----------------------------------------------------------------------------------------------

  override frame.Equals(another) = 
    match another with
    | null -> false
    | :? Frame<'TRowKey, 'TColumnKey> as another -> 
        frame.RowIndex.Equals(another.RowIndex) &&
        frame.ColumnIndex.Equals(another.ColumnIndex) &&
        frame.Data.Equals(another.Data) 
    | _ -> false

  override frame.GetHashCode() =
    let (++) h1 h2 = ((h1 <<< 5) + h1) ^^^ h2
    frame.RowIndex.GetHashCode() ++ frame.ColumnIndex.GetHashCode() ++ frame.Data.GetHashCode()

  // ----------------------------------------------------------------------------------------------
  // Frame data and formatting
  // ----------------------------------------------------------------------------------------------

  /// [category:Formatting and raw data access]
  member frame.GetFrameData() = 
    // Get keys (as object lists with multiple levels)
    let getKeys (index:IIndex<_>) = seq { 
      let maxLevel = 
        match index.Keys |> Seq.headOrNone with 
        | Some colKey -> CustomKey.Get(colKey).Levels | _ -> 1
      for KeyValue(key, _) in index.Mappings ->
        [| for level in 0 .. maxLevel - 1 -> 
             if level = 0 && maxLevel = 0 then box key
             else CustomKey.Get(key).GetLevel(level) |] }
    let rowKeys = getKeys frame.RowIndex 
    let colKeys = getKeys frame.ColumnIndex
    // Get columns as object options
    let columns = data.DataSequence |> Seq.map (fun col ->
      let boxedVec = boxVector col.Value
      col.Value.ElementType, boxedVec)
    // Return as VectorData
    { ColumnKeys = colKeys; RowKeys = rowKeys; Columns = columns }

  /// [category:Accessors and slicing]
  member x.GetSubrange(lo, hi) =
    let newRowIndex, cmd = indexBuilder.GetRange((rowIndex, Vectors.Return 0), (lo, hi))
    let newData = data.Select(VectorHelpers.transformColumn vectorBuilder newRowIndex.AddressingScheme cmd)
    Frame<_, _>(newRowIndex, columnIndex, newData, indexBuilder, vectorBuilder)

  /// Internal helper used by `skip`, `take`, etc.
  member frame.GetAddressRange(range) = 
    let newRowIndex, cmd = indexBuilder.GetAddressRange((frame.RowIndex, Vectors.Return 0), range)
    let newData = frame.Data.Select(VectorHelpers.transformColumn vectorBuilder newRowIndex.AddressingScheme cmd)
    Frame<_, _>(newRowIndex, columnIndex, newData, indexBuilder, vectorBuilder)

  member private frame.GetPrintedRowObservations(startCount:int, endCount:int) = 
    let smaller = frame.RowIndex.Mappings |> Seq.skipAtMost (startCount+endCount) |> Seq.isEmpty
    if smaller then
      seq { for obs in frame.Rows.Observations -> Choice1Of3(obs.Key, obs.Value) } 
    else
      let starts = frame.GetAddressRange(RangeRestriction.Start(int64 startCount))
      let ends = frame.GetAddressRange(RangeRestriction.End(int64 endCount))
      seq { for obs in starts.Rows.Observations do yield Choice1Of3(obs.Key, obs.Value)
            yield Choice2Of3()
            for obs in ends.Rows.Observations do yield Choice1Of3(obs.Key, obs.Value) }

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows. 
  /// [category:Formatting and raw data access]
  member frame.Format() =
    frame.Format(Formatting.StartItemCount, Formatting.EndItemCount)

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows. 
  ///
  /// ## Parameters
  ///  - `startCount` - The number of rows at the beginning to be printed
  ///  - `endCount` - The number of rows at the end of the frame to be printed
  ///  - `printTypes` - When true, the types of vectors storing column data are printed
  /// [category:Formatting and raw data access]
  member frame.Format(printTypes) =
    frame.Format(Formatting.StartItemCount, Formatting.EndItemCount, printTypes)

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows.
  ///
  /// ## Parameters
  ///  - `count` - The maximal total number of rows to be printed
  ///
  /// [category:Formatting and raw data access]
  member frame.Format(count) =
    let half = count / 2
    frame.Format(count, count)

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows.
  ///
  /// ## Parameters
  ///  - `startCount` - The number of rows at the beginning to be printed
  ///  - `endCount` - The number of rows at the end of the frame to be printed
  ///
  /// [category:Formatting and raw data access]
  member frame.Format(startCount, endCount) = 
    frame.Format(startCount, endCount, false)

  /// Shows the data frame content in a human-readable format. The resulting string
  /// shows all columns, but a limited number of rows.
  ///
  /// ## Parameters
  ///  - `startCount` - The number of rows at the beginning to be printed
  ///  - `endCount` - The number of rows at the end of the frame to be printed
  ///  - `printTypes` - When true, the types of vectors storing column data are printed
  ///
  /// [category:Formatting and raw data access]
  member frame.Format(startCount, endCount, printTypes) = 
    try
      // Get the number of levels in column/row index
      let colLevels = 
        if frame.ColumnIndex.IsEmpty then 1 
        else CustomKey.Get(frame.ColumnIndex.KeyAt(frame.ColumnIndex.AddressAt(0L))).Levels
      let rowLevels = 
        if frame.RowIndex.IsEmpty then 1 
        else CustomKey.Get(frame.RowIndex.KeyAt(frame.RowIndex.AddressAt(0L))).Levels

      /// Format type with a few special cases for common types
      let formatType (typ:System.Type) =
        if typ = typeof<obj> then "obj"
        elif typ = typeof<float> then "float"
        elif typ = typeof<int> then "int"
        elif typ = typeof<string> then "string"
        else typ.Name

      /// Get annotation for the specified level of the given key 
      /// (returns empty string if the value is the same as previous)
      let getLevel ordered previous reset maxLevel level (key:'K) = 
        let levelKey = 
          if level = 0 && maxLevel = 0 then box key
          else CustomKey.Get(key).GetLevel(level)
        if ordered && (Some levelKey = !previous) then "" 
        else previous := Some levelKey; reset(); levelKey.ToString()
        
      seq { 
        // Yield headers (for all column levels)
        for colLevel in 0 .. colLevels - 1 do 
          yield [
            // Prefix with appropriate number of (empty) row keys
            for i in 0 .. rowLevels - 1 do yield "" 
            yield ""
            let previous = ref None
            for KeyValue(colKey, _) in frame.ColumnIndex.Mappings do 
              yield getLevel frame.ColumnIndex.IsOrdered previous ignore colLevels colLevel colKey ]

        // If we want to print types, add another line with type information
        if printTypes then 
          yield [
            // Prefix with appropriate number of (empty) row keys
            for i in 0 .. rowLevels - 1 do yield "" 
            yield ""
            let previous = ref None
            for kvp in frame.ColumnIndex.Mappings do 
              let vector = frame.Data.GetValue(kvp.Value) 
              let typ = 
                if not vector.HasValue then "missing"
                else formatType vector.Value.ElementType
              yield String.Concat("(", typ, ")") ]
              
        // Yield row data
        let previous = Array.init rowLevels (fun _ -> ref None)
        let reset i () = for j in i + 1 .. rowLevels - 1 do previous.[j] := None
        for item in frame.GetPrintedRowObservations(startCount, endCount) do
          match item with 
          | Choice2Of3() ->
              yield [
                // Prefix with appropriate number of (empty) row keys
                for i in 0 .. rowLevels - 1 do yield if i = 0 then ":" else ""
                yield ""
                for i in 1 .. data.DataSequence |> Seq.length -> "..." ]
          | Choice1Of3(rowKey, row) | Choice3Of3(rowKey, row) ->
              yield [
                // Yield all row keys
                for rowLevel in 0 .. rowLevels - 1 do 
                  yield getLevel frame.RowIndex.IsOrdered previous.[rowLevel] (reset rowLevel) rowLevels rowLevel rowKey
                yield "->"
                for KeyValue(_, value) in SeriesExtensions.GetAllObservations(row) do  // TODO: is this good?
                  yield value.ToString() ] }
      |> array2D
      |> Formatting.formatTable
    with e -> sprintf "Formatting failed: %A" e

  // ----------------------------------------------------------------------------------------------
  // Interfaces - support the C# dynamic keyword
  // ----------------------------------------------------------------------------------------------

  interface IFrame with
    member x.Apply(op) = op.Invoke(x)

  interface IFsiFormattable with
    member x.Format() = (x :> Frame<_, _>).Format()

  interface INotifyCollectionChanged with
    [<CLIEvent>]
    member x.CollectionChanged = frameColumnsChanged.Publish

  interface System.Dynamic.IDynamicMetaObjectProvider with 
    member frame.GetMetaObject(expr) = 
      DynamicExtensions.createPropertyMetaObject expr frame
        (fun frame name retTyp -> 
          // Cast column key to the right type
          let colKey =
            if typeof<'TColumnKey> = typeof<string> then unbox<'TColumnKey> name
            else invalidOp "Dynamic operations are not supported on frames with non-string column key!"
          // In principle, 'retType' could be something else than 'obj', but C# never does this (?)
          <@@ box ((%%frame : Frame<'TRowKey, 'TColumnKey>).GetColumn<float>(colKey)) @@>)

        (fun frame name argTyp argExpr -> 
          // Cast column key to the right type
          let colKey =
            if typeof<'TColumnKey> = typeof<string> then unbox<'TColumnKey> name
            else invalidOp "Dynamic operations are not supported on frames with non-string column key!"
          // If it is a series, then we can just call ReplaceColumn using quotation
          if typeof<ISeries<'TRowKey>>.IsAssignableFrom argTyp then
            <@@ (%%frame : Frame<'TRowKey, 'TColumnKey>).ReplaceColumn(colKey, (%%(Expr.Coerce(argExpr, typeof<ISeries<'TRowKey>>)) : ISeries<'TRowKey>)) @@>
          else 
            // Try to find seq<'T> implementation
            let elemTy = 
              argTyp.GetInterfaces() 
              |> Seq.tryFind (fun inf -> 
                inf.IsGenericType && inf.GetGenericTypeDefinition() = typedefof<seq<_>>)
              |> Option.map (fun ty -> ty.GetGenericArguments().[0])
            match elemTy with
            | None -> invalidOp (sprintf "Cannot add value of type %s as a series!" argTyp.Name)
            | Some elemTy ->
                // Find the ReplaceColumn method taking seq<'T>
                let AddColumnSeq = 
                  typeof<Frame<'TRowKey, 'TColumnKey>>.GetMethods() |> Seq.find (fun mi -> 
                    mi.Name = "ReplaceColumn" && 
                      ( let pars = mi.GetParameters()
                        pars.Length = 2 && pars.[1].ParameterType.GetGenericTypeDefinition() = typedefof<seq<_>> ))
                // Generate call for the right generic specialization
                let seqTyp = typedefof<seq<_>>.MakeGenericType(elemTy)
                Expr.Call(frame, AddColumnSeq.MakeGenericMethod(elemTy), [Expr.Value name; Expr.Coerce(argExpr, seqTyp)] ) )


  [<Obsolete("GetAllColumns(bool) is obsolete. Use GetAllColumns(ConversionKind) instead.")>]
  member x.GetAllColumns<'R>(strict) = x.GetAllColumns<'R>(if strict then ConversionKind.Exact else ConversionKind.Flexible)
  [<Obsolete("ColumnApply(bool, Func) is obsolete. Use ColumnApply(ConversionKind, Func) instead.")>]
  member x.ColumnApply<'T>(strict:bool, f:Func<_, _>) = x.ColumnApply<'T>((if strict then ConversionKind.Exact else ConversionKind.Flexible), f)

// ------------------------------------------------------------------------------------------------
// Building frame from series of rows/columns (this has to be here, because we need it in 
// ColumnSeries/RowSeries (below) which are referenced by df.Rows, df.Columns (above)
// ------------------------------------------------------------------------------------------------

/// [omit]
/// Module with helper functions and operations that are needed by Frame<R, C>, but
/// are easier to write in a separate type (having them inside generic type 
/// can confuse the type inference in various ways).
and FrameUtils = 
  // Current vector builder to be used for creating frames
  static member vectorBuilder = VectorBuilder.Instance 
  // Current index builder to be used for creating frames
  static member indexBuilder = IndexBuilder.Instance

  /// Create data frame from a series of rows
  static member fromRowsAndColumnKeys<'TRowKey, 'TColumnKey, 'TSeries
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> ISeries<'TColumnKey>> 
      indexBuilder vectorBuilder (colKeys:ReadOnlyCollection<_>) (nested:Series<'TRowKey, 'TSeries>) =

    // Create column index from keys of all rows
    let columnIndex = colKeys |> Index.ofKeys

    // Row index is just the index of the series
    let rowIndex = nested.Index

    // Dispatcher that creates column vector of the right type
    let columnCreator key =
      { new VectorHelpers.ValueCallSite<IVector> with
          override x.Invoke<'T>(_:'T) = 
            let it = nested.SelectOptional(fun kvp ->
              if kvp.Value.HasValue then 
                kvp.Value.Value.TryGetObject(key) 
                |> OptionalValue.map (Convert.convertType<'T> ConversionKind.Flexible)
              else OptionalValue.Missing)
            it.Vector :> IVector }
      |> VectorHelpers.createValueDispatcher
    
    // Create data vectors
    let data = 
      columnIndex.Keys 
      |> Seq.map (fun key ->
          // Pick a witness from the column, so that we can use column creator
          // and try creating a typed IVector based on the column type
          try
            let someValue =
              nested |> Series.observations |> Seq.tryPick (fun (_, v) -> 
                v.TryGetObject(key) |> OptionalValue.asOption)
            let someValue = defaultArg someValue (obj())
            columnCreator key someValue
          with :? System.InvalidCastException | :? System.FormatException ->
            // If that fails, the sequence is heterogeneous
            // so we try again and pass object as a witness
            columnCreator key (obj()) )
      |> Array.ofSeq |> FrameUtils.vectorBuilder.Create
    Frame(rowIndex, columnIndex, data, indexBuilder, vectorBuilder)

  /// Create data frame from a series of rows
  static member fromRows<'TRowKey, 'TColumnKey, 'TSeries
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> ISeries<'TColumnKey>>
      indexBuilder vectorBuilder (nested:Series<'TRowKey, 'TSeries>) =

    // Create column index from keys of all rows
    let columnIndex = nested.Values |> Seq.collect (fun sr -> sr.Index.Keys) |> Seq.distinct |> ReadOnlyCollection.ofSeq
    FrameUtils.fromRowsAndColumnKeys indexBuilder vectorBuilder columnIndex nested

  /// Create data frame from a series of columns
  static member fromColumns<'TRowKey, 'TColumnKey, 'TSeries when 'TSeries :> ISeries<'TRowKey> 
        and 'TRowKey : equality and 'TColumnKey : equality>
      indexBuilder vectorBuilder (nested:Series<'TColumnKey, 'TSeries>) =
      nested |> Frame<int, int>.FromColumnsNonGeneric indexBuilder vectorBuilder (fun s -> s :> _)

// ------------------------------------------------------------------------------------------------
// These should really be extensions, but they take generic type parameter
// ------------------------------------------------------------------------------------------------

and Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality> with
  
  member internal frame.GroupByLabels labels n =
    let offsets = [0 .. n-1]

    let relocs = 
      labels 
      |> Seq.zip offsets                                // seq of (srcloc, label)
      |> Seq.zip frame.RowKeys                          // seq of (rowkey, (srcloc, label))
      |> Seq.groupBy (fun (rk, (i, l)) -> l)            // seq of (label, seq of (rowkey, (srcloc, label)))
      |> Seq.map (fun (k, s) -> s)                      // seq of (seq of (rowkey, (srcloc, label)))
      |> Seq.concat                                     // seq of (rowkey, (srcloc, label))
      |> Seq.zip offsets                                // seq of (dstloc, (rowkey, (srcloc, label)))
      |> Seq.map (fun (dst, (rowkey, (src, grp))) -> 
         (grp, rowkey), (dst, src))                     // seq of (label, rowkey), (dstloc, srcloc)
      |> ReadOnlyCollection.ofSeq

    let addressify (a, b) = (frame.RowIndex.AddressAt <| int64 a, frame.RowIndex.AddressAt <| int64 b)

    let keys = ReadOnlyCollection.map fst relocs 
    let locs = ReadOnlyCollection.map (snd >> addressify) relocs

    let newIndex = Index.ofKeys keys
    let cmd = VectorConstruction.Relocate(VectorConstruction.Return 0, int64 n, locs)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newIndex.AddressingScheme cmd)
    Frame<_, _>(newIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  member internal frame.NestRowsBy<'TGroup when 'TGroup : equality>(labels:seq<'TGroup>) =
    let indexBuilder = frame.IndexBuilder
    let vectorBuilder = frame.VectorBuilder
 
    let offsets = [0 .. frame.RowCount-1]
    
    let relocs = 
      offsets      
      |> Seq.zip frame.RowKeys                           // seq of (rowkey, offset) 
      |> Seq.zip labels                                  // seq of (label, (rowkey, offset)) 
      |> Seq.groupBy (fun (g, (r, i)) -> g)              // seq of (label, seq of (label * (rowkey * offset)
      |> Seq.map (fun (g, s) -> (g, s |> Seq.map snd))   // seq of (label, seq of (rowkey, offset))
      |> ReadOnlyCollection.ofSeq

    let newIndex  = Index.ofKeys (relocs |> ReadOnlyCollection.map fst) // index of labels
 
    let groups = relocs |> Seq.map (fun (g, idx) ->
      let newIndex = Index.ofKeys(idx |> Seq.map fst |> ReadOnlyCollection.ofSeq)    // index of rowkeys
      let newLocs  = idx |> Seq.map snd                                              // seq of offsets
      let cmd = 
        VectorConstruction.Relocate
          ( VectorConstruction.Return 0, int64 newIndex.KeyCount, 
            newLocs |> Seq.mapi (fun a b -> (newIndex.AddressAt(int64 a), newIndex.AddressAt(int64 b))))
      let newData  = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newIndex.AddressingScheme cmd)
      Frame<_, _>(newIndex, frame.ColumnIndex, newData, indexBuilder, vectorBuilder) )
 
    Series<_, _>(newIndex, Vector.ofValues groups, vectorBuilder, indexBuilder)

  member frame.GroupRowsBy<'TGroup when 'TGroup : equality>(colKey) =
    let col = frame.GetColumn<'TGroup>(colKey)    
    frame.GroupByLabels col.Values frame.RowCount

  member frame.GroupRowsByIndex(keySelector:Func<_, _>) =
    let labels = frame.RowIndex.Keys |> Seq.map keySelector.Invoke
    frame.GroupByLabels labels frame.RowCount

  member frame.GroupRowsUsing<'TGroup when 'TGroup : equality>(f:System.Func<_, _, 'TGroup>) =
    let labels = frame.Rows |> Series.map (fun k v -> f.Invoke(k, v)) |> Series.values
    frame.GroupByLabels labels frame.RowCount    

  /// Returns a data frame whose rows are grouped by `groupBy` and whose columns specified
  /// in `aggBy` are aggregated according to `aggFunc`.
  ///
  /// ## Parameters
  ///  - `groupBy` - sequence of columns to group by
  ///  - `aggBy` - sequence of columns to apply aggFunc to
  ///  - `aggFunc` - invoked in order to aggregate values
  ///
  /// [category:Windowing, chunking and grouping]
  member frame.AggregateRowsBy(groupBy:seq<_>, aggBy:seq<_>, aggFunc:Func<_,_>) =
    let keySet = HashSet(groupBy)
    let filterFunc k = keySet.Contains(k)
    let grpKeys = frame.ColumnKeys |> Seq.filter filterFunc
    let labels = frame.Rows.Values |> Seq.map (Series.getAll grpKeys >> Series.values >> Array.ofSeq) 
    let nested = frame.NestRowsBy(labels)
    nested
    |> Series.map (fun _ f -> [for c in aggBy -> (c, aggFunc.Invoke(f.GetColumn<_>(c)) |> box)] )
    |> Series.map (fun k v -> v |> Seq.append (k |> Seq.zip grpKeys) |> Series.ofObservations)
    |> Series.indexOrdinally
    |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder 

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. The generic type parameter is (typically) needed to specify the type of the 
  /// values in the required index column.
  ///
  /// ## Parameters
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///  - `keepColumn` - Specifies whether the column used as an index should be kept in the frame.
  ///
  /// [category:Indexing]
  member frame.IndexRows<'TNewRowIndex when 'TNewRowIndex : equality>(column, keepColumn) : Frame<'TNewRowIndex, _> = 
    let columnVec = frame.GetColumn<'TNewRowIndex>(column)
    
    // Reindex according to column & drop the column (if not keepColumn)
    let newRowIndex, rowCmd = frame.IndexBuilder.WithIndex(frame.RowIndex, columnVec.Vector, Vectors.Return 0)
    let newColumnIndex, colCmd = 
      if keepColumn then frame.ColumnIndex, Vectors.Return 0
      else frame.IndexBuilder.DropItem((frame.ColumnIndex, Vectors.Return 0), column)

    // Drop the specified column & transform the remaining columns
    let newData = 
      frame.VectorBuilder
        .Build(newColumnIndex.AddressingScheme, colCmd, [| frame.Data |])
        .Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme rowCmd)
    Frame<'TNewRowIndex, 'TColumnKey>(newRowIndex, newColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. The generic type parameter is (typically) needed to specify the type of the 
  /// values in the required index column. 
  ///
  /// The resulting frame will *not* contain the specified column. If you want to preserve the
  /// column, use the overload that takes `keepColumn` parameter.
  ///
  /// ## Parameters
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Indexing]
  member frame.IndexRows<'TNewRowIndex when 'TNewRowIndex : equality>(column) : Frame<'TNewRowIndex, _> = 
    frame.IndexRows<'TNewRowIndex>(column, false)


// ------------------------------------------------------------------------------------------------
// ColumnsSeries/RowSeries - returned by `frame.Rows`, `frame.DenseRows`, `frame.Columns`
// and `frame.DenseColumns`. These are essentially `Series<_, ObjectSeries<_, _>>` with the
// exception that slicing operations of the underlying series are hidden and replaced
// with new ones that re-create frames so e.g. `frame.Rows.[x .. y]` is a frame.
// ------------------------------------------------------------------------------------------------

/// Represents a series of columns from a frame. The type inherits from a series of 
/// series representing individual columns (`Series<'TColumnKey, ObjectSeries<'TRowKey>>`) but
/// hides slicing operations with new versions that return frames.
///
/// [category:Specialized frame and series types]
and ColumnSeries<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>(index, vector, vectorBuilder, indexBuilder) =
  inherit Series<'TColumnKey, ObjectSeries<'TRowKey>>(index, vector, vectorBuilder, indexBuilder)
  new(series:Series<'TColumnKey, ObjectSeries<'TRowKey>>) = 
    ColumnSeries(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder)

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetSlice(lo, hi) = base.GetSlice(lo, hi) |> FrameUtils.fromColumns indexBuilder vectorBuilder 
  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetByLevel(level) = base.GetByLevel(level) |> FrameUtils.fromColumns indexBuilder vectorBuilder 
  member x.Item with get(items) = x.GetItems(items) |> FrameUtils.fromColumns indexBuilder vectorBuilder 
  member x.Item with get(level) = x.GetByLevel(level)


/// Represents a series of rows from a frame. The type inherits from a series of 
/// series representing individual rows (`Series<'TRowKey, ObjectSeries<'TColumnKey>>`) but
/// hides slicing operations with new versions that return frames.
///
/// [category:Specialized frame and series types]
and [<AbstractClass>] RowSeries<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>
    (index:IIndex<'TRowKey>, vector:IVector<ObjectSeries<'TColumnKey>>, vectorBuilder, indexBuilder) = 
  inherit Series<'TRowKey, ObjectSeries<'TColumnKey>>(index, vector, vectorBuilder, indexBuilder) 

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  abstract GetSlice : 'TRowKey option * 'TRowKey option -> Frame<'TRowKey, 'TColumnKey>
  member internal x.BaseGetSlice(lo, hi) = base.GetSlice(lo, hi)

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetByLevel(level) = base.GetByLevel(level) |> FrameUtils.fromRows indexBuilder vectorBuilder 
  member x.Item with get(items) = x.GetItems(items) |> FrameUtils.fromRows indexBuilder vectorBuilder 
  member x.Item with get(level) = x.GetByLevel(level)

  /// Creates a `RowSeries` from a filtered series
  /// (and implements slicing in terms of the specified series)  
  static member FromSeries(series:Series<'TRowKey, ObjectSeries<'TColumnKey>>) =
    { new RowSeries<'TRowKey, 'TColumnKey>(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder) with
        override x.GetSlice(lo, hi) = 
          x.BaseGetSlice(lo, hi) 
          |> FrameUtils.fromRows series.IndexBuilder series.VectorBuilder  }

