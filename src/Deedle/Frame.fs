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
open System.Runtime.InteropServices
open System.Collections.Specialized
open Microsoft.FSharp.Quotations
open VectorHelpers

/// Represents the underlying (raw) data of the frame in a format that can
/// be used for exporting data frame to other formats etc. (DataTable, CSV, Excel)
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
/// interface is to allow writing code that works on arbitrary data frames, although
/// you 
type IFrame = 
  abstract Apply : IFrameOperation<'V> -> 'V

and IFrameOperation<'V> =
  abstract Invoke : Frame<'R, 'C> -> 'V

/// A frame contains one Index, with multiple Vecs
/// (because this is dynamic, we need to store them as IVec)
///
/// ## Joining, zipping and appending
/// More info
///
///
and Frame<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>
    internal ( rowIndex:IIndex<'TRowKey>, columnIndex:IIndex<'TColumnKey>, 
               data:IVector<IVector>) =

  // ----------------------------------------------------------------------------------------------
  // Internals (rowIndex, columnIndex, data and various helpers)
  // ----------------------------------------------------------------------------------------------

  let mutable isEmpty = rowIndex.IsEmpty && columnIndex.IsEmpty

  /// Vector builder
  let vectorBuilder = VectorBuilder.Instance
  let indexBuilder = IndexBuilder.Instance

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
  static let fromColumnsNonGeneric (seriesConv:'S -> ISeries<_>) (nested:Series<_, 'S>) = 
    let columns = Series.observations nested
    let rowIndex = Seq.headOrNone columns |> Option.map (fun (_, s) -> (seriesConv s).Index)
    match rowIndex with 
    | Some rowIndex when (columns |> Seq.forall (fun (_, s) -> Object.ReferenceEquals((seriesConv s).Index, rowIndex))) ->
        // OPTIMIZATION: If all series have the same index (same object), then no join is needed 
        // (This is particularly valuable for things like +, *, /, - operators on Frame)
        let vector = columns |> Seq.map (fun (_, s) -> 
          // When the nested series data is in 'IBoxedVector', get the unboxed representation
          unboxVector (seriesConv s).Vector) |> Vector.ofValues
        Frame<_, _>(rowIndex, Index.ofKeys (Seq.map fst columns), vector)

    | _ ->
        // Create new row index by unioning all keys
        let rowKeys = columns |> Seq.collect (fun (_, s) -> (seriesConv s).Index.Keys) |> Seq.distinct |> Array.ofSeq
        let rowIndex = nested.IndexBuilder.Create(rowKeys, None)
        // Create column index by taking all column keys
        let colKeys = nested |> Series.observationsAll |> Seq.map fst |> Array.ofSeq
        let colIndex = nested.IndexBuilder.Create(colKeys, None)
        // Build the data 
        let data = 
          nested |> Series.observationsAll |> Seq.map (fun (_, s) ->
            let series = match s with Some s -> seriesConv s | _ -> Series.Create([], []) :> ISeries<_>
            let cmd = nested.IndexBuilder.Reindex(series.Index, rowIndex, Lookup.Exact, Vectors.Return 0, fun _ -> true)
            // When the nested series data is in 'IBoxedVector', get the unboxed representation
            VectorHelpers.transformColumn nested.VectorBuilder cmd (unboxVector series.Vector) )
          |> Vector.ofValues
        Frame<_, _>(rowIndex, colIndex, data)

  static member internal FromColumnsNonGeneric seriesConv nested = fromColumnsNonGeneric seriesConv nested

  member private x.setColumnIndex newColumnIndex = 
    columnIndex <- newColumnIndex
    let args = NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset)
    frameColumnsChanged.Trigger([| x; args |])

  member internal x.IndexBuilder = indexBuilder
  member internal x.VectorBuilder = vectorBuilder

  member internal frame.RowIndex = rowIndex
  member internal frame.ColumnIndex = columnIndex
  member internal frame.Data = data

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
  ///  - `op` - A function that is applied to aligned values. The `Zip` operation is generic
  ///    in the type of this function and the type of function is used to determine which 
  ///    values in the frames are zipped and which are left unchanged.
  ///
  /// [category:Joining, zipping and appending]
  member frame1.Zip<'V1, 'V2, 'V3>(otherFrame:Frame<'TRowKey, 'TColumnKey>, columnKind, rowKind, lookup, op:Func<'V1, 'V2, 'V3>) =
    
    // Create transformations to join the rows (using the same logic as Join)
    // and make functions that transform vectors (when they are only available in first/second frame)
    let rowIndex, f1cmd, f2cmd = 
      createJoinTransformation indexBuilder rowKind lookup rowIndex otherFrame.RowIndex (Vectors.Return 0) (Vectors.Return 1)
    let f1trans = VectorHelpers.transformColumn vectorBuilder f1cmd
    let f2trans = VectorHelpers.transformColumn vectorBuilder (VectorHelpers.substitute (1, 0) f2cmd)

    // To join columns using 'Series.join', we create series containing raw "IVector" data 
    // (so that we do not convert each series to objects series)
    let s1 = Series(frame1.ColumnIndex, frame1.Data, frame1.VectorBuilder, frame1.IndexBuilder)
    let s2 = Series(otherFrame.ColumnIndex, otherFrame.Data, otherFrame.VectorBuilder, otherFrame.IndexBuilder)

    // Operations that try converting vectors to the required types for 'op'
    let asV1 = VectorHelpers.tryChangeType<'V1>
    let asV2 = VectorHelpers.tryChangeType<'V2>
    let (|TryConvert|_|) f inp = OptionalValue.asOption (f inp)

    let newColumns = 
      s1.Zip(s2, columnKind).Select(fun (KeyValue(_, (l, r))) -> 
        match l, r with
        | OptionalValue.Present (TryConvert asV1 lv), OptionalValue.Present (TryConvert asV2 rv) ->
            let lvVect : IVector<Choice<'V1, 'V2, 'V3>> = lv.Select(Choice1Of3)
            let lrVect : IVector<Choice<'V1, 'V2, 'V3>> = rv.Select(Choice2Of3)
            let res = Vectors.Combine(f1cmd, f2cmd, VectorValueTransform.CreateLifted (fun l r ->
              match l, r with
              | Choice1Of3 l, Choice2Of3 r -> op.Invoke(l, r) |> Choice3Of3
              | _ -> failwith "Zip: Got invalid vector while zipping" ))
            frame1.VectorBuilder.Build(res, [| lvVect; lrVect |]).
              Select(function Choice3Of3 v -> v | _ -> failwith "Zip: Produced invalid vector") :> IVector
        | OptionalValue.Present v, _ -> f1trans v
        | _, OptionalValue.Present v -> f2trans v
        | _ -> failwith "zipAlignInto: join failed." )
    Frame<_, _>(rowIndex, newColumns.Index, newColumns.Vector)

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
    frame1.Zip(otherFrame, JoinKind.Outer, JoinKind.Outer, Lookup.Exact, op)

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
  ///
  /// [category:Joining, zipping and appending]
  member frame.Join(otherFrame:Frame<'TRowKey, 'TColumnKey>, kind, lookup) =    
    // Union/intersect/align row indices and get transformations to apply to left/right vectors
    let newRowIndex, thisRowCmd, otherRowCmd = 
      createJoinTransformation indexBuilder kind lookup rowIndex otherFrame.RowIndex (Vectors.Return 0) (Vectors.Return 0)
    // Append the column indices and get transformation to combine them
    // (LeftOrRight - specifies that when column exist in both data frames then fail)
    let newColumnIndex, colCmd = 
      indexBuilder.Append( (columnIndex, Vectors.Return 0), (otherFrame.ColumnIndex, Vectors.Return 1), VectorValueTransform.LeftOrRight)
    // Apply transformation to both data vectors
    let newThisData = data.Select(transformColumn vectorBuilder thisRowCmd)
    let newOtherData = otherFrame.Data.Select(transformColumn vectorBuilder otherRowCmd)
    // Combine column vectors a single vector & return results
    let newData = vectorBuilder.Build(colCmd, [| newThisData; newOtherData |])
    Frame(newRowIndex, newColumnIndex, newData)

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


  /// Append two data frames with non-overlapping values. The operation takes the union of columns
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
  member frame.Append(otherFrame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.AppendN([ otherFrame ])

  /// Append multiple data frames with non-overlapping values. The operation takes the union of columns
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
  member frame.AppendN(otherFrames:Frame<'TRowKey, 'TColumnKey> seq) = 
    let frames = seq { yield frame; yield! otherFrames }

    // build the union of all row keys
    let rowUnion ix (f:Frame<'R, 'C>) = 
      let newIx, _, _ = indexBuilder.Union((ix, VectorConstruction.Empty), (f.RowIndex, VectorConstruction.Empty))
      newIx

    let allRowIx = frames |> Seq.fold rowUnion (Index.ofKeys [])

    // Create a row command: steps to build a result column from a list of input rows
    //  1) align each vector to the unioned row index
    //  2) merge across all vectors
    let rowCmd = 
      frames
      |> Seq.map (fun f -> f.RowIndex) 
      |> Seq.mapi (fun i x -> indexBuilder.Reindex(x, allRowIx, Lookup.Exact, Vectors.Return(i), fun _ -> true))
      |> fun vc -> Vectors.CombineN(Seq.toList vc, VectorValueListTransform.AtMostOne)

    // Define a function to construct result vector via the above row command, which will dynamically
    // dispatch to a type-specific generic implementation
    //
    // This uses the specified 'witnessVec' to invoke 'VectorCallSite' with the runtime
    // type of the witness vector as a static type argument, then it converts all vectors
    // to this specified type and appends them.
    let convertAndAppendVectors (witnessVec:IVector) (vectors:IVector list) =
      { new VectorCallSite<IVector> with
          override x.Invoke<'T>(_:IVector<'T>) =
            let typed = vectors |> Seq.map (VectorHelpers.changeType<'T>) |> Array.ofSeq
            vectorBuilder.Build(rowCmd, typed) :> IVector }
      |> witnessVec.Invoke

    // define the transformation itself, piecing components together
    let append = VectorValueListTransform.Create(fun (lst: OptionalValue<IVector> list) ->
      let witnessVec = 
        match lst |> Seq.tryFind (fun v -> v.HasValue) with
        | Some(v) -> v.Value
        | _       -> invalidOp "Logic error: could not find non-empty IVector ??"

      let empty = Vector.ofValues []
      let input = [ for v in lst do 
                    if v.HasValue 
                    then yield v.Value
                    else yield upcast Vector.ofValues [] ]
      input |> convertAndAppendVectors witnessVec |> fun r -> OptionalValue(r) )

    // build the union of all column keys
    let colUnion ix (f:Frame<'R, 'C>) = 
      let newIx, _, _ = indexBuilder.Union((ix, VectorConstruction.Empty), (f.ColumnIndex, VectorConstruction.Empty))
      newIx

    let allColIx = frames |> Seq.fold colUnion (Index.ofKeys [])

    // Create a frame command: steps to build a frame from a list of input frames
    //  1) align each column to the unioned column index
    //  2) merge the columns via the row command transform 
    let frameCmd = 
      frames 
      |> Seq.map (fun f -> f.ColumnIndex) 
      |> Seq.mapi (fun i x -> indexBuilder.Reindex(x, allColIx, Lookup.Exact, Vectors.Return(i), fun _ -> true))
      |> fun vc -> Vectors.CombineN(Seq.toList vc, append)

    let frameData = frames |> Seq.map (fun f -> f.Data) |> Seq.toArray
    
    let newData = vectorBuilder.Build(frameCmd, frameData)

    Frame(allRowIx, allColIx, newData)

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
  member frame.Columns = 
    ColumnSeries(Series.Create(columnIndex, data.Select(fun vect -> 
      Series.CreateUntyped(rowIndex, boxVector vect))))

  /// [category:Accessors and slicing]
  member frame.ColumnsDense = 
    ColumnSeries(Series.Create(columnIndex, data.SelectMissing(fun vect -> 
      // Assuming that the data has all values - which should be an invariant...
      let all = rowIndex.Mappings |> Seq.forall (fun (key, addr) -> vect.Value.GetObject(addr).HasValue)
      if all then OptionalValue(Series.CreateUntyped(rowIndex, boxVector vect.Value))
      else OptionalValue.Missing )))

  /// [category:Accessors and slicing]
  member frame.Rows =
    let getRow addr =
       let rowReader = createObjRowReader data vectorBuilder columnIndex.KeyCount addr
       Series.CreateUntyped(columnIndex, rowReader)
    let values = Array.init (int rowIndex.KeyCount) (fun a -> getRow (Address.ofInt a))
    RowSeries(Series<_, _>(rowIndex, vectorBuilder.Create(values), vectorBuilder, indexBuilder))

  /// [category:Accessors and slicing]
  member frame.RowsDense = 
    let emptySeries = Series<_, _>(rowIndex, Vector.ofValues [], vectorBuilder, indexBuilder)
    let res = emptySeries.SelectOptional (fun row ->
      let rowAddress = rowIndex.Locate(row.Key)
      let rowVec = createObjRowReader data vectorBuilder columnIndex.KeyCount rowAddress
      let all = columnIndex.Mappings |> Seq.forall (fun (key, addr) -> rowVec.GetValue(addr).HasValue)
      if all then OptionalValue(Series.CreateUntyped(columnIndex, rowVec))
      else OptionalValue.Missing )
    RowSeries(Series.dropMissing res)

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
    frame.RowIndex.KeyAt(Address.ofInt index)

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
    let rowAddress = Address.ofInt index
    Series.Create(columnIndex, createRowReader data vectorBuilder columnIndex.KeyCount rowAddress)

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
    if rowAddress = Address.Invalid then OptionalValue.Missing
    else OptionalValue(Series.Create(columnIndex, createRowReader data vectorBuilder columnIndex.KeyCount rowAddress))

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
    else OptionalValue(Series.Create(columnIndex, createRowReader data vectorBuilder columnIndex.KeyCount (snd rowAddress.Value)))

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
      let row = Series.Create(columnIndex, createRowReader data vectorBuilder columnIndex.KeyCount (snd rowAddress.Value))
      OptionalValue(KeyValuePair(fst rowAddress.Value, row))
  
  // ----------------------------------------------------------------------------------------------
  // Fancy accessors for frame columns and frame data
  // ----------------------------------------------------------------------------------------------

  /// [category:Fancy accessors]
  member frame.GetColumns<'R>() = 
    frame.Columns.SelectOptional(fun (KeyValue(k, vopt)) ->
      vopt |> OptionalValue.bind (fun ser -> ser.TryAs<'R>(false)))

  /// [category:Fancy accessors]
  member frame.GetAllValues<'R>() = frame.GetAllValues<'R>(false)

  /// [category:Fancy accessors]
  member frame.GetAllValues<'R>(strict) =
    seq { for (KeyValue(_, v)) in frame.GetAllSeries<'R>() do yield! v |> Series.values }

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
  member frame.AddSeries(column:'TColumnKey, series:seq<_>) = 
    frame.AddSeries(column, series, Lookup.Exact)

  /// Mutates the data frame by adding an additional data series
  /// as a new column with the specified column key. The operation 
  /// uses left join and aligns new series to the existing frame keys.
  ///
  /// ## Parameters
  ///  - `series` - A data series to be added (the row key type has to match)
  ///  - `column` - A key (or name) for the newly added column
  ///
  /// [category:Series operations]
  member frame.AddSeries(column:'TColumnKey, series:ISeries<_>) = 
    frame.AddSeries(column, series, Lookup.Exact)

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
  member frame.AddSeries(column:'TColumnKey, series:seq<'V>, lookup) = 
    if isEmpty then
      if typeof<'TRowKey> = typeof<int> then
        let series = unbox<Series<'TRowKey, 'V>> (Series.ofValues series)
        frame.AddSeries(column, series, lookup)
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
          let nulls = seq { for i in 1 .. rowCount - count -> None }
          Vector.ofOptionalValues (Seq.append (Seq.map Some series) nulls)

      let series = Series(frame.RowIndex, vector, vectorBuilder, indexBuilder)
      frame.AddSeries(column, series, lookup)

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
  member frame.AddSeries<'V>(column:'TColumnKey, series:ISeries<'TRowKey>, lookup) = 
    if isEmpty then
      // If the frame was empty, then initialize both indices
      rowIndex <- series.Index
      isEmpty <- false
      data <- Vector.ofValues [series.Vector]
      frame.setColumnIndex (Index.ofKeys [column])
    else
      let other = Frame(series.Index, Index.ofUnorderedKeys [column], Vector.ofValues [series.Vector])
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
  member frame.DropSeries(column:'TColumnKey) = 
    let newColumnIndex, colCmd = indexBuilder.DropItem( (columnIndex, Vectors.Return 0), column)
    data <- vectorBuilder.Build(colCmd, [| data |])
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
  member frame.ReplaceSeries(column:'TColumnKey, series:ISeries<_>, lookup) = 
    if columnIndex.Lookup(column, Lookup.Exact, fun _ -> true).HasValue then
      frame.DropSeries(column)
    frame.AddSeries(column, series, lookup)

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
  member frame.ReplaceSeries(column, data:seq<'V>, lookup) = 
    frame.ReplaceSeries(column, Series.Create(frame.RowIndex, Vector.ofValues data), lookup)

  /// Mutates the data frame by replacing the specified series with
  /// a new series. (If the series does not exist, only the new
  /// series is added.)
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A data series to be used (the row key type has to match)
  ///
  /// [category:Series operations]
  member frame.ReplaceSeries(column:'TColumnKey, series:ISeries<_>) = 
    frame.ReplaceSeries(column, series, Lookup.Exact)

  /// Mutates the data frame by replacing the specified series with
  /// a new data sequence . (If the series does not exist, only the new
  /// series is added.)
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A sequence of values to be added
  ///
  /// [category:Series operations]
  member frame.ReplaceSeries(column, data:seq<'V>) = 
    frame.ReplaceSeries(column, data, Lookup.Exact)



  /// [category:Series operations]
  member frame.Item 
    with get(column:'TColumnKey) = frame.GetSeries<float>(column)
    and set(column:'TColumnKey) (series:Series<'TRowKey, float>) = frame.ReplaceSeries(column, series)

  /// [category:Series operations]
  member frame.SeriesApply<'T>(f) = frame.SeriesApply<'T>(false, f)

  /// [category:Series operations]
  member frame.SeriesApply<'T>(strict, f:Func<Series<'TRowKey, 'T>, ISeries<_>>) = 
    frame.Columns |> Series.mapValues (fun os ->
      match os.TryAs<'T>(strict) with
      | OptionalValue.Present s -> f.Invoke s
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric id

  /// [category:Series operations]
  member frame.GetSeries<'R>(column:'TColumnKey, lookup) : Series<'TRowKey, 'R> = 
    match safeGetColVector(column, lookup, fun _ -> true) with
    | :? IVector<'R> as vec -> 
        Series.Create(rowIndex, vec)
    | colVector ->
        Series.Create(rowIndex, changeType colVector)

  /// [category:Series operations]
  member frame.GetSeriesAt<'R>(index:int) : Series<'TRowKey, 'R> = 
    frame.Columns.GetAt(index).As<'R>()

  /// [category:Series operations]
  member frame.GetSeries<'R>(column:'TColumnKey) : Series<'TRowKey, 'R> = 
    frame.GetSeries(column, Lookup.Exact)

  /// [category:Series operations]
  member frame.GetAllSeries<'R>() = frame.GetAllSeries<'R>(false)

  /// [category:Series operations]
  member frame.TryGetSeries<'R>(column:'TColumnKey, lookup) =
    tryGetColVector(column, lookup, fun _ -> true) 
    |> OptionalValue.map (fun v -> Series.Create(rowIndex, changeType<'R> v))

  /// [category:Series operations]
  member frame.TryGetSeriesObservation<'R>(column:'TColumnKey, lookup) =
    let columnIndex = columnIndex.Lookup(column, lookup, fun _ -> true)
    if not columnIndex.HasValue then 
      OptionalValue.Missing 
    else
      data.GetValue (snd columnIndex.Value) 
      |> OptionalValue.map (fun vec ->
        let ser = Series.Create(rowIndex, changeType<'R> vec)
        KeyValuePair(fst columnIndex.Value, ser) )

  /// [category:Series operations]
  member frame.GetAllSeries<'R>(strict) =
    frame.Columns.Observations |> Seq.choose (fun os -> 
      match os.Value.TryAs<'R>(strict) with
      | OptionalValue.Present s -> Some (KeyValuePair(os.Key, s))
      | _ -> None)

  /// [category:Series operations]
  member frame.RenameSeries(columnKeys) =
    if Seq.length columnIndex.Keys <> Seq.length columnKeys then 
      invalidArg "columnKeys" "The number of new column keys does not match with the number of columns"
    frame.setColumnIndex (Index.ofKeys columnKeys)

  /// [category:Series operations]
  member frame.RenameSeries(oldKey, newKey) =
    let newKeys = columnIndex.Keys |> Seq.map (fun k -> if k = oldKey then newKey else k)
    frame.setColumnIndex (Index.ofKeys newKeys)

  /// [category:Series operations]
  member frame.RenameSeries(mapping:Func<_, _>) =
    frame.setColumnIndex (Index.ofKeys (Seq.map mapping.Invoke columnIndex.Keys))

  /// [category:Series operations]
  static member (?<-) (frame:Frame<_, _>, column, series:Series<'T, 'V>) =
    frame.ReplaceSeries(column, series)

  /// [category:Series operations]
  static member (?<-) (frame:Frame<_, _>, column, data:seq<'V>) =
    frame.ReplaceSeries(column, data)

  /// [category:Series operations]
  static member (?) (frame:Frame<_, _>, column) : Series<'T, float> = 
    frame.GetSeries<float>(column)

  // ----------------------------------------------------------------------------------------------
  // Some operators
  // ----------------------------------------------------------------------------------------------

  // Apply operation 'op' with 'series' on the right to all columns convertible to 'T
  static member inline private PointwiseFrameSeriesR<'T>(frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, 'T>, op:'T -> 'T -> 'T) =
    frame.Columns |> Series.mapValues (fun os ->
      match os.TryAs<'T>(false) with
      | OptionalValue.Present s -> s.ZipInner(series) |> Series.mapValues (fun (v1, v2) -> op v1 v2) :> ISeries<_>
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric id

  // Apply operation 'op' to all columns that exist in both frames and are convertible to 'T
  static member inline internal PointwiseFrameFrame<'T>(frame1:Frame<'TRowKey, 'TColumnKey>, frame2:Frame<'TRowKey, 'TColumnKey>, op:'T -> 'T -> 'T) =
    frame1.Zip<'T, 'T, 'T>(frame2, JoinKind.Outer, JoinKind.Outer, Lookup.Exact, fun a b -> op a b)

  // Apply operation 'op' with 'scalar' on the right to all columns convertible to 'T
  static member inline private ScalarOperationR<'T>(frame:Frame<'TRowKey, 'TColumnKey>, scalar:'T, op:'T -> 'T -> 'T) : Frame<'TRowKey, 'TColumnKey> =
    frame.Columns |> Series.mapValues (fun os -> 
      match os.TryAs<'T>(false) with
      | OptionalValue.Present s -> (Series.mapValues (fun v -> op v scalar) s) :> ISeries<_>
      | _ -> os :> ISeries<_>)
    |> fromColumnsNonGeneric id

  // Apply operation 'op' with 'series' on the left to all columns convertible to 'T
  static member inline private PointwiseFrameSeriesL<'T>(frame:Frame<'TRowKey, 'TColumnKey>, series:Series<'TRowKey, 'T>, op:'T -> 'T -> 'T) =
    Frame<'TRowKey, 'TColumnKey>.PointwiseFrameSeriesR<'T>(frame, series, fun a b -> op b a)
  // Apply operation 'op' with 'scalar' on the left to all columns convertible to 'T
  static member inline private ScalarOperationL<'T>(frame:Frame<'TRowKey, 'TColumnKey>, scalar:'T, op:'T -> 'T -> 'T) : Frame<'TRowKey, 'TColumnKey> =
    Frame<'TRowKey, 'TColumnKey>.ScalarOperationR<'T>(frame, scalar, fun a b -> op b a)
  // Apply operation 'op' to all values in all columns convertible to 'T
  static member inline internal NullaryOperation<'T>(frame:Frame<'TRowKey, 'TColumnKey>, op : 'T -> 'T) = 
    frame.SeriesApply(false, fun (s:Series<'TRowKey, 'T>) -> (Series.mapValues op s) :> ISeries<_>)
  // Apply operation 'op' to all values in all columns convertible to 'T1 (the operation returns different type!)
  static member inline internal NullaryGenericOperation<'T1, 'T2>(frame:Frame<'TRowKey, 'TColumnKey>, op : 'T1 -> 'T2) =
    frame.SeriesApply(false, fun (s:Series<'TRowKey, 'T1>) -> (Series.mapValues op s) :> ISeries<_>)

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
  static member Acos(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, acos)
  /// [category:Operators]
  static member Asin(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, asin)
  /// [category:Operators]
  static member Atan(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, atan)
  /// [category:Operators]
  static member Sin(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, sin)
  /// [category:Operators]
  static member Sinh(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, sinh)
  /// [category:Operators]
  static member Cos(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, cos)
  /// [category:Operators]
  static member Cosh(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, cosh)
  /// [category:Operators]
  static member Tan(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, tan)
  /// [category:Operators]
  static member Tanh(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, tanh)

  // Actually useful

  /// [category:Operators]
  static member Abs(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, abs)
  /// [category:Operators]
  static member Ceiling(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, ceil)
  /// [category:Operators]
  static member Exp(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, exp)
  /// [category:Operators]
  static member Floor(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, floor)
  /// [category:Operators]
  static member Truncate(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, truncate)
  /// [category:Operators]
  static member Log(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, log)
  /// [category:Operators]
  static member Log10(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, log10)
  /// [category:Operators]
  static member Round(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryOperation<float>(frame, round)
  /// [category:Operators]
  static member Sign(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryGenericOperation<float, _>(frame, sign)
  /// [category:Operators]
  static member Sqrt(frame) = Frame<'TRowKey, 'TColumnKey>.NullaryGenericOperation<float, _>(frame, sqrt)

  // ----------------------------------------------------------------------------------------------
  // Constructor
  // ----------------------------------------------------------------------------------------------

  new(names:seq<'TColumnKey>, columns:seq<ISeries<'TRowKey>>) =
    let df = Frame(Index.ofKeys [], Index.ofKeys [], Vector.ofValues [])
    let df = (df, Seq.zip names columns) ||> Seq.fold (fun df (colKey, colData) ->
      let other = Frame(colData.Index, Index.ofUnorderedKeys [colKey], Vector.ofValues [colData.Vector])
      df.Join(other, JoinKind.Outer) )
    Frame(df.RowIndex, df.ColumnIndex, df.Data)

  member frame.Clone() =
    Frame<_, _>(rowIndex, columnIndex, data)

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
      for key, _ in index.Mappings ->
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
        match frame.ColumnIndex.Keys |> Seq.headOrNone with 
        Some colKey -> CustomKey.Get(colKey).Levels | _ -> 1
      let rowLevels = 
        match frame.RowIndex.Keys |> Seq.headOrNone with 
        Some rowKey -> CustomKey.Get(rowKey).Levels | _ -> 1

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
            for colKey, _ in frame.ColumnIndex.Mappings do 
              yield getLevel frame.ColumnIndex.IsOrdered previous ignore colLevels colLevel colKey ]

        // If we want to print types, add another line with type information
        if printTypes then 
          yield [
            // Prefix with appropriate number of (empty) row keys
            for i in 0 .. rowLevels - 1 do yield "" 
            yield ""
            let previous = ref None
            for _, colAddr in frame.ColumnIndex.Mappings do 
              let vector = frame.Data.GetValue(colAddr) 
              let typ = 
                if not vector.HasValue then "missing"
                else formatType vector.Value.ElementType
              yield String.Concat("(", typ, ")") ]
              
        // Yield row data
        let rows = frame.Rows
        let previous = Array.init rowLevels (fun _ -> ref None)
        let reset i () = for j in i + 1 .. rowLevels - 1 do previous.[j] := None
        for item in frame.RowIndex.Mappings |> Seq.startAndEnd startCount endCount do
          match item with 
          | Choice2Of3() ->
              yield [
                // Prefix with appropriate number of (empty) row keys
                for i in 0 .. rowLevels - 1 do yield if i = 0 then ":" else ""
                yield ""
                for i in 1 .. data.DataSequence |> Seq.length -> "..." ]
          | Choice1Of3(rowKey, addr) | Choice3Of3(rowKey, addr) ->
              let row = rows.[rowKey]
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
          <@@ box ((%%frame : Frame<'TRowKey, 'TColumnKey>).GetSeries<float>(colKey)) @@>)

        (fun frame name argTyp argExpr -> 
          // Cast column key to the right type
          let colKey =
            if typeof<'TColumnKey> = typeof<string> then unbox<'TColumnKey> name
            else invalidOp "Dynamic operations are not supported on frames with non-string column key!"
          // If it is a series, then we can just call ReplaceSeries using quotation
          if typeof<ISeries<'TRowKey>>.IsAssignableFrom argTyp then
            <@@ (%%frame : Frame<'TRowKey, 'TColumnKey>).ReplaceSeries(colKey, (%%(Expr.Coerce(argExpr, typeof<ISeries<'TRowKey>>)) : ISeries<'TRowKey>)) @@>
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
                // Find the ReplaceSeries method taking seq<'T>
                let addSeriesSeq = 
                  typeof<Frame<'TRowKey, 'TColumnKey>>.GetMethods() |> Seq.find (fun mi -> 
                    mi.Name = "ReplaceSeries" && 
                      ( let pars = mi.GetParameters()
                        pars.Length = 2 && pars.[1].ParameterType.GetGenericTypeDefinition() = typedefof<seq<_>> ))
                // Generate call for the right generic specialization
                let seqTyp = typedefof<seq<_>>.MakeGenericType(elemTy)
                Expr.Call(frame, addSeriesSeq.MakeGenericMethod(elemTy), [Expr.Value name; Expr.Coerce(argExpr, seqTyp)] ) )



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

  /// Create data frame containing a single column
  static member createColumn<'TColumnKey, 'TRowKey when 'TColumnKey : equality and 'TRowKey : equality>
      (column:'TColumnKey, series:ISeries<'TRowKey>) = 
    let data = Vector.ofValues [| series.Vector |]
    Frame(series.Index, Index.ofKeys [column], data)

  /// Create data frame containing a single row
  static member createRow(row:'TRowKey, series:Series<'TColumnKey, 'TValue>) = 
    let data = series.Vector.SelectMissing(fun v -> 
      let res = VectorBuilder.Instance.CreateMissing [| v |] 
      OptionalValue(res :> IVector))
    Frame(Index.ofKeys [row], series.Index, data)
    
  /// Create data frame from a series of rows
  static member fromRowsAndColumnKeys<'TRowKey, 'TColumnKey, 'TSeries
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> ISeries<'TColumnKey>> 
      colKeys (nested:Series<'TRowKey, 'TSeries>) =

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
                |> OptionalValue.map (Convert.changeType<'T>)
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
            // If that failes, the sequence is heterogeneous
            // so we try again and pass object as a witness
            columnCreator key (obj()) )
      |> Array.ofSeq |> FrameUtils.vectorBuilder.Create
    Frame(rowIndex, columnIndex, data)

  /// Create data frame from a series of rows
  static member fromRows<'TRowKey, 'TColumnKey, 'TSeries
        when 'TRowKey : equality and 'TColumnKey : equality and 'TSeries :> ISeries<'TColumnKey>>
      (nested:Series<'TRowKey, 'TSeries>) =

    // Create column index from keys of all rows
    let columnIndex = nested.Values |> Seq.collect (fun sr -> sr.Index.Keys) |> Seq.distinct |> Array.ofSeq

    FrameUtils.fromRowsAndColumnKeys columnIndex nested

  /// Create data frame from a series of columns
  static member fromColumns<'TRowKey, 'TColumnKey, 'TSeries when 'TSeries :> ISeries<'TRowKey> 
        and 'TRowKey : equality and 'TColumnKey : equality>
      (nested:Series<'TColumnKey, 'TSeries>) =
      nested |> Frame<int, int>.FromColumnsNonGeneric (fun s -> s :> _)

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

    let addressify (a, b) = (Address.ofInt a, Address.ofInt b)

    let keys, locs = relocs |> (fun r ->                
      Seq.map fst r, 
      Seq.map (snd >> addressify) r)

    let newIndex = Index.ofKeys keys
    let cmd = VectorConstruction.Relocate(VectorConstruction.Return 0, int64 n, locs)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder cmd)
    Frame<_, _>(newIndex, frame.ColumnIndex, newData)

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

    let newIndex  = Index.ofKeys (relocs |> Seq.map fst) // index of labels
 
    let groups = relocs |> Seq.map (fun (g, idx) ->
      let newIndex = Index.ofKeys(idx |> Seq.map fst)    // index of rowkeys
      let newLocs  = idx |> Seq.map snd                  // seq of offsets
      let cmd = VectorConstruction.Relocate(VectorConstruction.Return 0, int64 newIndex.KeyCount, newLocs |> Seq.mapi (fun a b -> (int64 a, int64 b)))
      let newData  = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder cmd)
      Frame<_, _>(newIndex, frame.ColumnIndex, newData) )
 
    Series<_, _>(newIndex, Vector.ofValues groups, vectorBuilder, indexBuilder)

  member frame.GroupRowsBy<'TGroup when 'TGroup : equality>(colKey) =
    let col = frame.GetSeries<'TGroup>(colKey)    
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
    |> Series.map (fun _ f -> [for c in aggBy -> (c, aggFunc.Invoke(f.GetSeries<_>(c)) |> box)] )
    |> Series.map (fun k v -> v |> Seq.append (k |> Seq.zip grpKeys) |> Series.ofObservations)
    |> Series.indexOrdinally
    |> FrameUtils.fromRows

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. The generic type parameter is (typically) needed to specify the type of the 
  /// values in the required index column.
  ///
  /// ## Parameters
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Indexing]
  member frame.IndexRows<'TNewRowIndex when 'TNewRowIndex : equality>(column) : Frame<'TNewRowIndex, _> = 
    let columnVec = frame.GetSeries<'TNewRowIndex>(column)
    let lookup addr = columnVec.Vector.GetValue(addr)
    let newRowIndex, rowCmd = frame.IndexBuilder.WithIndex(frame.RowIndex, lookup, Vectors.Return 0)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder rowCmd)
    Frame<'TNewRowIndex, 'TColumnKey>(newRowIndex, frame.ColumnIndex, newData)

// ------------------------------------------------------------------------------------------------
// 
// ------------------------------------------------------------------------------------------------

and ColumnSeries<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>(index, vector, vectorBuilder, indexBuilder) =
  inherit Series<'TColumnKey, ObjectSeries<'TRowKey>>(index, vector, vectorBuilder, indexBuilder)
  new(series:Series<'TColumnKey, ObjectSeries<'TRowKey>>) = 
    ColumnSeries(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder)

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetSlice(lo, hi) = base.GetSlice(lo, hi) |> FrameUtils.fromColumns
  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetByLevel(level) = base.GetByLevel(level) |> FrameUtils.fromColumns
  member x.Item with get(items) = x.GetItems(items) |> FrameUtils.fromColumns
  member x.Item with get(level) = x.GetByLevel(level)

and RowSeries<'TRowKey, 'TColumnKey when 'TRowKey : equality and 'TColumnKey : equality>(index, vector, vectorBuilder, indexBuilder) =
  inherit Series<'TRowKey, ObjectSeries<'TColumnKey>>(index, vector, vectorBuilder, indexBuilder)
  new(series:Series<'TRowKey, ObjectSeries<'TColumnKey>>) = 
    RowSeries(series.Index, series.Vector, series.VectorBuilder, series.IndexBuilder)

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetSlice(lo, hi) = base.GetSlice(lo, hi) |> FrameUtils.fromRows
  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetByLevel(level) = base.GetByLevel(level) |> FrameUtils.fromRows
  member x.Item with get(items) = x.GetItems(items) |> FrameUtils.fromRows
  member x.Item with get(level) = x.GetByLevel(level)