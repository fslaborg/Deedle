namespace FSharp.DataFrame

[<CompiledName("Column`1")>]
type column<'T>(value:obj) =
  member x.Value = value

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Frame = 
  open FSharp.DataFrame.Internal

  // ----------------------------------------------------------------------------------------------
  // Grouping
  // ----------------------------------------------------------------------------------------------

  let collapseCols (series:Series<'K, Frame<'K1, 'K2>>) = 
    series 
    |> Series.map (fun k1 df -> df.Columns |> Series.mapKeys(fun k2 -> (k1, k2)) |> FrameUtils.fromColumns)
    |> Series.values |> Seq.reduce (fun df1 df2 -> df1.Append(df2))
  let collapseRows (series:Series<'K, Frame<'K1, 'K2>>) = 
    series 
    |> Series.map (fun k1 df -> df.Rows |> Series.mapKeys(fun k2 -> (k1, k2)) |> FrameUtils.fromRows)
    |> Series.values |> Seq.reduce (fun df1 df2 -> df1.Append(df2))


  let groupRowsUsing selector (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupInto selector (fun k g -> g |> FrameUtils.fromRows) |> collapseRows
  let groupColsUsing selector (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.groupInto selector (fun k g -> g |> FrameUtils.fromColumns) |> collapseCols

  let groupRowsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupInto 
      (fun _ v -> v.GetAs<'K>(column)) 
      (fun k g -> g |> FrameUtils.fromRows)
    |> collapseRows

  let groupColsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.groupInto 
      (fun _ v -> v.GetAs<'K>(column)) 
      (fun k g -> g |> FrameUtils.fromColumns)
    |> collapseCols

  let groupRowsByObj column frame : Frame<obj * _, _> = groupRowsBy column frame
  let groupRowsByInt column frame : Frame<int * _, _> = groupRowsBy column frame
  let groupRowsByString column frame : Frame<string * _, _> = groupRowsBy column frame
  let groupColsByObj column frame : Frame<_, obj * _> = groupColsBy column frame
  let groupColsByInt column frame : Frame<_, int * _> = groupColsBy column frame
  let groupColsByString column frame : Frame<_, string * _> = groupColsBy column frame


  //let shiftRows offset (frame:Frame<'TRowKey, 'TColKey>) = 
  //  frame.Columns 
  //  |> Series.map (fun k col -> Series.shift offset col)
  //  |> Frame.ofColumns

  let takeLast count (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.takeLast count |> FrameUtils.fromRows

  let dropMissing (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowsDense |> Series.dropMissing |> FrameUtils.fromRows


  // ----------------------------------------------------------------------------------------------
  // Wrappers that simply call member functions of the data frame
  // ----------------------------------------------------------------------------------------------

  /// Creates a new data frame that contains all data from 
  /// the original data frame, together with additional series.
  [<CompiledName("AddColumn")>]
  let addCol column (series:Series<_, _>) (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.AddSeries(column, series); f

  /// Append two data frames. The columns of the resulting data frame
  /// will be the union of columns of the two data frames. The row keys
  /// may overlap, but the values must not - if there is a value for a
  /// certain column, at the same row index in both data frames, an exception
  /// is thrown. 
  [<CompiledName("Append")>]
  let append (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Append(frame2)

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing _series_ representing
  /// individual columns of the frame.
  [<CompiledName("Columns")>]
  let cols (frame:Frame<'TRowKey, 'TColKey>) = frame.Columns

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame.
  [<CompiledName("Rows")>]
  let rows (frame:Frame<'TRowKey, 'TColKey>) = frame.Rows

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing _series_ representing
  /// individual columns of the frame. This is similar to `Columns`, but it
  /// skips columns that contain missing value in _any_ row.
  [<CompiledName("ColumnsDense")>]
  let colsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.ColumnsDense

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame. This is similar to `Rows`, but it
  /// skips rows that contain missing value in _any_ column.
  [<CompiledName("RowsDense")>]
  let rowsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.RowsDense

  /// Creates a new data frame that contains all data from the original
  /// data frame without the specified series (column).
  [<CompiledName("DropColumn")>]
  let dropCol column (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.DropSeries(column); f

  /// Creates a new data frame where the specified column is repalced
  /// with a new series. (If the series does not exist, only the new
  /// series is added.)
  [<CompiledName("ReplaceColumn")>]
  let replaceCol column series (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.ReplaceSeries(column, series); f

  /// Returns a specified series (column) from a data frame. This 
  /// function uses exact matching semantics. Use `lookupSeries` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetColumn")>]
  let getCol column (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column)

  /// Returns a specified row from a data frame. This 
  /// function uses exact matching semantics. Use `lookupRow` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetRow")>]
  let getRow row (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row)

  let getRowLevel (key) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows.GetByLevel(key)

  let getColLevel (key) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns.GetByLevel(key)

  /// Returns a specified series (column) from a data frame. If the data frame has 
  /// ordered column index, the lookup semantics can be used to get series
  /// with nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupColumn")>]
  let lookupCol column lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column, lookup)

  /// Returns a specified row from a data frame. If the data frame has 
  /// ordered row index, the lookup semantics can be used to get row with 
  /// nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupRow")>]
  let lookupRow row lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row, lookup)

  /// Returns a data frame that contains the same data as the argument, 
  /// but whose rows are ordered series. This allows using inexact lookup
  /// for rows (e.g. using `lookupRow`) or inexact left/right joins.
  [<CompiledName("OrderRows")>]
  let orderRows (frame:Frame<'TRowKey, 'TColKey>) = 
    let newRowIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.RowIndex, Vectors.Return 0)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

  /// Returns a data frame that contains the same data as the argument, 
  /// but whose columns are ordered series. This allows using inexact lookup
  /// for columns (e.g. using `lookupCol`) or inexact left/right joins.
  [<CompiledName("OrderColumns")>]
  let orderCols (frame:Frame<'TRowKey, 'TColKey>) = 
    let newColIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.ColumnIndex, Vectors.Return 0)
    let newData = frame.VectorBuilder.Build(rowCmd, [| frame.Data |])
    Frame<_, _>(frame.RowIndex, newColIndex, newData)

  /// Creates a new data frame that uses the specified column as an row index.
  [<CompiledName("WithRowIndex")>]
  let withRowIndex (column:column<'TNewRowKey>) (frame:Frame<'TRowKey, 'TColKey>) = 
    let columnVec = frame.GetSeries<'TNewRowIndex>(unbox column.Value)
    let lookup addr = columnVec.Vector.GetValue(addr)
    let newRowIndex, rowCmd = frame.IndexBuilder.WithIndex(frame.RowIndex, lookup, Vectors.Return 0)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

  /// Creates a new data frame that uses the specified list of keys as a new column index.
  [<CompiledName("WithColumnKeys")>]
  let withColumnKeys (keys:seq<'TNewColKey>) (frame:Frame<'TRowKey, 'TColKey>) = 
    let columns = seq { for v in frame.Columns.Values -> v :> ISeries<_> }
    Frame<_, _>(keys, columns)

  /// Join two frames using the specified kind of join. This function uses
  /// exact matching on keys. If you want to align nearest smaller or greater
  /// keys in left or outer join, use `joinAlign`. 
  [<CompiledName("Join")>]
  let join kind (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Join(frame2, kind)

  /// Join two frames using the specified kind of join and 
  /// the specified lookup semantics.
  [<CompiledName("JoinAlign")>]
  let joinAlign kind lookup (frame1:Frame<'TRowKey, 'TColKey>) frame2 = frame1.Join(frame2, kind, lookup)

  
  /// Fills all missing values in all columns & rows of the data frame with the
  /// specified value (this can only be used when the data is homogeneous)
  [<CompiledName("WithMissing")>]
  let withMissingVal defaultValue (frame:Frame<'TRowKey, 'TColKey>) =
    let data = frame.Data.Select (VectorHelpers.fillNA defaultValue)
    Frame<'TRowKey, 'TColKey>(frame.RowIndex, frame.ColumnIndex, data)

  // ----------------------------------------------------------------------------------------------
  // TBD
  // ----------------------------------------------------------------------------------------------

  let inline filterRows f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.filter f |> FrameUtils.fromRows

  let inline filterRowValues f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.filterValues f |> FrameUtils.fromRows

  let inline mapRows f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.map f |> FrameUtils.fromRows

  let inline mapRowValues f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.mapValues f |> FrameUtils.fromRows

  let inline mapRowKeys f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.mapKeys f |> FrameUtils.fromRows

  let inline filterCols f (frame:Frame<'TColumnKey, 'TColKey>) = 
    frame.Columns |> Series.filter f |> FrameUtils.fromColumns

  let inline filterColValues f (frame:Frame<'TColumnKey, 'TColKey>) = 
    frame.Columns |> Series.filterValues f |> FrameUtils.fromColumns

  let inline mapCols f (frame:Frame<'TColumnKey, 'TColKey>) = 
    frame.Columns |> Series.map f |> FrameUtils.fromColumns

  let inline mapColValues f (frame:Frame<'TColumnKey, 'TColKey>) = 
    frame.Columns |> Series.mapValues f |> FrameUtils.fromColumns

  let inline mapColKeys f (frame:Frame<'TColumnKey, 'TColKey>) = 
    frame.Columns |> Series.mapKeys f |> FrameUtils.fromColumns


  let transpose (frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> FrameUtils.fromRows



  let getCols (columns:seq<_>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns.[columns]

  let getRows (rows:seq<_>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows.[rows]

  let inline maxRowBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.maxBy (fun row -> row.GetAs<float>(column))

  let inline minRowBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.minBy (fun row -> row.GetAs<float>(column))

  // ----------------------------------------------------------------------------------------------
  // Additional functions for working with data frames
  // ----------------------------------------------------------------------------------------------

  let mean (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.mean)

  let sum (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sum)

  let sdv (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdv)

  let median (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.median)

  let stat op (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.stat op)


  let inline countValues (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<obj>() |> Series.map (fun _ -> Series.countValues)

  let countKeys (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowIndex.Keys |> Seq.length

  let inline diff offset (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.mapValues (fun s -> Series.diff offset (s.As<float>())) |> FrameUtils.fromColumns

  // ----------------------------------------------------------------------------------------------
  // Hierarchical aggregation
  // ----------------------------------------------------------------------------------------------

  let meanBy keySelector (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.meanBy keySelector) |> FrameUtils.fromColumns

  let sumBy keySelector (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sumBy keySelector) |> FrameUtils.fromColumns

  let sdvBy keySelector (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdvBy keySelector) |> FrameUtils.fromColumns

  let medianBy keySelector (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.medianBy keySelector) |> FrameUtils.fromColumns

  let statBy keySelector op (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.statBy keySelector op) |> FrameUtils.fromColumns

  let foldBy keySelector op (frame:Frame<'TRowKeys, 'TColKey>) = 
    frame.Columns |> Series.map (fun _ -> Series.foldBy keySelector op) |> FrameUtils.fromColumns
