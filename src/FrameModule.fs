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
    |> Series.map (fun k1 df -> df.Columns |> Series.mapKeys(fun k2 -> MultiKey(k1, k2)) |> Frame.ofColumns)
    |> Series.values |> Seq.reduce (fun df1 df2 -> df1.Append(df2))

  let collapseRows (series:Series<'K, Frame<'K1, 'K2>>) = 
    series 
    |> Series.map (fun k1 df -> df.Rows |> Series.mapKeys(fun k2 -> MultiKey(k1, k2)) |> Frame.ofRows)
    |> Series.values |> Seq.reduce (fun df1 df2 -> df1.Append(df2))

  let groupRowsInto column f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupInto 
      (fun _ v -> v.Get(column)) 
      (fun k g -> g |> Frame.ofRows |> f)

  let groupRowsUsing selector (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupInto selector (fun k g -> g |> Frame.ofRows) |> collapseRows

  let groupRowsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    groupRowsInto column id frame |> collapseRows

  let groupColsInto column f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.groupInto 
      (fun _ v -> v.Get(column)) 
      (fun k g -> g |> Frame.ofColumns |> f)

  let groupColsUsing selector (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns |> Series.groupInto selector (fun k g -> g |> Frame.ofColumns) |> collapseCols

  let groupColsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    groupColsInto column id frame |> collapseCols


  //let shiftRows offset (frame:Frame<'TRowKey, 'TColKey>) = 
  //  frame.Columns 
  //  |> Series.map (fun k col -> Series.shift offset col)
  //  |> Frame.ofColumns

  let takeLast count (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.takeLast count |> Frame.ofRows

  let dropMissing (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowsDense |> Series.dropMissing |> Frame.ofRows


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

  let getRowLevel (HL key) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows.GetByLevel(key) |> Frame.ofRows

  let getColLevel (HL key) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns.GetByLevel(key) |> Frame.ofColumns

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
  let orderRows (frame:Frame<'TRowKey, 'TColKey>) = FrameExtensions.OrderRows(frame)

  /// Returns a data frame that contains the same data as the argument, 
  /// but whose columns are ordered series. This allows using inexact lookup
  /// for columns (e.g. using `lookupCol`) or inexact left/right joins.
  [<CompiledName("OrderColumns")>]
  let orderCols (frame:Frame<'TRowKey, 'TColKey>) = FrameExtensions.OrderColumns(frame)

  /// Creates a new data frame that uses the specified column as an row index.
  [<CompiledName("WithRowIndex")>]
  let withRowIndex (column:column<'TNewRowKey>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.WithRowIndex<'TNewRowKey>(unbox<'TColKey> column.Value)

  /// Creates a new data frame that uses the specified list of keys as a new column index.
  [<CompiledName("WithColumnKeys")>]
  let withColumnKeys (keys:seq<'TNewColKey>) (frame:Frame<'TRowKey, 'TColKey>) = frame.WithColumnIndex(keys)

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
    frame.WithMissing(defaultValue)

  // ----------------------------------------------------------------------------------------------
  // TBD
  // ----------------------------------------------------------------------------------------------

  let inline filterRows f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.Where(frame, fun kvp -> f kvp.Key kvp.Value)

  let inline filterRowValues f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.Where(frame, fun kvp -> f kvp.Value)

  let inline mapRows f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.Select(frame, fun kvp -> f kvp.Key kvp.Value) 

  let inline mapRowValues f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.Select(frame, fun kvp -> f kvp.Value) 

  let inline mapRowKeys f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.SelectRowKeys(frame, fun kvp -> f kvp.Key) 

  let transpose (frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> Frame.ofRows

  let getCols (columns:seq<_>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Columns.[columns]

  let getRows (rows:seq<_>) (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows.[rows]

  let inline mapColumnKeys f (frame:Frame<'TRowKey, 'TColKey>) = 
    FrameExtensions.SelectColumnKeys(frame, fun kvp -> f kvp.Key) 


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
    frame.Columns |> Series.mapValues (fun s -> Series.diff offset (s.As<float>())) |> Frame.ofColumns

  // ----------------------------------------------------------------------------------------------
  // Hierarchical aggregation
  // ----------------------------------------------------------------------------------------------

  let meanLevel level (frame:Frame<MultiKey<'TRowKey1, 'TRowKey2>, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.meanLevel level)

  let sumLevel level (frame:Frame<MultiKey<'TRowKey1, 'TRowKey2>, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sumLevel level)

  let sdvLevel level (frame:Frame<MultiKey<'TRowKey1, 'TRowKey2>, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdvLevel level)

  let medianLevel level (frame:Frame<MultiKey<'TRowKey1, 'TRowKey2>, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.medianLevel level)

  let statLevel level op (frame:Frame<MultiKey<'TRowKey1, 'TRowKey2>, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.statLevel level op)
