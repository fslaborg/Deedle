namespace Deedle

/// Frame module comment
///
/// ## Index manipulation
/// More documentation here
///
/// ## Joining, zipping and appending
/// More info
///
/// ## Missing values
/// More documentation here
///
/// ## Projection and filtering
/// TBD
///
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Frame = 
  open System
  open Deedle.Internal

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  let countRows (frame:Frame<'R, 'C>) = frame.RowIndex.Mappings |> Seq.length

  /// Returns the total number of column keys in the specified frame. This returns
  /// the total length of columns, including keys for which there is no 
  /// data available.
  let countCols (frame:Frame<'R, 'C>) = frame.ColumnIndex.Mappings |> Seq.length

  // ----------------------------------------------------------------------------------------------
  // Grouping
  // ----------------------------------------------------------------------------------------------

  let collapseCols (series:Series<'K, Frame<'K1, 'K2>>) = 
    series 
    |> Series.map (fun k1 df -> df.Columns |> Series.mapKeys(fun k2 -> (k1, k2)) |> FrameUtils.fromColumns)
    |> Series.values |> Seq.reduce (fun df1 df2 -> df1.Append(df2))

  let collapseRows (series:Series<'K, Frame<'K1, 'K2>>) = FrameUtils.collapseRows series

  let groupRowsUsing selector (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.groupInto selector (fun k g -> g |> FrameUtils.fromRows) |> collapseRows
  let groupColsUsing selector (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.groupInto selector (fun k g -> g |> FrameUtils.fromColumns) |> collapseCols

  let groupRowsBy column (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.groupInto 
      (fun _ v -> v.GetAs<'K>(column)) 
      (fun k g -> g |> FrameUtils.fromRows)
    |> collapseRows

  let groupColsBy column (frame:Frame<'R, 'C>) = 
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


  //let shiftRows offset (frame:Frame<'R, 'C>) = 
  //  frame.Columns 
  //  |> Series.map (fun k col -> Series.shift offset col)
  //  |> Frame.ofColumns

  let takeLast count (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.takeLast count |> FrameUtils.fromRows



  let window size (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.windowInto size FrameUtils.fromRows

  let windowInto size f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.windowInto size (FrameUtils.fromRows >> f)

  // ----------------------------------------------------------------------------------------------
  // Wrappers that simply call member functions of the data frame
  // ----------------------------------------------------------------------------------------------

  /// Creates a new data frame that contains all data from 
  /// the original data frame, together with additional series.
  [<CompiledName("AddColumn")>]
  let addCol column (series:Series<_, _>) (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.AddSeries(column, series); f

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing _series_ representing
  /// individual columns of the frame.
  [<CompiledName("Columns")>]
  let cols (frame:Frame<'R, 'C>) = frame.Columns

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame.
  [<CompiledName("Rows")>]
  let rows (frame:Frame<'R, 'C>) = frame.Rows

  /// Creates a new data frame that contains all data from the original
  /// data frame without the specified series (column).
  [<CompiledName("DropColumn")>]
  let dropCol column (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.DropSeries(column); f

  /// Creates a new data frame where the specified column is repalced
  /// with a new series. (If the series does not exist, only the new
  /// series is added.)
  [<CompiledName("ReplaceColumn")>]
  let replaceCol column series (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.ReplaceSeries(column, series); f

  /// Returns a specified series (column) from a data frame. This 
  /// function uses exact matching semantics. Use `lookupSeries` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetColumn")>]
  let getCol column (frame:Frame<'R, 'C>) = frame.GetSeries(column)

  /// Returns a specified row from a data frame. This 
  /// function uses exact matching semantics. Use `lookupRow` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetRow")>]
  let getRow row (frame:Frame<'R, 'C>) = frame.GetRow(row)

  let getRowLevel (key) (frame:Frame<'R, 'C>) = 
    frame.Rows.GetByLevel(key)

  let getColLevel (key) (frame:Frame<'R, 'C>) = 
    frame.Columns.GetByLevel(key)

  /// Returns a specified series (column) from a data frame. If the data frame has 
  /// ordered column index, the lookup semantics can be used to get series
  /// with nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupColumn")>]
  let lookupCol column lookup (frame:Frame<'R, 'C>) = frame.GetSeries(column, lookup)

  /// Returns a specified row from a data frame. If the data frame has 
  /// ordered row index, the lookup semantics can be used to get row with 
  /// nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupRow")>]
  let lookupRow row lookup (frame:Frame<'R, 'C>) = frame.GetRow(row, lookup)


  let getCols (columns:seq<_>) (frame:Frame<'R, 'C>) = 
    frame.Columns.[columns]

  let getRows (rows:seq<_>) (frame:Frame<'R, 'C>) = 
    frame.Rows.[rows]

  // ----------------------------------------------------------------------------------------------
  // Index operations
  // ----------------------------------------------------------------------------------------------

  /// Align the existing data to a specified collection of row keys. Values in the data frame
  /// that do not match any new key are dropped, new keys (that were not in the original data 
  /// frame) are assigned missing values.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame that is to be realigned.
  ///  - `keys` - A sequence of new row keys. The keys must have the same type as the original
  ///    frame keys (because the rows are realigned).
  ///
  /// [category:Index manipulation]
  [<CompiledName("RealignRows")>]
  let realignRows keys (frame:Frame<'R, 'C>) = 
    // Create empty frame with the required keys & left join all series
    let nf = Frame<_, _>(frame.IndexBuilder.Create(keys, None), frame.IndexBuilder.Create([], None), frame.VectorBuilder.Create [||])
    frame.Columns |> Series.observations |> Seq.iter nf.AddSeries
    nf

  /// Replace the row index of the frame with ordinarilly generated integers starting from zero.
  /// The rows of the frame are assigned index according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsOrdinally")>]
  let indexRowsOrdinally (frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame.Columns |> Series.mapValues Series.indexOrdinally |> FrameUtils.fromColumns

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. The generic type parameter is specifies the type of the values in the required 
  /// index column (and usually needs to be specified using a type annotation).
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRows")>]
  let indexRows column (frame:Frame<'R1, 'C>) : Frame<'R2, _> = 
    frame.IndexRows<'R2>(column)

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `obj`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsByObject")>]
  let indexRowsObj column (frame:Frame<'R1, 'C>) : Frame<obj, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `int`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsByInt")>]
  let indexRowsInt column (frame:Frame<'R1, 'C>) : Frame<int, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `DateTime`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsByDateTime")>]
  let indexRowsDate column (frame:Frame<'R1, 'C>) : Frame<DateTime, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `DateTimeOffset`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsByDateTimeOffset")>]
  let indexRowsDateOffs column (frame:Frame<'R1, 'C>) : Frame<DateTimeOffset, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `string`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsByString")>]
  let indexRowsString column (frame:Frame<'R1, 'C>) : Frame<string, _> = indexRows column frame

  /// Replace the column index of the frame with the provided sequence of column keys.
  /// The columns of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current column index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose column index are to be replaced.
  ///  - `keys` - A collection of new column keys.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexColumnsWith")>]
  let indexColsWith (keys:seq<'C2>) (frame:Frame<'R, 'C1>) = 
    if Seq.length frame.ColumnKeys <> Seq.length keys then invalidArg "keys" "New keys do not match current column index length"
    Frame<_, _>(frame.RowIndex, Index.ofKeys keys, frame.Data)

  /// Replace the row index of the frame with the provided sequence of row keys.
  /// The rows of the frame are assigned keys according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///  - `keys` - A collection of new row keys.
  ///
  /// [category:Index manipulation]
  [<CompiledName("IndexRowsWith")>]
  let indexRowsWith (keys:seq<'R2>) (frame:Frame<'R1, 'C>) = 
    let newRowIndex = frame.IndexBuilder.Create(keys, None)
    let getRange = VectorHelpers.getVectorRange frame.VectorBuilder frame.RowIndex.Range
    let newData = frame.Data.Select(getRange)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

  /// Returns a transposed data frame. The rows of the original data frame are used as the
  /// columns of the new one (and vice versa). Use this operation if you have a data frame
  /// and you mostly need to access its rows as a series (because accessing columns as a 
  /// series is more efficient).
  /// 
  /// ## Parameters
  ///  - `frame` - Source data frame to be transposed.
  /// 
  /// [category:Index manipulation]
  [<CompiledName("Transpose")>]
  let transpose (frame:Frame<'R, 'TColumnKey>) = 
    frame.Columns |> FrameUtils.fromRows

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Index manipulation]
  [<CompiledName("OrderRows")>]
  let orderRows (frame:Frame<'R, 'C>) = 
    let newRowIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.RowIndex, Vectors.Return 0)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData)

  /// Returns a data frame that contains the same data as the input, 
  /// but whose columns are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame to be ordered.
  /// 
  /// [category:Index manipulation]
  [<CompiledName("OrderColumns")>]
  let orderCols (frame:Frame<'R, 'C>) = 
    let newColIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.ColumnIndex, Vectors.Return 0)
    let newData = frame.VectorBuilder.Build(rowCmd, [| frame.Data |])
    Frame<_, _>(frame.RowIndex, newColIndex, newData)

  // ----------------------------------------------------------------------------------------------
  // Projection and filtering
  // ----------------------------------------------------------------------------------------------

  /// Returns a new data frame containing only the rows of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with the row key and object series that represents the row data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the predicate
  ///
  /// [category:Projection and filtering]
  [<CompiledName("WhereRows")>]
  let inline filterRows f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.filter f |> FrameUtils.fromRows

  /// Returns a new data frame containing only the rows of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with an object series that represents the row data (use `filterRows`
  /// if you need to access the row key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the predicate
  ///
  /// [category:Projection and filtering]
  [<CompiledName("WhereRowValues")>]
  let inline filterRowValues f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.filterValues f |> FrameUtils.fromRows

  /// Builds a new data frame whose rows are the results of applying the specified
  /// function on the rows of the input data frame. The function is called
  /// with the row key and object series that represents the row data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the row mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectRows")>]
  let inline mapRows f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.map f 

  /// Builds a new data frame whose rows are the results of applying the specified
  /// function on the rows of the input data frame. The function is called
  /// with an object series that represents the row data (use `mapRows`
  /// if you need to access the row key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the row mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectRowValues")>]
  let inline mapRowValues f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.mapValues f 

  /// Builds a new data frame whose row keys are the results of applying the
  /// specified function on the row keys of the original data frame.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the row key mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectRowKeys")>]
  let inline mapRowKeys (f:'R1 -> 'R2) (frame:Frame<_, 'C>) = 
    let newRowIndex = frame.IndexBuilder.Create(frame.RowIndex.Keys |> Seq.map f, None)
    Frame(newRowIndex, frame.ColumnIndex, frame.Data)


  /// Returns a new data frame containing only the columns of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with the column key and object series that represents the column data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the predicate
  ///
  /// [category:Projection and filtering]
  [<CompiledName("WhereColumns")>]
  let inline filterCols f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.filter f |> FrameUtils.fromColumns

  /// Returns a new data frame containing only the columns of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with an object series that represents the column data (use `filterCols`
  /// if you need to access the column key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the predicate
  ///
  /// [category:Projection and filtering]
  [<CompiledName("WhereColumnValues")>]
  let inline filterColValues f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.filterValues f |> FrameUtils.fromColumns

  /// Builds a new data frame whose columns are the results of applying the specified
  /// function on the columns of the input data frame. The function is called
  /// with the column key and object series that represents the column data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the column mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectColumns")>]
  let inline mapCols f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.map f |> FrameUtils.fromColumns

  /// Builds a new data frame whose columns are the results of applying the specified
  /// function on the columns of the input data frame. The function is called
  /// with an object series that represents the column data (use `mapCols`
  /// if you need to access the column key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the column mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectColumnValues")>]
  let inline mapColValues f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.mapValues f |> FrameUtils.fromColumns

  /// Builds a new data frame whose column keys are the results of applying the
  /// specified function on the column keys of the original data frame.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the column key mapping
  ///
  /// [category:Projection and filtering]
  [<CompiledName("SelectColumnKeys")>]
  let inline mapColKeys f (frame:Frame<'R, 'C>) = 
    let newColIndex = frame.IndexBuilder.Create(frame.ColumnIndex.Keys |> Seq.map f, None)
    Frame(frame.RowIndex, newColIndex, frame.Data)

  // ----------------------------------------------------------------------------------------------
  // Additional functions for working with data frames
  // ----------------------------------------------------------------------------------------------

  let mean (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.mean)

  let sum (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sum)

  let sdv (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdv)

  let median (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.median)

  let apply op (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.apply op)

  let reduce (op:'T -> 'T -> 'T) (frame:Frame<'R, 'C>) = 
    frame.GetColumns<'T>() |> Series.map (fun _ -> Series.reduce op) 

  let inline countValues (frame:Frame<'R, 'C>) = 
    frame.GetColumns<obj>() |> Series.map (fun _ -> Series.countValues)

  let countKeys (frame:Frame<'R, 'C>) = 
    frame.RowIndex.Keys |> Seq.length

  let inline maxRowBy column (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.maxBy (fun row -> row.GetAs<float>(column))

  let inline minRowBy column (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.minBy (fun row -> row.GetAs<float>(column))


  // ----------------------------------------------------------------------------------------------
  // Hierarchical aggregation
  // ----------------------------------------------------------------------------------------------

  let meanLevel keySelector (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.meanLevel keySelector) |> FrameUtils.fromColumns

  let sumLevel keySelector (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sumLevel keySelector) |> FrameUtils.fromColumns

  let countLevel keySelector (frame:Frame<'R, 'C>) = 
    frame.GetColumns<obj>() |> Series.map (fun _ -> Series.countLevel keySelector) |> FrameUtils.fromColumns

  let sdvLevel keySelector (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdvLevel keySelector) |> FrameUtils.fromColumns

  let medianLevel keySelector (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.medianLevel keySelector) |> FrameUtils.fromColumns

  let applyLevel keySelector op (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.applyLevel keySelector op) |> FrameUtils.fromColumns

  let reduceLevel keySelector (op:'T -> 'T -> 'T) (frame:Frame<'R, 'C>) = 
    frame.GetColumns<'T>() |> Series.map (fun _ -> Series.reduceLevel keySelector op) |> FrameUtils.fromColumns


  // other stuff

  let shift offset (frame:Frame<'R, 'C>) = 
    frame |> mapColValues (Series.shift offset)

  let diff offset (frame:Frame<'R, 'C>) = 
    frame.SeriesApply<float>(false, fun s -> Series.diff offset s :> ISeries<_>)

  // ----------------------------------------------------------------------------------------------
  // Missing values
  // ----------------------------------------------------------------------------------------------

  /// Fill missing values of a given type in the frame with a constant value.
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the provided filling value. The operation does not attempt to 
  /// convert between numeric values (so a series containing `float` will not be
  /// converted to a series of `int`).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `value` - A constant value that is used to fill all missing values
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingWith")>]
  let fillMissingWith (value:'T) (frame:Frame<'R, 'C>) =
    frame.SeriesApply(true, fun (s:Series<_, 'T>) -> Series.fillMissingWith value s :> ISeries<_>)

  /// Fill missing values in the data frame with the nearest available value
  /// (using the specified direction). Note that the frame may still contain
  /// missing values after call to this function (e.g. if the first value is not available
  /// and we attempt to fill series with previous values). This operation can only be
  /// used on ordered frames.
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissing")>]
  let fillMissing direction (frame:Frame<'R, 'C>) =
    frame.Columns |> Series.mapValues (fun s -> Series.fillMissing direction s) |> FrameUtils.fromColumns

  /// Fill missing values in the frame using the specified function. The specified
  /// function is called with all series and keys for which the frame does not 
  /// contain value and the result of the call is used in place of the missing value.
  ///
  /// The operation is only applied to columns (series) that contain values of the
  /// same type as the return type of the provided filling function. The operation 
  /// does not attempt to convert between numeric values (so a series containing 
  /// `float` will not be converted to a series of `int`).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filled
  ///  - `f` - A function that takes a series `Series<R, T>` together with a key `K` 
  ///    in the series and generates a value to be used in a place where the original 
  ///    series contains a missing value.
  ///
  /// [category:Missing values]
  [<CompiledName("FillMissingUsing")>]
  let fillMissingUsing (f:Series<'R, 'T> -> 'R -> 'T) (frame:Frame<'R, 'C>) =
    frame.SeriesApply(false, fun (s:Series<_, 'T>) -> Series.fillMissingUsing (f s) s :> ISeries<_>)

  /// Creates a new data frame that contains only those rows of the original 
  /// data frame that are _dense_, meaning that they have a value for each column.
  /// The resulting data frame has the same number of columns, but may have 
  /// fewer rows (or no rows at all).
  /// 
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filtered
  ///
  /// [category:Missing values]
  [<CompiledName("DropSparseRows")>]
  let dropSparseRows (frame:Frame<'R, 'C>) = 
    frame.RowsDense |> Series.dropMissing |> FrameUtils.fromRows

  /// Creates a new data frame that contains only those columns of the original 
  /// data frame that are _dense_, meaning that they have a value for each row.
  /// The resulting data frame has the same number of rows, but may have 
  /// fewer columns (or no columns at all).
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame that is to be filtered
  ///
  /// [category:Missing values]
  [<CompiledName("DropSparseColumns")>]
  let dropSparseCols (frame:Frame<'R, 'C>) = 
    frame.ColumnsDense |> Series.dropMissing |> FrameUtils.fromColumns

  /// Returns the columns of the data frame that do not have any missing values.
  /// The operation returns a series (indexed by the column keys of the source frame) 
  /// containing _series_ representing individual columns of the frame. This is similar 
  /// to `Columns`, but it skips columns that contain missing value in _any_ row.
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame containing columns to be filtered
  ///
  /// [category:Missing values]
  [<CompiledName("ColumnsDense")>]
  let colsDense (frame:Frame<'R, 'C>) = frame.ColumnsDense

  /// Returns the rows of the data frame that do not have any missing values. 
  /// The operation returns a series (indexed by the row keys of the source frame) 
  /// containing _series_ representing individual row of the frame. This is similar 
  /// to `Rows`, but it skips rows that contain missing value in _any_ column.
  ///
  /// ## Parameters
  ///  - `frame` - An input data frame containing rows to be filtered
  ///
  /// [category:Missing values]
  [<CompiledName("RowsDense")>]
  let rowsDense (frame:Frame<'R, 'C>) = frame.RowsDense


  // ----------------------------------------------------------------------------------------------
  // Joining, zipping and appending
  // ----------------------------------------------------------------------------------------------

  /// Join two data frames. The columns of the joined frames must not overlap and their
  /// rows are aligned and transformed according to the specified join kind.
  /// For more alignment options on ordered frames, see `joinAlign`.
  ///
  /// ## Parameters
  ///  - `frame1` - First data frame (left) to be used in the joining
  ///  - `frame2` - Other frame (right) to be joined with `frame1`
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///
  /// [category:Joining, zipping and appending]
  [<CompiledName("Join")>]
  let join kind (frame1:Frame<'R, 'C>) frame2 = frame1.Join(frame2, kind)

  /// Join two data frames. The columns of the joined frames must not overlap and their
  /// rows are aligned and transformed according to the specified join kind.
  /// When the index of both frames is ordered, it is possible to specify `lookup` 
  /// in order to align indices from other frame to the indices of the main frame
  /// (typically, to find the nearest key with available value for a key).
  ///
  /// ## Parameters
  ///  - `frame1` - First data frame (left) to be used in the joining
  ///  - `frame2` - Other frame (right) to be joined with `frame1`
  ///  - `kind` - Specifies the joining behavior on row indices. Use `JoinKind.Outer` and 
  ///    `JoinKind.Inner` to get the union and intersection of the row keys, respectively.
  ///    Use `JoinKind.Left` and `JoinKind.Right` to use the current key of the left/right
  ///    data frame.
  ///  - `lookup` - When `kind` is `Left` or `Right` and the two frames have ordered row index,
  ///    this parameter can be used to specify how to find value for a key when there is no
  ///    exactly matching key or when there are missing values.
  ///
  /// [category:Joining, zipping and appending]
  [<CompiledName("JoinAlign")>]
  let joinAlign kind lookup (frame1:Frame<'R, 'C>) frame2 = frame1.Join(frame2, kind, lookup)
  
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
  [<CompiledName("Append")>]
  let append (frame1:Frame<'R, 'C>) frame2 = frame1.Append(frame2)

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
  ///  - `frame1` - First frame to be aligned and zipped with the other instance
  ///  - `frame2` - Other frame to be aligned and zipped with the first  instance
  ///  - `columnKind` - Specifies how to align columns (inner, outer, left or right join)
  ///  - `rowKind` - Specifies how to align rows (inner, outer, left or right join)
  ///  - `lookup` - Specifies how to find matching value for a row (when using left or right join on rows)
  ///  - `op` - A function that is applied to aligned values. The `Zip` operation is generic
  ///    in the type of this function and the type of function is used to determine which 
  ///    values in the frames are zipped and which are left unchanged.
  ///
  /// [category:Joining, zipping and appending]
  [<CompiledName("ZipAlignInto")>]
  let zipAlign columnKind rowKind lookup (op:'V1->'V2->'V) (frame1:Frame<'R, 'C>) (frame2:Frame<'R, 'C>) : Frame<'R, 'C> =
    frame1.Zip(frame2, columnKind, rowKind, lookup, fun a b -> op a b)

  /// Aligns two data frames using both column index and row index and apply the specified operation
  /// on values of a specified type that are available in both data frames. This overload uses
  /// `JoinKind.Outer` for both columns and rows.
  ///
  /// Once aligned, the call `df1.Zip<T>(df2, f)` applies the specifed function `f` on all `T` values
  /// that are available in corresponding locations in both frames. For values of other types, the 
  /// value from `df1` is returned.
  ///
  /// ## Parameters
  ///  - `frame1` - First frame to be aligned and zipped with the other instance
  ///  - `frame2` - Other frame to be aligned and zipped with the first  instance
  ///  - `columnKind` - Specifies how to align columns (inner, outer, left or right join)
  ///  - `rowKind` - Specifies how to align rows (inner, outer, left or right join)
  ///  - `lookup` - Specifies how to find matching value for a row (when using left or right join on rows)
  ///  - `op` - A function that is applied to aligned values. The `Zip` operation is generic
  ///    in the type of this function and the type of function is used to determine which 
  ///    values in the frames are zipped and which are left unchanged.
  ///
  /// [category:Joining, zipping and appending]
  [<CompiledName("ZipInto")>]
  let zip (op:'V1->'V2->'V) (frame1:Frame<'R, 'C>) (frame2:Frame<'R, 'C>) : Frame<'R, 'C> =
    zipAlign JoinKind.Inner JoinKind.Inner Lookup.Exact op frame1 frame2

  // ----------------------------------------------------------------------------------------------
  // Hierarchical indexing
  // ----------------------------------------------------------------------------------------------

  let flatten (level:'R -> 'K) (op:_ -> 'V) (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.map (fun _ -> Series.flattenLevel level op)

  let flattenRows (level:'R -> 'K) op (frame:Frame<'R, 'C>) : Series<'K, 'V> = 
    frame.Rows |> Series.groupInto (fun k _ -> level k) (fun _ -> FrameUtils.fromRows >> op)

  let unstackBy keySelector (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.groupInto (fun k _ -> keySelector k) (fun nk s -> FrameUtils.fromRows s)

  let unstack (frame:Frame<'R1 * 'R2, 'C>) = 
    unstackBy fst frame
    |> Series.mapValues (mapRowKeys snd)

  let stack (series:Series<'R1, Frame<'R2, 'C>>) =
    series
    |> Series.map (fun k1 s -> s.Rows |> Series.mapKeys (fun k2 -> k1, k2) |> FrameUtils.fromRows)
    |> Series.values
    |> Seq.reduce append

