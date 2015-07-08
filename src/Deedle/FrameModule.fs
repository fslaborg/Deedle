namespace Deedle

/// The `Frame` module provides an F#-friendly API for working with data frames. 
/// The module follows the usual desing for collection-processing in F#, so the 
/// functions work well with the pipelining operator (`|>`). For example, given
/// a frame with two columns representing prices, we can use `Frame.diff` and 
/// numerical operators to calculate daily returns like this:
/// 
///     let df = frame [ "MSFT" => prices1; "AAPL" => prices2 ]
///     let past = df |> Frame.diff 1
///     let rets = past / df * 100.0
///     rets |> Stats.mean
///
/// Note that the `Stats.mean` operation is overloaded and works both on series 
/// (returning a number) and on frames (returning a series).
/// 
/// The functions in this module are designed to be used from F#. For a C#-friendly
/// API, see the `FrameExtensions` type. For working with individual series, see the
/// `Series` module. The functions in the `Frame` module are grouped in a number of 
/// categories and documented below.
///
/// Accessing frame data and lookup
/// -------------------------------
///
/// Functions in this category provide access to the values in the fame. You can 
/// also add and remove columns from a frame (which both return a new value).
///
/// - `addCol`, `replaceCol` and `dropCol` can be used to create a new data frame
///   with a new column, by replacing an existing column with a new one, or by dropping
///   an existing column
///
/// - `cols` and `rows` return the columns or rows of a frame as a series containing
///   objects; `getCols` and `getRows` return a generic series and cast the values to
///   the type inferred from the context (columns or rows of incompatible types are skipped);
///   `getNumericCols` returns columns of a type convertible to `float` for convenience.
///
/// - You can get a specific row or column using `get[Col|Row]` or `lookup[Col|Row]` functions.
///   The `lookup` variant lets you specify lookup behavior for key matching (e.g. find the
///   nearest smaller key than the specified value). There are also `[try]get` and `[try]Lookup`
///   functions that return optional values and functions returning entire observations
///   (key together with the series).
///
/// - `sliceCols` and `sliceRows` return a sub-frame containing only the specified columns
///   or rows. Finally, `toArray2D` returns the frame data as a 2D array.
///
/// Grouping, windowing and chunking
/// --------------------------------
///
/// The basic grouping functions in this category can be used to group the rows of a
/// data frame by a specified projection or column to create a frame with hierarchical
/// index such as `Frame<'K1 * 'K2, 'C>`. The functions always aggregate rows, so if you
/// want to group columns, you need to use `Frame.transpose` first.
///
/// The function `groupRowsBy` groups rows by the value of a specified column. Use
/// `groupRowsBy[Int|Float|String...]` if you want to specify the type of the column in
/// an easier way than using type inference; `groupRowsUsing` groups rows using the 
/// specified _projection function_ and `groupRowsByIndex` projects the grouping key just
/// from the row index.
///
/// More advanced functions include: `aggregateRowsBy` which groups the rows by a 
/// specified sequence of columns and aggregates each group into a single value; 
/// `pivotTable` implements the pivoting operation [as documented in the 
/// tutorials](../frame.html#pivot).
///
/// The `stack` and `unstack` functions turn the data frame into a single data frame
/// containing columns `Row`, `Column` and `Value` containing the data of the original
/// frame; `unstack` can be used to turn this representation back into an original frame.
///
/// A simple windowing functions that are exposed for an entire frame operations are
/// `window` and `windowInto`. For more complex windowing operations, you currently have
/// to use `mapRows` or `mapCols` and apply windowing on individual series.
///
/// Sorting and index manipulation
/// ------------------------------
///
/// A frame is indexed by row keys and column keys. Both of these indices can be sorted
/// (by the keys). A frame that is sorted allows a number of additional operations (such 
/// as lookup using the `Lookp.ExactOrSmaller` lookup behavior). The functions in this 
/// category provide ways for manipulating the indices. It is expected that most operations
/// are done on rows and so more functions are available in a row-wise way. A frame can
/// alwyas be transposed using `Frame.transpose`.
///
/// ### Index operations
/// 
/// The existing row/column keys can be replaced by a sequence of new keys using the 
/// `indexColsWith` and `indexRowsWith` functions. Row keys can also be replaced by 
/// ordinal numbers using `indexRowsOrdinally`.
///
/// The function `indexRows` uses the specified column of the original frame as the 
/// index. It removes the column from the resulting frame (to avoid this, use overloaded
/// `IndexRows` method). This function infers the type of row keys from the context, so it 
/// is usually more convenient to use `indexRows[Date|String|Int|...]` functions. Finally, 
/// if you want to calculate the index value based on multiple columns of the row, you
/// can use `indexRowsUsing`.
///
/// ### Sorting frame rows
/// 
/// Frame rows can be sorted according to the value of a specified column using the
/// `sortRows` function; `sortRowsBy` takes a projection function which lets you 
/// transform the value of a column (e.g. to project a part of the value). 
///
/// The functions `sortRowsByKey` and `sortColsByKey` sort the rows or columns 
/// using the default ordering on the key values. The result is a frame with ordered
/// index.
///
/// ### Expanding columns
///
/// When the frame contains a series with complex .NET objects such as F# records or 
/// C# classes, it can be useful to "expand" the column. This operation looks at the 
/// type of the objects, gets all properties of the objects (recursively) and 
/// generates multiple series representing the properties as columns.
///
/// The function `expandCols` expands the specified columns while `expandAllCols`
/// applies the expansion to all columns of the data frame.
///
/// Frame transformations
/// ---------------------
///
/// Functions in this category perform standard transformations on data frames including
/// projections, filtering, taking some sub-frame of the frame, aggregating values
/// using scanning and so on.
///
/// Projection and filtering functions such as `[map|filter][Cols|Rows]` call the 
/// specified function with the column or row key and an `ObjectSeries<'K>` representing
/// the column or row. You can use functions ending with `Values` (such as `mapRowValues`)
/// when you do not require the row key, but only the row series; `mapRowKeys` and 
/// `mapColKeys` can be used to transform the keys.
///
/// You can use `reduceValues` to apply a custom reduction to values of columns. Other
/// aggregations are available in the `Stats` module. You can also get a row with the 
/// greaterst or smallest value of a given column using `[min|max]RowBy`.
///
/// The functions `take[Last]` and `skip[Last]` can be used to take a sub-frame of the
/// original source frame by skipping a specified number of rows. Note that this 
/// does not require an ordered frame and it ignores the index - for index-based lookup
/// use slicing, such as `df.Rows.[lo .. hi]`, instead.
///
/// Finally the `shift` function can be used to obtain a frame with values shifted by 
/// the specified offset. This can be used e.g. to get previous value for each key using
/// `Frame.shift 1 df`. The `diff` function calculates difference from previous value using
/// `df - (Frame.shift offs df)`.
///
/// Processing frames with exceptions
/// ---------------------------------
/// 
/// The functions in this group can be used to write computations over frames that may fail.
/// They use the type `tryval<'T>` which is defined as a discriminated union:
///
///     type tryval<'T> = 
///       | Success of 'T
///       | Error of exn
///
/// Using `tryval<'T>` as a value in a data frame is not generally recommended, because
/// the type of values cannot be tracked in the type. For this reason, it is better to use
/// `tryval<'T>` with individual series. However, `tryValues` and `fillErrorsWith` functions
/// can be used to get values, or fill failed values inside an entire data frame.
///
/// The `tryMapRows` function is more useful. It can be used to write a transformation
/// that applies a computation (which may fail) to each row of a data frame. The resulting
/// series is of type `Series<'R, tryval<'T>>` and can be processed using the `Series` module
/// functions.
///
/// Missing values
/// --------------
///
/// This group of functions provides a way of working with missing values in a data frame.
/// The category provides the following functions that can be used to fill missing values:
///
///  * `fillMissingWith` fills missing values with a specified constant
///  * `fillMissingUsing` calls a specified function for every missing value
///  * `fillMissing` and variants propagates values from previous/later keys
///
/// We use the terms _sparse_ and _dense_ to denote series that contain some missing values
/// or do not contain any missing values, respectively. The functions `denseCols` and 
/// `denseRows` return a series that contains only dense columns or rows and all sparse
/// rows or columns are replaced with a missing value. The `dropSparseCols` and `dropSparseRows`
/// functions drop these missing values and return a frame with no missing values.
///
/// Joining, merging and zipping
/// ----------------------------
///
/// The simplest way to join two frames is to use the `join` operation which can be used to 
/// perform left, right, outer or inner join of two frames. When the row keys of the frames do
/// not match exactly, you can use `joinAlign` which takes an additional parameter that specifies
/// how to find matching key in left/right join (e.g. by taking the nearest smaller available key).
///
/// Frames that do not contian overlapping values can be combined using `merge` (when combining
/// just two frames) or using `mergeAll` (for larger number of frames). Tha latter is optimized
/// to work well for a large number of data frames.
///
/// Finally, frames with overlapping values can be combined using `zip`. It takes a function
/// that is used to combine the overlapping values. A `zipAlign` function provides a variant
/// with more flexible row key matching (as in `joinAlign`)
///
/// Hierarchical index operations
/// -----------------------------
///
/// A data frame has a hierarchical row index if the row index is formed by a tuple, such as
/// `Frame<'R1 * 'R2, 'C>`. Frames of this kind are returned, for example, by the grouping 
/// functions such as `Frame.groupRowsBy`. The functions in this category provide ways for 
/// working with data frames that have hierarchical row keys.
///
/// The functions `applyLevel` and `reduceLevel` can be used to reduce values according to 
/// one of the levels. The `applyLevel` function takes a reduction of type `Series<'K, 'T> -> 'T`
/// while `reduceLevel` reduces individual values using a function of type `'T -> 'T -> 'T`.
///
/// The functions `nest` and `unnest` can be used to convert between frames with 
/// hierarchical indices (`Frame<'K1 * 'K2, 'C>`) and series of frames that represent 
/// individual groups (`Series<'K1, Frame<'K2, 'C>>`). The `nestBy` function can be 
/// used to perform group by operation and return the result as a series of frems.
///
/// [category:Frame and series operations]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Frame = 
  open System
  open Deedle.Internal
  open Deedle.VectorHelpers
  open Deedle.Vectors
  open Deedle.Addressing

  // ----------------------------------------------------------------------------------------------
  // Accessing frame data and lookup
  // ----------------------------------------------------------------------------------------------

  /// Returns the total number of row keys in the specified frame. This returns
  /// the total length of the row series, including keys for which there is no 
  /// value available.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("CountRows")>]
  let countRows (frame:Frame<'R, 'C>) = frame.RowIndex.KeyCount |> int

  /// Returns the total number of column keys in the specified frame. This returns
  /// the total length of columns, including keys for which there is no 
  /// data available.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("CountColumns")>]
  let countCols (frame:Frame<'R, 'C>) = frame.ColumnIndex.KeyCount |> int

  /// Returns a series with the total number of values in each column. This counts
  /// the number of actual values, excluding the missing values or not available 
  /// values (such as `nan`, `null`, etc.)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("CountValues")>]
  let inline countValues (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.map (fun _ -> Series.countValues)

  /// Returns the columns of the data frame as a series (indexed by 
  /// the column keys of the source frame) containing untyped series representing
  /// individual columns of the frame.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("Columns")>]
  let cols (frame:Frame<'R, 'C>) = frame.Columns
  
  /// Returns a specified column from a data frame. This function uses exact matching 
  /// semantics on the key. Use `lookupCol` if you want to use inexact 
  /// matching (e.g. on dates)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("GetColumn")>]
  let getCol column (frame:Frame<'R, 'C>) : Series<'R, 'V> = 
    frame.GetColumn(column)

  /// Returns a series of columns of the data frame indexed by the column keys, 
  /// which contains those series whose values are convertible to `'T`, and with 
  /// missing values where the conversion fails.
  ///
  /// If you want to get numeric columns, you can use a simpler `numericCols` function
  /// instead. Note that this function typically requires a type annotation. This can
  /// be specified in various ways, for example by annotating the result value:
  ///
  ///     let (res:Series<_, Series<_, string>>) = frame |> getCols
  ///
  /// Here, the annotation on the values of the nested series specifies that we want
  /// to get columns containing `string` values.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("GetColumns")>]
  let getCols (frame:Frame<'R,'C>) : Series<'C,Series<'R,'T>> =
    frame.Columns 
    |> Series.map(fun _ v -> v.TryAs<'T>() |> OptionalValue.asOption) 
    |> Series.flatten

  /// Returns a series of columns of the data frame indexed by the column keys, 
  /// which contains those series whose values are convertible to float, and with 
  /// missing values where the conversion fails.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("GetNumericColumns")>]
  let getNumericCols (frame:Frame<'R,'C>) : Series<'C,Series<'R,float>> =
    frame |> getCols

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing untyped series representing
  /// individual row of the frame.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("Rows")>]
  let rows (frame:Frame<'R, 'C>) = frame.Rows

  /// Returns a specified row from a data frame. This function uses exact matching 
  /// semantics on the key. Use `lookupRow` if you want to use inexact matching 
  /// (e.g. on dates)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("GetRow")>]
  let getRow row (frame:Frame<'R, 'C>) = frame.GetRow(row)

  /// Returns a series of rows of the data frame indexed by the row keys, 
  /// which contains those rows whose values are convertible to 'T, and with 
  /// missing values where the conversion fails.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("GetRows")>]
  let getRows (frame:Frame<'R,'C>) : Series<'R,Series<'C,'T>> =
    frame.Rows 
    |> Series.map(fun _ v -> v.TryAs<'T>() |> OptionalValue.asOption) 
    |> Series.flatten

  /// Returns a specified series (column) from a data frame. If the data frame has 
  /// ordered column index, the lookup semantics can be used to get series
  /// with nearest greater/smaller key. For exact semantics, you can use `getCol`.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("LookupColumn")>]
  let lookupCol column lookup (frame:Frame<'R, 'C>) : Series<'R, 'V> = frame.GetColumn(column, lookup)

  /// Returns a specified series (column) from a data frame, or missing value if 
  /// column doesn't exist.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("TryLookupColumn")>]
  let tryLookupCol column lookup (frame:Frame<'R, 'C>) : option<Series<'R, 'V>> = 
    frame.TryGetColumn(column, lookup) |> OptionalValue.asOption

  /// Returns a specified key and series (column) from a data frame, or missing value if 
  /// doesn't exist.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("TryLookupColObservation")>]
  let tryLookupColObservation column lookup (frame:Frame<'R, 'C>) = 
    frame.TryGetColumnObservation(column, lookup) 
    |> OptionalValue.asOption 
    |> Option.map (fun kvp -> kvp.Key, kvp.Value)

  /// Returns a specified row from a data frame. If the data frame has 
  /// ordered row index, the lookup semantics can be used to get row with 
  /// nearest greater/smaller key. For exact semantics, you can use `getRow`.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("LookupRow")>]
  let lookupRow row lookup (frame:Frame<'R, 'C>) = frame.GetRow(row, lookup)

  /// Returns a specified series (row) from a data frame, or missing value if 
  /// row doesn't exit.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("TryLookupRow")>]
  let tryLookupRow row lookup (frame:Frame<'R, 'C>) = 
    frame.TryGetRow(row, lookup) |> OptionalValue.asOption

  /// Returns a specified series (row) and key from a data frame, or missing value if 
  /// row doesn't exit.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("TryLookupRowObservation")>]
  let tryLookupRowObservation row lookup (frame:Frame<'R, 'C>) = 
    frame.TryGetRowObservation(row, lookup) |> OptionalValue.asOption |> Option.map (fun kvp -> kvp.Key, kvp.Value)

  /// Returns a frame consisting of the specified columns from the original
  /// data frame. The function uses exact key matching semantics.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("SliceCols")>]
  let sliceCols (columns:seq<_>) (frame:Frame<'R, 'C>) = 
    frame.Columns.[columns]

  /// Returns a frame consisting of the specified rows from the original
  /// data frame. The function uses exact key matching semantics.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("SliceRows")>]
  let sliceRows (rows:seq<_>) (frame:Frame<'R, 'C>) = 
    frame.Rows.[rows]

  /// Creates a new data frame that contains all data from 
  /// the original data frame, together with an additional series.
  /// The operation uses left join and aligns new series to the 
  /// existing frame keys.
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the newly added column
  ///  - `series` - A data series to be added (the row key type has to match)
  ///  - `frame` - Source data frame (which is not mutated by the operation)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("AddColumn")>]
  let addCol column (series:Series<_, 'V>) (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.AddColumn(column, series); f

  /// Creates a new data frame that contains all data from the original
  /// data frame without the specified series (column). The operation throws
  /// if the column key is not found.
  ///
  /// ## Parameters
  ///  - `column` - The key (or name) to be dropped from the frame
  ///  - `frame` - Source data frame (which is not mutated by the operation)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("DropColumn")>]
  let dropCol column (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.DropColumn(column); f

  /// Creates a new data frame where the specified column is replaced
  /// with a new series. (If the series does not exist, only the new
  /// series is added.)
  ///
  /// ## Parameters
  ///  - `column` - A key (or name) for the column to be replaced or added
  ///  - `series` - A data series to be used (the row key type has to match)
  ///  - `frame` - Source data frame (which is not mutated by the operation)
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("ReplaceColumn")>]
  let replaceCol column series (frame:Frame<'R, 'C>) = 
    let f = frame.Clone() in f.ReplaceColumn(column, series); f

  /// Returns data of the data frame as a 2D array containing data as `float` values.
  /// Missing data are represented as `Double.NaN` in the returned array.
  ///
  /// [category:Accessing frame data and lookup]
  [<CompiledName("ToArray2D")>]
  let toArray2D (frame:Frame<'R, 'C>) = frame.ToArray2D<float>()

  // ----------------------------------------------------------------------------------------------
  // Grouping, windowing and chunking
  // ----------------------------------------------------------------------------------------------

  /// Group rows of a data frame using the specified `selector`. The selector is called with
  /// a row key and object series representing the row and should return a new key. The result
  /// is a frame with multi-level index, where the first level is formed by the newly created
  /// keys.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsUsing")>]
  let groupRowsUsing (selector:_ -> _ -> 'K) (frame:Frame<'R, 'C>) = 
    frame.GroupRowsUsing(Func<_,_,_>(selector))    

  /// Group rows of a data frame using the specified `column`. The type of the column is inferred
  /// from the usage of the resulting frame. The result is a frame with multi-level index, where 
  /// the first level is formed by the newly created keys. Use `groupRowsBy[Int|String|...]` to
  /// explicitly specify the type of the column.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsBy")>]
  let groupRowsBy column (frame:Frame<'R, 'C>) : Frame<('K * _), _> = 
    frame.GroupRowsBy(column)

  /// Groups the rows of a frame by a specified column in the same way as `groupRowsBy`.
  /// This function assumes that the values of the specified column are of type `obj`.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsByObj")>]
  let groupRowsByObj column (frame:Frame<'R, 'C>) : Frame<obj * _, _> = groupRowsBy column frame

  /// Groups the rows of a frame by a specified column in the same way as `groupRowsBy`.
  /// This function assumes that the values of the specified column are of type `int`.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsByInt")>]
  let groupRowsByInt column (frame:Frame<'R, 'C>) : Frame<int * _, _> = groupRowsBy column frame

  /// Groups the rows of a frame by a specified column in the same way as `groupRowsBy`.
  /// This function assumes that the values of the specified column are of type `string`.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsByString")>]
  let groupRowsByString column (frame:Frame<'R, 'C>) : Frame<string * _, _> = groupRowsBy column frame

  /// Groups the rows of a frame by a specified column in the same way as `groupRowsBy`.
  /// This function assumes that the values of the specified column are of type `bool`.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsByBool")>]
  let groupRowsByBool column (frame:Frame<'R, 'C>) : Frame<bool * _, _> = groupRowsBy column frame
  
  /// Group rows of a data frame using the specified `keySelector`. The selector is called with
  /// a key of each row and should return a new key. The result is a frame with multi-level index, 
  /// here the first level is formed by the newly created keys.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("GroupRowsByIndex")>]
  let groupRowsByIndex (keySelector:_ -> 'K) (frame:Frame<'R,'C>) =
    frame.GroupRowsByIndex (Func<_,_>(keySelector))
    
  /// Returns a data frame whose rows are grouped by `groupBy` and whose columns specified
  /// in `aggBy` are aggregated according to `aggFunc`.
  ///
  /// ## Parameters
  ///  - `groupBy` - sequence of columns to group by
  ///  - `aggBy` - sequence of columns to apply aggFunc to
  ///  - `aggFunc` - invoked in order to aggregate values
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("AggregateRowsBy")>]
  let aggregateRowsBy groupBy aggBy (aggFunc:Series<'R, 'V1> -> 'V2) (frame:Frame<'R,'C>) =
    frame.AggregateRowsBy(groupBy, aggBy, Func<_,_>(aggFunc))

  // ----------------------------------------------------------------------------------------------
  // Pivot table
  // ----------------------------------------------------------------------------------------------
  
  /// Creates a new data frame resulting from a 'pivot' operation. Consider a denormalized data 
  /// frame representing a table: column labels are field names & table values are observations
  /// of those fields. pivotTable buckets the rows along two axes, according to the results of 
  /// the functions `rowGrp` and `colGrp`; and then computes a value for the frame of rows that
  /// land in each bucket.
  ///
  /// ## Parameters
  ///  - `rowGrp` - A function from rowkey & row to group value for the resulting row index
  ///  - `colGrp` - A function from rowkey & row to group value for the resulting col index
  ///  - `op` - A function computing a value from the corresponding bucket frame 
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("PivotTable")>]
  let pivotTable (rowGrp:'R -> ObjectSeries<'C> -> 'RNew) (colGrp:'R -> ObjectSeries<'C> -> 'CNew) (op:Frame<'R, 'C> -> 'T) (frame:Frame<'R, 'C>): Frame<'RNew, 'CNew> =
    let fromRows = FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder
    frame.Rows                                                                    //    Series<'R,ObjectSeries<'C>>
    |> Series.groupInto (fun r g -> colGrp r g) (fun _ g -> g)                    // -> Series<'CNew, Series<'R,ObjectSeries<'C>>>
    |> Series.mapValues (Series.groupInto (fun c g -> rowGrp c g) (fun _ g -> g)) // -> Series<'CNew, Series<'RNew, Series<'R',ObjectSeries<'C>>>>
    |> Series.mapValues (Series.mapValues (fromRows >> op))                       // -> Series<'CNew, Series<'RNew, 'T>>
    |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder              // -> Frame<'RNew, 'CNew, 'T>

  /// Creates a sliding window using the specified size. The result is a series
  /// containing data frames that represent individual windows.
  /// This function skips incomplete chunks. 
  ///
  /// ## Parameters
  ///  - `size` - The size of the sliding window.
  ///  - `frame` - The input frame to be aggregated.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("Window")>]
  let window size (frame:Frame<'R, 'C>) = 
    let fromRows rs = rs |> FrameUtils.fromRowsAndColumnKeys frame.IndexBuilder frame.VectorBuilder frame.ColumnIndex.Keys
    frame.Rows |> Series.windowInto size fromRows

  /// Creates a sliding window using the specified size and then applies the provided 
  /// value selector `f` on each window to produce the result which is returned as a new series. 
  /// This function skips incomplete chunks. 
  ///
  /// ## Parameters
  ///  - `size` - The size of the sliding window.
  ///  - `frame` - The input frame to be aggregated.
  ///  - `f` - A function that is called on each created window.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("WindowInto")>]
  let windowInto size f (frame:Frame<'R, 'C>) = 
    let fromRows rs = rs |> FrameUtils.fromRowsAndColumnKeys frame.IndexBuilder frame.VectorBuilder frame.ColumnIndex.Keys 
    frame.Rows |> Series.windowInto size (fromRows >> f)


  /// Returns a data frame with three columns named `Row`, `Column`
  /// and `Value` that contains the data of the original data frame 
  /// in individual rows.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("Stack")>]
  let stack (frame:Frame<'R, 'C>) =
    let rowKeys = frame.RowIndex.Keys
    let colKeys = frame.ColumnIndex.Keys

    // Build arrays with row keys, column keys and values
    let rowVec = ResizeArray<_>()
    let colVec = ResizeArray<_>()
    let valVec = ResizeArray<_>()
    for rowKey in rowKeys do
      for colKey in colKeys do
        let vec = frame.Data.GetValue(frame.ColumnIndex.Locate(colKey))
        if vec.HasValue then 
          let value = vec.Value.GetObject(frame.RowIndex.Locate(rowKey))
          if value.HasValue then 
            rowVec.Add(rowKey)
            colVec.Add(colKey)
            valVec.Add(value.Value)

    // Infer type of the values in the "value" vector 
    let valTyp = 
      frame.Data.DataSequence 
      |> Seq.choose (fun dt ->
        if dt.HasValue then Some(dt.Value.ElementType) else None) 
      |> VectorHelpers.findCommonSupertype
    
    let colIndex = Index.ofKeys ["Row"; "Column"; "Value"]
    let rowIndex = Index.ofKeys (Array.init valVec.Count id)
    let data = 
      [ Vector.ofValues (rowVec.ToArray()) :> IVector
        Vector.ofValues (colVec.ToArray()) :> IVector
        VectorHelpers.createTypedVector frame.VectorBuilder valTyp (valVec.ToArray()) ]
      |> Vector.ofValues
    Frame(rowIndex, colIndex, data, frame.IndexBuilder, frame.VectorBuilder)


  /// This function is the opposite of `stack`. It takes a data frame
  /// with three columns named `Row`, `Column` and `Value` and reconstructs
  /// a data frame by using `Row` and `Column` as row and column index keys,
  /// respectively.
  ///
  /// [category:Grouping, windowing and chunking]
  [<CompiledName("Unstack")>]
  let unstack (frame:Frame<'O, string>) : Frame<'R, 'C> =
    FrameUtils.fromValues frame.Rows.Values 
      (fun row -> row.GetAs<'C>("Column"))
      (fun row -> row.GetAs<'R>("Row"))
      (fun row -> row.GetAs<obj>("Value"))

  // ----------------------------------------------------------------------------------------------
  // Sorting and index manipulation
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
  /// [category:Sorting and index manipulation]
  [<CompiledName("RealignRows")>]
  let realignRows keys (frame:Frame<'R, 'C>) = 
    // form realignment on index, then apply column-wise
    let newIdx = Index.ofKeys (ReadOnlyCollection.ofSeq keys)
    let relocs = frame.IndexBuilder.Reindex(frame.RowIndex, newIdx, Lookup.Exact, VectorConstruction.Return 0, fun _ -> true)
    let cmd v =  VectorHelpers.transformColumn frame.VectorBuilder newIdx.AddressingScheme relocs v
    Frame<_, _>(newIdx, frame.ColumnIndex, frame.Data.Select(cmd), frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. The generic type parameter is specifies the type of the values in the required 
  /// index column (and usually needs to be specified using a type annotation). The specified
  /// column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRows")>]
  let indexRows column (frame:Frame<'R1, 'C>) : Frame<'R2, _> = frame.IndexRows<'R2>(column)

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `obj`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsByObject")>]
  let indexRowsObj column (frame:Frame<'R1, 'C>) : Frame<obj, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `int`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsByInt")>]
  let indexRowsInt column (frame:Frame<'R1, 'C>) : Frame<int, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `DateTime`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsByDateTime")>]
  let indexRowsDate column (frame:Frame<'R1, 'C>) : Frame<DateTime, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `DateTimeOffset`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsByDateTimeOffset")>]
  let indexRowsDateOffs column (frame:Frame<'R1, 'C>) : Frame<DateTimeOffset, _> = indexRows column frame

  /// Returns a data frame whose rows are indexed based on the specified column of the original
  /// data frame. This function casts (or converts) the column key to values of type `string`
  /// (a generic variant that may require some type annotation is `Frame.indexRows`)
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index is to be replaced.
  ///  - `column` - The name of a column in the original data frame that will be used for the new
  ///    index. Note that the values in the column need to be unique.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsByString")>]
  let indexRowsString column (frame:Frame<'R1, 'C>) : Frame<string, _> = indexRows column frame

  /// Replace the column index of the frame with the provided sequence of column keys.
  /// The columns of the frame are assigned keys according to the provided order.
  /// The specified column is removed from the resulting frame.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose column index are to be replaced.
  ///  - `keys` - A collection of new column keys.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexColumnsWith")>]
  let indexColsWith (keys:seq<'C2>) (frame:Frame<'R, 'C1>) = 
    if Seq.length frame.ColumnKeys <> Seq.length keys then invalidArg "keys" "New keys do not match current column index length"
    Frame<_, _>(frame.RowIndex, Index.ofKeys (ReadOnlyCollection.ofSeq keys), frame.Data, frame.IndexBuilder, frame.VectorBuilder)

  /// Replace the row index of the frame with the provided sequence of row keys.
  /// The rows of the frame are assigned keys according to the provided order.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///  - `keys` - A collection of new row keys.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsWith")>]
  let indexRowsWith (keys:seq<'R2>) (frame:Frame<'R1, 'C>) = 
    let newRowIndex = frame.IndexBuilder.Create(keys, None)
    let range = RangeRestriction.Fixed(frame.RowIndex.AddressAt(0L), frame.RowIndex.AddressAt(frame.RowIndex.KeyCount-1L))
    let cmd = VectorConstruction.GetRange(Vectors.Return 0, range)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme cmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Replace the row index of the frame with ordinarilly generated integers starting from zero.
  /// The rows of the frame are assigned index according to the current order, or in a
  /// non-deterministic way, if the current row index is not ordered.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsOrdinally")>]
  let indexRowsOrdinally (frame:Frame<'TRowKey, 'TColumnKey>) = 
    frame |> indexRowsWith [0 .. frame.RowCount-1]

  /// Replace the row index of the frame with a sequence of row keys generated using
  /// a function invoked on each row.
  ///
  /// ## Parameters
  ///  - `frame` - Source data frame whose row index are to be replaced.
  ///  - `f` - A function from row (as object series) to new row key value
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("IndexRowsUsing")>]
  let indexRowsUsing (f: ObjectSeries<'C> -> 'R2) (frame:Frame<'R1,'C>) =
    indexRowsWith (frame.Rows |> Series.map (fun k v -> f v) |> Series.values) frame

  /// Returns a transposed data frame. The rows of the original data frame are used as the
  /// columns of the new one (and vice versa). Use this operation if you have a data frame
  /// and you mostly need to access its rows as a series (because accessing columns as a 
  /// series is more efficient).
  /// 
  /// [category:Sorting and index manipulation]
  [<CompiledName("Transpose")>]
  let transpose (frame:Frame<'R, 'TColumnKey>) = 
    frame.Columns |> FrameUtils.fromRows frame.IndexBuilder frame.VectorBuilder

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("SortRowsByKey")>]
  let sortRowsByKey (frame:Frame<'R, 'C>) = 
    let newRowIndex, rowCmd = frame.IndexBuilder.OrderIndex(frame.RowIndex, Vectors.Return 0)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are ordered on a particular column of the frame. 
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("SortRows")>]
  let sortRows colKey (frame:Frame<'R,'C>) =
    let newRowIndex, rowCmd = frame.GetColumn(colKey) |> Series.sortWithCommand compare
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are ordered on a particular column of the frame. 
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("SortRowsWith")>]
  let sortRowsWith colKey compareFunc (frame:Frame<'R,'C>) =
    let newRowIndex, rowCmd = frame.GetColumn(colKey) |> Series.sortWithCommand compareFunc
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame that contains the same data as the input, 
  /// but whose rows are ordered on a particular column of the frame. 
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("SortRowBy")>]
  let sortRowsBy colKey (f:'T -> 'V) (frame:Frame<'R,'C>) =
    let newRowIndex, rowCmd = frame.GetColumn(colKey) |> Series.sortByCommand f
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme rowCmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a data frame that contains the same data as the input, 
  /// but whose columns are an ordered series. This allows using operations that are
  /// only available on indexed series such as alignment and inexact lookup.
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("SortColumnsByKey")>]
  let sortColsByKey (frame:Frame<'R, 'C>) = 
    let newColIndex, colCmd = frame.IndexBuilder.OrderIndex(frame.ColumnIndex, Vectors.Return 0)
    let newData = frame.VectorBuilder.Build(newColIndex.AddressingScheme, colCmd, [| frame.Data |])
    Frame<_, _>(frame.RowIndex, newColIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Creates a new data frame where all columns are expanded based on runtime
  /// structure of the objects they store. The expansion is performed recrusively
  /// to the specified depth. A column can be expanded if it is `Series<string, T>` 
  /// or `IDictionary<K, V>` or if it is any .NET object with readable
  /// properties. 
  ///
  /// ## Parameters
  ///  - `nesting` - The nesting level for expansion. When set to 0, nothing is done.
  ///  - `frame` - Input data frame whose columns will be expanded
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("ExpandAllColumns")>]
  let expandAllCols nesting (frame:Frame<'R, string>) = 
    FrameUtils.expandVectors nesting false frame

  /// Creates a new data frame where the specified columns are expanded based on runtime
  /// structure of the objects they store. A column can be expanded if it is 
  /// `Series<string, T>` or `IDictionary<K, V>` or if it is any .NET object with readable
  /// properties. 
  ///
  /// ## Example
  /// Given a data frame with a series that contains tuples, you can expand the
  /// tuple members and get a frame with columns `S.Item1` and `S.Item2`:
  /// 
  ///     let df = frame [ "S" => series [ 1 => (1, "One"); 2 => (2, "Two") ] ]  
  ///     df |> Frame.expandCols ["S"]
  ///
  /// ## Parameters
  ///  - `names` - Names of columns in the original data frame to be expanded
  ///  - `frame` - Input data frame whose columns will be expanded
  ///
  /// [category:Sorting and index manipulation]
  [<CompiledName("ExpandColumns")>]
  let expandCols names (frame:Frame<'R, string>) = 
    FrameUtils.expandColumns (set names) frame

  // ----------------------------------------------------------------------------------------------
  // Frame transformations
  // ----------------------------------------------------------------------------------------------

  /// Internal helper used by `skip`, `take`, etc.
  let internal getRange lo hi (frame:Frame<'R, 'C>) = 
    if hi < lo then 
      // Create empty vectors of the same type as the inputs
      let newData = frame.Data.Select(fun (v:IVector) -> 
        { new VectorCallSite<IVector> with
            member x.Invoke<'T>(v:IVector<'T>) = 
              Vector.ofValues ([]:'T list) :> IVector }
        |> v.Invoke)
      Frame(Index.ofKeys [], frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder) 
    else
      let cmd = GetRange(Return 0, RangeRestriction.Fixed(lo, hi))
      let newIndex, _ = 
        frame.RowIndex.Builder.GetAddressRange
          ( (frame.RowIndex, Vectors.Return 0), RangeRestriction.Fixed(lo,hi) )
      let newData = frame.Data.Select(transformColumn frame.VectorBuilder newIndex.AddressingScheme cmd)
      Frame(newIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder) 

  /// Returns a frame that contains the specified `count` of rows from the 
  /// original frame; `count` must be smaller or equal to the original number of rows.
  ///
  /// [category:Frame transformations]
  [<CompiledName("Take")>]
  let take count (frame:Frame<'R, 'C>) =
    if count > frame.RowCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange (frame.RowIndex.AddressAt(0L)) (frame.RowIndex.AddressAt(count - 1 |> int64)) frame

  /// Returns a frame that contains the specified `count` of rows from the 
  /// original frame. The rows are taken from the end of the frame; `count`
  /// must be smaller or equal to the original number of rows.
  ///
  /// [category:Frame transformations]
  [<CompiledName("TakeLast")>]
  let takeLast count (frame:Frame<'R, 'C>) =
    if count > frame.RowCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of rows."
    getRange (frame.RowIndex.AddressAt(frame.RowCount-count |> int64))  (frame.RowIndex.AddressAt(frame.RowCount-1 |> int64)) frame

  /// Returns a frame that contains the data from the original frame,
  /// except for the first `count` rows; `count` must be smaller or equal 
  /// to the original number of rows.
  ///
  /// [category:Frame transformations]
  [<CompiledName("Skip")>]
  let skip count (frame:Frame<'R, 'C>) =
    if count > frame.RowCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of rows."
    getRange (frame.RowIndex.AddressAt(count |> int64)) (frame.RowIndex.AddressAt((frame.RowCount-1) |> int64)) frame

  /// Returns a frame that contains the data from the original frame,
  /// except for the last `count` rows; `count` must be smaller or equal to the
  /// original number of rows.
  ///
  /// [category:Frame transformations]
  [<CompiledName("SkipLast")>]
  let skipLast count (frame:Frame<'R, 'C>) = 
    if count > frame.RowCount || count < 0 then 
      invalidArg "count" "Must be greater than zero and less than the number of keys."
    getRange (frame.RowIndex.AddressAt(0L)) (frame.RowIndex.AddressAt((frame.RowCount-1-count) |> int64))  frame

  /// Returns a new data frame containing only the rows of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with the row key and object series that represents the row data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the predicate
  ///
  /// [category:Frame transformations]
  [<CompiledName("WhereRows")>]
  let filterRows f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.filter f |> FrameUtils.fromRowsAndColumnKeys frame.IndexBuilder frame.VectorBuilder frame.ColumnIndex.Keys

  /// Returns a new data frame containing only the rows of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with an object series that represents the row data (use `filterRows`
  /// if you need to access the row key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the predicate
  ///
  /// [category:Frame transformations]
  [<CompiledName("WhereRowValues")>]
  let filterRowValues f (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.filterValues f |> FrameUtils.fromRowsAndColumnKeys frame.IndexBuilder frame.VectorBuilder frame.ColumnIndex.Keys


  /// Returns a new data frame containing only the rows of the input frame
  /// for which the specified `column` has the specified `value`. The operation
  /// may be implemented via an index for virtualized Deedle frames.
  /// 
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `column` - The name of the column to be matched
  ///  - `value` - Required value of the column. Note that the function
  ///    is generic and no conversions are performed, so the value has
  ///    to match including the actual type.
  ///
  /// [category:Frame transformations]
  [<CompiledName("WhereRowsBy")>]
  let filterRowsBy column (value:'V) (frame:Frame<'R, 'C>) = 
    let column = frame.GetColumn<'V>(column)
    let newRowIndex, cmd = 
      frame.IndexBuilder.Search( (frame.RowIndex, Vectors.Return 0), column.Vector, value)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme cmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)


  /// Builds a new data frame whose rows are the results of applying the specified
  /// function on the rows of the input data frame. The function is called
  /// with the row key and object series that represents the row data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the row mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("SelectRows")>]
  let inline mapRows (f:_ -> _ -> 'V) (frame:Frame<'R, 'C>) = 
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
  /// [category:Frame transformations]
  [<CompiledName("SelectRowValues")>]
  let inline mapRowValues (f:_ -> 'V) (frame:Frame<'R, 'C>) = 
    frame.Rows |> Series.mapValues f 

  /// Builds a new data frame whose row keys are the results of applying the
  /// specified function on the row keys of the original data frame.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the row key mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("SelectRowKeys")>]
  let mapRowKeys (f:'R1 -> 'R2) (frame:Frame<_, 'C>) = 
    let newRowIndex = frame.IndexBuilder.Create(frame.RowIndex.Keys |> Seq.map f, None)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme (Vectors.Return 0))
    Frame(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a new data frame containing only the columns of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with the column key and object series that represents the column data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the predicate
  ///
  /// [category:Frame transformations]
  [<CompiledName("WhereColumns")>]
  let filterCols f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.filter f |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Returns a new data frame containing only the columns of the input frame
  /// for which the specified predicate returns `true`. The predicate is called
  /// with an object series that represents the column data (use `filterCols`
  /// if you need to access the column key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the predicate
  ///
  /// [category:Frame transformations]
  [<CompiledName("WhereColumnValues")>]
  let filterColValues f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.filterValues f |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Builds a new data frame whose columns are the results of applying the specified
  /// function on the columns of the input data frame. The function is called
  /// with the column key and object series that represents the column data.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of two arguments that defines the column mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("SelectColumns")>]
  let mapCols f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.map f |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Builds a new data frame whose columns are the results of applying the specified
  /// function on the columns of the input data frame. The function is called
  /// with an object series that represents the column data (use `mapCols`
  /// if you need to access the column key).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the column mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("SelectColumnValues")>]
  let mapColValues f (frame:Frame<'R, 'C>) = 
    frame.Columns |> Series.mapValues f |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Builds a new data frame whose column keys are the results of applying the
  /// specified function on the column keys of the original data frame.
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function of one argument that defines the column key mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("SelectColumnKeys")>]
  let mapColKeys f (frame:Frame<'R, 'C>) = 
    let newColIndex = frame.IndexBuilder.Create(frame.ColumnIndex.Keys |> Seq.map f, None)
    let newData = frame.VectorBuilder.Build(newColIndex.AddressingScheme, Vectors.Return 0, [| frame.Data |])
    Frame(frame.RowIndex, newColIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Builds a new data frame whose values are the results of applying the specified
  /// function on these values, but only for those columns which can be converted 
  /// to the appropriate type for input to the mapping function (use `map` if you need 
  //// to access the row and column keys).
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function that defines the mapping
  ///
  /// [category:Frame transformations]
  [<CompiledName("MapValues")>]
  let mapValues f (frame:Frame<'R, 'C>) = frame.SelectValues(Func<_,_>(f))

  /// Builds a new data frame whose values are the results of applying the specified
  /// function on these values, but only for those columns which can be converted 
  /// to the appropriate type for input to the mapping function. 
  ///
  /// ## Parameters
  ///  - `frame` - Input data frame to be transformed
  ///  - `f` - Function that defines the mapping
  ///
  /// [category:Frame transformations]
  let map f (frame:Frame<'R, 'C>) = frame.Select(Func<_,_,_,_>(f))

  /// Returns a series that contains the results of aggregating each column
  /// to a single value. The function takes columns that can be converted to 
  /// the type expected by the specified `op` function and reduces the values
  /// in each column using `Series.reduceValues`. 
  ///
  /// ## Example
  /// The following sums the values in each column that can be converted to
  /// `float` and returns the result as a new series:
  ///
  ///     df |> Frame.reduceValues (fun (a:float) b -> a + b)
  ///
  /// [category:Frame transformations]
  [<CompiledName("ReduceValues")>]
  let reduceValues (op:'T -> 'T -> 'T) (frame:Frame<'R, 'C>) = 
    frame.GetColumns<'T>() |> Series.map (fun _ -> Series.reduceValues op) 

  /// Returns a row of the data frame which has the greatest value of the
  /// specified `column`. The row is returned as an optional value (which is
  /// `None` for empty frame) and contains a key together with an object
  /// series representing the row.
  ///
  /// [category:Frame transformations]
  [<CompiledName("MaxRowBy")>]
  let inline maxRowBy column (frame:Frame<'R, 'C>) = 
    frame.Rows |> Stats.maxBy (fun row -> row.GetAs<float>(column))

  /// Returns a row of the data frame which has the smallest value of the
  /// specified `column`. The row is returned as an optional value (which is
  /// `None` for empty frame) and contains a key together with an object
  /// series representing the row.
  ///
  /// [category:Frame transformations]
  [<CompiledName("MinRowBy")>]
  let inline minRowBy column (frame:Frame<'R, 'C>) = 
    frame.Rows |> Stats.minBy (fun row -> row.GetAs<float>(column))

  /// Returns a frame with columns shifted by the specified offset. When the offset is 
  /// positive, the values are shifted forward and first `offset` keys are dropped. When the
  /// offset is negative, the values are shifted backwards and the last `offset` keys are dropped.
  /// Expressed in pseudo-code:
  ///
  ///     result[k] = series[k - offset]
  ///
  /// ## Parameters
  ///  - `offset` - Can be both positive and negative number.
  ///  - `frame` - The input frame whose columns are to be shifted.
  ///
  /// ## Remarks
  /// If you want to calculate the difference, e.g. `df - (Frame.shift 1 df)`, you can
  /// use `Frame.diff` which will be a little bit faster.
  ///
  /// [category:Frame transformations]
  [<CompiledName("Shift")>]
  let shift offset (frame:Frame<'R, 'C>) = 
    let newRowIndex, cmd = frame.RowIndex.Builder.Shift((frame.RowIndex, Vectors.Return 0), offset)
    let vectorBuilder = VectorBuilder.Instance
    let newData = frame.Data.Select(VectorHelpers.transformColumn vectorBuilder newRowIndex.AddressingScheme cmd)
    Frame(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Returns a frame with columns containing difference between an original value and
  /// a value at the specified offset. For example, calling `Frame.diff 1 s` returns a 
  /// frame where previous column values is subtracted from the current ones. In pseudo-code, the
  /// function behaves as follows:
  ///
  ///     result[k] = series[k] - series[k - offset]
  ///
  /// Columns that cannot be converted to `float` are left without a change.
  ///
  /// ## Parameters
  ///  - `offset` - When positive, subtracts the past values from the current values;
  ///    when negative, subtracts the future values from the current values.
  ///  - `frame` - The input frame containing at least some `float` columns.
  ///
  /// [category:Frame transformations]
  [<CompiledName("Diff")>]
  let diff offset (frame:Frame<'R, 'C>) = 
    let vectorBuilder = VectorBuilder.Instance
    let newRowIndex, vectorR = frame.RowIndex.Builder.Shift((frame.RowIndex, Vectors.Return 0), offset)
    let _, vectorL = frame.RowIndex.Builder.Shift((frame.RowIndex, Vectors.Return 0), -offset)
    let cmd = Vectors.Combine(lazy newRowIndex.KeyCount, [vectorL; vectorR], BinaryTransform.Create<float>(OptionalValue.map2 (-)))
    let newData = frame.Data.Select(function
        | AsFloatVector vf -> VectorBuilder.Instance.Build(newRowIndex.AddressingScheme, cmd, [| vf |]) :> IVector
        | vector -> vector)
    Frame(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)


  // ----------------------------------------------------------------------------------------------
  // Processing frames with exceptions
  // ----------------------------------------------------------------------------------------------

  /// Returns a series, obtained by applying the specified projection function `f` to all rows
  /// of the input frame. The resulting series wraps the results in `tryval<'V>`. When the projection 
  /// function fails, the exception is wrapped using the `Error` case.
  ///
  /// [category:Processing frames with exceptions]
  [<CompiledName("TryMapRows")>]
  let tryMapRows (f:_ -> _ -> 'V) (frame:Frame<'R, 'C>) : Series<_, _ tryval> = 
    frame |> mapRows (fun k row -> try TryValue.Success(f k row) with e -> TryValue.Error e)

  /// Given a data frame containing columns of type `tryval<'T>`, returns a new data frame
  /// that contains the underlying values of type `'T`. When the frame contains one or more 
  /// failures, the operation throws `AggregateException`. Otherwise, it returns a frame containing values.
  ///
  /// [category:Processing frames with exceptions]
  [<CompiledName("TryValues")>]
  let tryValues (frame:Frame<'R, 'C>) = 
    let newTryData = frame.Data.Select(VectorHelpers.tryValues)
    let exceptions = newTryData.DataSequence |> Seq.choose OptionalValue.asOption |> Seq.choose (fun v ->
      if v.HasValue then None else Some v.Exception) |> List.ofSeq
    if List.isEmpty exceptions then 
      // All succeeded, so we can build new data frame
      let newData = newTryData.Select(fun (v:tryval<_>) -> v.Value)
      Frame<_, _>(frame.RowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)
    else
      // Some exceptions, aggregate all of them
      let exceptions = exceptions |> List.collect (function
        | :? AggregateException as ae -> ae.InnerExceptions |> List.ofSeq | e -> [e])
      raise (new AggregateException(exceptions))

  /// Fills all error cases of a `tryval<'T>` value in a data frame with the specified 
  /// `value`. The function takes all columns of type `tryval<'T>` and uses `Series.fillErrorsWith`
  /// to fill the error values with the specified default value.
  /// 
  /// [category:Processing frames with exceptions]
  [<CompiledName("FillErrorsWith")>]
  let fillErrorsWith (value:'T) (frame:Frame<'R, 'C>) = 
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<_, 'T tryval>) -> 
      (Series.fillErrorsWith value s) :> ISeries<_>)

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
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<_, 'T>) -> Series.fillMissingWith value s :> ISeries<_>)

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
    let fillCmd = Vectors.FillMissing(Vectors.Return 0, VectorFillMissing.Direction direction)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder frame.RowIndex.AddressingScheme fillCmd)
    Frame<_, _>(frame.RowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

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
    frame.ColumnApply(ConversionKind.Safe, fun (s:Series<_, 'T>) -> Series.fillMissingUsing (f s) s :> ISeries<_>)

  /// Creates a new data frame that contains only those rows of the original 
  /// data frame that are _dense_, meaning that they have a value for each column.
  /// The resulting data frame has the same number of columns, but may have 
  /// fewer rows (or no rows at all).
  /// 
  /// [category:Missing values]
  [<CompiledName("DropSparseRows")>]
  let dropSparseRows (frame:Frame<'R, 'C>) = 
    // Create a combined vector that has 'true' for rows which have some values
    let hasSomeFlagVector = 
      frame.Data 
      |> createRowVector 
          frame.VectorBuilder frame.RowIndex.AddressingScheme (lazy frame.RowIndex.KeyCount) 
          frame.ColumnIndex.KeyCount frame.ColumnIndex.AddressAt
          (fun rowReader -> rowReader.DataSequence |> Seq.exists (fun opt -> opt.HasValue))
    // Collect all rows that have at least some values
    let newRowIndex, cmd = 
      frame.IndexBuilder.Search( (frame.RowIndex, Vectors.Return 0), hasSomeFlagVector, true)
    let newData = frame.Data.Select(VectorHelpers.transformColumn frame.VectorBuilder newRowIndex.AddressingScheme cmd)
    Frame<_, _>(newRowIndex, frame.ColumnIndex, newData, frame.IndexBuilder, frame.VectorBuilder)

  /// Creates a new data frame that contains only those columns of the original 
  /// data frame that are _dense_, meaning that they have a value for each row.
  /// The resulting data frame has the same number of rows, but may have 
  /// fewer columns (or no columns at all).
  ///
  /// [category:Missing values]
  [<CompiledName("DropSparseColumns")>]
  let dropSparseCols (frame:Frame<'R, 'C>) = 
    let newColKeys, newData =
      [| for KeyValue(colKey, addr) in frame.ColumnIndex.Mappings do
            match frame.Data.GetValue(addr) with
            | OptionalValue.Present(vec) when vec.ObjectSequence |> Seq.exists (fun o -> o.HasValue) ->
                yield colKey, vec
            | _ -> () |] |> Array.unzip
    let colIndex = frame.IndexBuilder.Create(ReadOnlyCollection.ofArray newColKeys, None)
    Frame(frame.RowIndex, colIndex, frame.VectorBuilder.Create(newData), frame.IndexBuilder, frame.VectorBuilder )

  /// Returns the columns of the data frame that do not have any missing values.
  /// The operation returns a series (indexed by the column keys of the source frame) 
  /// containing _series_ representing individual columns of the frame. This is similar 
  /// to `Columns`, but it skips columns that contain missing value in _any_ row.
  ///
  /// [category:Missing values]
  [<CompiledName("DenseColumns")>]
  let denseCols (frame:Frame<'R, 'C>) = frame.ColumnsDense

  /// Returns the rows of the data frame that do not have any missing values. 
  /// The operation returns a series (indexed by the row keys of the source frame) 
  /// containing _series_ representing individual row of the frame. This is similar 
  /// to `Rows`, but it skips rows that contain missing value in _any_ column.
  ///
  /// [category:Missing values]
  [<CompiledName("DenseRows")>]
  let denseRows (frame:Frame<'R, 'C>) = frame.RowsDense


  // ----------------------------------------------------------------------------------------------
  // Joining, merging and zipping
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
  /// [category:Joining, merging and zipping]
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
  /// [category:Joining, merging and zipping]
  [<CompiledName("JoinAlign")>]
  let joinAlign kind lookup (frame1:Frame<'R, 'C>) frame2 = frame1.Join(frame2, kind, lookup)

  /// Append a sequence of data frames with non-overlapping values. The operation takes the union of 
  /// columns and rows of the source data frames and then unions the values. An exception is thrown when 
  /// both data frames define value for a column/row location, but the operation succeeds if one
  /// all frames but one has a missing value at the location.
  ///
  /// Note that the rows are *not* automatically reindexed to avoid overlaps. This means that when
  /// a frame has rows indexed with ordinal numbers, you may need to explicitly reindex the row
  /// keys before calling append.
  ///
  /// [category:Joining, merging and zipping]
  [<CompiledName("MergeAll")>]
  let mergeAll (frames:Frame<'R, 'C> seq) =
    if frames |> Seq.isEmpty then 
      Frame([], [])
    else 
      let head = frames |> Seq.head 
      head.Merge(frames |> Seq.skip 1)

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
  ///  - `frame1` - First of the two frames to be merged (combined)
  ///  - `frame2` - The other frame to be merged (combined) with the first instance
  ///
  /// [category:Joining, merging and zipping]
  [<CompiledName("Merge")>]
  let merge (frame1:Frame<'R, 'C>) frame2 = mergeAll [frame1; frame2]

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
  /// [category:Joining, merging and zipping]
  [<CompiledName("ZipAlignInto")>]
  let zipAlign columnKind rowKind lookup (op:'V1->'V2->'V) (frame1:Frame<'R, 'C>) (frame2:Frame<'R, 'C>) : Frame<'R, 'C> =
    frame1.Zip<'V1, 'V2, 'V>(frame2, columnKind, rowKind, lookup, fun a b -> op a b)

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
  /// [category:Joining, merging and zipping]
  [<CompiledName("ZipInto")>]
  let zip (op:'V1->'V2->'V) (frame1:Frame<'R, 'C>) (frame2:Frame<'R, 'C>) : Frame<'R, 'C> =
    zipAlign JoinKind.Inner JoinKind.Inner Lookup.Exact op frame1 frame2

  // ----------------------------------------------------------------------------------------------
  // Hierarchical index operations
  // ----------------------------------------------------------------------------------------------

  /// Reduce the values in each series according to the specified level of a hierarchical row key.
  /// For each group of rows as specified by `levelSel`, the function reduces the values in each series 
  /// using the preovided function `op` by applying `Series.reduceLevel`. Columns that cannot be
  /// converted to a type required by `op` are skipped.
  ///
  /// ## Example
  /// To sum the values in all numerical columns according to the first component of a two level
  /// row key `'K1 * 'K2`, you can use the following:
  ///
  ///     df |> Frame.reduceLevel fst (fun (a:float) b -> a + b)
  //
  /// ## Remarks
  /// This function reduces values using a function `'T -> 'T -> 'T`. If you want to process
  /// an entire group of values at once, you can use `applyLevel` instead.
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("ReduceLevel")>]
  let reduceLevel (levelSel:_ -> 'K) (op:'T -> 'T -> 'T) (frame:Frame<'R, 'C>) = 
    frame.GetColumns<'T>() 
    |> Series.map (fun _ -> Series.reduceLevel levelSel op) 
    |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Apply a specified function to a group of values in each series according to the specified 
  /// level of a hierarchical row key. For each group of rows as specified by `levelSel`, the function 
  /// applies the specified function `op` to all columns. Columns that cannot be converted to a 
  /// type required by `op` are skipped. 
  ///
  /// ## Example
  /// To get the standard deviation of values in all numerical columns according to the first 
  /// component of a two level row key `'K1 * 'K2`, you can use the following:
  ///
  ///     df |> Frame.applyLevel fst Stats.stdDev
  //
  /// ## Remarks
  /// This function reduces a series of values using a function `Series<'R, 'T> -> 'T`. If
  /// you want to reduce values using a simpler function `'T -> 'T -> 'T`, you can use
  /// `Frame.reduceLevel` instead.
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("ApplyLevel")>]
  let applyLevel (levelSel:_ -> 'K) (op:_ -> 'T) (frame:Frame<'R, 'C>) = 
    frame.GetColumns<'T>() 
    |> Series.map (fun _ s -> Series.applyLevel levelSel op s) 
    |> FrameUtils.fromColumns frame.IndexBuilder frame.VectorBuilder

  /// Given a frame with two-level row index, returns a series indexed by the first
  /// part of the key, containing frames representing individual groups. This function
  /// can be used if you want to perform a transformation individually on each group
  /// (e.g. using `Series.mapValues` after calling `Frame.nest`).
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("Nest")>]
  let nest (frame:Frame<'R1 * 'R2, 'C>) = 
    let labels = frame.RowKeys |> Seq.map fst
    frame.NestRowsBy<'R1>(labels) 
    |> Series.map (fun r df -> df |> indexRowsWith (df.RowKeys |> Seq.map snd))

  /// Given a data frame, use the specified `keySelector` to generate a new, first-level
  /// of indices based on the current indices. Returns a series (indexed by the first-level)
  /// of frames (indexed by the second-level). 
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("NestBy")>]
  let nestBy (keySelector:_ -> 'K1) (frame:Frame<'K2, 'C>) = 
    let labels = (frame.RowKeys |> Seq.map keySelector)
    frame.GroupByLabels labels frame.RowCount |> nest

  /// Given a series of frames, returns a new data frame with two-level hierarchical
  /// row index, using the series keys as the first component. This function is the
  /// dual of `Frame.nest`.
  ///
  /// [category:Hierarchical index operations]
  [<CompiledName("Unnest")>]
  let unnest (series:Series<'R1, Frame<'R2, 'C>>) =
    series
    |> Series.map (fun k1 df -> 
      df.RowKeys 
      |> Seq.map (fun k2 -> (k1, k2)) 
      |> (fun ix -> indexRowsWith ix df))
    |> Series.values
    |> mergeAll
