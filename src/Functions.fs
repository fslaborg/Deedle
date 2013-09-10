namespace FSharp.DataFrame

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series = 
  open System.Linq
  open FSharp.DataFrame.Common
  open FSharp.DataFrame.Vectors
  open MathNet.Numerics.Statistics

  [<CompiledName("Sum")>]
  let inline sum (series:Series<_, _>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> IReadOnlyList.sum list
    | VectorData.SparseList list -> IReadOnlyList.sumOptional list
    | VectorData.Sequence seq -> Seq.sum (Seq.choose OptionalValue.asOption seq)

  [<CompiledName("Mean")>]
  let inline mean (series:Series<_, _>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> IReadOnlyList.average list
    | VectorData.SparseList list -> IReadOnlyList.averageOptional list
    | VectorData.Sequence seq -> Seq.average (Seq.choose OptionalValue.asOption seq)

  [<CompiledName("StandardDeviation")>]
  let inline sdv (series:Series<_, _>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> StreamingStatistics.StandardDeviation list
    | VectorData.SparseList list -> StreamingStatistics.StandardDeviation (Seq.choose OptionalValue.asOption list)
    | VectorData.Sequence seq -> StreamingStatistics.StandardDeviation (Seq.choose OptionalValue.asOption seq)

  let observations (series:Series<'K, 'T>) = series.Observations

  /// Create a new series that contains values for all provided keys.
  /// Use the specified lookup semantics - for exact matching, use `getAll`
  let lookupAll keys lookup (series:Series<'K, 'T>) = series.GetItems(keys, lookup)

  /// Create a new series that contains values for all provided keys.
  /// Uses exact lookup semantics for key lookup - use `lookupAll` for more options
  let getAll keys (series:Series<'K, 'T>) = series.GetItems(keys)

  /// Get the value for the specified key.
  /// Use the specified lookup semantics - for exact matching, use `get`
  let lookup key lookup (series:Series<'K, 'T>) = series.Get(key, lookup)

  /// Get the value for the specified key.
  /// Uses exact lookup semantics for key lookup - use `lookupAll` for more options
  let get key (series:Series<'K, 'T>) = series.Get(key)

  let withOrdinalIndex (series:Series<'K, 'T>) = 
    series.WithOrdinalIndex()

  let filter f (series:Series<'K, 'T>) = 
    series.Where(fun kvp -> f kvp.Key kvp.Value)

  let map (f:'K -> 'T -> 'R) (series:Series<'K, 'T>) = 
    series.Select(fun kvp -> f kvp.Key kvp.Value)

  let filterAll f (series:Series<'K, 'T>) = 
    series.WhereOptional(fun kvp -> f kvp.Key (OptionalValue.asOption kvp.Value))

  let mapAll (f:_ -> _ -> option<'R>) (series:Series<'K, 'T>) = 
    series.SelectOptional(fun kvp -> 
      f kvp.Key (OptionalValue.asOption kvp.Value) |> OptionalValue.ofOption)

  let pairwise (series:Series<'K, 'T>) = series.Pairwise()
  
  let pairwiseWith f (series:Series<'K, 'T>) = series.Pairwise() |> map f

  (**
  Windowing, Chunking and Grouping
  ----------------------------------------------------------------------------------------------

  The functions with name starting with `windowed` take a series and generate floating 
  (overlapping) windows. The `chunk` functions 

  *)

  let aggregate aggregation valueSelector keySelector (series:Series<'K, 'T>) =
    series.Aggregate(aggregation, valueSelector >> OptionalValue.ofOption, keySelector)

  let inline windowSizeInto size f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.WindowSize(size), f >> OptionalValue.ofOption)

  let inline windowSize distance (series:Series<'K, 'T>) = 
    windowSizeInto distance (fun s -> Some(s)) series 

  let inline windowDistInto distance f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.WindowWhile(fun skey ekey -> (ekey - skey) < distance), f >> OptionalValue.ofOption)

  let inline windowDist distance (series:Series<'K, 'T>) = 
    windowDistInto distance (fun s -> Some(s)) series 

  let inline windowWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.WindowWhile(cond), f >> OptionalValue.ofOption)

  let inline windowWhile cond (series:Series<'K, 'T>) = 
    windowWhileInto cond (fun s -> Some(s)) series 


  let inline chunkSizeInto size f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.ChunkSize(size), f >> OptionalValue.ofOption)

  let inline chunkSize distance (series:Series<'K, 'T>) = 
    chunkSizeInto distance (fun s -> Some(s)) series 

  let inline chunkDistInto distance f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.ChunkWhile(fun skey ekey -> (ekey - skey) < distance), f >> OptionalValue.ofOption)

  let inline chunkDist distance (series:Series<'K, 'T>) = 
    chunkDistInto distance (fun s -> Some(s)) series 

  let inline chunkWhileInto cond f (series:Series<'K, 'T>) =
    series.Aggregate(Aggregation.ChunkWhile(cond), f >> OptionalValue.ofOption)

  let inline chunkWhile cond (series:Series<'K, 'T>) = 
    chunkWhileInto cond (fun s -> Some(s)) series 


  let groupByInto (keySelector:'K -> 'T -> 'TNewKey) f (series:Series<'K, 'T>) : Series<'TNewKey, 'TNewValue> =
    series.GroupBy(keySelector, fun k s -> OptionalValue.ofOption (f k s))

  let groupBy (keySelector:'K -> 'T -> 'TNewKey) (series:Series<'K, 'T>) =
    groupByInto keySelector (fun k s -> Some(s)) series

  // ----------------------------------------------------------------------------------------------
  // Counting & checking if values are present
  // ----------------------------------------------------------------------------------------------

  let countValues (series:Series<'K, 'T>) = series.CountValues
  let countKeys (series:Series<'K, 'T>) = series.CountKeys

  let hasAll keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).IsSome)
  let hasSome keys (series:Series<'K, 'T>) = 
    keys |> Seq.exists (fun k -> series.TryGet(k).IsSome)
  let hasNone keys (series:Series<'K, 'T>) = 
    keys |> Seq.forall (fun k -> series.TryGet(k).IsNone)
  let has key (series:Series<'K, 'T>) = series.TryGet(key).IsSome
  let hasNot key (series:Series<'K, 'T>) = series.TryGet(key).IsNone

  // ----------------------------------------------------------------------------------------------
  // Handling of missing values
  // ----------------------------------------------------------------------------------------------

  let dropMissing (series:Series<'K, 'T>) = series.DropMissing()

  let fillMissingUsing f (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> Some(f k)
      | value -> value)

  let fillMissingWith value (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> Some(value)
      | value -> value)

  let fillMissing lookup (series:Series<'K, 'T>) = 
    series |> mapAll (fun k -> function 
      | None -> series.TryGet(k, lookup)
      | value -> value)

  let shift offset (series:Series<'K, 'T>) = 
    let shifted = 
      if offset < 0 then
        let offset = -offset
        series |> aggregate (WindowSize(offset + 1)) 
          (fun s -> Some(s.Values |> Seq.nth offset)) 
          (fun s -> s.Keys.First())
      else
        series |> aggregate (WindowSize(offset + 1)) 
          (fun s -> Some(s.Values |> Seq.head)) 
          (fun s -> s.Keys.Last())
    shifted.GetItems(series.Keys)

type Column<'T> = C

[<AutoOpen>]
module ColumnExtensions = 
  let column<'T> : Column<'T> = C

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Frame = 
  // ----------------------------------------------------------------------------------------------
  // Grouping
  // ----------------------------------------------------------------------------------------------

  let groupRowsByInto column f (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.Rows |> Series.groupByInto 
      (fun _ v -> v.Get(column))
      (fun k g -> g |> Frame.ofRows |> f |> Some)

  let groupRowsBy column (frame:Frame<'TRowKey, 'TColKey>) = 
    groupRowsByInto column id frame

  //let shiftRows offset (frame:Frame<'TRowKey, 'TColKey>) = 
  //  frame.Columns 
  //  |> Series.map (fun k col -> Series.shift offset col)
  //  |> Frame.ofColumns

  // ----------------------------------------------------------------------------------------------
  // Wrappers that simply call member functions of the data frame
  // ----------------------------------------------------------------------------------------------

  /// Creates a new data frame that contains all data from 
  /// the original data frame, together with additional series.
  [<CompiledName("AddSeries")>]
  let addSeries column series (frame:Frame<'TRowKey, 'TColKey>) = 
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
  let columns (frame:Frame<'TRowKey, 'TColKey>) = frame.Columns

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
  let columnsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.ColumnsDense

  /// Returns the rows of the data frame as a series (indexed by 
  /// the row keys of the source frame) containing _series_ representing
  /// individual row of the frame. This is similar to `Rows`, but it
  /// skips rows that contain missing value in _any_ column.
  [<CompiledName("RowsDense")>]
  let rowsDense (frame:Frame<'TRowKey, 'TColKey>) = frame.RowsDense

  /// Creates a new data frame that contains all data from the original
  /// data frame without the specified series (column).
  [<CompiledName("DropSeries")>]
  let dropSeries column (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.DropSeries(column); f

  /// Creates a new data frame where the specified column is repalced
  /// with a new series. (If the series does not exist, only the new
  /// series is added.)
  [<CompiledName("ReplaceSeries")>]
  let replaceSeries column series (frame:Frame<'TRowKey, 'TColKey>) = 
    let f = frame.Clone() in f.ReplaceSeries(column, series); f

  /// Returns a specified series (column) from a data frame. This 
  /// function uses exact matching semantics. Use `lookupSeries` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetSeries")>]
  let getSeries column (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column)

  /// Returns a specified row from a data frame. This 
  /// function uses exact matching semantics. Use `lookupRow` if you
  /// want to use inexact matching (e.g. on dates)
  [<CompiledName("GetRow")>]
  let getRow row (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row)

  /// Returns a specified series (column) from a data frame. If the data frame has 
  /// ordered column index, the lookup semantics can be used to get series
  /// with nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupSeries")>]
  let lookupSeries column lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetSeries(column, lookup)

  /// Returns a specified row from a data frame. If the data frame has 
  /// ordered row index, the lookup semantics can be used to get row with 
  /// nearest greater/smaller key. For exact semantics, you can use `getSeries`.
  [<CompiledName("LookupRow")>]
  let lookupRow row lookup (frame:Frame<'TRowKey, 'TColKey>) = frame.GetRow(row, lookup)

  /// Returns a data frame that contains the same data as the argument, 
  /// but whose rows are ordered series. This allows using inexact lookup
  /// for rows (e.g. using `lookupRow`) or inexact left/right joins.
  [<CompiledName("OrderRows")>]
  let orderRows (frame:Frame<'TRowKey, 'TColKey>) = frame.WithOrderedRows()

  /// Creates a new data frame that uses the specified column as an row index.
  [<CompiledName("WithRowIndex")>]
  let withRowIndex (columnType:Column<'TNewRowKey>) column (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.WithRowIndex<'TNewRowKey>(column)

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
  // Additional functions for working with data frames
  // ----------------------------------------------------------------------------------------------

  let inline mean (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.mean)

  let inline sum (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sum)

  let inline countValues (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<obj>() |> Series.map (fun _ -> Series.countValues)

  let countKeys (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.RowIndex.Keys |> Seq.length

  let inline sdv (frame:Frame<'TRowKey, 'TColKey>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> Series.sdv)

      