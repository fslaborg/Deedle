namespace FSharp.DataFrame

/// Series module comment..
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

[<AutoOpen>]
module FSharp1 =
  open System

  type Series = 
    static member ofObservations(observations) = 
      Series(Seq.map fst observations, Seq.map snd observations)
    static member ofValues(values) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values)
    static member ofNullables(values:seq<Nullable<_>>) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values).Select(fun kvp -> kvp.Value.Value)
    
