namespace FSharp.DataFrame

open System
open System.ComponentModel
open System.Collections.Generic
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.VectorHelpers

/// This enumeration specifies joining behavior for `Join` method provided
/// by `Series` and `Frame`. Outer join unions the keys (and may introduce
/// missing values), inner join takes the intersection of keys; left and
/// right joins take the keys of the first or the second series/frame.
type JoinKind = 
  /// Combine the keys available in both structures, align the values that
  /// are available in both of them and mark the remaining values as missing.
  | Outer = 0
  /// Take the intersection of the keys available in both structures and align the 
  /// values of the two structures. The resulting structure cannot contain missing values.
  | Inner = 1
  /// Take the keys of the left (first) structure and align values from the right (second)
  /// structure with the keys of the first one. Values for keys not available in the second
  /// structure will be missing.
  | Left = 2
  /// Take the keys of the right (second) structure and align values from the left (first)
  /// structure with the keys of the second one. Values for keys not available in the first
  /// structure will be missing.
  | Right = 3


/// This enumeration specifeis the behavior of `Union` operation on series when there are
/// overlapping keys in two series that are being unioned. The options include prefering values
/// from the left/right series or throwing an exception when both values are available.
type UnionBehavior =
  /// When there are values available in both series that are being unioned, prefer the left value.
  | PreferLeft = 0
  /// When there are values available in both series that are being unioned, prefer the right value.
  | PreferRight = 1
  /// When there are values available in both series that are being unioned, raise an exception.
  | Exclusive = 2

// ------------------------------------------------------------------------------------------------
// Series
// ------------------------------------------------------------------------------------------------

/// Represents an untyped series with keys of type `K` and values of some unknown type
/// (This type should not generally be used directly, but it can be used when you need
/// to write code that works on a sequence of series of heterogeneous types).
type ISeries<'K when 'K : equality> =
  /// Returns the vector containing data of the series (as an untyped vector)
  abstract Vector : FSharp.DataFrame.IVector
  /// Returns the index containing keys of the series 
  abstract Index : IIndex<'K>
  /// Attempts to get the value at a specified key and return it as `obj`
  abstract TryGetObject : 'K -> OptionalValue<obj>


/// The type `Series<K, V>` represents a data series consisting of values `V` indexed by
/// keys `K`. The keys of a series may or may not be ordered 
and Series<'K, 'V when 'K : equality>
    ( index:IIndex<'K>, vector:IVector<'V>,
      vectorBuilder : IVectorBuilder, indexBuilder : IIndexBuilder ) as this =
  
  member internal x.VectorBuilder = vectorBuilder
  member internal x.IndexBuilder = indexBuilder

  /// Returns the index associated with this series. This member should not generally
  /// be accessed directly, because all functionality is exposed through series operations.
  member x.Index = index

  /// Returns the vector associated with this series. This member should not generally
  /// be accessed directly, because all functionality is exposed through series operations.
  member x.Vector = vector

  /// Returns a collection of keys that are defined by the index of this series.
  /// Note that the length of this sequence does not match the `Values` sequence
  /// if there are missing values. To get matching sequence, use the `Observations`
  /// property or `Series.observation`.
  member x.Keys = seq { for key, _ in index.Mappings -> key }

  /// Returns a collection of values that are available in the series data.
  /// Note that the length of this sequence does not match the `Keys` sequence
  /// if there are missing values. To get matching sequence, use the `Observations`
  /// property or `Series.observation`.
  member x.Values = seq { 
    for _, a in index.Mappings do 
      let v = vector.GetValue(a) 
      if v.HasValue then yield v.Value }

  /// Returns a collection of observations that form this series. Note that this property
  /// skips over all missing (or NaN) values. Observations are returned as `KeyValuePair<K, V>` 
  /// objects. For an F# alternative that uses tuples, see `Series.observations`.
  member x.Observations = seq {
    for k, a in index.Mappings do
      let v = vector.GetValue(a)
      if v.HasValue then yield KeyValuePair(k, v.Value) }

  // ----------------------------------------------------------------------------------------------
  // Equlity
  // ----------------------------------------------------------------------------------------------

  override series.Equals(another) = 
    match another with
    | null -> false
    | :? Series<'K, 'V> as another -> 
        series.Index.Equals(another.Index) && series.Vector.Equals(another.Vector)
    | _ -> false

  override series.GetHashCode() =
    let combine h1 h2 = ((h1 <<< 5) + h1) ^^^ h2
    combine (series.Index.GetHashCode()) (series.Vector.GetHashCode())

  // ----------------------------------------------------------------------------------------------
  // IEnumerable
  // ----------------------------------------------------------------------------------------------

  interface ISeries<'K> with
    member x.TryGetObject(k) = this.TryGet(k) |> OptionalValue.map box
    member x.Vector = vector :> IVector
    member x.Index = index

  interface IFsiFormattable with
    member series.Format() = 
      if vector.SuppressPrinting then "(Suppressed)" else
        seq { for item in index.Mappings |> Seq.startAndEnd Formatting.StartItemCount Formatting.EndItemCount  do
                match item with 
                | Choice1Of3(k, a) | Choice3Of3(k, a) -> 
                    let v = vector.GetValue(a)
                    yield [ k.ToString(); "->"; v.ToString() ]
                | Choice2Of3() -> yield [ "..."; "->"; "..."] }
        |> array2D
        |> Formatting.formatTable

(*
  interface System.Collections.Generic.IEnumerable<'V> with
    member x.GetEnumerator() = 
      seq { for k, v in x.ObservationsOptional do
              if v.HasValue then yield v.Value }
      |> Seq.getEnumerator
  interface System.Collections.IEnumerable with
    member x.GetEnumerator() = (x :> seq<_>).GetEnumerator() :> System.Collections.IEnumerator
*)
  // ----------------------------------------------------------------------------------------------
  // Accessors
  // ----------------------------------------------------------------------------------------------

  member x.GetSubrange(lo, hi) =
    let newIndex, newVector = indexBuilder.GetRange(index, lo, hi, Vectors.Return 0)
    let newVector = vectorBuilder.Build(newVector, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  [<EditorBrowsable(EditorBrowsableState.Never)>]
  member x.GetSlice(lo, hi) =
    let inclusive v = v |> Option.map (fun v -> v, BoundaryBehavior.Inclusive)
    x.GetSubrange(inclusive lo, inclusive hi)

  /// Returns a new series with an index containing the specified keys.
  /// When the key is not found in the current series, the newly returned
  /// series will contain a missing value. When the second parameter is not
  /// specified, the keys have to exactly match the keys in the current series
  /// (`Lookup.Exact`).
  ///
  /// Parameters:
  ///  * `keys` - A collection of keys in the current series.
  member x.GetItems(keys) = x.GetItems(keys, Lookup.Exact)

  /// Returns a new series with an index containing the specified keys.
  /// When the key is not found in the current series, the newly returned
  /// series will contain a missing value. When the second parameter is not
  /// specified, the keys have to exactly match the keys in the current series
  /// (`Lookup.Exact`).
  ///
  /// Parameters:
  ///  * `keys` - A collection of keys in the current series.
  ///  * `lookup` - Specifies the lookup behavior when searching for keys in 
  ///    the current series. `Lookup.NearestGreater` and `Lookup.NearestSmaller`
  ///    can be used when the current series is ordered.
  member x.GetItems(keys, lookup) =    
    let newIndex = indexBuilder.Create<_>(keys, None)
    let newVector = vectorBuilder.Build(indexBuilder.Reindex(index, newIndex, lookup, Vectors.Return 0), [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)


  member x.TryGetObservation(key, lookup) =
    let address = index.Lookup(key, lookup, fun addr -> vector.GetValue(addr).HasValue) 
    match address with
    | OptionalValue.Missing -> OptionalValue.Missing
    | OptionalValue.Present(key, addr) -> vector.GetValue(addr) |> OptionalValue.map (fun v -> KeyValuePair(key, v))

  member x.GetObservation(key, lookup) =
    let res = x.TryGetObservation(key, lookup) 
    if not res.HasValue then raise (KeyNotFoundException(key.ToString()))
    else res.Value

  member x.TryGet(key, lookup) =
    x.TryGetObservation(key, lookup) |> OptionalValue.map (fun (KeyValue(_, v)) -> v)

  member x.Get(key, lookup) =
    x.GetObservation(key, lookup).Value

  /// Attempts to get a value at the specified `key`
  member x.TryGetObservation(key) = x.TryGetObservation(key, Lookup.Exact)
  member x.GetObservation(key) = x.GetObservation(key, Lookup.Exact)
  member x.TryGet(key) = x.TryGet(key, Lookup.Exact)
  member x.Get(key) = x.Get(key, Lookup.Exact)


  member x.Item with get(a) = x.Get(a)
  member x.Item with get(items) = x.GetItems items

  static member (?) (series:Series<_, _>, name:string) = series.Get(name, Lookup.Exact)

  // ----------------------------------------------------------------------------------------------
  // Operations
  // ----------------------------------------------------------------------------------------------

  member x.KeyRange = index.KeyRange

 // TODO: Avoid duplicating code here and in Frame.Join

  member series.Join<'V2>(otherSeries:Series<'K, 'V2>) =
    series.Join(otherSeries, JoinKind.Outer, Lookup.Exact)

  member series.Join<'V2>(otherSeries:Series<'K, 'V2>, kind) =
    series.Join(otherSeries, kind, Lookup.Exact)

  member series.Join<'V2>(otherSeries:Series<'K, 'V2>, kind, lookup) =
    let restrictToThisIndex (restriction:IIndex<_>) (sourceIndex:IIndex<_>) vector = 
      if restriction.Ordered && sourceIndex.Ordered then
        let min, max = index.KeyRange
        sourceIndex.Builder.GetRange(sourceIndex, Some(min, BoundaryBehavior.Inclusive), Some(max, BoundaryBehavior.Inclusive), vector)
      else sourceIndex, vector

    // Union row indices and get transformations to apply to left/right vectors
    let newIndex, thisRowCmd, otherRowCmd = 
      match kind with 
      | JoinKind.Inner ->
          indexBuilder.Intersect( (index, Vectors.Return 0), (otherSeries.Index, Vectors.Return 1) )
      | JoinKind.Left ->
          let otherRowIndex, vector = restrictToThisIndex index otherSeries.Index (Vectors.Return 1)
          let otherRowCmd = indexBuilder.Reindex(otherRowIndex, index, lookup, vector)
          index, Vectors.Return 0, otherRowCmd
      | JoinKind.Right ->
          let thisRowIndex, vector = restrictToThisIndex otherSeries.Index index (Vectors.Return 0)
          let thisRowCmd = indexBuilder.Reindex(thisRowIndex, otherSeries.Index, lookup, vector)
          otherSeries.Index, thisRowCmd, Vectors.Return 1
      | JoinKind.Outer | _ ->
          indexBuilder.Union( (index, Vectors.Return 0), (otherSeries.Index, Vectors.Return 1) )

    // ....
    let combine =
      VectorValueTransform.Create<Choice<'V, 'V2, 'V * 'V2>>(fun left right ->
        match left, right with 
        | OptionalValue.Present(Choice1Of3 l), OptionalValue.Present(Choice2Of3 r) -> 
            OptionalValue(Choice3Of3(l, r))
        | OptionalValue.Present(v), _
        | _, OptionalValue.Present(v) -> OptionalValue(v)
        | _ -> failwith "Series.Join: Unexpected vector structure")

    let inputThis : IVector<Choice<'V, 'V2, 'V * 'V2>> = vector.Select Choice1Of3 
    let inputThat : IVector<Choice<'V, 'V2, 'V * 'V2>> = otherSeries.Vector.Select Choice2Of3

    let combinedCmd = Vectors.Combine(thisRowCmd, otherRowCmd, combine)
    let newVector = vectorBuilder.Build(combinedCmd, [| inputThis; inputThat |])
    let newVector = newVector.Select(function 
      | Choice3Of3(l, r) -> OptionalValue(l), OptionalValue(r)
      | Choice1Of3(l) -> OptionalValue(l), OptionalValue.Missing
      | Choice2Of3(r) -> OptionalValue.Missing, OptionalValue(r))
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  member series.Union(another:Series<'K, 'V>) = 
    series.Union(another, UnionBehavior.PreferLeft)
  
  member series.Union(another:Series<'K, 'V>, behavior) = 
    let newIndex, vec1, vec2 = indexBuilder.Union( (series.Index, Vectors.Return 0), (another.Index, Vectors.Return 1) )
    let transform = 
      match behavior with
      | UnionBehavior.PreferRight -> VectorHelpers.VectorValueTransform.RightIfAvailable
      | UnionBehavior.Exclusive -> VectorHelpers.VectorValueTransform.LeftOrRight
      | _ -> VectorHelpers.VectorValueTransform.LeftIfAvailable
    let vecCmd = Vectors.Combine(vec1, vec2, transform)
    let newVec = vectorBuilder.Build(vecCmd, [| series.Vector; another.Vector |])
    Series(newIndex, newVec, vectorBuilder, indexBuilder)


  // TODO: Series.Select & Series.Where need to use some clever index/vector functions

  member x.Where(f:System.Func<KeyValuePair<'K, 'V>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          let included = 
            // If a required value is missing, then skip over this
            opt.HasValue && f.Invoke (KeyValuePair(key, opt.Value)) 
          if included then yield key, opt  |]
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateMissing(optValues),
        vectorBuilder, indexBuilder )

  member x.WhereOptional(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          if f.Invoke (KeyValuePair(key, opt)) then yield key, opt |]
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateMissing(optValues),
        vectorBuilder, indexBuilder )

  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, 'R>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.bind (fun v -> 
             // If a required value is missing, then skip over this
             OptionalValue(f.Invoke(KeyValuePair(key, v))) ) |]
    Series<'K, 'R>(index, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder )

  member x.SelectKeys<'R when 'R : equality>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, 'R>) = 
    let newKeys =
      [| for key, addr in index.Mappings -> 
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))) |]
    let newIndex = indexBuilder.Create(newKeys, None)
    Series<'R, _>(newIndex, vector, vectorBuilder, indexBuilder )

  member x.SelectOptional<'R>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, OptionalValue<'R>>) = 
    let newVector =
      index.Mappings |> Array.ofSeq |> Array.map (fun (key, addr) ->
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))))
    Series<'K, 'R>(index, vectorBuilder.CreateMissing(newVector), vectorBuilder, indexBuilder)

  //member x.FillMissing() = 
  //  x.Vector.SelectOptional(

  member x.DropMissing() =
    x.WhereOptional(fun (KeyValue(k, v)) -> v.HasValue)

  // Seq.head
  member x.Aggregate<'TNewKey, 'R when 'TNewKey : equality>(aggregation, valueSelector:Func<_, _>, keySelector:Func<_, _>) =
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, aggregation, Vectors.Return 0, 
          (fun (kind, (index, cmd)) -> 
              let window = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              OptionalValue(valueSelector.Invoke(DataSegment(kind, window)))),
          (fun (kind, (index, cmd)) -> 
              keySelector.Invoke(DataSegment(kind, Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)))) )
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  member x.Aggregate<'R>(aggregation, valueSelector) =
    x.Aggregate<'K, 'R>(aggregation, valueSelector, Func<_, _>(fun k -> k.Data.Keys |> Seq.head))

  member x.GroupBy(keySelector, valueSelector) =
    let newIndex, newVector = 
      indexBuilder.GroupBy
        ( x.Index, 
          (fun key -> keySelector key (x.Get(key))), Vectors.Return 0, 
          (fun (newKey, (index, cmd)) -> 
              let group = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              valueSelector newKey group) )
    Series<'TNewKey, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  member x.WithOrdinalIndex() = 
    let newIndex = indexBuilder.Create(x.Index.Keys |> Seq.mapi (fun i _ -> i), Some true)
    Series<int, _>(newIndex, vector, vectorBuilder, indexBuilder)

  member x.WithIndex(keys) = 
    let newIndex = indexBuilder.Create(keys, None)
    Series<'TNewKey, _>(newIndex, vector, vectorBuilder, indexBuilder)

  member x.Pairwise(?boundary, ?direction) =
    let boundary = defaultArg boundary Boundary.Skip
    let direction = defaultArg direction Direction.Backward
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, WindowSize(2, boundary), Vectors.Return 0, 
          (fun (kind, (index, cmd)) -> 
              let actualVector = vectorBuilder.Build(cmd, [| vector |])
              let obs = [ for k, addr in index.Mappings -> actualVector.GetValue(addr) ]
              match obs with
              | [ OptionalValue.Present v1; OptionalValue.Present v2 ] -> 
                  OptionalValue( DataSegment(kind, (v1, v2)) )
              | [ _; _ ] -> OptionalValue.Missing
              | _ -> failwith "Pairwise: failed - expected two values" ),
          (fun (kind, (index, vector)) -> 
              if direction = Direction.Backward then index.Keys |> Seq.last
              else index.Keys |> Seq.head ) )
    Series<'K, DataSegment<'V * 'V>>(newIndex, newVector, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Operators
  // ----------------------------------------------------------------------------------------------

  static member inline internal NullaryGenericOperation<'K, 'T1, 'T2>(series:Series<'K, 'T1>, op : 'T1 -> 'T2) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal NullaryOperation<'T>(series:Series<'K, 'T>, op : 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal ScalarOperationL<'T>(series:Series<'K, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline internal ScalarOperationR<'T>(scalar, series:Series<'K, 'T>, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op scalar v)

  static member inline internal VectorOperation<'T>(series1:Series<'K, 'T>, series2:Series<'K, 'T>, op) : Series<_, 'T> =
    let joined = series1.Join(series2)
    joined.SelectOptional(fun (KeyValue(_, v)) -> 
      match v with
      | OptionalValue.Present(OptionalValue.Present a, OptionalValue.Present b) -> 
          OptionalValue(op a b)
      | _ -> OptionalValue.Missing )

  static member (+) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (+))
  static member (+) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (+))
  static member (-) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (-))
  static member (-) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (-))
  static member (*) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (*))
  static member (*) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (*))
  static member (/) (scalar, series) = Series<'K, _>.ScalarOperationR<int>(scalar, series, (/))
  static member (/) (series, scalar) = Series<'K, _>.ScalarOperationL<int>(series, scalar, (/))

  static member (+) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (+))
  static member (+) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (+))
  static member (-) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (-))
  static member (-) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (-))
  static member (*) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (*))
  static member (*) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (*))
  static member (/) (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, (/))
  static member (/) (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, (/))
  static member Pow (scalar, series) = Series<'K, _>.ScalarOperationR<float>(scalar, series, ( ** ))
  static member Pow (series, scalar) = Series<'K, _>.ScalarOperationL<float>(series, scalar, ( ** ))

  static member (+) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (+))
  static member (-) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (-))
  static member (*) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (*))
  static member (/) (s1, s2) = Series<'K, _>.VectorOperation<int>(s1, s2, (/))

  static member (+) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (+))
  static member (-) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (-))
  static member (*) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (*))
  static member (/) (s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, (/))
  static member Pow(s1, s2) = Series<'K, _>.VectorOperation<float>(s1, s2, ( ** ))
  
  // Trigonometric
  static member Acos(series) = Series<'K, _>.NullaryOperation<float>(series, acos)
  static member Asin(series) = Series<'K, _>.NullaryOperation<float>(series, asin)
  static member Atan(series) = Series<'K, _>.NullaryOperation<float>(series, atan)
  static member Sin(series) = Series<'K, _>.NullaryOperation<float>(series, sin)
  static member Sinh(series) = Series<'K, _>.NullaryOperation<float>(series, sinh)
  static member Cos(series) = Series<'K, _>.NullaryOperation<float>(series, cos)
  static member Cosh(series) = Series<'K, _>.NullaryOperation<float>(series, cosh)
  static member Tan(series) = Series<'K, _>.NullaryOperation<float>(series, tan)
  static member Tanh(series) = Series<'K, _>.NullaryOperation<float>(series, tanh)

  // Actually useful
  static member Abs(series) = Series<'K, _>.NullaryOperation<float>(series, abs)
  static member Abs(series) = Series<'K, _>.NullaryOperation<int>(series, abs)
  static member Ceiling(series) = Series<'K, _>.NullaryOperation<float>(series, ceil)
  static member Exp(series) = Series<'K, _>.NullaryOperation<float>(series, exp)
  static member Floor(series) = Series<'K, _>.NullaryOperation<float>(series, floor)
  static member Truncate(series) = Series<'K, _>.NullaryOperation<float>(series, truncate)
  static member Log(series) = Series<'K, _>.NullaryOperation<float>(series, log)
  static member Log10(series) = Series<'K, _>.NullaryOperation<float>(series, log10)
  static member Round(series) = Series<'K, _>.NullaryOperation<float>(series, round)
  static member Sign(series) = Series<'K, _>.NullaryGenericOperation<_, float, _>(series, sign)
  static member Sqrt(series) = Series<'K, _>.NullaryGenericOperation<_, float, _>(series, sqrt)

  // ----------------------------------------------------------------------------------------------
  // Nicer constructor
  // ----------------------------------------------------------------------------------------------

  member series.JoinInner<'V2>(otherSeries:Series<'K, 'V2>) : Series<'K, 'V * 'V2> =
    let joined = series.Join(otherSeries, JoinKind.Inner, Lookup.Exact)
    joined.Select(fun (KeyValue(_, v)) ->
      match v with
      | OptionalValue.Present l, OptionalValue.Present r -> l, r 
      | _ -> failwith "JoinInner: Unexpected missing value")
    

  new(keys:seq<_>, values:seq<_>) = 
    let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
    let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
    Series( Index.ofKeys keys, vectorBuilder.Create (Array.ofSeq values),
            vectorBuilder, indexBuilder )

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

type ObjectSeries<'K when 'K : equality> internal(index:IIndex<_>, vector, vectorBuilder, indexBuilder) = 
  inherit Series<'K, obj>(index, vector, vectorBuilder, indexBuilder)
  
  member x.GetAs<'R>(column) : 'R = 
    System.Convert.ChangeType(x.Get(column), typeof<'R>) |> unbox
  member x.TryGetAs<'R>(column) : OptionalValue<'R> = 
    x.TryGet(column) |> OptionalValue.map (fun v -> System.Convert.ChangeType(v, typeof<'R>) |> unbox)
  static member (?) (series:ObjectSeries<_>, name:string) = series.GetAs<float>(name)

  member x.As<'R>() =
    match box vector with
    | :? IVector<'R> as vec -> Series(index, vec, vectorBuilder, indexBuilder)
    | _ -> Series(index, VectorHelpers.changeType vector, vectorBuilder, indexBuilder)
