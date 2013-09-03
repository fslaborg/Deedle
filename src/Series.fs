namespace FSharp.DataFrame

open System.Collections.Generic
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices
open FSharp.DataFrame.Vectors

type ValueMissingException(column) =
  inherit System.Exception(sprintf "The value for index '%s' is empty." column)
  member x.Column = column

// ------------------------------------------------------------------------------------------------
// Series
// ------------------------------------------------------------------------------------------------

type ISeries<'TKey when 'TKey : equality> =
  abstract Vector : FSharp.DataFrame.IVector<int>
  abstract Index : IIndex<'TKey, int>

// :-(
type internal SeriesOperations = 
  abstract OuterJoin<'TKey, 'TValue when 'TKey : equality> : 
    Series<'TKey, 'TValue> * Series<'TKey, 'TValue> -> 
    Series<'TKey, Series<int, obj>>

/// A series contains one Index and one Vec
and Series<'TKey, 'TValue when 'TKey : equality>
    internal (index:IIndex<'TKey, int>, vector:IVector<int, 'TValue>) =
  
  /// Vector & index builders
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
  let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

  // :-(((((
  static let ensureInit = Lazy.Create(fun _ ->
    let ty = System.Reflection.Assembly.GetExecutingAssembly().GetType("FSharp.DataFrame.Frame")
    ty.GetMethod("Register").Invoke(null, [||]) |> ignore )

  member internal x.Index = index
  member x.Vector = vector
  member x.ObservationsOptional = seq { for key, address in index.Mappings -> key, vector.GetValue(address) }
  member x.Observations = seq { for key, v in x.ObservationsOptional do if v.HasValue then yield key, v.Value }
  member x.Keys = seq { for key, _ in index.Mappings -> key }
  member x.Values = seq { for _, v in x.Observations -> v }

  // ----------------------------------------------------------------------------------------------
  // IEnumerable
  // ----------------------------------------------------------------------------------------------

  interface ISeries<'TKey> with
    member x.Vector = vector :> IVector<int>
    member x.Index = index

  interface IFormattable with
    member series.Format() = 
      seq { for k, v in series.ObservationsOptional do
              yield [ k.ToString(); v.ToString() ] }
      |> array2D
      |> Formatting.formatTable

(*
  interface System.Collections.Generic.IEnumerable<'TValue> with
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

  member x.GetItems(items,?semantics) =
    // TODO: Should throw when item is not in the sereis?
    let semantics = defaultArg semantics LookupSemantics.Exact
    let newIndex = indexBuilder.Create<_, int>(items, None)
    let newVector = vectorBuilder.Build(indexBuilder.Reindex(index, newIndex, semantics, Vectors.Return 0), [| vector |])
    Series(newIndex, newVector)

  member x.GetSlice(lo, hi) =
    let newIndex, newVector = indexBuilder.GetRange(index, lo, hi, Vectors.Return 0)
    let newVector = vectorBuilder.Build(newVector, [| vector |])
    Series(newIndex, newVector) 

  member x.TryGet(key, ?semantics) =
    let semantics = defaultArg semantics LookupSemantics.Exact
    let address = index.Lookup(key, semantics) 
    if not address.HasValue then invalidArg "key" (sprintf "The index '%O' is not present in the series." key)
    let value = vector.GetValue(address.Value)
    value |> OptionalValue.asOption
  
  member x.Get(key, ?semantics) =
    match x.TryGet(key, ?semantics=semantics) with
    | None -> raise (ValueMissingException(key.ToString()))
    | Some v -> v

  static member (?) (series:Series<_, _>, name:string) = series.Get(name, LookupSemantics.Exact)

  // ----------------------------------------------------------------------------------------------
  // Operations
  // ----------------------------------------------------------------------------------------------
  
  member x.Count = x.ObservationsOptional |> Seq.filter (fun (k, v) -> v.HasValue) |> Seq.length
  member x.CountOptional = x.ObservationsOptional |> Seq.length


  // TODO: Series.Select & Series.Where need to use some clever index/vector functions

  member x.Where(f:System.Func<KeyValuePair<'TKey, 'TValue>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          let included = 
            // If a required value is missing, then skip over this
            try opt.HasValue && f.Invoke (KeyValuePair(key, opt.Value)) 
            with :? ValueMissingException -> false
          if included then yield key, opt  |]
      |> Array.unzip
    Series<_, _>(indexBuilder.Create<_, int>(keys, None), vectorBuilder.CreateOptional(optValues))

  member x.WhereOptional(f:System.Func<KeyValuePair<'TKey, OptionalValue<'TValue>>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          if f.Invoke (KeyValuePair(key, opt)) then yield key, opt |]
      |> Array.unzip
    Series<_, _>(indexBuilder.Create<_, int>(keys, None), vectorBuilder.CreateOptional(optValues))

  member x.Select<'R>(f:System.Func<KeyValuePair<'TKey, 'TValue>, 'R>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.bind (fun v -> 
             // If a required value is missing, then skip over this
             try OptionalValue(f.Invoke(KeyValuePair(key, v)))
             with :? ValueMissingException -> OptionalValue.Missing ) |]
    Series<'TKey, 'R>(index, vectorBuilder.CreateOptional(newVector))

  member x.SelectOptional<'R>(f:System.Func<KeyValuePair<'TKey, OptionalValue<'TValue>>, OptionalValue<'R>>) = 
    let newVector =
      index.Mappings |> Array.ofSeq |> Array.map (fun (key, addr) ->
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))))
    Series<'TKey, 'R>(index, vectorBuilder.CreateOptional(newVector))

  member x.DropNA() =
    x.WhereOptional(fun (KeyValue(k, v)) -> v.HasValue)

  // ----------------------------------------------------------------------------------------------
  // Operators
  // ----------------------------------------------------------------------------------------------

  // :-((
  static member val internal SeriesOperations : SeriesOperations = Unchecked.defaultof<_> with get, set

  // Float
  static member inline ScalarOperationL<'TKey, 'T>(series:Series<'TKey, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline ScalarOperationR<'TKey, 'T>(scalar, series:Series<'TKey, 'T>, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op scalar v)

  static member (+) (scalar, series) = Series<_, _>.ScalarOperationR<_, int>(scalar, series, (+))
  static member (+) (series, scalar) = Series<_, _>.ScalarOperationL<_, int>(series, scalar, (+))
  static member (-) (scalar, series) = Series<_, _>.ScalarOperationR<_, int>(scalar, series, (-))
  static member (-) (series, scalar) = Series<_, _>.ScalarOperationL<_, int>(series, scalar, (-))
  static member (*) (scalar, series) = Series<_, _>.ScalarOperationR<_, int>(scalar, series, (*))
  static member (*) (series, scalar) = Series<_, _>.ScalarOperationL<_, int>(series, scalar, (*))
  static member (/) (scalar, series) = Series<_, _>.ScalarOperationR<_, int>(scalar, series, (/))
  static member (/) (series, scalar) = Series<_, _>.ScalarOperationL<_, int>(series, scalar, (/))

  static member (+) (scalar, series) = Series<_, _>.ScalarOperationR<_, float>(scalar, series, (+))
  static member (+) (series, scalar) = Series<_, _>.ScalarOperationL<_, float>(series, scalar, (+))
  static member (-) (scalar, series) = Series<_, _>.ScalarOperationR<_, float>(scalar, series, (-))
  static member (-) (series, scalar) = Series<_, _>.ScalarOperationL<_, float>(series, scalar, (-))
  static member (*) (scalar, series) = Series<_, _>.ScalarOperationR<_, float>(scalar, series, (*))
  static member (*) (series, scalar) = Series<_, _>.ScalarOperationL<_, float>(series, scalar, (*))
  static member (/) (scalar, series) = Series<_, _>.ScalarOperationR<_, float>(scalar, series, (/))
  static member (/) (series, scalar) = Series<_, _>.ScalarOperationL<_, float>(series, scalar, (/))

  // Float
  static member inline internal VectorOperation<'TKey, 'T>(series1:Series<'TKey, 'T>, series2:Series<'TKey, 'T>, op) : Series<_, 'T> =
    ensureInit.Value
    let joined = Series<_, _>.SeriesOperations.OuterJoin(series1, series2)
    joined.SelectOptional(fun (KeyValue(_, v)) -> 
      match v.Value.TryGet(0), v.Value.TryGet(1) with
      | Some a, Some b -> OptionalValue(op (a :?> 'T) (b :?> 'T))
      | _ -> OptionalValue.Missing )

  static member (+) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (+))
  static member (-) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (-))
  static member (*) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (*))
  static member (/) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (/))

  static member (+) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (+))
  static member (-) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (-))
  static member (*) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (*))
  static member (/) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (/))

  // ----------------------------------------------------------------------------------------------
  // Nicer constructor
  // ----------------------------------------------------------------------------------------------

  new(keys:seq<_>, values:seq<_>) = 
    let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
    Series(Index.Create keys, vectorBuilder.CreateNonOptional (Array.ofSeq values))

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

type ObjectSeries<'TKey when 'TKey : equality> internal(index:IIndex<_, _>, vector) = 
  inherit Series<'TKey, obj>(index, vector)
  
  member x.GetAs<'R>(column) : 'R = 
    System.Convert.ChangeType(x.Get(column), typeof<'R>) |> unbox
  member x.TryGetAs<'R>(column) : 'R option = 
    x.TryGet(column) |> Option.map (fun v -> System.Convert.ChangeType(v, typeof<'R>) |> unbox)
  static member (?) (series:ObjectSeries<_>, name:string) = series.GetAs<float>(name)

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

type Series = 
  static member Create(data:seq<'TValue>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'TValue>(Index.Create(lookup), Vector.Create(data))
  static member Create(index:seq<'TKey>, data:seq<'TValue>) =
    Series<'TKey, 'TValue>(Index.Create(index), Vector.Create(data))
  static member Create(index:IIndex<'TKey, int>, data:IVector<int, 'TValue>) = 
    Series<'TKey, 'TValue>(index, data)
  static member CreateUntyped(index:IIndex<'TKey, int>, data:IVector<int, obj>) = 
    ObjectSeries<'TKey>(index, data)

type SeriesBuilder<'TKey when 'TKey : equality>() = 
  let mutable keys = []
  let mutable values = []

  member x.Add<'TValue>(key:'TKey, value) =
    keys <- key::keys
    values <- (box value)::values
  
  member x.Series =
    Series.CreateUntyped(Index.Create (List.rev keys), Vector.Create(List.rev values))

  static member (?<-) (builder:SeriesBuilder<string>, name:string, value) =
    builder.Add(name, value)
  

// ------------------------------------------------------------------------------------------------
// Operations etc.
// ------------------------------------------------------------------------------------------------

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series = 
  let inline sum (series:Series<_, _>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> IReadOnlyList.sum list
    | VectorData.SparseList list -> IReadOnlyList.sumOptional list
    | VectorData.Sequence seq -> Seq.sum (Seq.choose OptionalValue.asOption seq)
  let inline mean (series:Series<_, _>) = 
    match series.Vector.Data with
    | VectorData.DenseList list -> IReadOnlyList.average list
    | VectorData.SparseList list -> IReadOnlyList.averageOptional list
    | VectorData.Sequence seq -> Seq.average (Seq.choose OptionalValue.asOption seq)

[<AutoOpen>] 
module SeriesExtensions =
  type Series<'TKey, 'TValue when 'TKey : equality> with
    member x.Item with get(a) = x.Get(a)
    member x.Item with get(a, b) = x.GetItems [a; b] 
    member x.Item with get(a, b, c) = x.GetItems [a; b; c] 
    member x.Item with get(a, b, c, d) = x.GetItems [a; b; c; d]
    member x.Item with get(a, b, c, d, e) = x.GetItems [a; b; c; d; e]
    member x.Item with get(a, b, c, d, e, f) = x.GetItems [a; b; c; d; e; f]
    member x.Item with get(a, b, c, d, e, f, g) = x.GetItems [a; b; c; d; e; f; g]
    member x.Item with get(a, b, c, d, e, f, g, h) = x.GetItems [a; b; c; d; e; f; g; h] 

