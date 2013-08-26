namespace FSharp.DataFrame

open System.Collections.Generic
open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices

// ------------------------------------------------------------------------------------------------
// Series
// ------------------------------------------------------------------------------------------------

// :-(
type SeriesOperations = 
  abstract OuterJoin<'TIndex, 'TValue when 'TIndex : equality> : 
    Series<'TIndex, 'TValue> * Series<'TIndex, 'TValue> -> 
    Series<'TIndex, Series<int, obj>>

/// A series contains one Index and one Vec
and Series<'TIndex, 'TValue when 'TIndex : equality>(index:IIndex<'TIndex, int>, vector:IVector<int, 'TValue>) =
  
  /// Vector builder
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance

  // :-(((((
  static let ensureInit = Lazy.Create(fun _ ->
    let ty = System.Reflection.Assembly.GetExecutingAssembly().GetType("FSharp.DataFrame.Frame")
    ty.GetMethod("Register").Invoke(null, [||]) |> ignore )

  member internal x.Index = index
  member x.Vector = vector
  member x.Observations = seq { for key, address in index.Mappings -> key, vector.GetValue(address) }
  member x.Keys = seq { for key, _ in index.Mappings -> key }

  // ----------------------------------------------------------------------------------------------
  // Accessors

  member x.GetItems(items) =
    // TODO: Should throw when item is not in the sereis?
    let newIndex = Index.CreateUnsorted(items)
    let newVector = vectorBuilder.Build(index.Reindex(newIndex, Vectors.Return 0), [| vector |])
    Series(newIndex, newVector)

  member x.GetSlice(lo, hi) =
    let newIndex, newVector = index.GetRange(lo, hi, Vectors.Return 0)
    let newVector = vectorBuilder.Build(newVector, [| vector |])
    Series(newIndex, newVector) 

  member x.TryGet(key) =
    let address = index.Lookup(key) 
    if not address.HasValue then invalidArg "key" (sprintf "The index '%O' is not present in the series." key)
    let value = vector.GetValue(address.Value)
    value |> OptionalValue.asOption
  
  member x.Get(key) =
    match x.TryGet(key) with
    | None -> invalidArg "key" (sprintf "The value for index '%O' is empty." key)
    | Some v -> v

  static member (?) (series:Series<_, _>, name:string) = series.Get(name)

  // ----------------------------------------------------------------------------------------------
  // Operations
  
  // TODO: Series.Select & Series.Where need to use some clever index/vector functions

  member x.Where(f:System.Func<KeyValuePair<'TIndex, 'TValue>, bool>) = 
    let newVector =
      [| for key, addr in index.Mappings ->
          let opt = vector.GetValue(addr)
          if opt.HasValue && (f.Invoke (KeyValuePair(key, opt.Value))) then opt
          else OptionalValue.Empty |]
    Series<'TIndex, 'TValue>(index, vectorBuilder.CreateOptional(newVector))

  member x.Select<'R>(f:System.Func<KeyValuePair<'TIndex, 'TValue>, 'R>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.map (fun v -> f.Invoke(KeyValuePair(key, v))) |]
    Series<'TIndex, 'R>(index, vectorBuilder.CreateOptional(newVector))

  member x.SelectMissing<'R>(f:System.Func<KeyValuePair<'TIndex, OptionalValue<'TValue>>, OptionalValue<'R>>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))) |]
    Series<'TIndex, 'R>(index, vectorBuilder.CreateOptional(newVector))

  // ----------------------------------------------------------------------------------------------
  // Operators

  // :-((
  static member val SeriesOperations : SeriesOperations = Unchecked.defaultof<_> with get, set

  // Float
  static member inline ScalarOperationL<'TIndex, 'T>(series:Series<'TIndex, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline ScalarOperationR<'TIndex, 'T>(scalar, series:Series<'TIndex, 'T>, op : 'T -> 'T -> 'T) = 
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
  static member inline internal VectorOperation<'TIndex, 'T>(series1:Series<'TIndex, 'T>, series2:Series<'TIndex, 'T>, op) : Series<_, 'T> =
    ensureInit.Value
    let joined = Series<_, _>.SeriesOperations.OuterJoin(series1, series2)
    joined.SelectMissing(fun (KeyValue(_, v)) -> 
      match v.Value.TryGet(0), v.Value.TryGet(1) with
      | Some a, Some b -> OptionalValue(op (a :?> 'T) (b :?> 'T))
      | _ -> OptionalValue.Empty )

  static member (+) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (+))
  static member (-) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (-))
  static member (*) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (*))
  static member (/) (s1, s2) = Series<_, _>.VectorOperation<_, int>(s1, s2, (/))

  static member (+) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (+))
  static member (-) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (-))
  static member (*) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (*))
  static member (/) (s1, s2) = Series<_, _>.VectorOperation<_, float>(s1, s2, (/))

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

type Series<'TIndex when 'TIndex : equality>(index, vector) = 
  inherit Series<'TIndex, obj>(index, vector)
  
  member x.GetAs<'R>(column) : 'R = 
    System.Convert.ChangeType(x.Get(column), typeof<'R>) |> unbox
  member x.TryGetAs<'R>(column) : 'R option = 
    x.TryGet(column) |> Option.map (fun v -> System.Convert.ChangeType(v, typeof<'R>) |> unbox)
  static member (?) (series:Series<_>, name:string) = series.GetAs<float>(name)

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

type Series = 
  static member Create(data:seq<'TValue>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'TValue>(Index.Create(lookup), Vector.Create(data))
  static member Create(index:seq<'TIndex>, data:seq<'TValue>) =
    Series<'TIndex, 'TValue>(Index.Create(index), Vector.Create(data))
  static member Create(index:IIndex<'TIndex, int>, data:IVector<int, 'TValue>) = 
    Series<'TIndex, 'TValue>(index, data)
  static member CreateUntyped(index:IIndex<'TIndex, int>, data:IVector<int, obj>) = 
    Series<'TIndex>(index, data)

// ------------------------------------------------------------------------------------------------
// Operations etc.
// ------------------------------------------------------------------------------------------------

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series = 
  let inline sum (series:Series<_, _>) = 
    match series.Vector.Data with
    | DenseList list -> IReadOnlyList.sum list
    | SparseList list -> IReadOnlyList.sumOptional list
    | Sequence seq -> Seq.sum (Seq.choose OptionalValue.asOption seq)
  let inline mean (series:Series<_, _>) = 
    match series.Vector.Data with
    | DenseList list -> IReadOnlyList.average list
    | SparseList list -> IReadOnlyList.averageOptional list
    | Sequence seq -> Seq.average (Seq.choose OptionalValue.asOption seq)

[<AutoOpen>] 
module SeriesExtensions =
  type Series<'TIndex, 'TValue when 'TIndex : equality> with
    member x.Item with get(a) = x.Get(a)
    member x.Item with get(a, b) = x.GetItems [a; b] 
    member x.Item with get(a, b, c) = x.GetItems [a; b; c] 
    member x.Item with get(a, b, c, d) = x.GetItems [a; b; c; d]
    member x.Item with get(a, b, c, d, e) = x.GetItems [a; b; c; d; e]
    member x.Item with get(a, b, c, d, e, f) = x.GetItems [a; b; c; d; e; f]
    member x.Item with get(a, b, c, d, e, f, g) = x.GetItems [a; b; c; d; e; f; g]
    member x.Item with get(a, b, c, d, e, f, g, h) = x.GetItems [a; b; c; d; e; f; g; h] 

