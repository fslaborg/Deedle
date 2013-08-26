namespace FSharp.DataFrame

open FSharp.DataFrame.Common
open FSharp.DataFrame.Indices

// ------------------------------------------------------------------------------------------------
// Series
// ------------------------------------------------------------------------------------------------

/// A series contains one Index and one Vec
type Series<'TIndex, 'TValue when 'TIndex : equality>(index:IIndex<'TIndex, int>, vector:IVector<int, 'TValue>) =
  
  /// Vector builder
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance

  member internal x.Index = index
  member x.Vector = vector
  member x.Observations = seq { for key, address in index.Mappings -> key, vector.GetValue(address) }
  member x.Keys = seq { for key, _ in index.Mappings -> key }

  // ----------------------------------------------------------------------------------------------
  // Accessors

  member x.GetItems(items) =
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

  member x.Map<'R>(f:'TIndex -> 'TValue -> 'R) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.map (f key) |]
    Series<'TIndex, 'R>(index, vectorBuilder.CreateOptional(newVector))

  member x.MapMissing<'R>(f:'TIndex -> option<'TValue> -> option<'R>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.asOption |> f key |> OptionalValue.ofOption |]
    Series<'TIndex, 'R>(index, vectorBuilder.CreateOptional(newVector))

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

