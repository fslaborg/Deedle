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

type ISeries<'K when 'K : equality> =
  abstract Vector : FSharp.DataFrame.IVector
  abstract Index : IIndex<'K>

// :-(
type internal SeriesOperations = 
  abstract OuterJoin<'K, 'V when 'K : equality> : 
    Series<'K, 'V> * Series<'K, 'V> -> 
    Series<'K, Series<int, obj>>

/// A series contains one Index and one Vec
and Series<'K, 'V when 'K : equality>
    ( index:IIndex<'K>, vector:IVector<'V>,
      vectorBuilder : IVectorBuilder, indexBuilder : IIndexBuilder ) =
  
  // :-(((((
  static let ensureInit = Lazy.Create(fun _ ->
    let ty = System.Reflection.Assembly.GetExecutingAssembly().GetType("FSharp.DataFrame.Frame")
    let mi = ty.GetMethod("Register", System.Reflection.BindingFlags.Static ||| System.Reflection.BindingFlags.NonPublic)
    mi.Invoke(null, [||]) |> ignore )

  member internal x.Index = index
  member x.Vector = vector
  member x.ObservationsOptional = seq { for key, address in index.Mappings -> key, vector.GetValue(address) }
  member x.Observations = seq { for key, v in x.ObservationsOptional do if v.HasValue then yield key, v.Value }
  member x.Keys = seq { for key, _ in index.Mappings -> key }
  member x.Values = seq { for _, v in x.Observations -> v }

  // ----------------------------------------------------------------------------------------------
  // IEnumerable
  // ----------------------------------------------------------------------------------------------

  interface ISeries<'K> with
    member x.Vector = vector :> IVector
    member x.Index = index

  interface IFormattable with
    member series.Format() = 
      if vector.SuppressPrinting then "(Suppressed)" else
        seq { for item in series.ObservationsOptional |> Seq.startAndEnd 10 10 do
                match item with 
                | Choice1Of3(k, v) | Choice3Of3(k, v) -> yield [ k.ToString(); v.ToString() ]
                | Choice2Of3() -> yield [ "..."; "..."] }
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

  member x.GetItems(items,?lookup) =
    // TODO: Should throw when item is not in the sereis?
    let lookup = defaultArg lookup Lookup.Exact
    let newIndex = indexBuilder.Create<_>(items, None)
    let newVector = vectorBuilder.Build(indexBuilder.Reindex(index, newIndex, lookup, Vectors.Return 0), [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  member x.GetSlice(lo, hi) =
    let newIndex, newVector = indexBuilder.GetRange(index, lo, hi, Vectors.Return 0)
    let newVector = vectorBuilder.Build(newVector, [| vector |])
    Series(newIndex, newVector, vectorBuilder, indexBuilder)

  member x.TryGet(key, ?lookup) =
    let lookup = defaultArg lookup Lookup.Exact
    let address = index.Lookup(key, lookup) 
    if not address.HasValue then invalidArg "key" (sprintf "The index '%O' is not present in the series." key)
    let value = vector.GetValue(address.Value)
    value |> OptionalValue.asOption
  
  member x.Get(key, ?lookup) =
    match x.TryGet(key, ?lookup=lookup) with
    | None -> raise (ValueMissingException(key.ToString()))
    | Some v -> v

  static member (?) (series:Series<_, _>, name:string) = series.Get(name, Lookup.Exact)

  // ----------------------------------------------------------------------------------------------
  // Operations
  // ----------------------------------------------------------------------------------------------
  
  member x.Count = x.ObservationsOptional |> Seq.filter (fun (k, v) -> v.HasValue) |> Seq.length
  member x.CountOptional = x.ObservationsOptional |> Seq.length


  // TODO: Series.Select & Series.Where need to use some clever index/vector functions

  member x.Where(f:System.Func<KeyValuePair<'K, 'V>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          let included = 
            // If a required value is missing, then skip over this
            try opt.HasValue && f.Invoke (KeyValuePair(key, opt.Value)) 
            with :? ValueMissingException -> false
          if included then yield key, opt  |]
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateOptional(optValues),
        vectorBuilder, indexBuilder )

  member x.WhereOptional(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, bool>) = 
    let keys, optValues =
      [| for key, addr in index.Mappings do
          let opt = vector.GetValue(addr)
          if f.Invoke (KeyValuePair(key, opt)) then yield key, opt |]
      |> Array.unzip
    Series<_, _>
      ( indexBuilder.Create<_>(keys, None), vectorBuilder.CreateOptional(optValues),
        vectorBuilder, indexBuilder )

  member x.Select<'R>(f:System.Func<KeyValuePair<'K, 'V>, 'R>) = 
    let newVector =
      [| for key, addr in index.Mappings -> 
           vector.GetValue(addr) |> OptionalValue.bind (fun v -> 
             // If a required value is missing, then skip over this
             try OptionalValue(f.Invoke(KeyValuePair(key, v)))
             with :? ValueMissingException -> OptionalValue.Missing ) |]
    Series<'K, 'R>(index, vectorBuilder.CreateOptional(newVector), vectorBuilder, indexBuilder )

  member x.SelectOptional<'R>(f:System.Func<KeyValuePair<'K, OptionalValue<'V>>, OptionalValue<'R>>) = 
    let newVector =
      index.Mappings |> Array.ofSeq |> Array.map (fun (key, addr) ->
           f.Invoke(KeyValuePair(key, vector.GetValue(addr))))
    Series<'K, 'R>(index, vectorBuilder.CreateOptional(newVector), vectorBuilder, indexBuilder)

  member x.DropNA() =
    x.WhereOptional(fun (KeyValue(k, v)) -> v.HasValue)

  member x.Aggregate(aggregation, valueSelector, ?keySelector) =
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, aggregation, Vectors.Return 0, 
          (fun (index, cmd) -> 
              let window = Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder)
              valueSelector window),
          (fun (index, cmd) -> 
              match keySelector with 
              | None -> index.Keys |> Seq.head
              | Some f -> f (Series<_, _>(index, vectorBuilder.Build(cmd, [| vector |]), vectorBuilder, indexBuilder))) )
    Series<'K, 'R>(newIndex, newVector, vectorBuilder, indexBuilder)

  member x.WithOrdinalIndex() = 
    let newIndex = indexBuilder.Create(x.Index.Keys |> Seq.mapi (fun i _ -> i), Some true)
    Series<int, _>(newIndex, vector, vectorBuilder, indexBuilder)

  member x.Pairwise() =
    let newIndex, newVector = 
      indexBuilder.Aggregate
        ( x.Index, WindowSize 2, Vectors.Return 0, 
          (fun (index, cmd) -> 
              let actualVector = vectorBuilder.Build(cmd, [| vector |])
              let obs = [ for k, addr in index.Mappings -> actualVector.GetValue(addr) ]
              match obs with
              | [ OptionalValue.Present v1; OptionalValue.Present v2 ] -> OptionalValue( (v1, v2) )
              | [ _; _ ] -> OptionalValue.Missing
              | _ -> failwith "Pairwise: failed - expected two values" ),
          (fun (index, vector) -> index.Keys |> Seq.head) )
    Series<'K, 'V * 'V>(newIndex, newVector, vectorBuilder, indexBuilder)

  // ----------------------------------------------------------------------------------------------
  // Operators
  // ----------------------------------------------------------------------------------------------

  // :-((
  static member val internal SeriesOperations : SeriesOperations = Unchecked.defaultof<_> with get, set

  // Float
  static member inline internal NullaryGenericOperation<'K, 'T1, 'T2>(series:Series<'K, 'T1>, op : 'T1 -> 'T2) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal NullaryOperation<'K, 'T>(series:Series<'K, 'T>, op : 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v)
  static member inline internal ScalarOperationL<'K, 'T>(series:Series<'K, 'T>, scalar, op : 'T -> 'T -> 'T) = 
    series.Select(fun (KeyValue(k, v)) -> op v scalar)
  static member inline internal ScalarOperationR<'K, 'T>(scalar, series:Series<'K, 'T>, op : 'T -> 'T -> 'T) = 
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
  static member inline internal VectorOperation<'K, 'T>(series1:Series<'K, 'T>, series2:Series<'K, 'T>, op) : Series<_, 'T> =
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

  // Trigonometric
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Acos(series) = Series<_, _>.NullaryOperation<_, float>(series, acos)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Asin(series) = Series<_, _>.NullaryOperation<_, float>(series, asin)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Atan(series) = Series<_, _>.NullaryOperation<_, float>(series, atan)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Sin(series) = Series<_, _>.NullaryOperation<_, float>(series, sin)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Sinh(series) = Series<_, _>.NullaryOperation<_, float>(series, sinh)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Cos(series) = Series<_, _>.NullaryOperation<_, float>(series, cos)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Cosh(series) = Series<_, _>.NullaryOperation<_, float>(series, cosh)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Tan(series) = Series<_, _>.NullaryOperation<_, float>(series, tan)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Tanh(series) = Series<_, _>.NullaryOperation<_, float>(series, tanh)

  // Actually useful
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Abs(series) = Series<_, _>.NullaryOperation<_, float>(series, abs)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Abs(series) = Series<_, _>.NullaryOperation<_, int>(series, abs)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Ceiling(series) = Series<_, _>.NullaryOperation<_, float>(series, ceil)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Exp(series) = Series<_, _>.NullaryOperation<_, float>(series, exp)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Floor(series) = Series<_, _>.NullaryOperation<_, float>(series, floor)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Truncate(series) = Series<_, _>.NullaryOperation<_, float>(series, truncate)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Log(series) = Series<_, _>.NullaryOperation<_, float>(series, log)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Log10(series) = Series<_, _>.NullaryOperation<_, float>(series, log10)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Round(series) = Series<_, _>.NullaryOperation<_, float>(series, round)

  // May return different type  
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Sign(series) = Series<_, _>.NullaryGenericOperation<_, float, _>(series, sign)
  [<CompilerMessage(null, -1, IsHidden=true, IsError=false)>]
  static member Sqrt(series) = Series<_, _>.NullaryGenericOperation<_, float, _>(series, sqrt)

  // TODO: **

  // ----------------------------------------------------------------------------------------------
  // Nicer constructor
  // ----------------------------------------------------------------------------------------------

  new(keys:seq<_>, values:seq<_>) = 
    let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
    let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
    Series( Index.Create keys, vectorBuilder.CreateNonOptional (Array.ofSeq values),
            vectorBuilder, indexBuilder )

// ------------------------------------------------------------------------------------------------
// Untyped series
// ------------------------------------------------------------------------------------------------

type ObjectSeries<'K when 'K : equality> internal(index:IIndex<_>, vector, vectorBuilder, indexBuilder) = 
  inherit Series<'K, obj>(index, vector, vectorBuilder, indexBuilder)
  
  member x.GetAs<'R>(column) : 'R = 
    System.Convert.ChangeType(x.Get(column), typeof<'R>) |> unbox
  member x.TryGetAs<'R>(column) : 'R option = 
    x.TryGet(column) |> Option.map (fun v -> System.Convert.ChangeType(v, typeof<'R>) |> unbox)
  static member (?) (series:ObjectSeries<_>, name:string) = series.GetAs<float>(name)

// ------------------------------------------------------------------------------------------------
// Construction
// ------------------------------------------------------------------------------------------------

type internal Series = 
  /// Vector & index builders
  static member vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
  static member indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

  static member Create(data:seq<'V>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'V>(Index.Create(lookup), Vector.Create(data), Series.vectorBuilder, Series.indexBuilder)
  static member Create(index:seq<'K>, data:seq<'V>) =
    Series<'K, 'V>(Index.Create(index), Vector.Create(data), Series.vectorBuilder, Series.indexBuilder)
  static member Create(index:IIndex<'K>, data:IVector<'V>) = 
    Series<'K, 'V>(index, data, Series.vectorBuilder, Series.indexBuilder)
  static member CreateUntyped(index:IIndex<'K>, data:IVector<obj>) = 
    ObjectSeries<'K>(index, data, Series.vectorBuilder, Series.indexBuilder)

type SeriesBuilder<'K when 'K : equality>() = 
  let mutable keys = []
  let mutable values = []

  member x.Add<'V>(key:'K, value) =
    keys <- key::keys
    values <- (box value)::values
  
  member x.Series =
    Series.CreateUntyped(Index.Create (List.rev keys), Vector.Create(List.rev values))

  static member (?<-) (builder:SeriesBuilder<string>, name:string, value) =
    builder.Add(name, value)
  

// ------------------------------------------------------------------------------------------------
// Operations etc.
// ------------------------------------------------------------------------------------------------

[<AutoOpen>] 
module SeriesExtensions =
  type Series<'K, 'V when 'K : equality> with
    member x.Item with get(a) = x.Get(a)
    member x.Item with get(a, b) = x.GetItems [a; b] 
    member x.Item with get(a, b, c) = x.GetItems [a; b; c] 
    member x.Item with get(a, b, c, d) = x.GetItems [a; b; c; d]
    member x.Item with get(a, b, c, d, e) = x.GetItems [a; b; c; d; e]
    member x.Item with get(a, b, c, d, e, f) = x.GetItems [a; b; c; d; e; f]
    member x.Item with get(a, b, c, d, e, f, g) = x.GetItems [a; b; c; d; e; f; g]
    member x.Item with get(a, b, c, d, e, f, g, h) = x.GetItems [a; b; c; d; e; f; g; h] 

