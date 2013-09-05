#I "../bin"
#I "../lib"
#I "../packages"
#load "FSharp.DataFrame.fsx"
#load "FSharp.Charting.fsx"
open System
open FSharp.DataFrame
open FSharp.Charting
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.Indices

/// --------------------------------------------------------------------------------------

type LazyVector internal (min:DateTime, max:DateTime, column) = 
  let vector = Lazy.Create(fun () ->
    printfn "QUERY: %A to %A" min max
    let count = int (max - min).TotalDays 
    let data = [| for i in 0 .. count - 1 -> 3.0 |]
    ArrayVector.ArrayVectorBuilder.Instance.CreateNonOptional(data) )

  member x.Minimal = min
  member x.Maximal = max
  member x.Column = column

  interface IVector with
    member val ElementType = typeof<float>
    member x.SuppressPrinting = true
    member x.GetObject(index) = vector.Value.GetObject(index)
  interface IVector<float> with
    member x.GetValue(index) = vector.Value.GetValue(index)
    member x.Data = vector.Value.Data
    member x.SelectOptional(f) = vector.Value.SelectOptional(f)
    member x.Select(f) = vector.Value.Select(f)

type LazyVectorBuilder() = 
  let builder = ArrayVector.ArrayVectorBuilder.Instance
  interface IVectorBuilder with
    member x.CreateNonOptional(values) =
      builder.CreateNonOptional(values)
    member x.CreateOptional(optValues) =
      builder.CreateOptional(optValues)
    member x.Build<'T>(command:VectorConstruction, arguments:IVector<'T>[]) = 
      match command with 
      | GetRange(vector, (Address.Custom(:? DateTime as lo), Address.Custom(:? DateTime as hi))) ->
          match builder.Build(vector, arguments) with
          | :? LazyVector as lv ->
            unbox<IVector<'T>> (LazyVector(max lv.Minimal lo, min lv.Maximal hi, lv.Column))
          | _ ->           
            builder.Build(command, arguments)
      | _ -> builder.Build(command, arguments)

/// --------------------------------------------------------------------------------------

type LazyIndex(min:DateTime, max:DateTime) =

  let index = Lazy.Create(fun () ->
    let count = int (max - min).TotalDays 
    let keys = [| for i in 0 .. count - 1 -> min.AddDays(float i) |]
    Linear.LinearIndexBuilder.Instance.Create(keys, Some true) )

  interface IIndex<DateTime> with
    member x.Keys = index.Value.Keys
    member x.Lookup(key, semantics) = index.Value.Lookup(key, semantics)
    member x.Mappings = index.Value.Mappings
    member x.Range = index.Value.Range
    member x.Ordered = index.Value.Ordered
    member x.Comparer = index.Value.Comparer

type LazyIndexBuilder() =
  let builder = Linear.LinearIndexBuilder.Instance
  interface IIndexBuilder with
    member x.Create(keys, ordered) = builder.Create(keys, ordered)
    member x.Aggregate(index, aggregation, vector, valueSel, keySel) = builder.Aggregate(index, aggregation, vector, valueSel, keySel)
    member x.OrderIndex(index, vector) = builder.OrderIndex(index, vector)
    member x.Union(index1, index2, vector1, vector2) = builder.Union(index1, index2, vector1, vector2)
    member x.Append(index1, index2, vector1, vector2, transform) = builder.Append(index1, index2, vector1, vector2, transform)
    member x.Intersect(index1, index2, vector1, vector2) = builder.Intersect(index1, index2, vector1, vector2)
    member x.WithIndex(index1, f, vector) = builder.WithIndex(index1, f, vector)
    member x.Reindex(index1, index2, semantics, vector) = builder.Reindex(index1, index2, semantics, vector)
    member x.DropItem(index, key, vector) = builder.DropItem(index, key, vector)
    member x.GetRange(index, lo:option<'K>, hi:option<'K>, vector) = 
      if typeof<'K> = typeof<DateTime> then
        let lo = defaultArg (unbox<option<DateTime>> lo) DateTime.MinValue
        let hi = defaultArg (unbox<option<DateTime>> hi) DateTime.MaxValue
        let cmd = Vectors.GetRange(vector, (Address.Custom(box lo), Address.Custom(box hi)))
        index, cmd 
      else
        builder.GetRange(index, lo, hi, vector)

/// --------------------------------------------------------------------------------------


let loadSeries min max = 
  let index = LazyIndex(min, max)
  let vector = LazyVector(min, max, "Open")
  Series(index, vector, LazyVectorBuilder(), LazyIndexBuilder())


let s1 = loadSeries (DateTime(2000,1,1)) (DateTime(2015,1,1))
let s2 = s1.[DateTime(2013,1,1) .. ]
let s3 = s2.[ .. DateTime(2013,2,1)]

s3.Count