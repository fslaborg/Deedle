namespace Deedle

// ------------------------------------------------------------------------------------------------
// Wrapping in Deedle
// ------------------------------------------------------------------------------------------------

open Deedle
open Deedle.Internal

open Deedle.Vectors
open Deedle.Indices
open System.Collections.Generic
open System.Collections.ObjectModel

type BlockStore<'K, 'V when 'K : equality> = 
  abstract Length : int64
  abstract KeyAt : int64 -> 'K
  abstract LookupKey : 'K * Lookup * (Addressing.Address -> bool) -> OptionalValue<'K * int64>
  abstract ValueAt : int64 -> OptionalValue<'V>
  abstract GetAddressRange : int64 * int64 -> BlockStore<'K, 'V>
  
/// A polymorphic function that is passed to IBlockStoreIndex.Invoke
type BlockStoreIndexFunction<'K, 'R when 'K : equality> = 
  abstract Invoke<'V> : BlockStoreIndex<'K, 'V> -> 'R

/// In the BlockStoreIndexBuilder, we do not know the type of values, so this 
/// is a less generic interface that gives us a way for accessing it...
and IBlockStoreIndex<'K when 'K : equality> =
  abstract Invoke<'R> : BlockStoreIndexFunction<'K, 'R> -> 'R

and BlockStoreIndex<'K, 'V when 'K : equality>(store:BlockStore<'K, 'V>) =
  member x.BlockStore = store
  interface IBlockStoreIndex<'K> with
    member x.Invoke(func) = func.Invoke(x)
  interface IIndex<'K> with
    member x.KeyAt(index) = store.KeyAt(index)
    member x.KeyCount = store.Length
    member x.IsEmpty = store.Length = 0L
    member x.Builder = failwith "Builder: TODO!!" :> IIndexBuilder
    member x.KeyRange = store.KeyAt(0L), store.KeyAt(store.Length - 1L)
    member x.Keys = Array.init (int store.Length) (int64 >> store.KeyAt) |> ReadOnlyCollection.ofArray 
    member x.Locate(key) = 
      let loc = store.LookupKey(key, Lookup.Exact, fun _ -> true)
      if loc.HasValue then snd loc.Value else Addressing.Address.Invalid
    member x.Lookup(key, semantics, check) = store.LookupKey(key, semantics, check)
    member x.Mappings = seq { for i in 0L .. store.Length-1L -> store.KeyAt(i), i }
    member x.IsOrdered = true
    member x.Comparer = Comparer<'K>.Default

and BlockStoreVector<'K, 'V when 'K : equality>(store:BlockStore<'K, 'V>) = 
  member x.BlockStore = store
  interface IVector with
    member val ElementType = typeof<'V>
    member x.SuppressPrinting = false
    member x.GetObject(index) = store.ValueAt(index) |> OptionalValue.map box
    member x.ObjectSequence = seq { for i in 0L .. store.Length-1L -> store.ValueAt(i) |> OptionalValue.map box }
    member x.Invoke(site) = site.Invoke<'V>(x)
  interface IVector<'V> with
    member x.GetValue(index) = store.ValueAt(index)
    member x.Data = seq { for i in 0L .. store.Length-1L -> store.ValueAt(i) } |> VectorData.Sequence
    member x.SelectMissing(f) = failwith "SelectMissing: TODO!!"
    member x.Select(f) = failwith "Select: TODO!!"

and BlockStoreIndexBuilder() = 
  let builder = IndexBuilder.Instance
  interface IIndexBuilder with
    member x.Create<'K when 'K : equality>(keys:seq<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Create<'K when 'K : equality>(keys:ReadOnlyCollection<'K>, ordered:option<bool>) : IIndex<'K> = failwith "Create"
    member x.Aggregate(index, aggregation, vector, selector) = failwith "Aggregate"
    member x.GroupBy(index, keySel, vector) = failwith "GroupBy"
    member x.OrderIndex(sc) = failwith "OrderIndex"
    member x.Shift(sc, offset) = failwith "Shift"
    member x.Union(sc1, sc2) = failwith "Union"
    member x.Intersect(sc1, sc2) = failwith "Intersect"
    member x.Merge(scs, transform) = failwith "Merge"
    member x.LookupLevel(sc, key) = failwith "LookupLevel"
    member x.WithIndex(index1, f, vector) = failwith "WithIndex"
    member x.Reindex(index1, index2, semantics, vector, cond) = failwith "ReIndex"
    member x.DropItem(sc, key) = failwith "DropItem"
    member x.Resample(index, keys, close, vect, selector) = failwith "Resample"
    member x.GetAddressRange((index, vector), (lo, hi)) = 
      match index with 
      | :? IBlockStoreIndex<'K> as bi ->       
          { new BlockStoreIndexFunction<'K, SeriesConstruction<'K>> with
              member x.Invoke<'V>(bi:BlockStoreIndex<'K, 'V>) = 
                let store = bi.BlockStore.GetAddressRange(lo, hi)
                let newIndex = BlockStoreIndex(store) :> IIndex<'K>
                //printfn "NEW INDEX: %A" (newIndex.KeyCount)
                //let newVector = BlockStoreVector(store) :> IVector // But what if this is a different vector???
                let (|Singleton|) list = List.head list
                let cmd = Vectors.CustomCommand([vector], fun (Singleton vector) ->
                  match vector with
                  | :? BlockStoreVector<'K, 'V> as lv -> 
                      if System.Object.ReferenceEquals(lv.BlockStore, bi.BlockStore) then 
                        BlockStoreVector(store) :> IVector 
                      else
                        failwith "GetAddressRange: Different source"
                        //let source = restrictSource lv.Source.Ranges lv.Source.Loader
                        //DelayedVector(source) :> IVector
                  | _ -> 
                      //let  = builder.GetRange(index, optLo, optHi, Vectors.Return 0)
                      //ArrayVector.ArrayVectorBuilder.Instance.Build(cmd, [| vector |])
                    

                      failwith "TODO: This should probably be supported?")


                newIndex, cmd } //newVector } 
          |> bi.Invoke
      | _ -> builder.GetAddressRange((index, vector), (lo, hi))

    member x.Project(index:IIndex<'K>) = failwith "Project"
    member x.AsyncMaterialize( (index:IIndex<'K>, vector) ) = failwith "AsyncMaterialize"
    member x.GetRange(index, optLo:option<'K * _>, optHi:option<'K * _>, vector) = failwith "GetRange"

// ------------------------------------------------------------------------------------------------


