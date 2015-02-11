#if INTERACTIVE
#I "../../bin/"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.VirtualPartitionFrame
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Addressing
open Deedle.Virtual
open Deedle.Vectors.Virtual

// ------------------------------------------------------------------------------------------------
// Tracking source
// ------------------------------------------------------------------------------------------------

type PartitionShape = int[]

module Address =
  let ofIntPair (part:int, idx:int) : Address = 
    (int64 part <<< 32) ||| (int64 idx) |> LanguagePrimitives.Int64WithMeasure 
  let asIntPair (addr:Address) = 
    let addr = int64 addr
    int (addr >>> 32), int (addr &&& 0xffffffffL)

type PartitionStore<'T>(shape:PartitionShape, valueAt:int -> int -> 'T) = 
  member x.Shape = shape
  member x.ValueAt(partition, index) = 
    let size = shape.[partition]
    if index >= 0 && index <= size then 
      OptionalValue(valueAt partition index)
    else OptionalValue.Missing

type AddressOperations(shape:PartitionShape) = 
  override x.Equals(o) = 
    match o with
    | :? AddressOperations as y -> failwith "AddressOperations.Equals"
    | _ -> false
  override x.GetHashCode() = shape.GetHashCode()
  interface IAddressOperations with
    member x.FirstElement = Address.ofIntPair(0, 0)
    member x.LastElement = Address.ofIntPair(shape.Length-1, shape.[shape.Length-1]-1)
    member x.Range = 
      seq { for s in shape do
              for i in 0 .. s-1 do 
                yield Address.ofIntPair(s, i) }
    member x.OffsetOf(addr) = failwith "AddressOperations.OffsetOf"
    member x.AddressOf(idx) = 
      let rec loop partIndex idx =
        if partIndex = shape.Length then failwith "AddressOperations.AddressOf - out of range"
        if idx < int64 (shape.[partIndex]) then Address.ofIntPair(partIndex, int idx)
        else loop (partIndex+1) (idx - int64 (shape.[partIndex]))
      loop 0 idx

    member x.Next(addr) = 
      let part, idx = Address.asIntPair addr
      let part, idx = if idx + 1 = shape.[part] then part + 1, idx else part, idx + 1
      if part = shape.Length then failwith "AddressOperations.Next - out of range"
      Address.ofIntPair(part, idx)
      
type TrackingSource<'T>(store:PartitionStore<'T>) = 
  interface IVirtualVectorSource with
    member x.Length = store.Shape |> Seq.sum |> int64
    member x.ElementType = typeof<'T>
    member x.AddressOperations = AddressOperations(store.Shape) :> _

  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) = failwith "MergeWith"
    member x.LookupRange(v) = failwith "LookupRange"
    member x.LookupValue(k, l, c) = failwith "LookupValue"
    member x.ValueAt addr = store.ValueAt(Address.asIntPair addr)
    member x.GetSubVector(range) = failwithf "GetSubVector %A" range

let createTimeSeries () =
  let shape = [| for i in 1 .. 10 -> i * 1000 |]
  let idxStore = PartitionStore<DateTimeOffset>(shape, fun part idx -> 
    DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx)))
  let valStore = PartitionStore<float>(shape, fun part idx -> 
    (float part) * 1000000.0 + (float idx))
  let idxSrc = TrackingSource<DateTimeOffset>(idxStore)
  let valSrc = TrackingSource<float>(valStore)
  let sv = Virtual.CreateSeries(idxSrc, valSrc) in sv.Format()
  idxSrc, valSrc, sv
