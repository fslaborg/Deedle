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
// Partitioned source for testing
// ------------------------------------------------------------------------------------------------

/// Represents the shape of a partitioned data store
type PartitionShape = int[]

/// Encoding and decoding int32 pairs as int64 values
module Address =
  let ofIntPair (part:int, idx:int) : Address = 
    (int64 part <<< 32) ||| (int64 idx) |> LanguagePrimitives.Int64WithMeasure 
  let asIntPair (addr:Address) = 
    let addr = int64 addr
    int (addr >>> 32), int (addr &&& 0xffffffffL)
  let asIntRange (loAddr, hiAddr) = 
    asIntPair loAddr, asIntPair hiAddr
  let inRange (loAddr, hiAddr) addr = 
    let (loPart, loIdx), (hiPart, hiIdx) = asIntPair loAddr, asIntPair hiAddr
    let addrPart, addrIdx = asIntPair addr
    ( (addrPart > loPart) || (addrPart = loPart && addrIdx >= loIdx) ) &&
    ( (addrPart < hiPart) || (addrPart = hiPart && addrIdx <= hiIdx) ) 


/// The data source keeps the global shape of the store 
/// together with a range in that global space (and a 
/// function for generating values for testing purposes)
type PartitionSource<'T>
    ( shape:PartitionShape, range, accessListRef,
      valueAt:int -> int -> 'T, partOf : option<'T -> int>,
      asLong:option<'T -> int64> ) = 
  member x.Shape = shape
  member x.Range = range
  member x.AccessListRef = accessListRef
  member x.ValueAtFunc = valueAt
  member x.PartitionOf(v) = match partOf with Some f -> f v | _ -> failwith "PartitionOf - not specified"
  member x.AsLong(v) = match asLong with Some f -> f v | _ -> failwith "AsLong - not specified"

  member x.ValueAt(partition, index) = 
    // Return value in the global address space (and check ranges)
    if not (Address.inRange range (Address.ofIntPair (partition, index))) then
      failwithf "ValueAt - Value at %A is out of range" (partition, index)
    let size = shape.[partition]
    if index >= 0 && index <= size then 
      accessListRef := (partition, index) :: accessListRef.Value
      OptionalValue(valueAt partition index)
    else failwithf "ValueAt - Value at %A is out of range" (partition, index)
  member x.With(?newRange) = 
    PartitionSource(shape, defaultArg newRange range, accessListRef, valueAt, partOf, asLong)

  static member Create(shape, range, valueAt) = 
    PartitionSource(shape, range, ref [], valueAt, None, None)
  static member Create(shape, range, valueAt, partOf, asLong) = 
    PartitionSource(shape, range, ref [], valueAt, Some partOf, Some asLong)

/// Address operations are associated to a sub-range of the global address space
type AddressOperations(shape:PartitionShape, range, rangeListRef) = 
  
  // Equality checks that shape & sub-range are the same
  member x.Shape = shape
  member x.Range = range
  override x.Equals(o) = 
    match o with
    | :? AddressOperations as y -> x.Shape = y.Shape && x.Range = y.Range
    | _ -> false
  override x.GetHashCode() = shape.GetHashCode()

  interface IAddressOperations with
    // Return the address in the global address space
    member x.FirstElement = fst range
    member x.LastElement = snd range

    // Iterate over addresses within the subrange specified by 'range'
    member x.Range = 
      let (loPart, loIdx), (hiPart, hiIdx) = Address.asIntRange range
      seq { for pi in loPart .. hiPart do
              rangeListRef := pi::rangeListRef.Value
              let lo, hi = 
                if pi = loPart && pi = hiPart then loIdx, hiIdx
                elif pi = loPart then loIdx, shape.[pi]-1
                elif pi = hiPart then 0, hiIdx
                else 0, shape.[pi]-1
              for i in lo .. hi do 
                yield Address.ofIntPair(pi, i) }
    
    member x.AddressOf(idx) = 
      let (loPart, loIdx), (hiPart, hiIdx) = Address.asIntRange range
      let rec loop partIndex idx =
        // After last partition or on the last partition, but after last index
        if (partIndex = hiPart + 1) ||
          (partIndex = hiPart && idx > (int64 hiIdx)) then 
          failwith "AddressOperations.AddressOf - out of range"
        // At first partition, we skip 'loIdx' elements
        let idx = if partIndex = loPart then idx + int64 loIdx else idx
        if idx < int64 (shape.[partIndex]) then Address.ofIntPair(partIndex, int idx)
        else loop (partIndex+1) (idx - int64 (shape.[partIndex]))

      loop loPart idx

    member x.OffsetOf(addr) = 
      failwith "AddressOperations.OffsetOf"
      

/// Vector source that represents a range as determined by the 'source'
type TrackingSource<'T>(source:PartitionSource<'T>, ?rangeListRef) = 
  let rangeListRef = defaultArg rangeListRef (ref [])
  let addressing = AddressOperations(source.Shape, source.Range, rangeListRef) :> IAddressOperations
  member x.AccessList = source.AccessListRef.Value
  member x.RangeAccessList = rangeListRef.Value
  interface IVirtualVectorSource with
    member x.Length = addressing.Range |> Seq.length |> int64
    member x.ElementType = typeof<'T>
    member x.AddressOperations = addressing
    member x.Invoke(op) = op.Invoke(x)

  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) = failwith "MergeWith"
    member x.LookupRange(v) = failwith "LookupRange"
    member x.LookupValue(k, l, c) = 
      let scanPart part = 
        let partLength = source.Shape.[part]
        IndexUtilsModule.binarySearch 
          (int64 partLength) 
          (Func<_, _>(fun i -> source.AsLong(source.ValueAt(part, int i).Value)))
          (source.AsLong k) l
          (Func<_, _>(fun i -> c.Invoke(Address.ofIntPair(part, int i)) ))
  
      let part = source.PartitionOf(k)
      let part = 
        let firstValue = source.AsLong(source.ValueAt(part, 0).Value)
        let lastValue = source.AsLong(source.ValueAt(part, source.Shape.[part]-1).Value)
        let searchValue = source.AsLong(k)
        // Adjust partition based on border conditions
        if (firstValue > searchValue && (l = Lookup.Smaller || l = Lookup.ExactOrSmaller)) then part - 1
        elif (firstValue = searchValue && l = Lookup.Smaller) then part - 1
        elif (lastValue < searchValue && (l = Lookup.Greater || l = Lookup.ExactOrGreater)) then part + 1
        elif (lastValue = searchValue && l = Lookup.Greater) then part + 1
        else part

      scanPart part
      |> OptionalValue.map (fun i -> source.ValueAt(part, int i).Value, Address.ofIntPair(part, int i) )

    member x.ValueAt loc = source.ValueAt(Address.asIntPair loc.Address)
    member x.GetSubVector(range) = 
      match range with 
      | AddressRange.Start(count) ->
          let loPart, loIdx = Address.asIntPair (fst source.Range)
          let mutable hiPart, hiIdx = loPart, loIdx
          let mutable count = count
          while count > 0L do
            if count <= int64 (source.Shape.[hiPart]-hiIdx) then
              hiIdx <- int (count - 1L)
              count <- 0L
            else
              count <- count - int64 source.Shape.[hiPart]
              hiIdx <- 0
              hiPart <- hiPart + 1
          let ps = source.With(newRange = (Address.ofIntPair(loPart, loIdx), Address.ofIntPair(hiPart, hiIdx)))
          TrackingSource<'T>(ps, rangeListRef) :> IVirtualVectorSource<_>

      | AddressRange.End(count) ->
          let hiPart, hiIdx = Address.asIntPair (snd source.Range)
          let mutable loPart, loIdx = hiPart, hiIdx
          let mutable count = count
          while count > 0L do
            if count <= int64 (loIdx+1) then
              loIdx <- loIdx+1-(int count)
              count <- 0L
            else
              count <- count - int64 (loIdx+1)
              loPart <- loPart - 1
              loIdx <- source.Shape.[loPart]-1
          let ps = source.With(newRange = (Address.ofIntPair(loPart, loIdx), Address.ofIntPair(hiPart, hiIdx)))
          TrackingSource<'T>(ps, rangeListRef) :> IVirtualVectorSource<_>

      | AddressRange.Fixed(lo, hi) ->
          let ps = source.With(newRange = (lo, hi))
          TrackingSource<'T>(ps, rangeListRef) :> IVirtualVectorSource<_>
      | _ -> failwithf "GetSubVector - custom %A" range


/// Returns accessed partitions of a tracking source
let accessedPartitions (src:TrackingSource<_>) =
  src.AccessList |> Seq.map fst |> Seq.distinct |> Seq.sort |> List.ofSeq
/// Returns partitions that were accessed via 'Range'
let accessedRangePartitions (src:TrackingSource<_>) =
  src.RangeAccessList |> Seq.distinct |> Seq.sort |> List.ofSeq

let date part idx = 
  DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx))

// ------------------------------------------------------------------------------------------------
// Testing with simple time series
// ------------------------------------------------------------------------------------------------

let createTimeSeries partNum partSize =
  let shape =  [| for i in 1 .. partNum -> partSize i |]
  let range = Address.ofIntPair(0, 0), Address.ofIntPair(shape.Length-1, shape.[shape.Length-1]-1)
  let idxStore = 
    PartitionSource<DateTimeOffset>.Create
      ( shape, range, 
        (fun part idx -> DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx))), 
        (fun dto -> dto.Year - 2000),
        (fun dto -> dto.UtcTicks) )
  let valStore = 
    PartitionSource<float>.Create
      ( shape, range, 
        fun part idx -> (float part) * 1000000.0 + (float idx) )
  let idxSrc = TrackingSource<DateTimeOffset>(idxStore)
  let valSrc = TrackingSource<float>(valStore)
  let sv = Virtual.CreateSeries(idxSrc, valSrc)
  idxSrc, valSrc, sv


[<Test>]
let ``Printing series (with small partitions) accesses border partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.KeyCount |> shouldEqual 10000
  ts.Format() |> should containStr "0"
  ts.Format() |> should containStr "1000000"
  ts.Format() |> should containStr "998000005"
  ts.Format() |> should containStr "999000009"
  accessedPartitions idxSrc |> shouldEqual <| [0; 1; 2; 3; 998; 999]

[<Test>]
let ``Printing series does not require counting items in partitions``() = 
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 100)
  ts.Format() |> ignore
  accessedRangePartitions idxSrc |> shouldEqual <| [0; 999]

[<Test>]
let ``Printing series (with large partitions) accesses border partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 100 (fun n -> 10000)
  ts.KeyCount |> shouldEqual 1000000
  ts.Format() |> should containStr "0"
  ts.Format() |> should containStr "14"
  ts.Format() |> should containStr "99009985"
  ts.Format() |> should containStr "99009999"
  accessedPartitions idxSrc |> shouldEqual <| [0; 99]

[<Test>]
let ``Lookup accesses only relevant partition`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.[date 10 5] |> shouldEqual 10000005.
  valSrc.AccessList |> shouldEqual [10, 5]
  accessedPartitions idxSrc |> shouldEqual [10]

[<Test>]
let ``Lookup can search within a single partition`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.TryGet((date 10 5).AddMinutes 30.0).HasValue |> shouldEqual false
  ts.Get((date 10 5).AddMinutes 30.0, Lookup.Smaller) |> shouldEqual 10000005.
  ts.Get((date 10 5).AddMinutes 30.0, Lookup.Greater) |> shouldEqual 10000006.
  ts.Get(date 10 5, Lookup.Smaller) |> shouldEqual 10000004.
  ts.Get(date 10 5, Lookup.Greater) |> shouldEqual 10000006.
  ts.Get(date 10 5, Lookup.ExactOrSmaller) |> shouldEqual 10000005.
  ts.Get(date 10 5, Lookup.ExactOrGreater) |> shouldEqual 10000005.
  valSrc.AccessList |> Seq.distinct |> Seq.sort |> List.ofSeq 
  |> shouldEqual <| [(10, 4); (10, 5); (10, 6)]
  accessedPartitions idxSrc |> shouldEqual [10]

[<Test>]
let ``Lookup can search accross partition boundaries`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.Get((date 0 9).AddDays(1.0), Lookup.ExactOrSmaller) |> shouldEqual 9.0
  ts.Get((date 0 9).AddDays(1.0), Lookup.ExactOrGreater) |> shouldEqual 1000000.0
  ts.Get(date 0 9, Lookup.Greater) |> shouldEqual 1000000.0

[<Test>]
let ``Getting a subseries accesses only relevant partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.[date 10 0 .. date 10 9].KeyCount |> shouldEqual 10
  ts.[date 9 0 .. date 11 9].KeyCount |> shouldEqual 30
  accessedPartitions idxSrc |> shouldEqual [9; 10; 11]
  valSrc.AccessList |> shouldEqual []