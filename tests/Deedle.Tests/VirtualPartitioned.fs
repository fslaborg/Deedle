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
open Deedle.Ranges
open Deedle.Addressing
open Deedle.Virtual
open Deedle.Vectors.Virtual

// ------------------------------------------------------------------------------------------------
// Helper modules
// ------------------------------------------------------------------------------------------------

/// Helper function for creating data from partiton/offset
let date part idx = 
  DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx))

/// Encoding and decoding int32 pairs as int64 values.
///
/// In this test, we access virtual data source that has 
/// partitions and offsets within partitions. We represent
/// this as int32 pairs - or equivalently int64 values.
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

// ------------------------------------------------------------------------------------------------
// Partitioned source for testing
// ------------------------------------------------------------------------------------------------

/// Represents the shape of a partitioned data store
type PartitionShape = int[]

/// The following provides operations for working with keys - here, the keys are 
/// `DateTimeOffset` values (which can be directly mapped to addresses, because 
/// we use a scheme where: partition = year - 2000; offset = <hours since jan 1>
///
/// The `RangeKeyOperations` interface requires operations such as a distance 
/// (number of keys) between two keys; enumeration of all keys in a range etc.
///
/// To implement those, we generally iterate over the partitions and count/add/etc.
/// sizes of individual partitions. We also log all accessed partitions 
/// to 'tangeListRef'
///
type PartitionRangeOperations(shape:PartitionShape, rangeListRef:ref<_>) = 
  // Helpers for converting to/from addresses
  let firstDate (dt:DateTimeOffset) = 
    DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, dt.Offset)
  let hours (dt:DateTimeOffset) = 
    int (dt - firstDate dt).TotalHours          

  // Implementing BigDeedle interface for Ranges<'T>
  interface RangeKeyOperations<DateTimeOffset> with
    member x.Compare(dt1, dt2) = 
      compare dt1.UtcTicks dt2.UtcTicks

    member x.Distance(dt1, dt2) =             
      // Add "+1" for skipping to next partition and then subtract 
      // "1" at the end, because we got distance of 1 element after
      if dt1 > dt2 then failwith "Distance: assume dt1 <= dt2"
      let partSizes = seq {
        for y in dt1.Year .. dt2.Year ->
          rangeListRef := y - 2000 :: !rangeListRef
          let count = shape.[y - 2000]
          let lo = if y = dt1.Year then hours dt1 else 0
          let hi = if y = dt2.Year then hours dt2 else count-1
          int64 (hi - lo + 1) }
      (Seq.sum partSizes) - 1L

    member x.Range(dt1, dt2) =
      // Here, we need to generate range in both directions (when
      // dt1 is larger, we need to produce range in 'reversed' order)
      let step = if dt2 > dt1 then +1 else -1 
      seq{ 
        for y in dt1.Year .. step .. dt2.Year do
          rangeListRef := y - 2000 :: !rangeListRef
          let count = shape.[y - 2000]
          let first, last = 
            // Find first & last element on the partition. This is 
            // generally 0 .. count-1 except for boundary partitions.
            if dt2 > dt1 then
              (if y = dt1.Year then hours dt1 else 0), 
              (if y = dt2.Year then hours dt2 else count-1)
            else 
              (if y = dt1.Year then hours dt1 else count-1),
              (if y = dt2.Year then hours dt2 else 0)
          let dt = DateTimeOffset(y, 1, 1, 0, 0, 0, dt1.Offset)
          for h in first .. step .. last do yield dt.AddHours(float h) }

    member x.IncrementBy(dt, offset) =
      if offset < 0L then failwith "IncrementBy: Assume offset >= 0"
      let rec loop y offset = 
        // Increment the starting position of 'dt' by the specified 'offset' by
        // iterating over partitions and adding their sizes, until we have enough
        // (or until we run out of partitions)
        if y - 2000 = shape.Length then raise (new IndexOutOfRangeException())
        rangeListRef := y - 2000 :: !rangeListRef
        let count = shape.[y - 2000]
        let start = if y = dt.Year then hours dt else 0
        if offset < int64 (count - start) then
          DateTimeOffset(y, 1, 1, 0, 0, 0, dt.Offset).AddHours(float offset)
        else loop (y + 1) (offset - int64 (count - start))        
      loop dt.Year offset        

    member x.ValidateKey(dt, lookup) =
      // Find the nearest valid key - when (part, offs) is out of range
      // The ValidateKey operation can ignore 'Smaller' or 'Greater' (without Exact)
      if dt.Year < 2000 || dt.Year - 2000 >= shape.Length then
        // If Year is out of range
        if dt.Year < 2000 && (lookup &&& Lookup.Greater = Lookup.Greater) then 
          OptionalValue(date 0 0) // Jump to start
        elif dt.Year - 2000 >= shape.Length && (lookup &&& Lookup.Smaller = Lookup.Smaller) then 
          OptionalValue(date (shape.Length-1) (shape.[shape.Length-1]-1)) // Jump to end
        else OptionalValue.Missing
      else 
        let part = dt.Year - 2000
        let count = shape.[part]
        let offs = (dt - date part 0).TotalHours
        if offs - round(offs) = 0.0 && offs >= 0.0 && offs < float count then
          OptionalValue(date (part) (int offs))   // Happy path
        elif ceil offs >= float count && part+1 < shape.Length && (lookup &&& Lookup.Greater = Lookup.Greater) then
          OptionalValue(date (part+1) 0)          // Move to next year
        elif lookup &&& Lookup.Greater = Lookup.Greater then
          OptionalValue(date part (int offs + 1)) // Next valid hour
        elif lookup &&& Lookup.Smaller = Lookup.Smaller then
          OptionalValue(date part (min (count-1) (int offs))) // Previous valid hour
        else OptionalValue.Missing

/// We use the same 'TrackingSource' (below) as a data source for both keys and values.
/// This interface specifies how to convert between `'T` and `DateTimeOffset` - this is
/// either identity (for keys) or impossible and never done (for values)
type TrackingSourceValue<'T> = 
  abstract CanLookup : bool 
  // Trating 'T as the series key ('T = DateTimeOffset)
  abstract AsDate : 'T -> DateTimeOffset
  abstract OfDate : DateTimeOffset -> 'T 
  // Treating 'T as the series value ('T = float)
  abstract ValueAt : Address -> 'T

/// Represents BigDeedle data source for virtual vectors and indices.
/// This is fairly simple, because we delegate most of the work to `Ranges<T>`
/// abstraction provided by Deedle.
type TrackingSource<'T>
    ( accessRef:ref<_> * ref<int list>, valueOps:TrackingSourceValue<'T>, 
      ranges:Ranges<DateTimeOffset> ) = 

  // We keep `Ranges<DateTimeOffset>` (which is equivalent to keeping)
  let asAddress (dt:DateTimeOffset) = 
    let hours = (dt - DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, dt.Offset)).TotalHours
    if (hours - round(hours) > 1e-9) then failwith "asAddress: Invalid key!"
    Address.ofIntPair(dt.Year-2000, int hours)
  let ofAddress (addr) = 
    let part, idx = Address.asIntPair(addr)
    DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx))
  let addressing = RangesAddressOperations<DateTimeOffset>(ranges, Func<_, _>(asAddress), Func<_, _>(ofAddress)) :> IAddressOperations

  member x.Ranges = ranges
  member x.AccessedData = (fst accessRef).Value
  member x.AccessedMeta = (snd accessRef).Value

  // Implements non-generic source (boilerplate)

  interface IVirtualVectorSource with
    member x.Length = ranges.Length
    member x.ElementType = typeof<'T>
    member x.AddressOperations = addressing
    member x.Invoke(op) = op.Invoke(x)

  // Implement generic source interface - mostly boilerplate or
  // delegation to `Ranges` (except for LookupRange which we don't do)

  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) = 
      TrackingSource<'T>(accessRef, valueOps, ranges.MergeWith(sources |> Seq.map (function
        | :? TrackingSource<'T> as t -> t.Ranges
        | _ -> failwith "MergeWith: other is not partitioned source!"))) :> _

    member x.LookupRange(v) = 
      failwith "LookupRange: not supported"

    member x.LookupValue(value, lookup, check) = 
      // This is good enough for testing, but it is not perfect
      // When we are trying to find a 'DateTimeOffset', we can directly
      // calculate the partition where it belongs (that's the year - 2000).
      // The following does scan over partitions from the beginning (skipping
      // the number of elements in partitions), which may be inefficient.
      if valueOps.CanLookup then
          ranges.Lookup(valueOps.AsDate value, lookup, fun k offs -> check.Invoke(asAddress k))
          |> OptionalValue.map (fun (k, offs) -> (valueOps.OfDate k, asAddress k))
      else failwith "LookupValue: This source cannot be used for lookup"

    member x.ValueAt loc = 
      let pair = loc.Address |> Address.asIntPair 
      fst accessRef := pair ::(fst accessRef).Value
      OptionalValue(valueOps.ValueAt(loc.Address))

    member x.GetSubVector(restriction) = 
      let newRanges = ranges.Restrict(restriction |> RangeRestriction.map ofAddress)
      TrackingSource<'T>(accessRef, valueOps, newRanges) :> _


/// Returns accessed partitions of a tracking source
let accessedMetaParts (src:TrackingSource<_>) =
  src.AccessedMeta |> Seq.distinct |> Seq.sort |> List.ofSeq
/// Returns partitions that were accessed via 'Range'
let accessedDataParts (src:TrackingSource<_>) =
  src.AccessedData |> Seq.map fst |> Seq.distinct |> Seq.sort |> List.ofSeq  

// ------------------------------------------------------------------------------------------------
// Create time series using the partitoned virtual data source
// ------------------------------------------------------------------------------------------------

let createTimeSeries partNum partSize =
  let shape  = [| for i in 1 .. partNum -> partSize i |]
  let range  = [date 0 0, date (shape.Length-1) (shape.[shape.Length-1]-1)]
  let accessRef = ref [], ref []
  let ranges = Ranges.create (PartitionRangeOperations(shape, snd accessRef)) range
  let idxValues =
    { new TrackingSourceValue<DateTimeOffset> with
        member x.CanLookup = true
        member x.AsDate(d) = d
        member x.OfDate(d) = d
        member x.ValueAt(addr) = let part, idx = Address.asIntPair addr in date part idx }
  let valValues =
    { new TrackingSourceValue<float> with
        member x.CanLookup = false
        member x.AsDate(d) = failwith "AsDate not supported"
        member x.OfDate(d) = failwith "OfDate not supported"
        member x.ValueAt(addr) = let part, idx = Address.asIntPair addr in (float part) * 1000000.0 + (float idx) }
  let idxSrc = TrackingSource<DateTimeOffset>(accessRef, idxValues, ranges)
  let valSrc = TrackingSource<float>(accessRef, valValues, ranges)
  let sv = Virtual.CreateSeries(idxSrc, valSrc)
  idxSrc, valSrc, sv

// ------------------------------------------------------------------------------------------------
// Printing and accessing meta-data about series
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Counting keys (with small partitions) accesses meta, but no data`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.KeyCount |> shouldEqual 10000
  accessedDataParts idxSrc |> shouldEqual <| []
  accessedMetaParts idxSrc |> shouldEqual <| [0 .. 999]

[<Test>]
let ``Printing series (with small partitions) accesses border partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.Format() |> should containStr "0"
  ts.Format() |> should containStr "1000000"
  ts.Format() |> should (containStr >> not') "1000005"
  ts.Format() |> should containStr "998000005"
  ts.Format() |> should containStr "999000009"
  ts.Format() |> should (containStr >> not') "998000004"
  accessedDataParts idxSrc |> shouldEqual <| [0; 1; 2; 3; 998; 999]
  accessedMetaParts idxSrc |> shouldEqual <| [0; 1; 2; 3; 998; 999]

[<Test>]
let ``Printing series does not require counting items in partitions``() = 
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 100)
  ts.Format() |> ignore
  accessedMetaParts idxSrc |> shouldEqual <| [0; 999]
  accessedDataParts idxSrc |> shouldEqual <| [0; 999]

[<Test>]
let ``Printing series (with large partitions) accesses border partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 100 (fun n -> 5000)
  ts.KeyCount |> shouldEqual 1000000
  ts.Format() |> should containStr "0"
  ts.Format() |> should containStr "14"
  ts.Format() |> should containStr "99004985"
  ts.Format() |> should containStr "99004999"
  accessedDataParts idxSrc |> shouldEqual <| [0; 99]

// ------------------------------------------------------------------------------------------------
// Testing lookup
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Lookup accesses only relevant partition`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.[date 10 5] |> shouldEqual 10000005.
  valSrc.AccessedData |> shouldEqual [10, 5]

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
  valSrc.AccessedData |> Seq.distinct |> Seq.sort |> List.ofSeq 
  |> shouldEqual <| [(10, 4); (10, 5); (10, 6)]
  accessedDataParts idxSrc |> shouldEqual [10]

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
  accessedMetaParts idxSrc |> shouldEqual [0 .. 11]
  valSrc.AccessedData |> shouldEqual []
