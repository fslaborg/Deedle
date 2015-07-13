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

/// Time zone offset used for all dates in this test
let tzOffset = TimeSpan.FromHours 5.
/// Helper function for creating data from partiton/offset
let date part idx = 
  DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx), tzOffset)
/// Helper to create DateTimeOffset without too much typing
let dateOffs y m d =
  DateTimeOffset(DateTime(y, m, d), tzOffset)
/// Helper to create TimeSpan
let mins m = TimeSpan(0, m, 0)

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
/// The `IRangeKeyOperations` interface requires operations such as a distance 
/// (number of keys) between two keys; enumeration of all keys in a range etc.
///
/// To implement those, we generally iterate over the partitions and count/add/etc.
/// sizes of individual partitions. We also log all accessed partitions 
/// to 'tangeListRef'
///
type PartitionRangeOperations(shape:PartitionShape, rangeListRef:ref<_>) = 
  // Helpers for converting to/from addresses
  let firstDate (dt:DateTimeOffset) = 
    DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, tzOffset)
  let hours (dt:DateTimeOffset) = 
    int (dt - firstDate dt).TotalHours          

  // Implementing BigDeedle interface for Ranges<'T>
  interface IRangeKeyOperations<DateTimeOffset> with
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
          let dt = DateTimeOffset(y, 1, 1, 0, 0, 0, tzOffset)
          for h in first .. step .. last do yield dt.AddHours(float h) }

    member x.IncrementBy(dt, offset) =
      let rec loop y offset = 
        // Increment the starting position of 'dt' by the specified 'offset' by
        // iterating over partitions and adding their sizes, until we have enough
        // (or until we run out of partitions)
        if y - 2000 < 0 || y - 2000 = shape.Length then raise (new IndexOutOfRangeException())
        rangeListRef := y - 2000 :: !rangeListRef
        let count = shape.[y - 2000]
        if offset >= 0L  then
          let start = if y = dt.Year then hours dt else 0
          if offset < int64 (count - start) then
            DateTimeOffset(y, 1, 1, 0, 0, 0, tzOffset).AddHours(float start + float offset)
          else loop (y + 1) (offset - int64 (count - start))        
        else
          let last = if y = dt.Year then hours dt else count-1
          if -offset <= int64 last then
            DateTimeOffset(y, 1, 1, 0, 0, 0, tzOffset).AddHours(float last - float offset)
          else loop (y - 1) (offset - int64 (last + 1))        
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
    let hours = (dt - DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, tzOffset)).TotalHours
    if (hours - round(hours) > 1e-9) then failwith "asAddress: Invalid key!"
    Address.ofIntPair(dt.Year-2000, int hours)
  let ofAddress (addr) = 
    let part, idx = Address.asIntPair(addr)
    DateTimeOffset(DateTime(2000 + part, 1, 1).AddHours(float idx), tzOffset)
  let addressing = RangesAddressOperations<DateTimeOffset>(ranges, Func<_, _>(asAddress), Func<_, _>(ofAddress)) :> IAddressOperations

  member x.Ranges = ranges
  member x.AccessedData = (fst accessRef).Value
  member x.AccessedMeta = (snd accessRef).Value

  // Implements non-generic source (boilerplate)

  interface IVirtualVectorSource with
    member x.AddressingSchemeID = "it"
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

let idxValues =
  { new TrackingSourceValue<DateTimeOffset> with
      member x.CanLookup = true
      member x.AsDate(d) = d
      member x.OfDate(d) = d
      member x.ValueAt(addr) = let part, idx = Address.asIntPair addr in date part idx }

let valValues f =
  { new TrackingSourceValue<float> with
      member x.CanLookup = false
      member x.AsDate(d) = failwith "AsDate not supported"
      member x.OfDate(d) = failwith "OfDate not supported"
      member x.ValueAt(addr) = let part, idx = Address.asIntPair addr in f (float part) (float idx) }

let createRanges partNum partSize =
  let shape  = [| for i in 1 .. partNum -> partSize i |]
  let range  = [date 0 0, date (shape.Length-1) (shape.[shape.Length-1]-1)]
  let accessMeta = ref []
  accessMeta, Ranges.create (PartitionRangeOperations(shape, accessMeta)) range

let createTimeSeries partNum partSize =
  let accessMeta, ranges = createRanges partNum partSize
  let idxSrc = TrackingSource<DateTimeOffset>((ref [], accessMeta), idxValues, ranges)
  let valSrc = TrackingSource<float>((ref [], accessMeta), valValues (fun part idx -> part * 1000000.0 + idx), ranges)
  let sv = Virtual.CreateSeries(idxSrc, valSrc)
  idxSrc, valSrc, sv

let createOrdinalSeries partNum partSize =
  let accessMeta, ranges = createRanges partNum partSize
  let valSrc = TrackingSource<float>((ref [], accessMeta), valValues (fun part idx -> part * 1000000.0 + idx), ranges)
  let sv = Virtual.CreateOrdinalSeries(valSrc)
  valSrc, sv

// ------------------------------------------------------------------------------------------------
// Printing and accessing meta-data about series
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Counting keys (with small partitions) accesses meta but no data`` () =
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
  ts.KeyCount |> shouldEqual 500000
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
  accessedDataParts idxSrc |> shouldEqual []
  accessedDataParts valSrc |> shouldEqual [10]

[<Test>]
let ``Lookup can search accross partition boundaries`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.Get((date 0 9).AddDays(1.0), Lookup.ExactOrSmaller) |> shouldEqual 9.0
  ts.Get((date 0 9).AddDays(1.0), Lookup.ExactOrGreater) |> shouldEqual 1000000.0
  ts.Get(date 0 9, Lookup.Greater) |> shouldEqual 1000000.0

// ------------------------------------------------------------------------------------------------
// Slicing and merging
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Getting a subseries accesses only relevant partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 10)
  ts.[date 10 0 .. date 10 9].KeyCount |> shouldEqual 10
  ts.[date 9 0 .. date 11 9].KeyCount |> shouldEqual 30
  accessedMetaParts idxSrc |> shouldEqual [0 .. 11]
  valSrc.AccessedData |> shouldEqual []

[<Test>]
let ``Lookup into subseries works and accesses correct partitions`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let sub = ts.[dateOffs 2010 2 1 .. dateOffs 2011 6 1] 

  // Lookup before and after the start/end of the range
  sub.TryGet(dateOffs 2010 1 1, Lookup.ExactOrGreater)
  |> shouldEqual <| sub.TryGet(fst sub.KeyRange)
  sub.TryGet(dateOffs 2011 6 1, Lookup.ExactOrSmaller)
  |> shouldEqual <| sub.TryGet(snd sub.KeyRange)

  // Additional lookup tests around the boundary
  let lookupTests = 
    [ dateOffs 2010 2 1, Lookup.Greater 
      dateOffs 2010 2 1, Lookup.ExactOrGreater
      dateOffs 2010 2 1, Lookup.Exact
      dateOffs 2010 2 1 - mins 10, Lookup.ExactOrGreater
      dateOffs 2011 6 1, Lookup.Smaller
      dateOffs 2011 6 1, Lookup.ExactOrSmaller
      dateOffs 2011 6 1, Lookup.Exact ]
  for dt, semantics in lookupTests do
    sub.TryGet(dt, semantics) |> shouldEqual <| ts.TryGet(dt, semantics)
  
  // We only accessed the two relevant years  
  accessedDataParts idxSrc |> shouldEqual <| [10; 11]


[<Test>]
let ``Merging series sub-ranges works as expected`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let y15a = ts.[dateOffs 2015 2 1 .. dateOffs 2015 3 1 - mins 1 ]
  let y15b = ts.[dateOffs 2015 3 1 .. dateOffs 2015 4 1 ]
  let y17a = ts.[dateOffs 2017 2 1 .. dateOffs 2017 3 1 - mins 1 ]
  let y17b = ts.[dateOffs 2017 3 1 .. dateOffs 2017 4 1 ]
  let merged = y15a.Merge(y17b, y15b, y17a)

  // Merging correctly joins adjacent ranges (the above is just 2 blocks)
  let ranges = 
    ((merged.Index :?> Deedle.Indices.Virtual.VirtualOrderedIndex<DateTimeOffset>).Source 
      :?> TrackingSource<DateTimeOffset>).Ranges
  ranges.Ranges.Length |> shouldEqual 2

  // Check number of keys in the merged series
  merged.KeyCount |> shouldEqual 
    <| y15a.KeyCount + y15b.KeyCount + y17a.KeyCount + y17b.KeyCount

  // Lookup in the hole between two merged ranges
  merged.TryGet(dateOffs 2016 2 1, Lookup.Exact).HasValue 
  |> shouldEqual false
  merged.Get(dateOffs 2016 2 1, Lookup.Greater) 
  |> shouldEqual <| y17a.[fst y17a.KeyRange]
  merged.Get(dateOffs 2016 2 1, Lookup.Smaller) 
  |> shouldEqual <| y15b.[snd y15b.KeyRange]

  // The accessed values are the same too..
  merged |> Stats.sum |> should (equalWithin 1e-100) <| 
    ([y15a; y15b; y17a; y17b] |> Seq.sumBy Stats.sum)


[<Test>]
let ``Slicing with out of range keys or reversed order produces empty series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  ts.[dateOffs 1990 1 1 .. dateOffs 1991 1 1].KeyCount |> shouldEqual 0
  ts.[dateOffs 2500 1 1 .. dateOffs 2400 1 1].KeyCount |> shouldEqual 0
  ts.[date 999 4999 .. dateOffs 3000 1 1].KeyCount |> shouldEqual 1
  ts.[dateOffs 1990 1 1 .. date 0 0].KeyCount |> shouldEqual 1
  
// ------------------------------------------------------------------------------------------------
// Operations over ordinal series
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Cen perform slicing on an ordinally indexed series`` () =
  let valSrc, ts = createOrdinalSeries 1000 (fun n -> 10)
  let ss = ts.[1005L .. 1014L]

  ss.GetKeyAt(0) |> shouldEqual 1005L
  ss.GetKeyAt(int ss.KeyCount-1) |> shouldEqual 1014L  
  for k in 1005L .. 1014L do
    ss.[k] |> shouldEqual ts.[k]

[<Test>]
let ``Cen perform slicing and merging on an ordinally indexed series`` () =
  let valSrc, ts = createOrdinalSeries 1000 (fun n -> 10)
  let ss = Series.mergeAll [ ts.[1005L .. 1014L]; ts.[2005L .. 2014L] ]
  for k in ss.Keys do
    ss.[k] |> shouldEqual ts.[k]

// ------------------------------------------------------------------------------------------------
// Projection, joins and other operations
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can project using Series.map without evaluating the series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let projected = ts |> Series.mapValues (fun v -> int v % 10)
  projected.GetAt(0) |> shouldEqual 0
  projected.GetAt(projected.KeyCount-1) |> shouldEqual 9
  valSrc.AccessedData |> shouldEqual [999,4999; 0,0]

[<Test>]
let ``Can project using Select method without evaluating the series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let projected = ts.SelectValues(fun v -> int v % 10)
  projected.GetAt(0) |> shouldEqual 0
  projected.GetAt(projected.KeyCount-1) |> shouldEqual 9
  valSrc.AccessedData |> shouldEqual [999,4999; 0,0]

[<Test>]
let ``Can project using SelectOptional method without evaluating the series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let projected = ts.SelectOptional(fun kvp -> OptionalValue(int kvp.Value.Value % 10))
  projected.GetAt(0) |> shouldEqual 0
  projected.GetAt(projected.KeyCount-1) |> shouldEqual 9
  valSrc.AccessedData |> shouldEqual [999,4999; 0,0]

[<Test>]
let ``Can subtract series from another (calculated from itself)`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let res = (sin ts) / (cos ts) - (tan ts) |> Series.mapValues (fun v -> Math.Round(v, 10))
  res |> Series.take 10 |> Series.values |> List.ofSeq |> shouldEqual [ for i in 0 .. 9 -> 0.0 ]
  res |> Series.takeLast 10 |> Series.values |> List.ofSeq |> shouldEqual [ for i in 0 .. 9 -> 0.0 ]
  valSrc.AccessedData |> Seq.distinct |> Seq.length |> shouldEqual 20

[<Test>]
let ``Returning `nan` from Series.map produces missing value`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let missings = ts |> Series.mapValues (fun v -> if (int v) % 4 = 0 then nan else v)
  let last5 = missings |> Series.takeLast 5 
  last5.KeyCount |> shouldEqual 5
  last5.ValueCount |> shouldEqual 4  

[<Test>]
let ``Adding column using exact match does not fully evaluate series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let df =
    Frame.ofRowKeys 
      [ for y in 2100 .. 2200 do
          for m in 1 .. 12 do yield dateOffs y m 1 ]
  df.AddColumn("Values", ts, Lookup.Exact)
  for k, v in Series.observations df?Values do
    ts.[k] |> shouldEqual v
  accessedDataParts idxSrc |> shouldEqual <| [ 100 ]

[<Test>]
let ``Equality returns false and works on very large series`` () =
  let _, _, ts1 = createTimeSeries 5000 (fun n -> 7500)
  let ts2 = series [date 1 1 => 0.0]
  ts1 = ts2 |> shouldEqual false
  ts2 = ts1 |> shouldEqual false

[<Test>]
let ``Equality test returns true on small virutal series`` () =
  let _, _, ts1 = createTimeSeries 1000 (fun n -> 5000)
  let ts1sm = ts1 |> Series.take 10
  let ts2sm = series [ for i in 0 .. 9 -> date 0 i => float i ]
  ts1sm = ts2sm |> shouldEqual true
  ts2sm = ts1sm |> shouldEqual true
 
[<Test>]
let ``Can access elements of a large series using GetItems`` () =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.GetItems [for i in 0 .. 10 -> date 2 i]
  |> Series.values
  |> List.ofSeq |> shouldEqual [2000000.0 .. 2000010.0]
 
[<Test>]
let ``Can sample large time series using explicitly specified list of dates`` () =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s |> Series.sample [ for y in 0 .. 999 -> date y 0] |> Series.values 
  |> List.ofSeq |> shouldEqual [ 0.0 .. 1000000.0 .. 999000000.0 ]

[<Test>]
let ``Can sample large time series by time without evauating it`` () =
  let _, valSrc, s = createTimeSeries 1000 (fun n -> 5000)
  let sampled = s |> Series.sampleTimeInto (TimeSpan.FromDays 10000.0) Direction.Forward id
  valSrc.AccessedData |> shouldEqual []

  let values = sampled |> Series.mapValues (Series.firstValue) 
  values |> Stats.sum |> ignore
  valSrc.AccessedData |> Seq.length |> shouldEqual values.KeyCount

// ------------------------------------------------------------------------------------------------
// Operations that materialize the series
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can merge non-virtual series with small virtual series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let ts1 = ts.[date 10 4900 .. date 11 100 ]
  let ts2 = series [ for k, v in ts1 |> Series.observations -> k + mins 30 => v + 0.5 ]
  let merged = ts1.Merge(ts2)

  merged |> Series.diff 1 |> Series.values |> Seq.distinct |> List.ofSeq
  |> shouldEqual [0.5; 995000.5]

[<Test>]
let ``Can sort small sub-series of a virtual series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let proj v = int v % 10
  let subseries = ts.[date 100 4990 .. date 101 10]
  let sorted = subseries |> Series.sortBy proj

  // It has actually been sorted
  let allValues = sorted.Values |> Seq.map proj |> List.ofSeq
  List.sort allValues |> shouldEqual <| allValues

  // It should contain the same values
  let sortedVals = sorted.Values |> List.ofSeq |> List.sort
  let origVals = subseries.Values |> List.ofSeq |> List.sort 
  sortedVals |> shouldEqual <| origVals

  // The right data positions were accessed
  valSrc.AccessedData |> Seq.distinct |> Seq.sort |> List.ofSeq 
  |> shouldEqual <| 
      [ for i in 4990 .. 4999 do yield 100, i
        for i in 0 .. 10 do yield 101, i ]

[<Test>]
let ``Can drop missing values from a small sub-series of a virtual series``() =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let missings = ts |> Series.mapValues (fun v -> if (int v) % 4 = 0 then nan else v)
  let dropped = missings.[date 1 4000 .. date 2 1000] |> Series.dropMissing
  dropped.KeyCount |> shouldEqual 1500
  dropped.ValueCount |> shouldEqual 1500

[<Test>]
let ``Calling SelectValues on a small series returns correct result``() =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100].SelectValues(fun f -> int f).Values
  |> List.ofSeq |> shouldEqual [2000000 .. 2000100]

[<Test>]
let ``Calling SelectOptional on a small series returns correct result``() =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100].SelectOptional(fun kvp -> OptionalValue.map int kvp.Value).Values
  |> List.ofSeq |> shouldEqual [2000000 .. 2000100]

[<Test>]
let ``Indexing small series ordinally returns correct result`` () =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100]
  |> Series.indexOrdinally |> Series.values
  |> List.ofSeq |> shouldEqual [2000000.0 .. 2000100.0]

[<Test>]
let ``Indexing small series with list of keys returns correct result`` () =
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100]
  |> Series.indexWith [ 0 .. 100 ] |> Series.values
  |> List.ofSeq |> shouldEqual [2000000.0 .. 2000100.0]

  s.[date 2 0 .. date 2 100]
  |> Series.indexWith [ 0 .. 50 ] |> Series.values
  |> List.ofSeq |> shouldEqual [2000000.0 .. 2000050.0]

[<Test>]
let ``Can diff small virtual series`` () = 
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100] |> Series.diff 1 |> Stats.sum |> shouldEqual 100.0

[<Test>]
let ``Can perform grouping on a small virtual series`` () = 
  let _, _, s = createTimeSeries 1000 (fun n -> 5000)
  s.[date 2 0 .. date 2 100] |> Series.groupInto (fun k _ -> k.Day) (fun _ s -> Stats.mean s)
  |> shouldEqual <| series [1 => 2000011.5; 2 => 2000035.5; 3 => 2000059.5; 4 => 2000083.5; 5 => 2000098. ]

// ------------------------------------------------------------------------------------------------
// Creating frames with vitual series
// ------------------------------------------------------------------------------------------------

let createSmallFrame partNum partSize =
  let accessMeta, ranges = createRanges partNum partSize
  let idxSrc = TrackingSource<DateTimeOffset>((ref [], accessMeta), idxValues, ranges)
  let valSrc1 = TrackingSource<float>((ref [], accessMeta), valValues (fun part idx -> part * 1000000.0 + idx), ranges)
  let valSrc2 = TrackingSource<float>((ref [], accessMeta), valValues (fun part idx -> part * 1000000.0 + idx + 1.0), ranges)
  Virtual.CreateFrame(idxSrc, ["A";"B"], [ valSrc1 :> IVirtualVectorSource; valSrc2 :> IVirtualVectorSource])

[<Test>]
let ``Indexing small frame ordinally returns correct result`` () = 
  let df = createSmallFrame 1000 (fun n -> 5000)
  df.Rows.[date 2 0 .. date 2 100]
  |> Frame.indexRowsOrdinally
  |> Frame.getCol "B"
  |> Series.values
  |> List.ofSeq |> shouldEqual [2000001.0 .. 2000101.0]

[<Test>]
let ``Can merge boxed column series`` () = 
  let df = createSmallFrame 1000 (fun n -> 5000)
  let rows1 = df.Rows.[date 3 0 .. date 3 10].Columns.["A"]
  let rows2 = df.Rows.[date 2 0 .. date 2 10].Columns.["A"]
  let sum1 = rows1.As<float>().Sum() + rows2.As<float>().Sum()
  let merged = Series.mergeAll [rows1; rows2]
  ObjectSeries(merged).As<float>().Sum() |> shouldEqual sum1

[<Test>]
let ``Transforming row keys of a small frame returns correct result`` () = 
  let df = createSmallFrame 1000 (fun n -> 5000)
  df.Rows.[date 2 0 .. date 2 100]
  |> Frame.mapRowKeys (fun dt -> dt.Ticks)
  |> Frame.getCol "B"
  |> Series.values
  |> List.ofSeq |> shouldEqual [2000001.0 .. 2000101.0]

[<Test>]
let ``Can create frame with two virtual series`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let ts2 = ts |> Series.mapValues sin
  let df = frame [ "Value" => ts; "Sin" => ts2 ]
  
  df.Format() |> ignore
  df.GetRowAt<float>(ts.KeyCount/2) |> 
    shouldEqual <| series ["Value" => 500000000.0; "Sin" => sin 500000000.0]

  accessedDataParts valSrc |> shouldEqual [0; 500; 999]


[<Test>]
let ``Can use ColumnApply and 'abs' on a created frame`` () =
  let idxSrc, valSrc, ts = createTimeSeries 1000 (fun n -> 5000)
  let ts2 = ts |> Series.mapValues sin
  let f1 = frame [ "Value" => ts; "Sin" => ts2 ]

  let f2 = f1.ColumnApply(fun (s:Series<_, float>) -> s |> Series.mapValues (fun v -> abs v) :> _) 
  let f3 = abs f1 
  let f4 = f2 - f3

  f4.Rows.[date 510 0 .. date 511 4999] |> Stats.sum
  |> shouldEqual <| series ["Value" => 0.0; "Sin" => 0.0]