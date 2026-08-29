#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualFrame
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Internal
open Deedle.Addressing
open Deedle.Vectors
open Deedle.Virtual
open Deedle.Vectors.Virtual
open Deedle.Tests.VirtualInstrumentation

// ------------------------------------------------------------------------------------------------
// Tracking source
// ------------------------------------------------------------------------------------------------

type LinearSubRange =
  { Offset : int; Step : int }
  interface IRangeRestriction<Address> with
    member x.Count = failwith "Count not supported"
  interface seq<Address> with
    member x.GetEnumerator() : System.Collections.Generic.IEnumerator<Address> = failwith "hard!"
  interface System.Collections.IEnumerable with
    member x.GetEnumerator() : System.Collections.IEnumerator = failwith "hard!"

module Address = LinearAddress

type TrackingSource<'T>(ranges:(int64*int64) list, valueAt:int64 -> 'T, ?asLong:'T -> int64, ?search) =
  member val AccessListCell : int64 list ref = ref [] with get, set
  member val LookupListCell = ref [] with get, set
  member val IsTracking = true with get, set
  member val HasMissing = true with get, set
  member x.AccessList = List.rev x.AccessListCell.Value
  member x.LookupList = List.rev x.LookupListCell.Value
  member x.Ranges = ranges
  member x.Length = ranges |> Seq.sumBy (fun (lo, hi) -> hi - lo + 1L)
  member x.AddressAt(index) =
    let res = Address.ofInt64 index
    res
  member x.IndexAt(address) =
    let res = Address.asInt64 address
    res

  interface IVirtualVectorSource with
    member x.Length = x.Length
    member x.AddressingSchemeID = "it"
    member x.ElementType = typeof<'T>
    member x.AddressOperations = Indices.Linear.LinearAddressOperations(0L, int64 x.Length-1L) :> _
    member x.Invoke(op) = op.Invoke(x)

  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) =
      let ranges = [ yield! x.Ranges; for x in sources do yield! (x :?> TrackingSource<'T>).Ranges ]
      TrackingSource
        ( ranges, valueAt, ?asLong=asLong, HasMissing = x.HasMissing, IsTracking = x.IsTracking,
          LookupListCell = x.LookupListCell, AccessListCell = x.AccessListCell ) :> _

    member x.LookupRange(v) =
      match search with
      | Some f ->
          let o, s = f v
          if o < 0 || s <= 0 then
            let addrs: Address list = []
            ({ new IRangeRestriction<Address> with
                member _.Count = 0L
               interface seq<Address> with
                 member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator()
               interface System.Collections.IEnumerable with
                 member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator() :> System.Collections.IEnumerator }
             |> RangeRestriction.Custom)
          else RangeRestriction.Custom { Offset = o; Step = s }
      | None -> failwith "Search not supported"

    member x.LookupValue(k, l, c) =
      let c = Func<int64,bool>(fun i -> c.Invoke(Address.ofInt64 i))
      if x.IsTracking then x.LookupListCell := (k, l) :: !x.LookupListCell
      let asLong = match asLong with None -> failwith "Lookup not supported" | Some g -> g
      let found = ranges |> Seq.fold (fun state (lo, hi) ->
          match state with
          | Choice1Of2(offset) ->
              let res =
                IndexUtilsModule.binarySearch (hi - lo + 1L) (Func<_, _>(fun i -> asLong (valueAt (lo + i)))) (asLong k) l c
                |> OptionalValue.map (fun i -> valueAt (lo + i), (offset + i) )
              if res.HasValue then Choice2Of2(res)
              else Choice1Of2(offset + hi - lo + 1L)
          | res -> res ) (Choice1Of2 0L)
      match found with
      | Choice2Of2 r -> OptionalValue((fst r.Value, Address.ofInt64(snd r.Value)))
      | _ -> OptionalValue.Missing

    member x.ValueAt loc =
      let r = ranges
      let res =
        r |> List.fold (fun (state:Choice<int64,int64>) (lo, hi) ->
          match state with
          | Choice1Of2 offset ->
              if (Address.asInt64 loc.Address) >= offset && (Address.asInt64 loc.Address) <= offset+hi-lo then
                Choice2Of2(((int64 loc.Address) - offset) + lo)
              else Choice1Of2(offset + hi - lo + 1L)
          | res -> res) (Choice1Of2 0L)
      match res with
      | Choice2Of2 absAddr ->
          if x.IsTracking then x.AccessListCell := absAddr  :: !x.AccessListCell
          if x.HasMissing && (absAddr % 3L = 0L) then OptionalValue.Missing
          else OptionalValue(valueAt absAddr )
      | Choice1Of2 oor -> failwith <| "ValueAt: out of range: " + oor.ToString()

    member x.GetSubVector(range) =
      match range.AsAbsolute(x.Length) with
      | Choice1Of2(nlo, nhi) ->
          if nhi < nlo then invalidOp "hi < lo"
          elif nlo < x.AddressAt(0L) then invalidOp "lo < 0"
          elif nhi > x.AddressAt(x.Length-1L) then invalidOp "hi > max" // TODO -1

          // This is not entirely correct, but it works well enough for tests..
          let _, ranges =
            ranges |> List.fold (fun (offset, ranges) (lo, hi) ->
              let ranges = ((offset, offset+hi-lo), (lo, hi)) :: ranges
              (offset + hi - lo + 1L), ranges ) (0L, [])
          let ranges = List.rev ranges

          let subRange =
            ranges |> List.tryPick (fun ((lo, hi), (absLo, absHi)) ->
              if nlo >= x.AddressAt lo && nhi <= x.AddressAt hi then
                Some(absLo + (x.IndexAt(nlo) - lo), absHi + (x.IndexAt(nhi) - hi))
              else None)

          let absLo, absHi =
            match subRange with Some(r) -> r | _ -> failwith "GetSubVector: TODO - get sub range not handled"

          TrackingSource
            ( [absLo, absHi], valueAt, ?asLong=asLong, HasMissing = x.HasMissing, IsTracking = x.IsTracking,

              LookupListCell = x.LookupListCell, AccessListCell = x.AccessListCell ) :> _
      | Choice2Of2(:? LinearSubRange as lr) ->
          let lo, hi =
            match ranges with
            | [lo, hi] -> lo, hi
            | _ -> failwith "Getting linear subrange is not implemented on merged sources"
          let valueAt i = valueAt(lo + int64 lr.Offset + (int64 lr.Step * i))
          let count = (hi + lo + 1L) / int64 lr.Step
          let count = if (hi + lo + 1L) % int64 lr.Step > int64 lr.Offset then count+1L else count
          TrackingSource
            ( [0L, count-1L], valueAt, ?asLong=asLong, HasMissing = x.HasMissing, IsTracking = x.IsTracking,
              LookupListCell = x.LookupListCell, AccessListCell = x.AccessListCell ) :> _
      | _ -> failwith "unexpected custom range!"

type TrackingSource =
  static member CreateLongs(lo, hi) = TrackingSource<int64>([lo, hi], id, id)
  static member CreateFloats(lo, hi) = TrackingSource<float>([lo, hi], float)
  static member CreateStrings(lo, hi) =
    let strings = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let search v =
      match strings |> Seq.tryFindIndex ((=) v) with
      | Some i -> i, strings.Length
      | None -> -1, max 1 strings.Length
    TrackingSource<string>([lo, hi], (fun i -> strings.[int i % strings.Length]), search=search)
  static member CreateTicks(lo, hi) =
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let asTicks ticks = start.Ticks + ticks * 987654321L
    TrackingSource<int64>([lo, hi], asTicks, id, HasMissing=false)
  static member CreateTimes(lo, hi) =
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let asDto ticks = start.AddTicks(ticks * 123456789L)
    TrackingSource<DateTimeOffset>([lo, hi], asDto, (fun dto -> dto.UtcTicks), HasMissing=false)

let date (y: int) (m: int) (d: int) = DateTimeOffset(DateTime(y, m, d), TimeSpan.FromHours(-1.0))
let ith i = (date 2000 1 1).AddTicks(i * 123456789L)
let fromTicks (t:int64) = DateTimeOffset(t, TimeSpan.FromHours(0.0)).ToOffset(TimeSpan.FromHours(8.0))
let toTicks (dto:DateTimeOffset) = dto.UtcTicks

// ------------------------------------------------------------------------------------------------
// Index search helpers (IndexUtilsModule in src/Deedle/VirtualFrame.fs)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can lookup exact key with IndexUtils binary search`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 4L valueAt 20L Lookup.Exact check
  |> shouldEqual (OptionalValue 1L)

[<Test>]
let ``Can lookup greater key with IndexUtils binary search`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 4L valueAt 25L Lookup.Greater check
  |> shouldEqual (OptionalValue 2L)

[<Test>]
let ``Can lookup smaller key with IndexUtils binary search`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 4L valueAt 25L Lookup.Smaller check
  |> shouldEqual (OptionalValue 1L)

[<Test>]
let ``Can return missing for absent key with IndexUtils exact lookup`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 4L valueAt 25L Lookup.Exact check
  |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Can return missing for IndexUtils binary search on empty range`` () =
  let valueAt = Func<_, _>(fun _ -> 0L)
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 0L valueAt 1L Lookup.Exact check
  |> shouldEqual OptionalValue.Missing

// ------------------------------------------------------------------------------------------------
// Virtual construction API (src/Deedle/VirtualFrame.fs)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can throw when CreateOrdinalFrame source lengths differ`` () =
  let short = InstrumentedOrdinalSource.createLongs 10L |> snd
  let long = InstrumentedOrdinalSource.createLongs 20L |> snd
  (fun () ->
    Virtual.CreateOrdinalFrame(["A"; "B"], [short :> IVirtualVectorSource; long :> IVirtualVectorSource])
    |> ignore)
  |> should throw typeof<System.ArgumentException>

[<Test>]
let ``Can throw when CreateOrdinalFrame has no columns`` () =
  (fun () -> Virtual.CreateOrdinalFrame([], []) |> ignore)
  |> should throw typeof<System.ArgumentException>

[<Test>]
let ``Can create ordinal virtual series and read boundary keys`` () =
  let src = OrdinalVirtualSource(5L, (fun i -> OptionalValue(i)), "test")
  let series = Virtual.CreateOrdinalSeries(src)
  series.KeyCount |> shouldEqual 5
  series.TryGet(0L) |> shouldEqual (OptionalValue 0L)
  series.TryGet(4L) |> shouldEqual (OptionalValue 4L)
  series.TryGet(5L) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Can create ordinal virtual frame with expected shape`` () =
  let _, colA = InstrumentedOrdinalSource.createLongs 8L
  let _, colB = InstrumentedOrdinalSource.createFloats 8L
  let frame = Virtual.CreateOrdinalFrame(["A"; "B"], [colA :> IVirtualVectorSource; colB :> IVirtualVectorSource])
  frame.RowCount |> shouldEqual 8
  frame.ColumnCount |> shouldEqual 2
  frame.ColumnKeys |> Seq.toList |> shouldEqual ["A"; "B"]
  frame.GetColumn<int64>("A").GetAt(3) |> shouldEqual 3L
  frame.GetColumn<float>("B").GetAt(3) |> shouldEqual 3.0

[<Test>]
let ``Can lookup ExactOrGreater and ExactOrSmaller with IndexUtils on exact hit`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtilsModule.binarySearch 4L valueAt 20L Lookup.ExactOrGreater check
  |> shouldEqual (OptionalValue 1L)
  IndexUtilsModule.binarySearch 4L valueAt 20L Lookup.ExactOrSmaller check
  |> shouldEqual (OptionalValue 1L)

[<Test>]
let ``IndexUtils static BinarySearch wrapper matches module`` () =
  let values = [| 1L; 3L; 5L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun _ -> true)
  IndexUtils.BinarySearch(3L, valueAt, 3L, Lookup.Exact, check)
  |> shouldEqual (IndexUtilsModule.binarySearch 3L valueAt 3L Lookup.Exact check)

[<Test>]
let ``IndexUtils binary search skips rows rejected by check`` () =
  let values = [| 10L; 20L; 30L; 40L |]
  let valueAt = Func<_, _>(fun i -> values.[int i])
  let check = Func<_, _>(fun i -> i % 2L = 0L)
  IndexUtilsModule.binarySearch 4L valueAt 20L Lookup.Exact check
  |> shouldEqual OptionalValue.Missing
  IndexUtilsModule.binarySearch 4L valueAt 30L Lookup.Exact check
  |> shouldEqual (OptionalValue 2L)

[<Test>]
let ``Can read ValueAt from OrdinalVirtualSource`` () =
  let src = OrdinalVirtualSource(3L, (fun i -> OptionalValue(float i * 2.0)), "test") :> IVirtualVectorSource<float>
  src.ValueAt(KnownLocation(Address.ofInt64 1L, 1L)) |> shouldEqual (OptionalValue 2.0)
  src.Length |> shouldEqual 3L

[<Test>]
let ``Can throw when OrdinalVirtualSource MergeWith has mismatched source types`` () =
  let a = OrdinalVirtualSource(4L, (fun i -> OptionalValue(i)), "test") :> IVirtualVectorSource<int64>
  let mapped = VirtualVectorSource.map None (fun _ ov -> ov) a
  (fun () -> a.MergeWith([mapped]) |> ignore)
  |> should throw typeof<InvalidOperationException>

[<Test>]
let ``Can MergeWith two OrdinalVirtualSources and LookupValue Exact`` () =
  let a = OrdinalVirtualSource(3L, (fun i -> OptionalValue(i)), "test", asLong=id)
  let b = OrdinalVirtualSource(2L, (fun i -> OptionalValue(i + 100L)), "test", asLong=id)
  let merged = (a :> IVirtualVectorSource<_>).MergeWith([ b :> IVirtualVectorSource<_> ])
  merged.Length |> shouldEqual 5L
  merged.ValueAt(KnownLocation(Address.ofInt64 3L, 3L)) |> shouldEqual (OptionalValue 100L)
  let hit = merged.LookupValue(100L, Lookup.Exact, fun _ -> true)
  hit.HasValue |> shouldEqual true
  Address.asInt64 (snd hit.Value) |> shouldEqual 3L

[<Test>]
let ``Can throw when OrdinalVirtualSource LookupValue has no asLong`` () =
  let src = OrdinalVirtualSource(3L, (fun i -> OptionalValue(float i)), "test") :> IVirtualVectorSource<float>
  (fun () -> src.LookupValue(1.0, Lookup.Exact, fun _ -> true) |> ignore)
  |> should throw typeof<InvalidOperationException>

[<Test>]
let ``Can LookupRange and GetSubVector on OrdinalVirtualSource with Step mode`` () =
  let words = [| "a"; "b"; "c" |]
  let src =
    OrdinalVirtualSource(
      12L,
      (fun i -> OptionalValue(words.[int (i % 3L)])),
      "test",
      lookupRange=VirtualLookupRange.forRepeatingCycle words)
    :> IVirtualVectorSource<string>
  match src.LookupRange("b") with
  | RangeRestriction.Custom(:? Deedle.Virtual.StepRange as sr) ->
      sr.Offset |> shouldEqual 1
      sr.Step |> shouldEqual 3
  | other -> failwithf "expected StepRange, got %A" other
  let sub = src.GetSubVector(src.LookupRange("b"))
  sub.Length |> shouldEqual 4L
  sub.ValueAt(KnownLocation(Address.ofInt64 0L, 0L)) |> shouldEqual (OptionalValue "b")

// ------------------------------------------------------------------------------------------------
// Some trivial testing for TrackingSource
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Lookup and ValueAt works on merged tracking sources`` () =
  let source1 = TrackingSource.CreateTimes(0L, 10L) :> IVirtualVectorSource<_>
  let source2 = TrackingSource.CreateTimes(10000000L, 10000010L) :> IVirtualVectorSource<_>
  let sources = source1.MergeWith [source2]
  source1.ValueAt(KnownLocation(Address.ofInt64 0L, 0L)).Value |> shouldEqual (ith 0L)
  source2.ValueAt(KnownLocation(Address.ofInt64 0L, 0L)).Value |> shouldEqual (ith 10000000L)
  sources.ValueAt(KnownLocation(Address.ofInt64 11L, 11L)).Value |> shouldEqual (ith 10000000L)
  sources.LookupValue(ith 0L, Lookup.Exact, fun _ -> true).Value |> fst |> shouldEqual (ith 0L)
  sources.LookupValue(ith 10L, Lookup.Exact, fun _ -> true).Value |> fst |> shouldEqual (ith 10L)
  sources.LookupValue(ith 100L, Lookup.Exact, fun _ -> true).HasValue |> shouldEqual false
  sources.LookupValue(ith 10000000L, Lookup.Exact, fun _ -> true).Value |> fst |> shouldEqual (ith 10000000L)
  sources.LookupValue(ith 10000010L, Lookup.Exact, fun _ -> true).Value |> fst |> shouldEqual (ith 10000010L)

// ------------------------------------------------------------------------------------------------
// Virtual series tests
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Formatting accesses only printed values`` () =
  let src = TrackingSource.CreateLongs(0L, 1000000000L)
  let series = Virtual.CreateOrdinalSeries(src)
  series.Format(3, 3, false) |> ignore
  src.AccessList |> shouldEqual [ 0L; 1L; 2L; 1000000000L-2L; 1000000000L-1L; 1000000000L ]

[<Test>]
let ``Counting keys does not evaluate the series`` () =
  let src = TrackingSource.CreateLongs(0L, 1000000000L)
  let series = Virtual.CreateOrdinalSeries(src)
  series.KeyCount |> shouldEqual 1000000001
  src.AccessList |> shouldEqual []

[<Test>]
let ``Counting values does not run out of memory`` () =
  let src = TrackingSource.CreateLongs(0L, 10000000L, IsTracking=false)
  let series = Virtual.CreateOrdinalSeries(src)
  series.ValueCount |> shouldEqual 6666667

[<Test>]
let ``Can take skip etc. without evaluating the series`` () =
  let src = TrackingSource.CreateFloats(0L, 10000000L)
  let s1 = Virtual.CreateOrdinalSeries(src)
  s1 |> Series.take 10 |> Stats.sum |> shouldEqual 27.0
  src.AccessList |> Seq.length |> shouldEqual 10
  s1 |> Series.skipLast (10000000-9) |> Stats.sum |> shouldEqual 27.0
  src.AccessList |> Seq.length |> shouldEqual 20
  s1 |> Series.skip (10000000-9) |> Stats.sum |> shouldEqual 69999967.0
  src.AccessList |> Seq.length |> shouldEqual 30
  s1 |> Series.takeLast 10 |> Stats.sum |> shouldEqual 69999967.0
  src.AccessList |> Seq.length |> shouldEqual 40

[<Test>]
let ``Sliced series contains same values as original series`` () =
  let src = TrackingSource.CreateFloats(0L, 10000000L)
  let s1 = Virtual.CreateOrdinalSeries(src)
  let s2 = s1.[5123457L .. 5123557L]
  for i in 5123457L .. 5123557L do
    s1.TryGet(i) |> shouldEqual <| s2.TryGet(i)

[<Test>]
let ``Can perform slicing without evaluating the series`` () =
  let src = TrackingSource.CreateFloats(0L, 10000000L)
  let s1 = Virtual.CreateOrdinalSeries(src)
  let s2 = s1.[10000000L-9L ..]
  let s3 = s1.[.. 9L]

  (Stats.sum s2) + (Stats.sum s3) |> shouldEqual 69999994.0
  src.AccessList |> Seq.length |> shouldEqual 20
  src.AccessList |> Seq.sum |> shouldEqual 100000000L

[<Test>]
let ``Can access elements by key-based lookup`` () =
  let src = TrackingSource.CreateFloats(0L, 10000000L)
  let s1 = Virtual.CreateOrdinalSeries(src)
  s1.TryGet(1234567L) |> shouldEqual (OptionalValue 1234567.0)
  s1.TryGet(1234568L) |> shouldEqual (OptionalValue 1234568.0)
  s1.TryGet(1234569L) |> shouldEqual OptionalValue.Missing
  src.AccessList |> shouldEqual [1234567L; 1234568L; 1234569L]

[<Test>]
let ``Can materialize virtual series and access it repeatedly`` () =
  let src = TrackingSource.CreateFloats(0L, 10000000L)
  let sv = Virtual.CreateOrdinalSeries(src)
  let sm = sv.[100L .. 200L].Materialize()
  sm |> Stats.mean |> ignore
  sm |> Stats.sum |> ignore
  src.AccessList |> shouldEqual [ 100L .. 200L ]

// ------------------------------------------------------------------------------------------------
// Virtual series with ordered index
// ------------------------------------------------------------------------------------------------

let createTimeSeries () =
  let idxSrc = TrackingSource.CreateTimes(0L, 10000000L)
  let valSrc = TrackingSource.CreateFloats(0L, 10000000L)
  let sv = Virtual.CreateSeries(idxSrc, valSrc)
  idxSrc, valSrc, sv

[<Test>]
let ``Can access elements in an ordered time series without evaluating it`` () =
  let isrc, vsrc, s = createTimeSeries()
  s.[ith 5000000L] |> shouldEqual 5000000.0
  s.TryGet(ith 5000001L) |> shouldEqual OptionalValue.Missing
  isrc.LookupList |> shouldEqual [ith 5000000L, Lookup.Exact; ith 5000001L, Lookup.Exact]
  isrc.AccessList |> shouldEqual []
  vsrc.LookupList |> shouldEqual []
  vsrc.AccessList |> shouldEqual [5000000L; 5000001L]

[<Test>]
let ``Can use different lookup behaviours when accessing time series values`` () =
  let isrc, vsrc, s = createTimeSeries()
  s.Get(ith 5000001L, Lookup.ExactOrGreater) |> shouldEqual 5000002.0
  s.Get(ith 5000001L, Lookup.ExactOrSmaller) |> shouldEqual 5000000.0
  s.Get(ith 5000000L, Lookup.Greater) |> shouldEqual 5000002.0
  s.Get(ith 5000000L, Lookup.Smaller) |> shouldEqual 4999999.0
  isrc.LookupList |> Seq.length |> shouldEqual 4
  isrc.AccessList |> shouldEqual []
  set vsrc.AccessList |> shouldEqual <| set [ 4999999L .. 5000002L ]

[<Test>]
let ``Can perform slicing on time series without evaluating it`` () =
  let isrc, vsrc, s1 = createTimeSeries()

  // TODO: s1.[x] = s2.[x]
  // s1.[ith 2778364L]

  let s2 = s1.[date 2001 1 1 .. date 2001 2 1]
  // s2.[ith 2778364L]
  fst s2.KeyRange |> should be (greaterThanOrEqualTo (date 2001 1 1))
  snd s2.KeyRange |> should be (lessThanOrEqualTo (date 2001 2 1))
  s2.[ith 2700001L] |> shouldEqual <| s1.[ith 2700001L]
  isrc.AccessList |> set |> Seq.length |> shouldEqual 2
  isrc.LookupList |> set |> Seq.length |> shouldEqual 3
  vsrc.AccessList |> set |> Seq.length |> shouldEqual 1

// ------------------------------------------------------------------------------------------------
// Virtual frame tests
// ------------------------------------------------------------------------------------------------

let createSimpleFrameSize size =
  let s1 = TrackingSource.CreateLongs(0L, size)
  let s2 = TrackingSource.CreateStrings(0L, size)
  let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1; s2])
  s1, s2, frame

let createSimpleFrame() = createSimpleFrameSize(10000000L)

let createSimpleTimeFrame() =
  let idxSrc = TrackingSource.CreateTimes(0L, 10000000L)
  let s1 = TrackingSource.CreateLongs(0L, 10000000L)
  let s2 = TrackingSource.CreateStrings(0L, 10000000L, HasMissing=false)
  let frame = Virtual.CreateFrame(idxSrc, ["S1"; "S2"], [s1; s2] )
  idxSrc, s1, s2, frame

let createNumericFrame() =
  let s1 = TrackingSource.CreateFloats(0L, 10000000L, HasMissing=false)
  let s2 = TrackingSource.CreateFloats(0L, 10000000L)
  let frame = Virtual.CreateOrdinalFrame( ["Dense"; "Sparse"], [s1; s2] )
  s1, s2, frame

let createTicksFrame() =
  let s1 = TrackingSource.CreateTicks(0L, 10000000L)
  let s2 = TrackingSource.CreateFloats(0L, 10000000L)
  let frame = Virtual.CreateOrdinalFrame( ["Ticks"; "Values"], [s1; s2] )
  s1, s2, frame

// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can format virtual frame without evaluating it`` () =
  let s1, s2, frame = createSimpleFrame()
  frame.Format(2, 2, 2, 2) |> ignore
  s1.AccessList |> shouldEqual [0L; 1L; 9999999L; 10000000L]
  s2.AccessList |> shouldEqual [0L; 1L; 9999999L; 10000000L]

[<Test>]
let ``Accessing row evaluates only the required values`` () =
  let s1, s2, frame = createSimpleFrame()
  frame.GetRow<obj>(5000000L).["S1"] |> shouldEqual <| box 5000000L
  frame.["S2", 5000000L] |> shouldEqual <| box "lorem"
  s1.AccessList |> shouldEqual [5000000L]
  s2.AccessList |> shouldEqual [5000000L]

[<Test>]
let ``Accessing series of rows accesses only required values`` () =
  let s1, s2, frame = createSimpleFrame()
  frame.Rows.Format(2,2, false) |> ignore
  s1.AccessList |> shouldEqual [0L; 1L; 9999999L; 10000000L]
  s2.AccessList |> shouldEqual [0L; 1L; 9999999L; 10000000L]

[<Test>]
let ``Can use ColumnsApply and 'sin' without evaluating a frame`` () =
  let s1 = TrackingSource.CreateFloats(0L, 10000000000L)
  let s2 = TrackingSource.CreateFloats(0L, 10000000000L)
  let f1 = Virtual.CreateOrdinalFrame( ["S1"; "S2"], [s1; s2] )
  let f2 = f1.ColumnApply<float>(fun s -> s |> Series.mapValues (fun v -> v / 1000000000.0) :> _)
  let f3 = sin f2
  f3.GetRow<float>(3141592654L) |> Stats.mean |> should (equalWithin 1.0e-8) 0.0
  s1.AccessList |> shouldEqual [3141592654L]
  s2.AccessList |> shouldEqual [3141592654L]

[<Test>]
let ``Can map over frame rows without evaluating it`` () =
  let s1, s2, frame = createSimpleFrame()
  let mapped = frame |> Frame.mapRows (fun k row -> sqrt row?S1)
  mapped.[10000L] |> shouldEqual 100.0
  s1.AccessList |> shouldEqual [10000L]
  s2.AccessList |> shouldEqual []

[<Test>]
let ``Can perform slicing on frame using the Rows property`` () =
  let s1, s2, f1 = createSimpleFrame()
  let f2 = f1.Rows.[100L .. 999900L]
  let f3 = f2.Rows.[1000L .. 999000L]
  let f4 = f3.Rows.[500000L .. 500005L]

  f4.RowIndex.KeyRange
  |> shouldEqual (500000L, 500005L)

  let expected =
    [ for i in 500000L .. 500005L ->
        f1.GetColumn<string>("S2").TryGet(i) |> OptionalValue.asOption ]

  f4.GetColumn<string>("S2")
  |> Series.valuesAll
  |> List.ofSeq
  |> shouldEqual expected

[<Test>]
let ``Can access Columns of a virtual frame without evaluating the data`` () =
  let s1, s2, f = createSimpleFrame()
  let cols = f.Columns
  cols.Keys |> List.ofSeq |> shouldEqual ["S1"; "S2"]
  cols.["S1"].[10L] |> unbox |> shouldEqual 10L
  s1.AccessList |> shouldEqual [10L]
  s2.AccessList |> shouldEqual []

// TODO: Fix the following test case which creates ObjectSeries with 3 keys and 2 values (issue 330)
[<Test>]
let ``Can add computed series as a new column to a frame with the same index``() =
  let s1, s2, f = createNumericFrame()
  let times = f |> Frame.mapRows (fun _ row ->
    let t = row.GetAs<int64>("Dense")
    DateTimeOffset(DateTime(2000,1,1).AddTicks(t * 1233456789L), TimeSpan.FromHours(1.0)) )
  f.AddColumn("Times", times)
  f.GetRow<obj>(5000001L).["Dense"] |> shouldEqual (box 5000001L)
  f.GetRow<obj>(5000001L).TryGet("Sparse") |> shouldEqual OptionalValue.Missing
  (f.GetRow<obj>(5000001L).["Times"] |> unbox<DateTimeOffset>).Year |> shouldEqual 2019
  set s1.AccessList |> shouldEqual <| set [5000001L]
  set s2.AccessList |> shouldEqual <| set [5000001L]

[<Test>]
let ``Can index frame by an ordered column computed using series transform`` () =
  let s1, s2, f = createTicksFrame()
  f?Times <- f.GetColumn<int64>("Ticks") |> Series.convert fromTicks toTicks
  let byTimes = f |> Frame.indexRowsDateTimeOffset "Times"

  byTimes.Rows.TryGet(date 2010 1 1, Lookup.Exact) |> shouldEqual OptionalValue.Missing
  let prev = byTimes.Rows.Get(date 2010 1 1, Lookup.ExactOrSmaller).["Ticks"] |> unbox<int64> |> fromTicks
  let next = byTimes.Rows.Get(date 2010 1 1, Lookup.ExactOrGreater).["Ticks"] |> unbox<int64> |> fromTicks
  prev < date 2010 1 1 |> shouldEqual true
  next > date 2010 1 1 |> shouldEqual true
  ((date 2010 1 1) - prev).Ticks + (next - (date 2010 1 1)).Ticks |> shouldEqual 987654321L

[<Test>]
let ``Can replace column in a frame with a computed column (with the same index)``() =
  let s1, s2, f = createNumericFrame()
  let byDense = f.IndexRows<int>("Dense")
  let sparseAsString = byDense.GetColumn<string>("Sparse")
  byDense.ReplaceColumn("Sparse", sparseAsString)
  // The 'Sparse' column is represented as vector of strings
  f.Format(true).Contains("(string)") |> shouldEqual false
  byDense.Format(true).Contains("(string)") |> shouldEqual true

[<Test>]
let ``Sorting a virtual frame that is already sorted does not throw an exception`` () =
  let s1, s2, f1 = createSimpleFrame()
  let f2 = f1.SortRowsByKey()
  f1.RowIndex.IsOrdered |> shouldEqual true
  f2.RowIndex.IsOrdered |> shouldEqual true

[<Test>]
let ``Can merge ordinally-indexed virtual frames`` () =
  let s1, s2, f = createSimpleFrame()
  let fs = f.Rows.[1000000L .. 2000000L]
  let fe = f.Rows.[5000001L .. 6000001L]
  let m = fs.Merge(fe)

  for idx, fpart in [ 1000000L,fs; 1500000L,fs; 2000000L,fs; 5000001L,fe; 5500001L,fe; 6000001L,fe ] do
    m.Rows.[idx] |> shouldEqual f.Rows.[idx]
    m.Rows.[idx] |> shouldEqual fpart.Rows.[idx]

[<Test>]
let ``Can merge ordinally-indexed virtual series of rows`` () =
  let s1, s2, f = createSimpleFrame()
  let fsr = f.Rows.[1000000L .. 2000000L].Rows
  let fer = f.Rows.[5000001L .. 6000001L].Rows
  let fmr = fsr.Merge(fer)

  fmr.KeyCount |> shouldEqual (fsr.KeyCount + fer.KeyCount)
  for idx, fpart in [ 1000000L,fsr; 1500000L,fsr; 2000000L,fsr; 5000001L,fer; 5500001L,fer; 6000001L,fer ] do
    fmr.[idx] |> shouldEqual f.Rows.[idx]
    fmr.[idx] |> shouldEqual fpart.[idx]

[<Test>]
let ``Merging overlapping ordinally-indexed virtual frames fails`` () =
  let s1, s2, f = createSimpleFrame()
  let f0 = f.Rows.[1000000L .. 2000000L]
  (fun _ -> f0.Merge(f.Rows.[2000000L .. 3000000L]) |> ignore) |> shouldThrow<InvalidOperationException>
  (fun _ -> f0.Merge(f.Rows.[0900000L .. 1000000L]) |> ignore) |> shouldThrow<InvalidOperationException>
  (fun _ -> f0.Merge(f.Rows.[0500000L .. 1500000L]) |> ignore) |> shouldThrow<InvalidOperationException>
  (fun _ -> f0.Merge(f.Rows.[1500000L .. 2500000L]) |> ignore) |> shouldThrow<InvalidOperationException>

// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can filter virtual frame by a value in a non-index column`` () =
  let idx, s1, s2, f = createSimpleTimeFrame()
  let partsLength =
    "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    |> Seq.map (fun s -> f |> Frame.filterRowsBy "S2" s)
    |> Seq.map (fun f -> f.RowCount)
    |> Seq.sum
  partsLength |> shouldEqual f.RowCount
  idx.AccessList |> shouldEqual []
  s1.AccessList |> shouldEqual []
  s2.AccessList |> shouldEqual []

[<Test>]
let ``Can access items of a virtual filtered frame without evaluating it`` () =
  let idx, s1, s2, f = createSimpleTimeFrame()
  let lorem = f |> Frame.filterRowsBy "S2" "lorem"
  lorem.Rows.[ith 5000000L].["S2"] |> unbox |> shouldEqual "lorem"
  lorem.Rows.TryGet(ith 5000001L) |> shouldEqual OptionalValue.Missing
  lorem.Rows.Get(date 2001 1 1, Lookup.ExactOrSmaller).["S2"] |> unbox |> shouldEqual "lorem"
  lorem.Rows.Get(date 2001 1 1, Lookup.ExactOrGreater).["S2"] |> unbox |> shouldEqual "lorem"
  lorem.Rows.Get(date 2001 1 1, Lookup.ExactOrGreater)?S1 - lorem.Rows.Get(date 2001 1 1, Lookup.ExactOrSmaller)?S1 |> shouldEqual 8.0
  set s1.AccessList |> shouldEqual <| set [320176L; 320177L]
  set s2.AccessList |> shouldEqual <| set [320176L; 320177L; 625000L]

[<Test>]
let ``Filtering items by value behaves correctly at the beginning & end`` () =
  let idx, s1, s2, f = createSimpleTimeFrame()
  let lastValues =
    "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    |> Seq.map (fun s -> f |> Frame.filterRowsBy "S2" s)
    |> Seq.map (fun f -> int (f?S1.GetAt(int (f.RowIndex.KeyCount - 1L))))
    |> set
  lastValues |> shouldEqual <| set [9999993; 9999994; 9999995; 9999996; 9999997; 9999998; 9999999; 10000000]


[<Test>]
let ``Can merge virtual frames indexed by time`` () =
  let idx, s1, s2, f = createSimpleTimeFrame()
  let fs = f.Rows.[date 2000 1 1 .. date 2001 1 1]
  let fe = f.Rows.[date 2002 1 1 .. date 2003 1 1]
  let m = fs.Merge(fe)
  m.RowCount |> shouldEqual (fs.RowCount + fe.RowCount)
  m.Rows.[ith 1L] |> shouldEqual <| fs.Rows.[ith 1L]
  m.Rows.[ith 1000000L] |> shouldEqual <| fs.Rows.[ith 1000000L]
  m.Rows.[ith 7000000L] |> shouldEqual <| fe.Rows.[ith 7000000L]
  m.Rows.[ith 7670246L] |> shouldEqual <| fe.Rows.[ith 7670246L]


[<Test>]
let ``Can merge virtual series of rows indexed by time`` () =
  let idx, s1, s2, f = createSimpleTimeFrame()
  let fsr = f.Rows.[date 2000 1 1 .. date 2001 1 1].Rows
  let fer = f.Rows.[date 2002 1 1 .. date 2003 1 1].Rows
  let fmr = fsr.Merge(fer)
  fmr.KeyCount |> shouldEqual (fsr.KeyCount + fer.KeyCount)
  fmr.[ith 1L] |> shouldEqual <| fsr.[ith 1L]
  fmr.[ith 1000000L] |> shouldEqual <| fsr.[ith 1000000L]
  fmr.[ith 7000000L] |> shouldEqual <| fer.[ith 7000000L]
  fmr.[ith 7670246L] |> shouldEqual <| fer.[ith 7670246L]

// ------------------------------------------------------------------------------------------------
// Batch materialization iterator
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can materialize float batches lazily reading only per-batch rows`` () =
  let len = 100L
  let batchSize = 10L

  let counters = AccessCounters()
  let srcA =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), counters, hasMissing=false)
    :> IVirtualVectorSource
  let srcB =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i + 1_000_000.0), counters, hasMissing=false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame(["A"; "B"], [ srcA; srcB ])

  let batches = Virtual.MaterializeFloatBatches(frame, batchSize, ["A"; "B"])
  counters.ValueAtCount |> shouldEqual 0

  use e = batches.GetEnumerator()
  ignore (e.MoveNext())
  let b1 = e.Current

  b1.Features.Length |> shouldEqual 10
  b1.Features.[0].Length |> shouldEqual 2
  b1.Features.[0].[0] |> shouldEqual 0.0
  b1.Features.[9].[0] |> shouldEqual 9.0
  b1.Features.[0].[1] |> shouldEqual 1_000_000.0

  let after1 = counters.Snapshot()
  after1.ValueAtCount |> shouldEqual (int (batchSize * 2L))
  after1.ValueAtCount |> should be (lessThan (int (len * 2L)))

  ignore (e.MoveNext())
  let _b2 = e.Current
  let after2 = counters.Snapshot()
  after2.ValueAtCount |> shouldEqual (int (batchSize * 2L * 2L)) // 2 batches × 10 rows × 2 columns

[<Test>]
let ``Can map missing values to NaN in float batches by default`` () =
  let len = 12L
  let batchSize = 12L

  let counters = AccessCounters()
  let srcA =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), counters, hasMissing=true, addrMap=id)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame(["A"], [ srcA ])

  let (batch : FloatBatch<int64>) = Virtual.MaterializeFloatBatches(frame, batchSize, ["A"]) |> Seq.head
  let f = batch.Features

  // Missing condition in InstrumentedOrdinalSource: absAddr % 3L = 0L => indices 0, 3, 6, 9
  (Double.IsNaN f.[0].[0]) |> shouldEqual true
  f.[1].[0] |> shouldEqual 1.0
  (Double.IsNaN f.[3].[0]) |> shouldEqual true
  (Double.IsNaN f.[9].[0]) |> shouldEqual true

[<Test>]
let ``Can materialize a partial last batch with optional row keys`` () =
  let len = 25L
  let batchSize = 10L

  let counters = AccessCounters()
  let srcA =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), counters, hasMissing=false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame(["A"], [ srcA ])

  let batches = Virtual.MaterializeFloatBatches(frame, batchSize, ["A"], includeRowKeys=true)
  let bs = batches |> Seq.toArray

  bs.Length |> shouldEqual 3
  bs.[0].Features.Length |> shouldEqual 10
  bs.[1].Features.Length |> shouldEqual 10
  bs.[2].Features.Length |> shouldEqual 5

  let rowKeys = bs.[2].RowKeys.Value
  rowKeys |> shouldEqual [| 20L; 21L; 22L; 23L; 24L |]

  // Row-key extraction should not introduce extra value reads (ordinal keys are derived from address ranges).
  counters.ValueAtCount |> shouldEqual 25

[<Test>]
let ``Can fill missing float-batch cells with a caller-supplied value`` () =
  let len = 6L
  let counters = AccessCounters()
  let srcA =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), counters, hasMissing=true, addrMap=id)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame(["A"], [ srcA ])

  let (batch : FloatBatch<int64>) =
    Virtual.MaterializeFloatBatches(frame, len, ["A"], missingPolicy=FloatMissingPolicy.Value(-1.0))
    |> Seq.head
  let f = batch.Features

  f.[0].[0] |> shouldEqual -1.0
  f.[1].[0] |> shouldEqual 1.0
  f.[3].[0] |> shouldEqual -1.0

[<Test>]
let ``Can throw when float-batch size is not positive`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 4L
  let frame = Virtual.CreateOrdinalFrame(["A"], [ src :> IVirtualVectorSource ])
  (fun () -> Virtual.MaterializeFloatBatches(frame, 0L, ["A"]) |> ignore)
  |> should throw typeof<System.ArgumentException>
  (fun () -> Virtual.MaterializeFloatBatches(frame, -5L, ["A"]) |> ignore)
  |> should throw typeof<System.ArgumentException>

[<Test>]
let ``Can throw when float-batch column key is unknown`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 4L
  let frame = Virtual.CreateOrdinalFrame(["A"], [ src :> IVirtualVectorSource ])
  (fun () ->
    Virtual.MaterializeFloatBatches(frame, 2L, ["Missing"])
    |> Seq.head
    |> ignore)
  |> should throw typeof<System.ArgumentException>

[<Test>]
let ``Can materialize float batches with empty column list`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 5L
  let frame = Virtual.CreateOrdinalFrame(["A"], [ src :> IVirtualVectorSource ])
  let batch = Virtual.MaterializeFloatBatches(frame, 5L, []) |> Seq.head
  batch.Features.Length |> shouldEqual 5
  batch.Cols |> shouldEqual 0
  batch.Features.[0].Length |> shouldEqual 0
  batch.FeaturesFlat.Length |> shouldEqual 0
  batch.RowKeys |> shouldEqual None

[<Test>]
let ``Can materialize float batches with labels and missing mask`` () =
  let len = 6L
  let srcX =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let srcY =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i * 10.0), AccessCounters(), hasMissing = true, addrMap = id)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "X"; "Y" ], [ srcX; srcY ])
  let batch =
    Virtual.MaterializeFloatBatches(
      frame,
      len,
      [ "X" ],
      labelsColumn = "Y",
      includeMissingMask = true)
    |> Seq.head
  batch.FeaturesFlat.[0] |> shouldEqual 0.0
  batch.Labels.Value.[1] |> shouldEqual 10.0
  batch.MissingMask.Value.[0] |> shouldEqual false
  Double.IsNaN batch.Labels.Value.[3] |> shouldEqual true

[<Test>]
let ``Can materialize float batches from int64 columns`` () =
  let len = 4L
  let src =
    InstrumentedOrdinalSource<int64>(len, (fun i -> i + 1L), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A" ], [ src ])
  let batch = Virtual.MaterializeFloatBatches(frame, len, [ "A" ]) |> Seq.head
  batch.FeaturesFlat.[0] |> shouldEqual 1.0
  batch.FeaturesFlat.[3] |> shouldEqual 4.0

[<Test>]
let ``Can materialize column-major float batches`` () =
  let len = 3L
  let srcA =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let srcB =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i + 100.0), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A"; "B" ], [ srcA; srcB ])
  let batch =
    Virtual.MaterializeFloatBatches(frame, len, [ "A"; "B" ], layout = FloatBatchLayout.ColumnMajor)
    |> Seq.head
  batch.FeaturesFlat.[0] |> shouldEqual 0.0
  batch.FeaturesFlat.[1] |> shouldEqual 1.0
  batch.FeaturesFlat.[2] |> shouldEqual 2.0
  batch.FeaturesFlat.[3] |> shouldEqual 100.0

[<Test>]
let ``Can cap float batch export with maxRows`` () =
  let len = 20L
  let src =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A" ], [ src ])
  let batches =
    Virtual.MaterializeFloatBatches(frame, 7L, [ "A" ], maxRows = 15L)
    |> Seq.toArray
  batches.Length |> shouldEqual 3
  batches.[0].Rows |> shouldEqual 7
  batches.[1].Rows |> shouldEqual 7
  batches.[2].Rows |> shouldEqual 1

[<Test>]
let ``Can materialize random float batches covering each row exactly once`` () =
  let len = 25L
  let batchSize = 7L
  let src =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A" ], [ src ])
  let allValues =
    Virtual.MaterializeFloatBatches(
      frame,
      batchSize,
      [ "A" ],
      order = FloatBatchOrder.ShuffledWithSeed 42)
    |> Seq.collect (fun b -> b.FeaturesFlat)
    |> Seq.toArray
  allValues.Length |> shouldEqual 25
  allValues |> Array.sort |> shouldEqual [| for i in 0.0 .. 24.0 -> i |]

[<Test>]
let ``Can reproduce random float batches with a fixed seed`` () =
  let len = 12L
  let src =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), AccessCounters(), hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A" ], [ src ])
  let first seed =
    Virtual.MaterializeFloatBatches(frame, 4L, [ "A" ], order = FloatBatchOrder.ShuffledWithSeed seed)
    |> Seq.head
    |> fun b -> b.FeaturesFlat |> Array.copy
  first 7 |> shouldEqual (first 7)
  first 7 |> should not' (equal (first 8))

[<Test>]
let ``Can materialize random float batches lazily one batch at a time`` () =
  let len = 100L
  let batchSize = 10L
  let counters = AccessCounters()
  let src =
    InstrumentedOrdinalSource<float>(len, (fun i -> float i), counters, hasMissing = false)
    :> IVirtualVectorSource
  let frame = Virtual.CreateOrdinalFrame([ "A" ], [ src ])
  let batches =
    Virtual.MaterializeFloatBatches(
      frame,
      batchSize,
      [ "A" ],
      order = FloatBatchOrder.ShuffledWithSeed 1)
  counters.ValueAtCount |> shouldEqual 0
  use e = batches.GetEnumerator()
  e.MoveNext() |> shouldEqual true
  e.Current.Rows |> shouldEqual 10
  counters.ValueAtCount |> shouldEqual 10
  e.MoveNext() |> shouldEqual true
  counters.ValueAtCount |> shouldEqual 20

[<Test>]
let ``Can throw when CreateOrdinalFrame has zero-length sources`` () =
  let src = OrdinalVirtualSource(0L, (fun _ -> OptionalValue 0.0), "empty") :> IVirtualVectorSource
  (fun () -> Virtual.CreateOrdinalFrame(["A"], [ src ]) |> ignore)
  |> should throw typeof<System.ArgumentException>

// TODO:
//  let idx, s1, s2, f = createSimpleTimeFrame()
//  f.DropColumn("S2")
//  f.AddColumn("S3", sin f?S1)
//  f |> Frame.dropSparseRows


 // ------------------------------------------------------------------------------------------------

//let idx, s1, s2, f = createSimpleTimeFrame() // TODO: THe reindexing only works with RAW frames atm.


// f.Rows
// |> Series.filter (fun _ row -> row.GetAs<string>("S2").Length > 5)

//let s1, s2, f = createSimpleFrameSize(100000L)
//let cond = f.Rows |> Series.map (fun _ row -> row.GetAs<string>("S2").Length > 5)
//f?Cond <- cond
//f |> Frame.filterRowsBy "Cond" true





// TODO: Tests for frame with datetimeoffset index

// TODO: Filtering ???
// TODO: Append/merge frames

// TODO: ColumnApply does not work when the frame contains non-numerical columns
// ...because we delay things, it delays the attempt to convert string -> float :-(
// We should be able to check the type of the column (at least)

// TODO: What if we need to build index from two columns, say 'utcTicks' and 'offset' ??
// This is not reversible: f |> Frame.mapRows (fun _ row -> niceTimeFromTicks (row.GetAs "Dense"))


// ------------------------------------------------------------------------------------------------
// Integrating virtual series with delayed series
// ------------------------------------------------------------------------------------------------

open Deedle.Indices
open Deedle.Indices.Virtual
open Deedle.Vectors.Virtual

/// Given a range, returns a virtual index * vector pair
/// representing data in the specified range
let dataLoader
      spy (lo:DateTimeOffset, lob:BoundaryBehavior)
      (hi:DateTimeOffset, hib:BoundaryBehavior) : Async<IIndex<_> * IVector<_>> = async {
  // TODO: Handle boundary conditions properly
  let asIndex (dt:DateTimeOffset) = (dt - date 2000 1 1).Ticks / 123456789L
  let loTicks, hiTicks = asIndex lo, asIndex hi
  let idxSrc = TrackingSource.CreateTimes(loTicks, hiTicks)
  let valSrc = TrackingSource.CreateFloats(loTicks, hiTicks)
  spy (idxSrc, valSrc)
  let s = Virtual.CreateSeries(idxSrc, valSrc)
  return VirtualOrderedIndex(idxSrc) :> _, VirtualVector(valSrc) :> _ }

[<Test>]
let ``Can materialize a delayed series into a virtual series`` () =
  // Delayed series from 1 Jan 2000 to 1 Jan 2100
  let r = Recorder()
  let delayed =
    DelayedSeries.FromIndexVectorLoader
      ( VirtualAddressingScheme("it"),
        VirtualVectorBuilder.Instance, VirtualIndexBuilder.Instance,
        date 2000 1 1, date 2100 1 1, dataLoader (spy1 r ignore) )

  // Materialize as virtual series for 5 years
  let limited = delayed.Between(date 2013 1 1, date 2018 1 1)
  let series = limited.Materialize()

  series.Format() |> ignore

  series.KeyCount |> shouldEqual 12779080
  r.Values.Length |> shouldEqual 1
  let idxSource, valSource = r.Values.Head
  let idxAccess = idxSource.AccessList |> Seq.distinct |> List.ofSeq |> List.sort
  let valAccess = valSource.AccessList |> Seq.distinct |> List.ofSeq |> List.sort
  // In 'idxAccesses', printing checks if skipping 30 elements gives an empty series
  idxAccess |> shouldEqual <| [ 33235401L .. 33235401L+30L ] @ [ 46014466L .. 46014480L ]
  valAccess |> shouldEqual <| [ 33235401L .. 33235401L+15L-1L ] @ [ 46014466L .. 46014480L ]
