#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualVector
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Addressing
open Deedle.Vectors
open Deedle.Vectors.Virtual
open Deedle.Virtual
open Deedle.Tests.VirtualInstrumentation

module Address = LinearAddress

let private customCount (range: RangeRestriction<Address>) =
  match range with
  | RangeRestriction.Custom ar -> Seq.length ar
  | RangeRestriction.Fixed(lo, hi) -> int (Address.asInt64 hi - Address.asInt64 lo + 1L)
  | RangeRestriction.Start n | RangeRestriction.End n -> int n

let private customAddrs (range: RangeRestriction<Address>) =
  match range with
  | RangeRestriction.Custom ar -> ar |> Seq.map Address.asInt64 |> Seq.toList
  | RangeRestriction.Fixed(lo, hi) -> [ Address.asInt64 lo .. Address.asInt64 hi ]
  | RangeRestriction.Start n -> [ 0L .. n - 1L ]
  | RangeRestriction.End n -> failwith "End restriction not used in these tests"

let private always = Func<Address, bool>(fun _ -> true)

// ------------------------------------------------------------------------------------------------
// VirtualVectorSource wrappers (src/Deedle/Vectors/VirtualVector.fs)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can read ValueAt through VirtualVector wrapper`` () =
  let src = OrdinalVirtualSource(4L, (fun i -> OptionalValue(i + 10L)), "test")
  let vec = VirtualVector(src) :> IVector<int64>
  vec.GetValueAtLocation(KnownLocation(Address.ofInt64 2L, 2L)) |> shouldEqual (OptionalValue 12L)

[<Test>]
let ``Can delegate LookupRange through boxed virtual source`` () =
  let words = [| "lorem"; "ipsum"; "dolor" |]
  let c, src = InstrumentedOrdinalSource.createSearchableStrings 30L words
  c.Reset()
  VirtualVectorSource.boxSource(src).LookupRange(box "lorem") |> ignore
  c.Snapshot().LookupRangeCount |> shouldEqual 1

[<Test>]
let ``Can scan LookupRange on boxed source when search is not the inner type`` () =
  let words = [| "lorem"; "ipsum"; "dolor" |]
  let c, src = InstrumentedOrdinalSource.createSearchableStrings 12L words
  c.Reset()
  let range = VirtualVectorSource.boxSource(src).LookupRange(box 42)
  customCount range |> shouldEqual 0
  c.Snapshot().LookupRangeCount |> shouldEqual 0
  c.Snapshot().ValueAtCount |> should be (greaterThan 0)

[<Test>]
let ``Can LookupValue Exact through boxed virtual source`` () =
  let _, src = InstrumentedOrdinalSource.createLongs 9L
  let boxed = VirtualVectorSource.boxSource(src)
  let hit = boxed.LookupValue(box 1L, Lookup.Exact, always)
  hit.HasValue |> shouldEqual true
  hit.Value |> fst |> shouldEqual (box 1L)
  Address.asInt64 (snd hit.Value) |> shouldEqual 1L
  boxed.LookupValue(box 99L, Lookup.Exact, always).HasValue |> shouldEqual false

[<Test>]
let ``Can scan LookupRange on mapped virtual source without reverse mapping`` () =
  let c, src = InstrumentedOrdinalSource.createFloats 16L
  let mapped = VirtualVectorSource.map None (fun _ ov -> OptionalValue.map (fun v -> v + 1.0) ov) src
  c.Reset()
  let range = mapped.LookupRange(3.0)
  customCount range |> shouldEqual 1
  customAddrs range |> shouldEqual [ 2L ]
  c.Snapshot().ValueAtCount |> should be (greaterThanOrEqualTo 1)

[<Test>]
let ``Can LookupValue Exact on mapped source without reverse mapping`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 16L
  let mapped = VirtualVectorSource.map None (fun _ ov -> OptionalValue.map (fun v -> v + 1.0) ov) src
  let hit = mapped.LookupValue(3.0, Lookup.Exact, always)
  hit.HasValue |> shouldEqual true
  hit.Value |> fst |> shouldEqual 3.0
  Address.asInt64 (snd hit.Value) |> shouldEqual 2L
  mapped.LookupValue(99.0, Lookup.Exact, always).HasValue |> shouldEqual false

[<Test>]
let ``Can delegate LookupRange through mapped source with reverse mapping`` () =
  let words = [| "a"; "b"; "c" |]
  let c, src = InstrumentedOrdinalSource.createStrings 12L words
  let mapped =
    VirtualVectorSource.map
      (Some(fun (s: string) -> s.ToLowerInvariant()))
      (fun _ ov -> OptionalValue.map (fun (s: string) -> s.ToUpperInvariant()) ov)
      src
  c.Reset()
  // ExactFixed on inner: reverse maps "B" -> "b" then Fixed(1,1) — no ValueAt scan.
  customAddrs (mapped.LookupRange("B")) |> shouldEqual [ 1L ]
  c.Snapshot().LookupRangeCount |> shouldEqual 1
  c.Snapshot().ValueAtCount |> shouldEqual 0

[<Test>]
let ``Can throw when non-exact LookupValue on wrapper is used`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 8L
  let mapped = VirtualVectorSource.map None (fun _ ov -> ov) src
  (fun () -> mapped.LookupValue(1.0, Lookup.Greater, always) |> ignore)
  |> should throw typeof<NotSupportedException>
  // Wrong CLR type on boxed → scan path (not inner LookupValue).
  let boxed = VirtualVectorSource.boxSource(src)
  (fun () -> boxed.LookupValue(box "nope", Lookup.ExactOrGreater, always) |> ignore)
  |> should throw typeof<NotSupportedException>

[<Test>]
let ``Can scan combined virtual source LookupRange instead of throwing`` () =
  let c, s1 = InstrumentedOrdinalSource.createFloats 16L
  let s2 = InstrumentedOrdinalSource<float>(16L, (fun i -> float (i + 1L)), c)
  let combined =
    VirtualVectorSource.combine
      (function
        | [a; b] when a.HasValue && b.HasValue -> OptionalValue(a.Value + b.Value)
        | _ -> OptionalValue.Missing)
      [ s1 :> IVirtualVectorSource<_>; s2 :> IVirtualVectorSource<_> ]
  let range = combined.LookupRange(5.0)
  customCount range |> shouldEqual 1
  customAddrs range |> shouldEqual [ 2L ]

[<Test>]
let ``Can LookupValue Exact on combined virtual source`` () =
  let _, s1 = InstrumentedOrdinalSource.createFloats 8L
  let s2 = InstrumentedOrdinalSource<float>(8L, (fun i -> float (i + 1L)), AccessCounters())
  let combined =
    VirtualVectorSource.combine
      (function
        | [a; b] when a.HasValue && b.HasValue -> OptionalValue(a.Value + b.Value)
        | _ -> OptionalValue.Missing)
      [ s1 :> IVirtualVectorSource<_>; s2 :> IVirtualVectorSource<_> ]
  let hit = combined.LookupValue(5.0, Lookup.Exact, always)
  hit.HasValue |> shouldEqual true
  Address.asInt64 (snd hit.Value) |> shouldEqual 2L

[<Test>]
let ``Row reader virtual source LookupRange does not throw`` () =
  let _, s1 = InstrumentedOrdinalSource.createFloats 8L
  let irt =
    { new IRowReaderTransform with
        member _.ColumnAddressAt(i) = Address.ofInt64 i
      interface INaryTransform with
        member _.GetFunction<'R>() = fun (_: OptionalValue<'R> list) -> OptionalValue.Missing }
  let ctor (src: IVirtualVectorSource<float>) = VirtualVector(src) :> IVector
  let vectors = Vector.ofValues [ ctor s1 ]
  let reader =
    VirtualVectorSource.createRowReader ctor VectorBuilder.Instance irt vectors [ s1 :> IVirtualVectorSource<_> ]
  customCount (reader.LookupRange(Unchecked.defaultof<_>)) |> shouldEqual 0

[<Test>]
let ``Can throw when MergeWith on boxed source gets non-boxed peers`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 4L
  let boxed = VirtualVectorSource.boxSource(src)
  let peer = OrdinalVirtualSource(4L, (fun i -> OptionalValue(box i)), "peer") :> IVirtualVectorSource<obj>
  (fun () -> boxed.MergeWith([ peer ]) |> ignore)
  |> should throw typeof<InvalidOperationException>

[<Test>]
let ``Can throw when MergeWith on mapped source gets non-mapped peers`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 4L
  let mapped = VirtualVectorSource.map None (fun _ ov -> ov) src
  (fun () -> mapped.MergeWith([ src :> IVirtualVectorSource<_> ]) |> ignore)
  |> should throw typeof<InvalidOperationException>

[<Test>]
let ``Can throw when MergeWith on combined source gets non-combined peers`` () =
  let _, s1 = InstrumentedOrdinalSource.createFloats 4L
  let s2 = InstrumentedOrdinalSource<float>(4L, float, AccessCounters())
  let combined =
    VirtualVectorSource.combine
      (function
        | [a; b] when a.HasValue && b.HasValue -> OptionalValue(a.Value + b.Value)
        | _ -> OptionalValue.Missing)
      [ s1 :> IVirtualVectorSource<_>; s2 :> IVirtualVectorSource<_> ]
  (fun () -> combined.MergeWith([ s1 :> IVirtualVectorSource<_> ]) |> ignore)
  |> should throw typeof<InvalidOperationException>

[<Test>]
let ``Can throw when AsyncBuild uses a virtual scheme`` () =
  let _, s = InstrumentedOrdinalSource.createOrdinalSeries 8L
  let ex =
    Assert.Throws<NotSupportedException>(fun () ->
      (VirtualVectorBuilder.Instance :> IVectorBuilder)
        .AsyncBuild(s.Vector.AddressingScheme, Return 0, [| s.Vector |])
      |> Async.RunSynchronously
      |> ignore)
  ex.Message.Contains("Materialize") |> shouldEqual true

// ------------------------------------------------------------------------------------------------
// withLinearAddressing / fillMissing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can apply withLinearAddressing as no-op on already linear ordinal source`` () =
  let src = OrdinalVirtualSource(5L, (fun i -> OptionalValue i), "test") :> IVirtualVectorSource<_>
  let wrapped = VirtualVectorSource.withLinearAddressing src
  Object.ReferenceEquals(src, wrapped) |> shouldEqual true

[<Test>]
let ``Can apply withLinearAddressing wrapping non-zero-based address ops idempotently`` () =
  let length = 10L
  let baseAddr = 100L
  let absOps =
    { new IAddressOperations with
        member _.FirstElement = Address.ofInt64 baseAddr
        member _.LastElement = Address.ofInt64 (baseAddr + length - 1L)
        member _.AddressOf(offset) = Address.ofInt64 (baseAddr + offset)
        member _.OffsetOf(addr) = Address.asInt64 addr - baseAddr
        member _.AdjustBy(addr, offset) = Address.ofInt64 (Address.asInt64 addr + offset)
        member _.Range =
          seq { for i in 0L .. length - 1L -> Address.ofInt64 (baseAddr + i) } }
  let shifted =
    { new IVirtualVectorSource<float> with
        member _.ValueAt(loc) =
          let i = Address.asInt64 loc.Address - baseAddr
          OptionalValue(float i)
        member _.LookupRange(_) =
          RangeRestriction.Fixed(Address.ofInt64 baseAddr, Address.ofInt64 (baseAddr + length - 1L))
        member _.LookupValue(_, _, _) = OptionalValue.Missing
        member _.GetSubVector(_) = invalidOp "not used"
        member _.MergeWith(_) = invalidOp "not used"
      interface IVirtualVectorSource with
        member _.Length = length
        member _.AddressingSchemeID = "abs-shifted"
        member _.ElementType = typeof<float>
        member _.AddressOperations = absOps
        member this.Invoke(op) = op.Invoke(this :?> IVirtualVectorSource<float>) }
  let once = VirtualVectorSource.withLinearAddressing shifted
  match once with
  | :? VirtualVectorSource.ILinearAddressedSource<float> -> ()
  | _ -> failwith "expected ILinearAddressedSource wrapper"
  let twice = VirtualVectorSource.withLinearAddressing once
  Object.ReferenceEquals(once, twice) |> shouldEqual true
  once.ValueAt(KnownLocation(Address.ofInt64 3L, 3L)) |> shouldEqual (OptionalValue 3.0)

[<Test>]
let ``fillMissing constant NaN on float source is a no-op`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 8L
  let filled =
    VirtualVectorSource.fillMissing (VectorFillMissing.Constant (box Double.NaN)) (src :> IVirtualVectorSource<_>)
  Object.ReferenceEquals(src :> obj, filled :> obj) |> shouldEqual true

[<Test>]
let ``fillMissing constant with incompatible type leaves source unchanged`` () =
  let _, src = InstrumentedOrdinalSource.createFloats 8L
  let filled =
    VirtualVectorSource.fillMissing (VectorFillMissing.Constant (box "nope")) (src :> IVirtualVectorSource<_>)
  Object.ReferenceEquals(src :> obj, filled :> obj) |> shouldEqual true

[<Test>]
let ``fillMissing constant replaces missing cells and stays virtual`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(9L, float, c, hasMissing=true)
  let s = Virtual.CreateOrdinalSeries(src)
  let filled = s |> Series.fillMissingWith -1.0
  SeriesProbe.isVirtual filled |> shouldEqual true
  filled.TryGet(0L) |> shouldEqual (OptionalValue -1.0)
  filled.TryGet(1L) |> shouldEqual (OptionalValue 1.0)

[<Test>]
let ``fillMissing Backward walks to the next present value`` () =
  let c = AccessCounters()
  let src = InstrumentedOrdinalSource<float>(9L, float, c, hasMissing=true)
  let s = Virtual.CreateOrdinalSeries(src)
  let filled = s |> Series.fillMissing Direction.Backward
  SeriesProbe.isVirtual filled |> shouldEqual true
  // Index 0 is missing (0 % 3 = 0); Backward walks forward to 1.
  filled.TryGet(0L) |> shouldEqual (OptionalValue 1.0)
  filled.TryGet(3L) |> shouldEqual (OptionalValue 4.0)
