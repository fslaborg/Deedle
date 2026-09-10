#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.VirtualInstrumentation
#endif

open System
open System.Collections.Generic
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Internal
open Deedle.Addressing
open Deedle.Vectors
open Deedle.Vectors.Virtual
open Deedle.Virtual
open Deedle.Vectors.Virtual

module Address = LinearAddress

/// LookupRange helpers aligned with production [`VirtualLookupRange`].
[<RequireQualifiedAccess>]
module VirtualLookupRangeTest =
  /// Step lookup for periodic vocabulary — same semantics as [`VirtualLookupRange.forRepeatingCycle`].
  let repeatingCycle (words: 'T[]) = VirtualLookupRange.forRepeatingCycle words

// ------------------------------------------------------------------------------------------------
// Access counters & snapshots (deterministic metrics — no wall clock)
// ------------------------------------------------------------------------------------------------

/// Mutable counters shared across GetSubVector / MergeWith clones of a source.
type AccessCounters() =
  let valueAtAddresses = ResizeArray<int64>()
  member val ValueAtCount = 0 with get, set
  member val LookupValueCount = 0 with get, set
  member val LookupRangeCount = 0 with get, set
  member val GetSubVectorCount = 0 with get, set
  member val MergeWithCount = 0 with get, set
  member x.ValueAtAddresses = valueAtAddresses :> IReadOnlyList<_>
  member x.RecordValueAt(addr: int64) =
    x.ValueAtCount <- x.ValueAtCount + 1
    valueAtAddresses.Add(addr)
  member x.Reset() =
    x.ValueAtCount <- 0
    x.LookupValueCount <- 0
    x.LookupRangeCount <- 0
    x.GetSubVectorCount <- 0
    x.MergeWithCount <- 0
    valueAtAddresses.Clear()
  member x.Snapshot() =
    { ValueAtCount = x.ValueAtCount
      LookupValueCount = x.LookupValueCount
      LookupRangeCount = x.LookupRangeCount
      GetSubVectorCount = x.GetSubVectorCount
      MergeWithCount = x.MergeWithCount
      ValueAtAddressList = List.ofSeq valueAtAddresses }

and AccessSnapshot =
  { ValueAtCount: int
    LookupValueCount: int
    LookupRangeCount: int
    GetSubVectorCount: int
    MergeWithCount: int
    ValueAtAddressList: int64 list }
  member x.TouchedData = x.ValueAtCount > 0
  member x.TotalOps =
    x.ValueAtCount + x.LookupValueCount + x.LookupRangeCount + x.GetSubVectorCount + x.MergeWithCount
  static member delta (before: AccessSnapshot) (after: AccessSnapshot) =
    { ValueAtCount = after.ValueAtCount - before.ValueAtCount
      LookupValueCount = after.LookupValueCount - before.LookupValueCount
      LookupRangeCount = after.LookupRangeCount - before.LookupRangeCount
      GetSubVectorCount = after.GetSubVectorCount - before.GetSubVectorCount
      MergeWithCount = after.MergeWithCount - before.MergeWithCount
      ValueAtAddressList =
        // Addresses recorded after `before` (suffix)
        let n = before.ValueAtAddressList.Length
        after.ValueAtAddressList |> List.skip n }

// ------------------------------------------------------------------------------------------------
// Virtual vs materialised classification
// ------------------------------------------------------------------------------------------------

type StorageKind =
  | VirtualStorage
  | LinearStorage
  | OtherStorage of string

type SeriesShape =
  | FullyVirtual
  | FullyLinear
  | Mixed of index: StorageKind * vector: StorageKind

module SchemeProbe =
  let kind (scheme: IAddressingScheme) =
    match scheme with
    | :? VirtualAddressingScheme -> VirtualStorage
    | :? LinearAddressingScheme -> LinearStorage
    | other -> OtherStorage(other.GetType().Name)

  let isVirtualScheme scheme =
    match kind scheme with
    | VirtualStorage -> true
    | _ -> false

module SeriesProbe =
  let indexKind (s: Series<'K, 'V>) = SchemeProbe.kind s.Index.AddressingScheme
  let vectorKind (s: Series<'K, 'V>) = SchemeProbe.kind s.Vector.AddressingScheme

  let classify (s: Series<'K, 'V>) =
    match indexKind s, vectorKind s with
    | VirtualStorage, VirtualStorage -> FullyVirtual
    | LinearStorage, LinearStorage -> FullyLinear
    | i, v -> Mixed(i, v)

  let isVirtual (s: Series<'K, 'V>) =
    match classify s with
    | FullyVirtual -> true
    | _ -> false

  let isLinear (s: Series<'K, 'V>) =
    match classify s with
    | FullyLinear -> true
    | _ -> false

module FrameProbe =
  /// True when the row index uses a virtual addressing scheme.
  let rowIndexIsVirtual (f: Frame<'R, 'C>) =
    SchemeProbe.isVirtualScheme f.RowIndex.AddressingScheme

// ------------------------------------------------------------------------------------------------
// Counting wrapper for library virtual sources (harness)
// ------------------------------------------------------------------------------------------------

type CountingVirtualSource<'T>(counters: AccessCounters, inner: IVirtualVectorSource<'T>) =
  interface IVirtualVectorSource with
    member _.Length = inner.Length
    member _.AddressingSchemeID = inner.AddressingSchemeID
    member _.ElementType = inner.ElementType
    member _.AddressOperations = inner.AddressOperations
    member _.Invoke(op) = op.Invoke(inner)

  interface IVirtualVectorSource<'T> with
    member _.MergeWith(sources) =
      counters.MergeWithCount <- counters.MergeWithCount + 1
      inner.MergeWith(sources)

    member _.LookupRange(v) =
      counters.LookupRangeCount <- counters.LookupRangeCount + 1
      inner.LookupRange(v)

    member _.LookupValue(k, l, check) =
      counters.LookupValueCount <- counters.LookupValueCount + 1
      inner.LookupValue(k, l, check)

    member _.ValueAt(loc) =
      counters.RecordValueAt(Address.asInt64 loc.Address)
      inner.ValueAt(loc)

    member _.GetSubVector(range) =
      counters.GetSubVectorCount <- counters.GetSubVectorCount + 1
      CountingVirtualSource<'T>(counters, inner.GetSubVector(range)) :> IVirtualVectorSource<'T>

module CountingVirtualSource =
  let Wrap (counters: AccessCounters) (source: IVirtualVectorSource) =
    source.Invoke
      { new IVirtualVectorSourceOperation<IVirtualVectorSource> with
          member _.Invoke<'T>(src: IVirtualVectorSource<'T>) =
            CountingVirtualSource<'T>(counters, src) :> IVirtualVectorSource }

// ------------------------------------------------------------------------------------------------
// Instrumented ordinal IVirtualVectorSource
// ------------------------------------------------------------------------------------------------
type InstrumentedOrdinalSource<'T>
    ( length: int64,
      valueAt: int64 -> 'T,
      counters: AccessCounters,
      ?asLong: 'T -> int64,
      ?lookupRange: LookupRangeMode<'T>,
      ?hasMissing: bool,
      ?addrMap: int64 -> int64 ) =

  let hasMissing = defaultArg hasMissing false
  let addrMap = defaultArg addrMap id
  let lookupRangeMode = defaultArg lookupRange LookupRangeUnsupported
  let addressing = Indices.Linear.LinearAddressOperations(0L, length - 1L) :> IAddressOperations

  let valueAtLoc (loc: IVectorLocation) =
    let i = Address.asInt64 loc.Address
    counters.RecordValueAt(i)
    OptionalValue(valueAt i)

  member x.Counters = counters
  member x.Length = length

  interface IVirtualVectorSource with
    member x.Length = length
    member x.AddressingSchemeID = "instrumented-ordinal"
    member x.ElementType = typeof<'T>
    member x.AddressOperations = addressing
    member x.Invoke(op) = op.Invoke(x)

  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) =
      counters.MergeWithCount <- counters.MergeWithCount + 1
      let parts =
        (length, valueAt)
        :: [ for s in sources ->
               match s with
               | :? InstrumentedOrdinalSource<'T> as src -> src.Length, src.RawValueAt
               | _ -> failwith "MergeWith: expected InstrumentedOrdinalSource" ]
      let total = parts |> List.sumBy fst
      let rec valueAtMerged i = function
        | [] -> failwithf "MergeWith: index %d out of range (len=%d)" i total
        | (len, vat)::rest ->
            if i < len then vat i
            else valueAtMerged (i - len) rest
      let mergedValueAt i = valueAtMerged i parts
      InstrumentedOrdinalSource<'T>
        (total, mergedValueAt, counters, ?asLong=asLong, lookupRange=lookupRangeMode, hasMissing=hasMissing, addrMap=addrMap) :> _

    member x.LookupRange(v) =
      counters.LookupRangeCount <- counters.LookupRangeCount + 1
      match lookupRangeMode with
      | LookupRangeUnsupported ->
          VirtualVectorSource.scanLookupRange addressing valueAtLoc v
      | mode ->
          LookupRangeExecutor.lookupRange length mode v "InstrumentedOrdinalSource"

    member x.LookupValue(k, l, check) =
      counters.LookupValueCount <- counters.LookupValueCount + 1
      let asLong =
        match asLong with
        | Some g -> g
        | None -> failwith "LookupValue: asLong not configured"
      let c = Func<int64, bool>(fun i -> check.Invoke(Address.ofInt64 i))
      let found =
        IndexUtilsModule.binarySearch length (Func<_, _>(fun i -> asLong (valueAt i))) (asLong k) l c
      found
      |> OptionalValue.map (fun i -> valueAt i, Address.ofInt64 i)

    member x.ValueAt(loc) =
      let absAddr = Address.asInt64 loc.Address
      counters.RecordValueAt(absAddr)
      if hasMissing && addrMap absAddr % 3L = 0L then OptionalValue.Missing
      else OptionalValue(valueAt absAddr)

    member x.GetSubVector(range) =
      counters.GetSubVectorCount <- counters.GetSubVectorCount + 1
      match LookupRangeExecutor.getSubVector length lookupRangeMode asLong range with
      | Choice1Of2 spec ->
          let subValueAt i = valueAt (spec.MapRow i)
          InstrumentedOrdinalSource<'T>
            (spec.Length, subValueAt, counters, ?asLong=spec.AsLong, lookupRange=spec.LookupRange,
             hasMissing=hasMissing, addrMap=(fun i -> addrMap (spec.MapRow i))) :> _
      | Choice2Of2 _ -> invalidOp "GetSubVector: unexpected range restriction"

  /// Read without recording (for MergeWith composition).
  member x.RawValueAt(i: int64) = valueAt i

module InstrumentedOrdinalSource =
  let createFloats (length: int64) =
    let c = AccessCounters()
    c, InstrumentedOrdinalSource<float>(length, float, c, hasMissing=false)

  let createLongs (length: int64) =
    let c = AccessCounters()
    c, InstrumentedOrdinalSource<int64>(length, id, c, asLong=id, hasMissing=false)

  let createStrings (length: int64) (words: string[]) =
    let c = AccessCounters()
    let valueAt i = words.[int (i % int64 words.Length)]
    // ExactFixed: tests filter known values only; unknown keys are not part of this profile.
    let indexOf v =
      let o = words |> Array.findIndex ((=) v) |> int64
      o, o
    c, InstrumentedOrdinalSource<string>(length, valueAt, c, lookupRange=LookupRangeExactFixed indexOf, hasMissing=false)

  let createSearchableStrings (length: int64) (words: string[]) =
    let c = AccessCounters()
    let valueAt i = words.[int (i % int64 words.Length)]
    c, InstrumentedOrdinalSource<string>(length, valueAt, c, lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)

  let createTimes (length: int64) =
    let c = AccessCounters()
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let valueAt i = start.AddTicks(i * 123456789L)
    let asLong (dto: DateTimeOffset) = dto.UtcTicks
    c, InstrumentedOrdinalSource<DateTimeOffset>(length, valueAt, c, asLong=asLong, hasMissing=false)

  let createOrdinalSeries (length: int64) =
    let c, src = createLongs length
    c, Virtual.CreateOrdinalSeries(src)

  let createFloatSeries (length: int64) =
    let c, src = createFloats length
    c, Virtual.CreateOrdinalSeries(src)

  /// Ordered DateTimeOffset index + float values sharing one AccessCounters.
  let createOrderedFloatSeries (length: int64) =
    let c = AccessCounters()
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let idx =
      InstrumentedOrdinalSource<DateTimeOffset>
        (length, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
    let vals = InstrumentedOrdinalSource<float>(length, float, c, hasMissing=false)
    c, Virtual.CreateSeries(idx, vals)

  /// Core factory: custom `valueAt` + LookupRange mode (LookupRange data profiles).
  let createOrderedSearchFrameCore
      (length: int64)
      (valueAt: int64 -> string)
      (lookupRange: LookupRangeMode<string>) =
    let c = AccessCounters()
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let idx =
      InstrumentedOrdinalSource<DateTimeOffset>
        (length, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
    let s1 = InstrumentedOrdinalSource<int64>(length, id, c, asLong=id, hasMissing=false)
    let s2 = InstrumentedOrdinalSource<string>(length, valueAt, c, lookupRange=lookupRange, hasMissing=false)
    let frame = Virtual.CreateFrame(idx, ["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
    c, frame

  /// Ordered time frame; `lookupRange` controls search-column LookupRange quality.
  /// Default data: 11-word repeating cycle (ideal Step case).
  let createOrderedSearchFrameWith (length: int64) (lookupRange: LookupRangeMode<string>) =
    let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let valueAt i = words.[int (i % int64 words.Length)]
    let lookup =
      match lookupRange with
      | LookupRangeStep _ -> VirtualLookupRangeTest.repeatingCycle words
      | other -> other
    let c, frame = createOrderedSearchFrameCore length valueAt lookup
    c, frame, words

  /// Large periodic vocabulary (e.g. 256 labels) — tests Step with bigger stride.
  let createOrderedSearchFrameLargeVocab (length: int64) (vocabSize: int) =
    let words = [| for i in 0 .. vocabSize - 1 -> sprintf "w%04d" i |]
    let valueAt i = words.[int (i % int64 vocabSize)]
    let lookup = VirtualLookupRangeTest.repeatingCycle words
    let c, frame = createOrderedSearchFrameCore length valueAt lookup
    c, frame, words

  /// Sparse matches at i ≡ remainder (mod modulus) — irregular; use IndexList or scan.
  let createOrderedSearchFrameSparse (length: int64) (modulus: int64) (remainder: int64) =
    let valueAt i = if i % modulus = remainder then "lorem" else sprintf "u%d" i
    let indices = [ for i in 0L .. length - 1L do if i % modulus = remainder then i ]
    let lookup = LookupRangeIndexList (function "lorem" -> indices | _ -> [])
    let c, frame = createOrderedSearchFrameCore length valueAt lookup
    c, frame, indices.Length

  /// Same sparse data but wrong Step LookupRange (assumes period 11 like the default corpus).
  let createOrderedSearchFrameSparseWrongStep (length: int64) (modulus: int64) (remainder: int64) =
    let valueAt i = if i % modulus = remainder then "lorem" else sprintf "u%d" i
    let trueCount =
      [ for i in 0L .. length - 1L do if i % modulus = remainder then i ] |> List.length
    let lookup = LookupRangeStep (fun _ -> int remainder, 11)
    let c, frame = createOrderedSearchFrameCore length valueAt lookup
    c, frame, trueCount

  /// Ordered time frame with a searchable string column (for filterRowsBy).
  let createOrderedSearchFrame (length: int64) =
    createOrderedSearchFrameWith length (LookupRangeStep (fun _ -> 0, 0))

  /// Ordered frame where the search column is a mapped virtual series (no reverse lookup).
  let createOrderedMappedSearchFrame (length: int64) =
    let c, frame, words = createOrderedSearchFrame length
    let mapped =
      frame.GetColumn<string>("S2")
      |> Series.mapValues (fun s -> s.ToUpperInvariant())
    let rebuilt = frame |> Frame.replaceCol "S2" mapped
    c, rebuilt, words

  /// Ordinal-index frame with Step LookupRange on the search column.
  let createOrdinalSearchFrame (length: int64) =
    let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let c = AccessCounters()
    let s1 = InstrumentedOrdinalSource<int64>(length, id, c, asLong=id, hasMissing=false)
    let s2 =
      InstrumentedOrdinalSource<string>
        (length, (fun i -> words.[int (i % int64 words.Length)]), c, lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)
    let frame = Virtual.CreateOrdinalFrame(["S1"; "S2"], [s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource])
    c, frame, words

  /// Ordered search frame plus float and string columns without LookupRange (scan fallback on filter).
  let createOrderedSearchWithScanColumnsFrame (length: int64) =
    let words = "lorem ipsum dolor sit amet consectetur adipiscing elit".Split(' ')
    let labels = "alpha beta gamma delta".Split(' ')
    let c = AccessCounters()
    let start = DateTimeOffset(DateTime(2000, 1, 1), TimeSpan.FromHours(-1.0))
    let idx =
      InstrumentedOrdinalSource<DateTimeOffset>
        (length, (fun i -> start.AddTicks(i * 123456789L)), c, asLong=(fun dto -> dto.UtcTicks), hasMissing=false)
    let s1 = InstrumentedOrdinalSource<int64>(length, id, c, asLong=id, hasMissing=false)
    let s2 =
      InstrumentedOrdinalSource<string>
        (length, (fun i -> words.[int (i % int64 words.Length)]), c, lookupRange=VirtualLookupRangeTest.repeatingCycle words, hasMissing=false)
    let s3 = InstrumentedOrdinalSource<float>(length, (fun i -> float i * 0.01), c, hasMissing=false)
    let s4 =
      InstrumentedOrdinalSource<string>
        (length, (fun i -> labels.[int (i % int64 labels.Length)]), c, hasMissing=false)
    let frame =
      Virtual.CreateFrame(
        idx,
        [ "S1"; "S2"; "S3"; "S4" ],
        [ s1 :> IVirtualVectorSource; s2 :> IVirtualVectorSource; s3 :> IVirtualVectorSource; s4 :> IVirtualVectorSource ])
    c, frame, words, 500.0, "alpha"

  /// Back-compat alias for benchmarks that only need the float scan column.
  let createOrderedSearchWithFloatFrame (length: int64) =
    let c, frame, words, floatFilter, _ = createOrderedSearchWithScanColumnsFrame length
    c, frame, words, floatFilter

