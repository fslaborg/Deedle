namespace Deedle.Virtual

open System
open Deedle
open Deedle.Addressing
open Deedle.Vectors.Virtual
open Deedle.VectorHelpers

module Address = LinearAddress

/// Strided custom range used by filter / Search (same shape as step-based LookupRange).
type StepRange =
  { Offset: int
    Step: int }
  interface IRangeRestriction<Address> with
    member _.Count = raise (NotSupportedException("Count not supported on StepRange"))
  interface seq<Address> with
    member _.GetEnumerator() = raise (NotSupportedException("enumeration not supported on StepRange"))
  interface System.Collections.IEnumerable with
    member _.GetEnumerator() = raise (NotSupportedException("enumeration not supported on StepRange"))

/// How `LookupRange` behaves on searchable virtual columns (quality / correctness axis).
type LookupRangeMode<'T> =
  | LookupRangeUnsupported
  /// Return a tight Fixed absolute index range for the searched value.
  | LookupRangeExactFixed of ('T -> int64 * int64)
  /// Return a Custom strided range (offset, step) over the ordinal domain.
  | LookupRangeStep of ('T -> int * int)
  /// Naive over-approximation: entire ordinal domain (wrong for sparse matches).
  | LookupRangeFullFixed
  /// Precomputed absolute indices (irregular/sparse matches).
  | LookupRangeIndexList of ('T -> int64 list)

/// LookupRange mode supplied explicitly for a searchable column.
type LookupRangeModeSpec =
  | String of LookupRangeMode<string>
  | Int64 of LookupRangeMode<int64>
  | Float of LookupRangeMode<float>

/// How to configure LookupRange for one searchable column at virtual frame load time.
type VirtualSearchColumnMode =
  | Infer
  | Explicit of LookupRangeModeSpec

/// One searchable column on [`Virtual.ReadCsv`] / [`Virtual.ReadParquet`].
type VirtualSearchColumn = { Name: string; Mode: VirtualSearchColumnMode }

/// Resolved LookupRange modes for one frame column (at most one kind is set).
type ResolvedColumnSearch =
  { String: LookupRangeMode<string> option
    Int64: LookupRangeMode<int64> option
    Float: LookupRangeMode<float> option
    /// `true` when the column was listed in `searchColumns` at load.
    Configured: bool }

  static member Empty =
    { String = None; Int64 = None; Float = None; Configured = false }

/// Classified LookupRange kind for virtual column diagnostics.
[<RequireQualifiedAccess>]
type VirtualColumnLookupRange =
  | Scan
  | Step of period: int
  | IndexList
  | ExactFixed
  | FullFixed

/// Helpers for building [`VirtualSearchColumn`] lists.
[<RequireQualifiedAccess>]
module VirtualSearchColumn =
  let infer name = { Name = name; Mode = VirtualSearchColumnMode.Infer }

  let withString name (mode: LookupRangeMode<string>) =
    { Name = name; Mode = VirtualSearchColumnMode.Explicit(LookupRangeModeSpec.String mode) }

  let withInt64 name (mode: LookupRangeMode<int64>) =
    { Name = name; Mode = VirtualSearchColumnMode.Explicit(LookupRangeModeSpec.Int64 mode) }

  let withFloat name (mode: LookupRangeMode<float>) =
    { Name = name; Mode = VirtualSearchColumnMode.Explicit(LookupRangeModeSpec.Float mode) }

/// Helpers for configuring searchable columns on virtual sources.
[<RequireQualifiedAccess>]
module VirtualLookupRange =
  /// Step LookupRange for values repeating on a fixed cycle (periodic categorical data).
  /// Unknown values yield an empty range (negative offset) instead of throwing.
  let forRepeatingCycle (values: 'T[]) =
    LookupRangeStep (fun v ->
      match values |> Array.tryFindIndex ((=) v) with
      | Some i -> i, values.Length
      | None -> -1, max 1 values.Length)

  /// IndexList LookupRange from a pre-built map of value -> row indices.
  let forCategorical (indicesByValue: Map<'T, int64 list>) =
    LookupRangeIndexList (fun v ->
      match indicesByValue.TryGetValue v with
      | true, xs -> xs
      | false, _ -> [])

  /// Build categorical IndexList by scanning column values once at frame construction.
  let forCategoricalScan (length: int64) (valueAt: int64 -> 'T) =
    [ for i in 0L .. length - 1L -> valueAt i, i ]
    |> List.groupBy fst
    |> List.map (fun (k, pairs) -> k, List.map snd pairs)
    |> Map.ofList
    |> forCategorical

  /// Build categorical IndexList by scanning present (non-missing) values once at frame construction.
  let forCategoricalScanPresent (length: int64) (valueAt: int64 -> 'T option) =
    [ for i in 0L .. length - 1L do
        match valueAt i with
        | Some v -> yield (v, i)
        | None -> () ]
    |> List.groupBy fst
    |> List.map (fun (k, pairs) -> k, List.map snd pairs)
    |> Map.ofList
    |> forCategorical

  /// Correct but O(N) per filter - scans all rows when LookupRange is invoked.
  let scan (length: int64) (valueAt: int64 -> 'T) =
    LookupRangeIndexList (fun v ->
      [ for i in 0L .. length - 1L do if valueAt i = v then i ])

  let exactFixed (selector: 'T -> int64 * int64) = LookupRangeExactFixed selector
  let fullFixed = LookupRangeFullFixed

  /// Classify a configured [`LookupRangeMode`] for diagnostics (`Virtual.TryGetLookupRange`).
  let classifyLookupRange (mode: LookupRangeMode<'T>) =
    match mode with
    | LookupRangeUnsupported -> VirtualColumnLookupRange.Scan
    | LookupRangeStep f ->
        let _, step = f Unchecked.defaultof<'T>
        VirtualColumnLookupRange.Step (max 1 step)
    | LookupRangeIndexList _ -> VirtualColumnLookupRange.IndexList
    | LookupRangeExactFixed _ -> VirtualColumnLookupRange.ExactFixed
    | LookupRangeFullFixed -> VirtualColumnLookupRange.FullFixed

  /// Maximum distinct non-empty string values for automatic LookupRange inference.
  /// Inference scans the full column at load time and may build an IndexList map; this cap
  /// keeps that work bounded for enum-like columns and forces an explicit LookupRange mode
  /// (e.g. scan) when cardinality is high. Not tied to an existing Deedle constant — chosen
  /// as a conservative "small categorical" threshold (typical search columns ≪ 64; tests use
  /// ~100+ distinct as the high-cardinality case).
  [<Literal>]
  let MaxInferredSearchCardinality = 64

  module private CycleInference =
    let tryDetectOptional (values: 'T option[]) (maxPeriod: int) (equals: 'T -> 'T -> bool) =
      if values.Length = 0 then None
      else
        let limit = min maxPeriod values.Length |> max 1
        [1 .. limit]
        |> List.tryPick (fun period ->
            let template = Array.init period (fun k -> values.[k])
            let matches =
              values
              |> Array.mapi (fun i v ->
                  match v with
                  | None -> true
                  | Some x ->
                      match template.[i % period] with
                      | None -> true
                      | Some t -> equals x t)
              |> Array.forall id
            if matches then
              let cycleValues =
                [| for k in 0 .. period - 1 ->
                    match template.[k] with
                    | Some v -> v
                    | None -> invalidArg "values" "missing value in repeating cycle template" |]
              Some(period, cycleValues)
            else None)

    let tryDetectStrings (values: string[]) (maxPeriod: int) =
      if values.Length = 0 then None
      else
        let limit = min maxPeriod values.Length |> max 1
        [1 .. limit]
        |> List.tryPick (fun period ->
            let template = Array.init period (fun k -> values.[k])
            let matches =
              values
              |> Array.mapi (fun i v -> v = "" || v = template.[i % period])
              |> Array.forall id
            if matches then
              let cycleValues =
                [| for k in 0 .. period - 1 ->
                    let v = template.[k]
                    if v = "" then invalidArg "values" "empty value in repeating cycle template"
                    else v |]
              Some(period, cycleValues)
            else None)

  /// Infer Step or categorical IndexList LookupRange from column values (ReadCsv / ReadParquet).
  let tryInferStringLookupRange (length: int64) (valueAt: int64 -> string) =
    if length = 0L then None
    else
      let values = [| for i in 0L .. length - 1L -> valueAt i |]
      let distinct =
        values |> Array.filter ((<>) "") |> Array.distinct
      if distinct.Length = 0 || distinct.Length > MaxInferredSearchCardinality then None
      else
        match CycleInference.tryDetectStrings values distinct.Length with
        | Some (period, cycleValues) ->
            Some(forRepeatingCycle cycleValues, sprintf "repeating cycle (period %d)" period)
        | None ->
            Some(
              forCategoricalScan length valueAt,
              sprintf "categorical IndexList (%d distinct; one-time O(N) scan per filter value)" distinct.Length)

  /// Infer Step or categorical IndexList LookupRange for int64 columns (≤ [`MaxInferredSearchCardinality`] distinct present values).
  let tryInferInt64LookupRange (length: int64) (valueAt: int64 -> int64 option) =
    if length = 0L then None
    else
      let values = [| for i in 0L .. length - 1L -> valueAt i |]
      let distinct =
        values
        |> Array.choose id
        |> Array.distinct
      if distinct.Length = 0 || distinct.Length > MaxInferredSearchCardinality then None
      else
        match CycleInference.tryDetectOptional values distinct.Length (=) with
        | Some (period, cycleValues) ->
            Some(forRepeatingCycle cycleValues, sprintf "repeating cycle (period %d)" period)
        | None ->
            Some(
              forCategoricalScanPresent length valueAt,
              sprintf "int64 categorical IndexList (%d distinct)" distinct.Length)

  /// Infer Step or categorical IndexList LookupRange for float columns (≤ [`MaxInferredSearchCardinality`] distinct present values).
  let tryInferFloatLookupRange (length: int64) (valueAt: int64 -> float option) =
    if length = 0L then None
    else
      let values = [| for i in 0L .. length - 1L -> valueAt i |]
      let distinct =
        values
        |> Array.choose id
        |> Array.distinct
      if distinct.Length = 0 || distinct.Length > MaxInferredSearchCardinality then None
      else
        match CycleInference.tryDetectOptional values distinct.Length (=) with
        | Some (period, cycleValues) ->
            Some(forRepeatingCycle cycleValues, sprintf "repeating cycle (period %d)" period)
        | None ->
            Some(
              forCategoricalScanPresent length valueAt,
              sprintf "float categorical IndexList (%d distinct)" distinct.Length)

  let private findSearchColumn (searchColumns: VirtualSearchColumn list) (columnName: string) =
    searchColumns
    |> List.tryFind (fun entry ->
      String.Equals(entry.Name, columnName, StringComparison.OrdinalIgnoreCase))

  let private inferString apiName columnName infer =
    match infer() with
    | Some (mode, desc) ->
        System.Diagnostics.Trace.WriteLine(
          sprintf "%s: inferred %s LookupRange for search column '%s'." apiName desc columnName)
        Some mode
    | None ->
        System.Diagnostics.Trace.WriteLine(
          sprintf "%s: search column '%s' has high cardinality; use VirtualSearchColumn.withString and VirtualLookupRange.scan, or omit from searchColumns to scan at filter time." apiName columnName)
        None

  let private inferNumeric<'T> apiName columnName (infer: unit -> (LookupRangeMode<'T> * string) option) =
    match infer() with
    | Some (mode, desc) ->
        System.Diagnostics.Trace.WriteLine(
          sprintf "%s: inferred %s LookupRange for search column '%s'." apiName desc columnName)
        Some mode
    | None ->
        System.Diagnostics.Trace.WriteLine(
          sprintf "%s: search column '%s' has high cardinality or unsupported inference; filters will scan rows." apiName columnName)
        None

  /// Resolve LookupRange for one column from [`VirtualReadCsvOptions.SearchColumns`] / Parquet equivalent.
  let resolveSearchColumnsLookupRange
      (apiName: string)
      (searchColumns: VirtualSearchColumn list)
      (columnName: string)
      (columnKind: string)
      (inferStringValues: unit -> (LookupRangeMode<string> * string) option)
      (inferInt64Values: unit -> (LookupRangeMode<int64> * string) option)
      (inferFloatValues: unit -> (LookupRangeMode<float> * string) option) =
    match findSearchColumn searchColumns columnName with
    | None -> ResolvedColumnSearch.Empty
    | Some entry ->
        match entry.Mode, columnKind with
        | VirtualSearchColumnMode.Infer, "string" ->
            { ResolvedColumnSearch.Empty with String = inferString apiName columnName inferStringValues; Configured = true }
        | VirtualSearchColumnMode.Explicit (LookupRangeModeSpec.String mode), "string" ->
            { ResolvedColumnSearch.Empty with String = Some mode; Configured = true }
        | VirtualSearchColumnMode.Infer, "int64" ->
            { ResolvedColumnSearch.Empty with Int64 = inferNumeric apiName columnName inferInt64Values; Configured = true }
        | VirtualSearchColumnMode.Explicit (LookupRangeModeSpec.Int64 mode), "int64" ->
            { ResolvedColumnSearch.Empty with Int64 = Some mode; Configured = true }
        | VirtualSearchColumnMode.Infer, "float" ->
            { ResolvedColumnSearch.Empty with Float = inferNumeric apiName columnName inferFloatValues; Configured = true }
        | VirtualSearchColumnMode.Explicit (LookupRangeModeSpec.Float mode), "float" ->
            { ResolvedColumnSearch.Empty with Float = Some mode; Configured = true }
        | mode, kind ->
            invalidArg "searchColumns"
              (sprintf
                 "%s: search column '%s' mode does not match inferred column kind '%s' (%A)."
                 apiName entry.Name kind mode)

/// Shared LookupRange / GetSubVector logic for ordinal virtual sources.
[<RequireQualifiedAccess>]
module LookupRangeExecutor =
  open Deedle.Internal

  let private emptyAddressRange () =
    let addrs: Address list = []
    ({ new IRangeRestriction<Address> with
        member _.Count = 0L
       interface seq<Address> with
         member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator()
       interface System.Collections.IEnumerable with
         member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator() :> System.Collections.IEnumerator }
     |> RangeRestriction.Custom)

  let lookupRange (length: int64) (mode: LookupRangeMode<'T>) (value: 'T) (context: string) =
    match mode with
    | LookupRangeUnsupported ->
        raise (NotSupportedException(
          sprintf
            "%s: LookupRange is not configured on this virtual column. List the column in searchColumns (VirtualSearchColumn.infer or .withString/.withInt64/.withFloat) on Virtual.ReadCsv / Virtual.ReadParquet, or filter on another column."
            context))
    | LookupRangeExactFixed f ->
        let lo, hi = f value
        RangeRestriction.Fixed(Address.ofInt64 lo, Address.ofInt64 hi)
    | LookupRangeStep f ->
        let offset, step = f value
        if offset < 0 || step <= 0 then emptyAddressRange ()
        else RangeRestriction.Custom { Offset = offset; Step = step }
    | LookupRangeFullFixed ->
        RangeRestriction.Fixed(Address.ofInt64 0L, Address.ofInt64(length - 1L))
    | LookupRangeIndexList f ->
        let addrs = f value |> List.map Address.ofInt64
        let count = int64 addrs.Length
        if count = 0L then emptyAddressRange ()
        else
          ({ new IRangeRestriction<Address> with
              member _.Count = count
             interface seq<Address> with
               member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator()
             interface System.Collections.IEnumerable with
               member _.GetEnumerator() = (addrs :> seq<_>).GetEnumerator() :> System.Collections.IEnumerator }
           |> RangeRestriction.Custom)

  let clipLookupRange (mode: LookupRangeMode<'T>) (lo: int64) (newLen: int64) =
    let hi = lo + newLen - 1L
    match mode with
    | LookupRangeUnsupported -> LookupRangeUnsupported
    | LookupRangeExactFixed f ->
        LookupRangeExactFixed(fun v ->
          let a, b = f v
          max 0L (a - lo), min (newLen - 1L) (b - lo))
    | LookupRangeStep f ->
        LookupRangeStep (fun v ->
          let offset, step = f v
          if offset < 0 || step <= 0 then (offset, step)
          else
            let firstAbs =
              if int64 offset >= lo then int64 offset
              else int64 offset + (lo - int64 offset + int64 step - 1L) / int64 step * int64 step
            if firstAbs > hi then (-1, step)
            else (int (firstAbs - lo), step))
    | LookupRangeFullFixed -> LookupRangeFullFixed
    | LookupRangeIndexList f ->
        LookupRangeIndexList (fun v ->
          f v
          |> List.choose (fun abs ->
              let local = abs - lo
              if local >= 0L && local < newLen then Some local else None))

  let private gcd (a: int) (b: int) =
    let rec loop x y = if y = 0 then abs x else loop y (x % y)
    loop a b

  /// Remap LookupRange modes after a Step sub-vector (parent abs = offset + step * local).
  /// Without this, a second `filterRowsBy` reuses the original stride on `0 .. newLen-1`.
  let private remapLookupRangeAfterStep (mode: LookupRangeMode<'T>) (parentOffset: int) (parentStep: int) (newLen: int64) =
    let mapAbsToLocal (abs: int64) =
      if parentStep <= 0 then None
      elif abs < int64 parentOffset then None
      elif (abs - int64 parentOffset) % int64 parentStep <> 0L then None
      else
        let local = (abs - int64 parentOffset) / int64 parentStep
        if local >= 0L && local < newLen then Some local else None

    match mode with
    | LookupRangeUnsupported -> LookupRangeUnsupported
    | LookupRangeFullFixed -> LookupRangeFullFixed
    | LookupRangeIndexList f ->
        LookupRangeIndexList (fun v -> f v |> List.choose mapAbsToLocal)
    | LookupRangeExactFixed f ->
        LookupRangeExactFixed (fun v ->
          let a, b = f v
          if parentStep <= 0 || newLen <= 0L || a > b then (0L, -1L)
          else
            let po, ps = int64 parentOffset, int64 parentStep
            let firstAbs =
              if a <= po then
                if po > b then None else Some po
              else
                let r = (a - po) % ps
                let cand = if r = 0L then a else a + (ps - r)
                if cand > b then None else Some cand
            match firstAbs with
            | None -> (0L, -1L)
            | Some fa ->
                let r = (b - po) % ps
                let la = if r = 0L then b else b - r
                if la < fa then (0L, -1L)
                else
                  let lo = (fa - po) / ps
                  let hi = (la - po) / ps
                  (max 0L lo, min (newLen - 1L) hi))
    | LookupRangeStep f ->
        LookupRangeStep (fun v ->
          let ao, so = f v
          if ao < 0 || so <= 0 || parentStep <= 0 || newLen <= 0L then (-1, 1)
          else
            let g = gcd parentStep so
            if (ao - parentOffset) % g <> 0 then (-1, 1)
            else
              let lcm = parentStep / g * so
              let m = max ao parentOffset
              let rem = ((m - parentOffset) % parentStep + parentStep) % parentStep
              let startA = if rem = 0 then m else m + (parentStep - rem)
              let maxSteps = abs so / g + 1
              let rec loop x guard =
                if guard <= 0 then None
                elif (x - ao) % so = 0 then Some x
                else loop (x + parentStep) (guard - 1)
              match loop startA maxSteps with
              | None -> (-1, 1)
              | Some startAbs ->
                  let localOffset = int ((int64 startAbs - int64 parentOffset) / int64 parentStep)
                  let localStep = lcm / parentStep
                  if int64 localOffset >= newLen then (-1, 1)
                  else (localOffset, localStep))

  /// Remap LookupRange after an irregular address-list sub-vector (IndexList / Custom).
  let private remapLookupRangeAfterAddresses (mode: LookupRangeMode<'T>) (addrs: int64[]) =
    let absToLocal = System.Collections.Generic.Dictionary<int64, int64>(addrs.Length)
    for i = 0 to addrs.Length - 1 do
      absToLocal.[addrs.[i]] <- int64 i
    let mapAbs abs =
      match absToLocal.TryGetValue abs with
      | true, local -> Some local
      | false, _ -> None

    match mode with
    | LookupRangeUnsupported -> LookupRangeUnsupported
    | LookupRangeFullFixed -> LookupRangeFullFixed
    | LookupRangeIndexList f ->
        LookupRangeIndexList (fun v -> f v |> List.choose mapAbs)
    | LookupRangeExactFixed f ->
        LookupRangeIndexList (fun v ->
          let a, b = f v
          [ for abs in addrs do
              if abs >= a && abs <= b then
                match mapAbs abs with
                | Some local -> yield local
                | None -> () ])
    | LookupRangeStep f ->
        LookupRangeIndexList (fun v ->
          let offset, step = f v
          if offset < 0 || step <= 0 then []
          else
            [ for abs in addrs do
                if abs >= int64 offset && (abs - int64 offset) % int64 step = 0L then
                  match mapAbs abs with
                  | Some local -> yield local
                  | None -> () ])

  /// Sub-vector plan: callers compose `valueAt << MapRow` so OptionalValue sources stay typed.
  type SubVectorSpec<'T> =
    { Length: int64
      MapRow: int64 -> int64
      AsLong: ('T -> int64) option
      LookupRange: LookupRangeMode<'T> }

  let getSubVector (length: int64) (mode: LookupRangeMode<'T>) (asLong: ('T -> int64) option) (range: RangeRestriction<Address>) =
    match range.AsAbsolute(length) with
    | Choice1Of2(nlo, nhi) ->
        let lo = Address.asInt64 nlo
        let hi = Address.asInt64 nhi
        if hi < lo then invalidOp "GetSubVector: hi < lo"
        let newLen = hi - lo + 1L
        Choice1Of2
          { Length = newLen
            MapRow = fun i -> lo + i
            AsLong = asLong
            LookupRange = clipLookupRange mode lo newLen }
    | Choice2Of2(:? StepRange as lr) ->
        let count =
          if length = 0L || lr.Offset < 0 || lr.Step <= 0 then 0L
          else
            let span = length
            let baseCount = span / int64 lr.Step
            if span % int64 lr.Step > int64 lr.Offset then baseCount + 1L else baseCount
        let newLen = max 0L count
        Choice1Of2
          { Length = newLen
            MapRow = fun i -> int64 lr.Offset + int64 lr.Step * i
            AsLong = asLong
            LookupRange = remapLookupRangeAfterStep mode lr.Offset lr.Step newLen }
    | Choice2Of2 ar ->
        let addrs = ar |> Seq.map Address.asInt64 |> Array.ofSeq
        Choice1Of2
          { Length = int64 addrs.Length
            MapRow = fun i -> addrs.[int i]
            AsLong = asLong
            LookupRange = remapLookupRangeAfterAddresses mode addrs }

  let private emptyRange =
    RangeRestriction.ofSeq 0L Array.empty

  /// Intersect two LookupRange results (same original address domain).
  /// Used to fuse two `filterRowsBy` predicates into one sub-vector restriction.
  /// Never enumerates [`StepRange`] (its enumerator throws by design).
  let intersect (a: RangeRestriction<Address>) (b: RangeRestriction<Address>) =
    let fromAddrs addrs =
      let arr = addrs |> Seq.distinct |> Array.ofSeq
      RangeRestriction.ofSeq (int64 arr.Length) arr
    let tryStep = function
      | RangeRestriction.Custom(:? StepRange as s) -> Some s
      | _ -> None
    let matchesStep (s: StepRange) (addr: Address) =
      let i = Address.asInt64 addr
      s.Step <> 0 &&
      i >= int64 s.Offset &&
      (i - int64 s.Offset) % int64 s.Step = 0L
    let filterAddrsByStep (s: StepRange) (addrs: seq<Address>) =
      fromAddrs (addrs |> Seq.filter (matchesStep s))
    let collectStepInFixed (s: StepRange) (lo64: int64) (hi64: int64) =
      if s.Step = 0 then emptyRange
      else
        let rec collect i acc =
          let addr = int64 s.Offset + int64 s.Step * i
          if addr > hi64 then List.rev acc
          elif addr < lo64 then collect (i + 1L) acc
          else collect (i + 1L) (Address.ofInt64 addr :: acc)
        fromAddrs (collect 0L [])
    match tryStep a, tryStep b, a, b with
    | Some s1, Some s2, _, _ ->
        if s1.Step = 0 || s2.Step = 0 then emptyRange
        else
          let p, q, ao, bo = s1.Step, s2.Step, s1.Offset, s2.Offset
          let g = gcd p q
          if (ao - bo) % g <> 0 then emptyRange
          else
            let lcm = p / g * q
            let m = max ao bo
            let rem = ((m - ao) % p + p) % p
            let startA = if rem = 0 then m else m + (p - rem)
            // Congruence guarantees a hit within one period of the other stride.
            let maxSteps = abs q / g + 1
            let rec loop x guard =
              if guard <= 0 then None
              elif (x - bo) % q = 0 then Some x
              else loop (x + p) (guard - 1)
            match loop startA maxSteps with
            | Some offset -> RangeRestriction.Custom { Offset = offset; Step = lcm }
            | None -> emptyRange
    | _, _, RangeRestriction.Fixed(lo1, hi1), RangeRestriction.Fixed(lo2, hi2) ->
        let lo = if lo1 > lo2 then lo1 else lo2
        let hi = if hi1 < hi2 then hi1 else hi2
        if lo <= hi then RangeRestriction.Fixed(lo, hi) else emptyRange
    | Some s, None, _, RangeRestriction.Fixed(lo, hi)
    | None, Some s, RangeRestriction.Fixed(lo, hi), _ ->
        collectStepInFixed s (Address.asInt64 lo) (Address.asInt64 hi)
    // Step ∩ IndexList (or any enumerable Custom): filter the enumerable; never enumerate Step.
    | Some s, None, _, RangeRestriction.Custom ar
    | None, Some s, RangeRestriction.Custom ar, _ ->
        filterAddrsByStep s ar
    | _, _, RangeRestriction.Custom ar1, RangeRestriction.Custom ar2 ->
        // Both non-Step Customs (IndexList ∩ IndexList, etc.)
        let set2 = System.Collections.Generic.HashSet<_>(ar2)
        fromAddrs (ar1 |> Seq.filter set2.Contains)
    | _, _, RangeRestriction.Custom ar, RangeRestriction.Fixed(lo, hi)
    | _, _, RangeRestriction.Fixed(lo, hi), RangeRestriction.Custom ar ->
        fromAddrs (ar |> Seq.filter (fun addr -> addr >= lo && addr <= hi))
    | _ -> emptyRange

