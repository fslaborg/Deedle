// ------------------------------------------------------------------------------------------------
// Ranges - represents a sub-range as a list of pairs of indices
//
// This is typically used when we need to keep ranges of some data source that 
// are created by slicing and merging (as you can do with Deedle series). This
// module hides most of the complexity behind manipulation with ranges.
// ------------------------------------------------------------------------------------------------

namespace Deedle.Ranges

open System
open Deedle
open Deedle.Internal
open Deedle.Addressing

/// A set of operations on keys that you need to implement in order to use the
/// `Ranges<'TKey>` type. The `'TKey` type is typically the key of a BigDeedle
/// series. It can represent different things, such as:
///
///  - `int64` - if you have ordinally indexed series 
///  - `Date` (of some sort) - if you have daily time series
///  - `DateTimeOffset` - if you have time series with DTO keys
///
/// The operations need to implement the *right* thing based on the logic of the
/// keys. So for example if you have one data point every hour, `IncrementBy` should
/// add the appropriate number of hours. Or if you have keys as business days, the
/// `IncrementBy` operation should add a number of business days (that is, the 
/// operations may be simple numerical addition, but may contain more logic).
///
type IRangeKeyOperations<'TKey> =
  /// Compare two keys. Return +1 if first is larger, -1 if second is larger, 0 otherwise
  abstract Compare : 'TKey * 'TKey -> int
  
  /// Get the n-th next key after the specified key (n is always non-negative)
  abstract IncrementBy : 'TKey * int64 -> 'TKey
  
  /// Return distance between two keys - return 0 if the keys are the same 
  /// (the second key is always larger than the first)
  abstract Distance : 'TKey * 'TKey -> int64
  
  /// Generate keys within the specific range (inclusively). Here, the second
  /// key _can_ be smaller (in which case the range should be from larger to smaller)
  abstract Range : 'TKey * 'TKey -> seq<'TKey>

  /// Find the first valid key around the specified `'TKey` value. This is used
  /// when not all `'TKey` values can appear as keys of the series. If lookup is
  /// `Exact`, it should just check validity; for other lookups, this should either
  /// find first smaller or first larger valid key (but not skip any keys).
  ///
  /// This is only called with `lookup = Lookup.Exact`, unless you are also using
  /// `Ranges.lookup` function inside `IVirtualVectorSource<'T>.LookupValue`.
  abstract ValidateKey : 'TKey * Lookup -> OptionalValue<'TKey>


/// Represents a sub-range of an ordinal index. The range can consist of 
/// multiple blocks, i.e. [ 0..9; 20..29 ]. The pairs represent indices
/// of first and last element (inclusively) and we also keep size so that
/// we do not have to recalculate it.
///
/// For more information, see also the documentation for the `Ranges` module.
type Ranges<'T when 'T : equality>(ranges:seq<'T * 'T>, ops:IRangeKeyOperations<'T>) = 
  let ranges = Array.ofSeq ranges 
  do
    for l, h in ranges do 
      if ops.Compare(l, h) > 0 then 
        invalidArg "ranges" "Invalid range (first offset is greater than second)"
  member x.Ranges = ranges
  member x.Operations = ops

/// Provides F# functions for working with the `Ranges<'T>` type. Note that 
/// most of the functions are also exposed as members. The terminology in the
/// functions below is:
///
///  - **offset** refers to an absolute `int64` offset of a key in the range
///  - **key** refers to a key value of type `'T`
/// 
/// Say, you have daily range `[ (2015-01-01, 2015-01-10); (2015-02-01, 2015-02-10) ]`.
/// Then, the keys are the dates and the offsets are 0 .. 9 for the first part and
/// 10 .. 19 for the second part.
module Ranges =  

  /// Create a range from a sequence of low-high pairs
  /// (range key operations used standard arithmetic)
  let inline inlineCreate succ (ranges:seq< ^T * ^T >) =
    let ranges = Array.ofSeq ranges 
    for l, h in ranges do if l > h then invalidArg "ranges" "Invalid range (first offset is greater than second)"
    let one = LanguagePrimitives.GenericOne
    let ops = 
      { new IRangeKeyOperations< ^T > with
          member x.Compare(a, b) = compare a b 
          member x.IncrementBy(a, i) = succ a i
          member x.Distance(l, h) = int64 (h - l)
          member x.Range(a, b) = if a <= b then Seq.range a b else Seq.rangeStep a (-one) b 
          member x.ValidateKey(k, _) = OptionalValue(k) }
    Ranges(ranges, ops)

  /// Create a range from a sequence of low-high pairs
  /// and an explicitly specified range key operations
  let inline create (ops:IRangeKeyOperations<'T>) (ranges:seq< 'T * 'T >) =
    Ranges(ranges, ops)

  /// Combine ranges - the arguments don't have to be sorted, but must not overlap
  /// (they can be aligned, in which case ranges are merged)
  let inline combine(ranges:seq<Ranges<'T>>) : Ranges<'T> =
    if Seq.isEmpty ranges then invalidArg "ranges" "Range cannot be empty"
    let ops = (Seq.head ranges).Operations
    let blocks = 
      [ for r in ranges do yield! r.Ranges ] 
      |> List.sortWith (fun (l1, _) (l2, _) -> ops.Compare(l1, l2))
    let rec loop (startl, endl) blocks = seq {
      match blocks with 
      | [] -> yield startl, endl
      | (s, e)::blocks when ops.Compare(s, endl) <= 0 -> invalidOp "Cannot combine overlapping ranges"
      | (s, e)::blocks when s = ops.IncrementBy(endl, 1L) -> yield! loop (startl, e) blocks
      | (s, e)::blocks -> 
          yield startl, endl
          yield! loop (s, e) blocks }
    create ops (loop (List.head blocks) (List.tail blocks))

  /// Represents invalid offset
  let invalid = -1L

  /// Returns the key at a given offset. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   0->10, 1->11, 2->12, 3->30, 4->31, 5->32
  ///
  /// When the offset is wrong, throws `IndexOutOfRangeException`
  let keyAtOffset offs (ranges:Ranges<'T>) =
    let rec loop sum idx = 
      if idx >= ranges.Ranges.Length then raise <| IndexOutOfRangeException() else
      let l, h = ranges.Ranges.[idx]

      // NOTE: More straightforward way to do this is to write:
      //
      //  let size = ranges.Operations.Distance(l, h)+1L
      //  if addr < sum + size then ranges.Operations.IncrementBy(l, addr - sum)
      //
      // But this is not good, because calculating distance may be expensive
      // (in BMC case, it iterates over all partitions). So instead, 
      // we use the following, which avoids getting the full size if it's not needed.

      let incremented = ranges.Operations.IncrementBy(l, offs - sum)
      if ranges.Operations.Compare(incremented, h) <= 0 then incremented
      else 
        let size = ranges.Operations.Distance(l, h)+1L
        loop (sum + size) (idx + 1)

    if offs < 0L then raise <| IndexOutOfRangeException()
    loop 0L 0

  /// Returns the offset of a given key. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   10->0, 11->1, 12->2, 30->3, 31->4, 32->5  
  ///
  /// When the key is wrong, returns `Ranges.invalid`
  let offsetOfKey key (ranges:Ranges<'T>) =
    let inline (<.) a b = ranges.Operations.Compare(a, b) < 0
    let inline (>=.) a b = ranges.Operations.Compare(a, b) >= 0
    let inline (<=.) a b = ranges.Operations.Compare(a, b) <= 0

    let rec loop offs idx = 
      if idx >= ranges.Ranges.Length then invalid else
      let l, h = ranges.Ranges.[idx]
      if key <. l then invalid
      elif key >=. l && key <=. h then offs + ranges.Operations.Distance(l, key)
      else loop (offs + ranges.Operations.Distance(l, h) + 1L) (idx + 1)
    
    if ranges.Operations.ValidateKey(key, Lookup.Exact).HasValue
    then loop 0L 0 else invalid

  /// Restricts the ranges according to the specified range restriction
  let restrictRanges restriction (ranges:Ranges<'T>) =
    let inline (<.) a b = ranges.Operations.Compare(a, b) < 0
    let inline (>.) a b = ranges.Operations.Compare(a, b) > 0
    let inline (>=.) a b = ranges.Operations.Compare(a, b) >= 0
    let inline (<=.) a b = ranges.Operations.Compare(a, b) <= 0
    let inline max a b = if a >=. b then a else b
    let inline min a b = if a <=. b then a else b

    match restriction with
    | RangeRestriction.Fixed(loRestr, hiRestr) ->
        // Restrict the current ranges to a range specified by lower and higher keys
        let newRanges = 
          [| for lo, hi in ranges.Ranges do
               if lo >=. loRestr && hi <=. hiRestr then yield lo, hi       
               elif hi <. loRestr || lo >. hiRestr then ()
               else 
                 let newLo, newHi = max lo loRestr, min hi hiRestr 
                 if newLo <=. newHi then yield newLo, newHi |]
        Ranges(newRanges, ranges.Operations)
    
    | Let (true, +1) ((isStart, step), RangeRestriction.Start(desiredCount)) 
    | Let (false,-1) ((isStart, step), RangeRestriction.End(desiredCount)) ->
        let rec loop rangeIdx desiredCount = seq {
          if desiredCount > 0 then
            if rangeIdx < 0 || rangeIdx >= ranges.Ranges.Length then
              invalidOp "Insufficient number of elements in the range"
            let lo, hi = ranges.Ranges.[rangeIdx]
            let range = if isStart then ranges.Operations.Range(lo, hi) else ranges.Operations.Range(hi, lo)
            let last, length = range |> Seq.truncate desiredCount |> Seq.lastAndLength
            yield if isStart then lo, last else last, hi
            yield! loop (rangeIdx + step) (desiredCount - length) }
        
        if isStart then Ranges(loop 0 (int desiredCount) |> Array.ofSeq, ranges.Operations)
        else Ranges(loop (ranges.Ranges.Length-1) (int desiredCount) |> Array.ofSeq |> Array.rev, ranges.Operations)

    | _ -> failwith "restrictRanges: Custom ranges are not supported"
      
  /// Returns the smallest & greatest overall key
  let inline keyRange (ranges:Ranges<'T>) =
    fst ranges.Ranges.[0], snd ranges.Ranges.[ranges.Ranges.Length-1]

  /// Returns a lazy sequence containing all keys
  let inline keys (ranges:Ranges<'T>) =
    seq { for l, h in ranges.Ranges do yield! ranges.Operations.Range(l, h) }

  /// Returns the length of the ranges
  let inline length (ranges:Ranges<'T>) =
    ranges.Ranges |> Array.sumBy (fun (l, h) -> 
      ranges.Operations.Distance(l, h)+1L)

  /// Implements a lookup using the specified semantics & check function.
  /// For `Exact` match, this is the same as `offsetOfKey`. In other cases,
  /// we first find the place where the key *would* be and then scan in one
  /// or the other direction until 'check' returns 'true' or we find the end.
  ///
  /// Returns the key together with its offset in the range.
  let lookup key semantics (check:'T -> int64 -> bool) (ranges:Ranges<'T>) =
    let inline (<.) a b = ranges.Operations.Compare(a, b) < 0
    let inline (<.) a b = ranges.Operations.Compare(a, b) < 0
    let inline (>=.) a b = ranges.Operations.Compare(a, b) >= 0
    let inline (<=.) a b = ranges.Operations.Compare(a, b) <= 0

    if semantics = Lookup.Exact then
      // For exact lookup, we only check one key
      let offs = offsetOfKey key ranges
      if offs <> invalid && check key offs then
        OptionalValue( (key, offs) )
      else OptionalValue.Missing
    else
      // Validate the key - returns key that is valid value 
      let keyOpt = ranges.Operations.ValidateKey(key, semantics) 
      if keyOpt = OptionalValue.Missing then OptionalValue.Missing else 
      let validKey = keyOpt.Value

      // Otherwise, we scan next keys in the required direction
      let step = 
        if semantics &&& Lookup.Greater = Lookup.Greater then (+) 1L
        elif semantics &&& Lookup.Smaller = Lookup.Smaller then (+) -1L
        else invalidArg "semantics" "Invalid lookup semantics (1)"

      // Find start
      let start, rangeIdx =
        let rec loop offs idx = 
          if idx >= ranges.Ranges.Length && (semantics &&& Lookup.Greater = Lookup.Greater) then invalid, -1
          elif idx >= ranges.Ranges.Length && (semantics &&& Lookup.Smaller = Lookup.Smaller) then offs-1L, ranges.Ranges.Length-1 
          else
            let l, h = ranges.Ranges.[idx]
            if validKey >=. l && validKey <=. h then offs + ranges.Operations.Distance(l, validKey), idx
            elif validKey <. l && (semantics &&& Lookup.Greater = Lookup.Greater) then offs, idx
            elif validKey <. l && (semantics &&& Lookup.Smaller = Lookup.Smaller) then offs - 1L, idx - 1
            else loop (offs + ranges.Operations.Distance(l, h) + 1L) (idx + 1)
        loop 0L 0

      if start = invalid || rangeIdx = -1 then OptionalValue.Missing else
      let keyStart = keyAtOffset start ranges

      // Scan until 'check' returns true, or until we reach invalid key
      let keysToScan = 
        if semantics = Lookup.Exact then seq { yield keyStart }
        elif semantics &&& Lookup.Greater = Lookup.Greater then
          seq { yield! ranges.Operations.Range(keyStart, snd ranges.Ranges.[rangeIdx])
                for lo, hi in ranges.Ranges.[rangeIdx + 1 ..] do
                  yield! ranges.Operations.Range(lo, hi) }
        elif semantics &&& Lookup.Smaller = Lookup.Smaller then
          seq { yield! ranges.Operations.Range(keyStart, fst ranges.Ranges.[rangeIdx])
                for lo, hi in ranges.Ranges.[.. rangeIdx-1] |> Array.rev do
                  yield! ranges.Operations.Range(hi, lo) }
        else invalidArg "semantics" "Invalid lookup semantics"

      // Skip one if needed
      Seq.zip keysToScan (Seq.unreduce step start)
      |>  ( if keyStart = key && (semantics = Lookup.Greater || semantics = Lookup.Smaller) 
            then Seq.skip 1 else id )
      |> Seq.tryFind (fun (k, a) -> check k a)
      |> OptionalValue.ofOption


// ------------------------------------------------------------------------------------------------
// Expose the functionality as members too
// ------------------------------------------------------------------------------------------------

type Ranges<'T> with
  /// Returns the first key in the range
  member x.FirstKey = fst (Ranges.keyRange x)
  /// Returns the last key in the range
  member x.LastKey = snd (Ranges.keyRange x)
  /// Returns the key at the specified offset
  member x.KeyAtOffset(offset) = Ranges.keyAtOffset offset x
  /// Returns the absolute offset of the key in the ranges
  member x.OffsetOfKey(key) = 
    let res = Ranges.offsetOfKey key x
    if res = Ranges.invalid then raise <| IndexOutOfRangeException()
    else res
  /// Returns a lazy sequence containing all keys
  member x.Keys = Ranges.keys x
  /// Returns the length of the ranges
  member x.Length = Ranges.length x
  /// Searches for a key in the range (see `Ranges.lookup` for more info)
  member x.Lookup(key, semantics, check:Func<_, _, _>) = 
    Ranges.lookup key semantics (fun a b -> check.Invoke(a, b)) x
  /// Merge the current range with other specified ranges
  member x.MergeWith(ranges) = 
    Ranges.combine [yield x; yield! ranges]
  /// Restricts the ranges according to the specified range restriction
  member x.Restrict(restriction) = 
    Ranges.restrictRanges restriction x