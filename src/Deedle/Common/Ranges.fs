// ------------------------------------------------------------------------------------------------
// Ranges - represents a sub-range as a list of pairs of indices
// ------------------------------------------------------------------------------------------------

namespace Deedle.Ranges

open System
open Deedle
open Deedle.Addressing

/// Represents a sub-range of an ordinal index. The range can consist of 
/// multiple blocks, i.e. [ 0..9; 20..29 ]. The pairs represent indices
/// of first and last element (inclusively) and we also keep size so that
/// we do not have to recalculate it.
type Ranges = 
  { Ranges : (int64 * int64) list; Size : int64 }
  
  /// Create a range from a sequence of low-high pairs
  static member Create(ranges:seq<_>) =
    let ranges = List.ofSeq ranges 
    if ranges = [] then invalidArg "ranges" "Range cannot be empty"
    let size = ranges |> List.sumBy (fun (l, h) -> h - l + 1L)
    for l, h in ranges do if l > h then invalidArg "ranges" "Invalid range (first offset is greater than second)"
    { Ranges = List.ofSeq ranges; Size = size }

  /// Combine ranges - the arguments don't have to be sorted, but must not overlap
  static member Combine(ranges:seq<Ranges>) =
    if Seq.isEmpty ranges then invalidArg "ranges" "Range cannot be empty"
    let blocks = [ for r in ranges do yield! r.Ranges ] |> List.sortBy fst
    let rec loop (startl, endl) blocks = seq {
      match blocks with 
      | [] -> yield startl, endl
      | (s, e)::blocks when s <= endl -> invalidOp "Cannot combine overlapping ranges"
      | (s, e)::blocks when s = endl + 1L -> yield! loop (startl, e) blocks
      | (s, e)::blocks -> 
          yield startl, endl
          yield! loop (s, e) blocks }
    Ranges.Create (loop (List.head blocks) (List.tail blocks))

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Ranges = 
  /// Represents invalid address
  let invalid = -1L

  /// Returns the key at a given address. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   0->10, 1->11, 2->12, 3->30, 4->31, 5->32
  ///
  /// When the address is wrong, throws `IndexOutOfRangeException`
  let inline keyOfAddress addr { Ranges = ranges } =
    let rec loop sum idx = 
      if idx >= ranges.Length then raise <| IndexOutOfRangeException() else
      let l, h = ranges.[idx]
      if addr < sum + (h - l + 1L) then l + (addr - sum)
      else loop (sum + h - l + 1L) (idx + 1)
    loop 0L 0

  /// Returns the address of a given key. For example, given
  /// ranges [10..12; 30..32], the function defines a mapping:
  ///
  ///   10->0, 11->1, 12->2, 30->3, 31->4, 32->5  
  ///
  /// When the key is wrong, returns `Address.Invalid`
  let inline addressOfKey key { Ranges = ranges } =
    let rec loop addr idx = 
      if idx >= ranges.Length then invalid else
      let l, h = ranges.[idx]
      if key < l then invalid
      elif key >= l && key <= h then addr + (key - l)
      else loop (addr + (h - l + 1L)) (idx + 1)
    loop 0L 0

  // Restriction has indices as local addresses 
  let restrictRanges restriction { Ranges = ranges } =
    match restriction with
    | AddressRange.Fixed(loRestr, hiRestr) ->
        // Restrict the current ranges to a range specified by lower and higher address
        let newRanges = 
          [ for lo, hi in ranges do
              if lo >= loRestr && hi <= hiRestr then yield lo, hi       
              elif hi < loRestr || lo > hiRestr then ()
              else yield max lo loRestr, min hi hiRestr ]
        Ranges.Create(newRanges)
    
    | Let (true, +1) ((isStart, step), AddressRange.Start(desiredCount)) 
    | Let (false,-1) ((isStart, step), AddressRange.End(desiredCount)) ->
        let rec loop rangeIdx desiredCount = seq {
          if desiredCount > 0 then
            if rangeIdx < 0 || rangeIdx >= ranges.Length then
              invalidOp "Insufficient number of elements in the range"
            let lo, hi = ranges.[rangeIdx]
            let range = if isStart then Seq.range lo hi else Seq.rangeStep hi lo -1L
            let last, length = range |> Seq.truncate desiredCount |> Seq.lastAndLength
            yield if isStart then lo, last else last, hi
            yield! loop (rangeIdx + step) (desiredCount - length) }
        
        if isStart then Ranges.Create(loop 0 (int desiredCount))
        else Ranges.Create(loop (ranges.Length-1) (int desiredCount) |> List.ofSeq |> List.rev)

    | _ -> failwith "restrictRanges: TODO"
      
  /// Returns the smallest & greatest overall key
  let inline keyRange { Ranges = ranges } = 
    fst ranges.[0], snd ranges.[ranges.Length-1]

  /// Returns an array containing all keys
  let inline keys { Ranges = ranges; Size = size } =
    let keys = Array.zeroCreate (int size)
    let mutable i = 0
    for l, h in ranges do
      for n in l .. h do 
        keys.[i] <- n
        i <- i + 1
    keys

  /// Returns a lazy sequence containing all keys
  let inline keysSeq { Ranges = ranges; Size = size } =
    seq { for l, h in ranges do for n in l .. h do yield n }

  /// Implements a lookup using the specified semantics & check function.
  /// For `Exact` match, this is the same as `addressOfKey`. In other cases,
  /// we first find the place where the key *would* be and then scan in one
  /// or the other direction until 'check' returns 'true' or we find the end.
  let inline lookup key semantics check ({ Ranges = ranges; Size = size } as input) =
    if semantics = Lookup.Exact then
      // For exact lookup, we only check one address
      let addr = addressOfKey key input
      if addr <> invalid && check addr then
        OptionalValue( (key, addr) )
      else OptionalValue.Missing
    else
      // Otherwise, we scan next addresses in the required direction
      let step = 
        if semantics &&& Lookup.Greater = Lookup.Greater then (+) 1L
        elif semantics &&& Lookup.Smaller = Lookup.Smaller then (+) -1L
        else invalidArg "semantics" "Invalid lookup semantics"

      // Find start; if we do *not* want exact, then skip to the next
      let start =
        let rec loop addr idx = 
          if idx >= ranges.Length && semantics &&& Lookup.Greater = Lookup.Greater then invalid 
          elif idx >= ranges.Length (* semantics &&& Lookup.Smaller = Lookup.Greater *) then size - 1L else
          let l, h = ranges.[idx]
          if key < l && semantics &&& Lookup.Greater = Lookup.Greater then addr
          elif key < l && semantics &&& Lookup.Smaller = Lookup.Smaller then addr - 1L
          elif key >= l && key <= h then addr + (key - l)
          else loop (addr + (h - l + 1L)) (idx + 1)
        let addr = loop 0L 0
        // If we do not want exact, but we found exact match, skip to the next
        if addr <> invalid && 
          ( keyOfAddress addr input = key && 
            (semantics = Lookup.Greater || semantics = Lookup.Smaller) )
            then step addr else addr

      if start = invalid then OptionalValue.Missing else
      // Scan until 'check' returns true, or until we reach invalid address
      let rec scan step addr =
        if addr < 0L || addr >= size then OptionalValue.Missing
        elif check addr then OptionalValue( (keyOfAddress addr input, addr) )
        else scan step (step addr)
      scan step start
