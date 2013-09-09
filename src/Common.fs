namespace FSharp.DataFrame

open System

// --------------------------------------------------------------------------------------
// Nullable value
// --------------------------------------------------------------------------------------

[<Struct>]
type OptionalValue<'T>(hasValue:bool, value:'T) = 
  member x.HasValue = hasValue
  member x.Value = 
    if hasValue then value
    else invalidOp "OptionalValue.Value: Value is not available" 
  member x.ValueOrDefault = value
  new (value:'T) = OptionalValue(true, value)
  static member Missing = OptionalValue(false, Unchecked.defaultof<'T>)
  override x.ToString() = 
    if hasValue then 
      if Object.Equals(null, value) then "<null>"
      else value.ToString() 
    else "missing"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OptionalValue = 
  let inline bind f (input:OptionalValue<_>) = 
    if input.HasValue then f input.Value
    else OptionalValue.Missing

  let inline map f (input:OptionalValue<_>) = 
    if input.HasValue then OptionalValue(f input.Value)
    else OptionalValue.Missing

  let inline ofSingletonList list = 
    match list with
    | [it] -> OptionalValue(it)
    | [] -> OptionalValue.Missing
    | _ -> invalidArg "list" "OptionalValue.ofSingletonList requires singleton or empty list"

  let inline ofTuple (b, value) =
    if b then OptionalValue(value) else OptionalValue.Missing

  let inline asOption (value:OptionalValue<'T>) = 
    if value.HasValue then Some value.Value else None

  let inline ofOption opt = 
    match opt with
    | None -> OptionalValue.Missing
    | Some v -> OptionalValue(v)

  let (|Missing|Present|) (optional:OptionalValue<'T>) =
    if optional.HasValue then Present(optional.Value)
    else Missing

// --------------------------------------------------------------------------------------
// Internals    
// --------------------------------------------------------------------------------------

namespace FSharp.DataFrame.Common

open System
open System.Linq
open System.Drawing
open FSharp.DataFrame
open System.Collections.Generic

module MissingValues =

  let isNA<'T> () =
    let ty = typeof<'T>
    let nanTest : 'T -> bool = 
      if ty = typeof<float> then unbox Double.IsNaN
      elif ty = typeof<float32> then unbox Single.IsNaN
      elif ty.IsValueType then (fun _ -> false)
      else (fun v -> Object.Equals(null, box v))
    nanTest

  let inline containsNA (data:'T[]) = 
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    Array.exists isNA data

  let inline containsMissingOrNA (data:OptionalValue<'T>[]) = 
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    data |> Array.exists (fun v -> not v.HasValue || isNA v.Value)

  let inline createNAArray (data:'T[]) =   
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    data |> Array.map (fun v -> if isNA v then OptionalValue.Missing else OptionalValue(v))

  let inline createMissingOrNAArray (data:OptionalValue<'T>[]) =   
    let isNA = isNA<'T>() // TODO: Optimize using static member constraints
    data |> Array.map (fun v -> 
      if not v.HasValue || isNA v.Value then OptionalValue.Missing else OptionalValue(v.Value))

module IReadOnlyList =
  /// Converts an array to IReadOnlyList. In F# 3.0, the language does not
  /// know that array implements IReadOnlyList, so this is just boxing/unboxing.
  let inline ofArray (array:'T[]) : IReadOnlyList<'T> = unbox (box array)

  /// Converts a lazy sequence to fully evaluated IReadOnlyList
  let inline ofSeq (array:seq<'T>) : IReadOnlyList<'T> = unbox (box (Array.ofSeq array))
  
  /// Sum elements of the IReadOnlyList
  let inline sum (list:IReadOnlyList<'T>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do total <- total + list.[i]
    total

  /// Sum elements of the IReadOnlyList
  let inline average (list:IReadOnlyList<'T>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do total <- total + list.[i]
    LanguagePrimitives.DivideByInt total list.Count

  /// Sum elements of the IReadOnlyList
  let inline sumOptional (list:IReadOnlyList<OptionalValue<'T>>) = 
    let mutable total = LanguagePrimitives.GenericZero
    for i in 0 .. list.Count - 1 do 
      if list.[i].HasValue then total <- total + list.[i].Value
    total

  /// Sum elements of the IReadOnlyList
  let inline averageOptional (list:IReadOnlyList<OptionalValue<'T>>) = 
    let mutable total = LanguagePrimitives.GenericZero
    let mutable count = 0 
    for i in 0 .. list.Count - 1 do 
      if list.[i].HasValue then 
        total <- total + list.[i].Value
        count <- count + 1
    LanguagePrimitives.DivideByInt total count

module Array = 
  /// Drop a specified range from a given array. The operation is inclusive on
  /// both sides. Given [ 1; 2; 3; 4 ] and indices (1, 2), the result is [ 1; 4 ]
  let inline dropRange first last (data:'T[]) =
    if last < first then invalidOp "The first index must be smaller than or equal to the last."
    if first < 0 || last >= data.Length then invalidArg "first" "The index must be within the array range."
    Array.append (data.[.. first - 1]) (data.[last + 1 ..])

  let inline existsAt low high f (data:'T[]) = 
    let rec test i = 
      if i > high then false
      elif f data.[i] then true
      else test (i + 1)
    test low

  let inline private binarySearch key (comparer:System.Collections.Generic.IComparer<'T>) (array:'T[]) =
    let rec search (lo, hi) =
      if lo = hi then lo else
      let mid = (lo + hi) / 2
      match comparer.Compare(key, array.[mid]) with 
      | 0 -> mid
      | n when n < 0 -> search (lo, max lo (mid - 1))
      | _ -> search (min hi (mid + 1), hi) 
    search (0, array.Length - 1) 

  let binarySearchNearestGreater key (comparer:System.Collections.Generic.IComparer<'T>) (array:'T[]) =
    let loc = binarySearch key comparer array
    if comparer.Compare(array.[loc], key) >= 0 then Some loc
    elif loc + 1 < array.Length && comparer.Compare(array.[loc + 1], key) >= 1 then Some (loc + 1)
    else None

  let binarySearchNearestSmaller key (comparer:System.Collections.Generic.IComparer<'T>) (array:'T[]) =
    let loc = binarySearch key comparer array
    if comparer.Compare(array.[loc], key) <= 0 then Some loc
    elif loc - 1 >= 0 && comparer.Compare(array.[loc - 1], key) <= 0 then Some (loc - 1)
    else None

module Seq = 
  let takeAtMost count (input:seq<_>) = input.Take(count)

  let headOrNone (input:seq<_>) = 
    (input |> Seq.map Some).FirstOrDefault()

  let getEnumerator (s:seq<_>) = s.GetEnumerator()

  let startAndEnd startCount endCount input = seq { 
    let lastItems = Array.zeroCreate endCount
    let lastPointer = ref 0
    let written = ref 0
    let skippedAny = ref false
    let writeNext(v) = 
      if !written < endCount then incr written; 
      lastItems.[!lastPointer] <- v; lastPointer := (!lastPointer + 1) % endCount
    let readNext() = let p = !lastPointer in lastPointer := (!lastPointer + 1) % endCount; lastItems.[p]
    let readRest() = 
      lastPointer := (!lastPointer + endCount - !written) % endCount
      seq { for i in 1 .. !written -> readNext() }

    use en = getEnumerator input 
    let rec skipToEnd() = 
      if en.MoveNext() then 
        writeNext(en.Current)
        skippedAny := true
        skipToEnd()
      else seq { if skippedAny.Value then 
                   yield Choice2Of3()
                   for v in readRest() -> Choice3Of3 v 
                 else for v in readRest() -> Choice1Of3 v }
    let rec fillRest count = 
      if count = endCount then skipToEnd()
      elif en.MoveNext() then 
        writeNext(en.Current)
        fillRest (count + 1)
      else seq { for v in readRest() -> Choice1Of3 v }
    let rec yieldFirst count = seq { 
      if count = 0 then yield! fillRest 0
      elif en.MoveNext() then 
        yield Choice1Of3 en.Current
        yield! yieldFirst (count - 1) }
    yield! yieldFirst startCount }
(*
  startAndEnd 4 4 [ 1 .. 6 ] |> Array.ofSeq = 
    [| Choice1Of3 1; Choice1Of3 2; Choice1Of3 3; Choice1Of3 4; Choice1Of3 5; Choice1Of3 6|]

  startAndEnd 3 3 [ 1 .. 6 ] |> Array.ofSeq = 
    [| Choice1Of3 1; Choice1Of3 2; Choice1Of3 3; Choice1Of3 4; Choice1Of3 5; Choice1Of3 6|]

  startAndEnd 2 2 [ 1 .. 6 ] |> Array.ofSeq = 
    [|Choice1Of3 1; Choice1Of3 2; Choice2Of3(); Choice3Of3 5; Choice3Of3 6|]

  startAndEnd 2 2 [ 1 .. 6 ] |> Array.ofSeq = 
    [|Choice1Of3 1; Choice1Of3 2; Choice2Of3(); Choice3Of3 5; Choice3Of3 6|]
*)

  let windowedWhile f input = seq {
    let windows = System.Collections.Generic.LinkedList()
    for v in input do
      windows.AddLast( (v, []) ) |> ignore
      // Walk over all windows; use 'f' to determine if the item
      // should be added - if so, add it, otherwise yield window
      let win = ref windows.First
      while win.Value <> null do 
        let start, items = win.Value.Value
        let next = win.Value.Next
        if f start v then win.Value.Value <- start, v::items
        else 
          yield items |> List.rev |> Array.ofList
          windows.Remove(win.Value)
        win := next
    for _, win in windows do
      yield win |> List.rev |> Array.ofList }

  let chunkedWhile f input = seq {
    let chunk = ref None
    for v in input do
      match chunk.Value with 
      | None -> chunk := Some(v, [v])
      | Some(start, items) ->
          if f start v then chunk := Some(start, v::items)
          else
            yield items |> List.rev |> Array.ofList
            chunk := Some(v, [v])
    match chunk.Value with
    | Some (_, items) -> yield items |> List.rev |> Array.ofList
    | _ -> () }

  (*
  chunkedWhile (fun f t -> t - f < 10) [ 1; 4; 11; 12; 13; 15; 20; 25 ] |> Array.ofSeq =
    [| [|1; 4|]; [|11; 12; 13; 15; 20|]; [|25|] |]

  windowedWhile (fun f t -> t - f < 10) [ 1; 4; 11; 12; 13; 15; 20; 25 ] |> Array.ofSeq =
    [| [|1; 4|]; [|4; 11; 12; 13|]; [|11; 12; 13; 15; 20|]; [|12; 13; 15; 20|];
       [|13; 15; 20|]; [|15; 20|]; [|20; 25|]; [|25|] |]
  *)

  let chunked size input = 
    input 
    |> Seq.windowed size
    |> Seq.mapi (fun i win -> i, win)
    |> Seq.filter (fun (i, _) -> i % size = 0)
    |> Seq.map snd

  /// Returns true if the specified sequence is sorted.
  let isSorted (data:seq<_>) (comparer:IComparer<_>) =
    let rec isSorted past (en:IEnumerator<'T>) =
      if not (en.MoveNext()) then true
      elif comparer.Compare(past, en.Current) > 0 then false
      else isSorted en.Current en
    let en = data.GetEnumerator()
    if not (en.MoveNext()) then true
    else isSorted en.Current en

  /// Align two ordered sequences of key * address pairs and produce a 
  /// collection that contains three-element tuples consisting of: 
  ///   * ordered keys (from one or the ohter sequence)
  ///   * optional address of the key in the first sequence
  ///   * optional address of the key in the second sequence
  let alignWithOrdering (seq1:seq<'T * 'TAddress>) (seq2:seq<'T * 'TAddress>) (comparer:IComparer<_>) = seq {
    let withIndex seq = Seq.mapi (fun i v -> i, v) seq
    use en1 = seq1.GetEnumerator()
    use en2 = seq2.GetEnumerator()
    let en1HasNext = ref (en1.MoveNext())
    let en2HasNext = ref (en2.MoveNext())
    let returnAll (en:IEnumerator<_>) hasNext f = seq { 
      if hasNext then
        yield f en.Current
        while en.MoveNext() do yield f en.Current }

    let rec next () = seq {
      if not en1HasNext.Value then yield! returnAll en2 en2HasNext.Value (fun (k, i) -> k, None, Some i)
      elif not en2HasNext.Value then yield! returnAll en1 en1HasNext.Value (fun (k, i) -> k, Some i, None)
      else
        let en1Val, en2Val = fst en1.Current, fst en2.Current
        let comparison = comparer.Compare(en1Val, en2Val)
        if comparison = 0 then 
          yield en1Val, Some(snd en1.Current), Some(snd en2.Current)
          en1HasNext := en1.MoveNext()
          en2HasNext := en2.MoveNext()
          yield! next()
        elif comparison < 0 then
          yield en1Val, Some(snd en1.Current), None
          en1HasNext := en1.MoveNext()
          yield! next ()
        else 
          yield en2Val, None, Some(snd en2.Current)
          en2HasNext := en2.MoveNext() 
          yield! next () }
    yield! next () }

  let unionWithOrdering (seq1:seq<'T * 'TAddress>) (seq2:seq<'T * 'TAddress>) = seq {
    let dict = Dictionary<_, _>()
    for key, addr in seq1 do
      dict.[key] <- (Some addr, None)
    for key, addr in seq2 do
      match dict.TryGetValue(key) with
      | true, (left, _) -> dict.[key] <- (left, Some addr)
      | _ -> dict.[key] <- (None, Some addr)
    for (KeyValue(k, (l, r))) in dict do
      yield k, l, r }

(*
alignWithOrdering [ 'a'; 'd' ] [ 'b'; 'c' ] |> List.ofSeq
alignWithOrdering [ 'd' ] [ 'a'; 'b'; 'c' ] |> List.ofSeq
alignWithOrdering [ 'a'; 'b' ] [ 'c'; 'd' ] |> List.ofSeq
alignWithOrdering [ 'a'; 'c'; 'd'; 'e' ] [ 'a'; 'b'; 'c'; 'e' ] |> List.ofSeq

alignWithOrdering [ ("b", 0); ("c", 1); ("d", 2) ] [ ("a", 0); ("b", 1); ("c", 2) ] (Comparer<string>.Default) |> List.ofSeq = 
  [("a", None, Some 0); ("b", Some 0, Some 1); ("c", Some 1, Some 2); ("d", Some 2, None)]

unionWithOrdering [ ("b", 0); ("c", 1); ("d", 2) ] [ ("b", 1); ("c", 2); ("a", 0); ] |> List.ofSeq |> set =
  set [("b", Some 0, Some 1); ("c", Some 1, Some 2); ("d", Some 2, None); ("a", None, Some 0)]
*)

type IFormattable =
  abstract Format : unit -> string

module Formatting = 
  let ItemCount = 10

  open System
  open System.IO
  open System.Text

  // Simple functions that pretty-print series and frames
  // (to be integrated as ToString and with F# Interactive)
  let formatTable (data:string[,]) =
    let sb = StringBuilder()
    use wr = new StringWriter(sb)

    let rows = data.GetLength(0)
    let columns = data.GetLength(1)
    let widths = Array.zeroCreate columns 
    data |> Array2D.iteri (fun r c str ->
      widths.[c] <- max (widths.[c]) (str.Length))
    for r in 0 .. rows - 1 do
      for c in 0 .. columns - 1 do
        wr.Write(data.[r, c].PadRight(widths.[c] + 1))
      wr.WriteLine()

    sb.ToString()