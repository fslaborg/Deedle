#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Common
#endif

open System
open System.Collections.Generic
open System.Collections.ObjectModel
open FsUnit
open FsCheck
open NUnit.Framework
open Deedle
open Deedle.Internal

[<Test>]
let ``NaN is recognized as a missing values, other floating point values are not`` () =
  let testFloat = MissingValues.isNA<float>()
  testFloat Double.NaN |> shouldEqual true
  testFloat Double.NegativeInfinity |> shouldEqual false
  testFloat 42.0 |> shouldEqual false

  let testFloat32 = MissingValues.isNA<float32>()
  testFloat32 Single.NaN |> shouldEqual true
  testFloat32 Single.NegativeInfinity |> shouldEqual false
  testFloat32 42.0f |> shouldEqual false

[<Test>]
let ``null is recognized as a missing value, non-null references are not`` () = 
  let testRand = MissingValues.isNA<Random>()
  testRand (Random()) |> shouldEqual false
  testRand null |> shouldEqual true

[<Test>]
let ``Nullable value is recognized as missing, if it has no value`` () = 
  let testNullable = MissingValues.isNA<Nullable<int>>()
  testNullable (Nullable(10)) |> shouldEqual false
  testNullable (Nullable()) |> shouldEqual true

[<Test>]
let ``Equality on OptionalValue type works as expected`` () =
  OptionalValue.Missing = OptionalValue<float>.Missing |> shouldEqual true
  OptionalValue(0.0) = OptionalValue(0.0) |> shouldEqual true
  OptionalValue.Missing = OptionalValue(0.0) |> shouldEqual false
  OptionalValue.Missing = OptionalValue(1.0) |> shouldEqual false

[<Test>]
let ``Array.dropRange drops inclusive range from an array`` () = 
  [| 1 .. 10 |] |> Array.dropRange 0 9 |> shouldEqual [| |]
  [| 1 .. 10 |] |> Array.dropRange 1 8 |> shouldEqual [| 1; 10 |]    

[<Test>]
let ``Binary searching for exact or greater value works`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 5 comparer true |> shouldEqual (Some 3)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 6 comparer true |> shouldEqual (Some 3)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 7 comparer true |> shouldEqual None
  new ReadOnlyCollection<_> [| |]
  |> Array.binarySearchNearestGreater 5 comparer true |> shouldEqual None
    
[<Test>]
let ``Binary searching for exact or smaller value works`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 5 comparer true |> shouldEqual (Some 2)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 6 comparer true |> shouldEqual (Some 3)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 0 comparer true |> shouldEqual None
  new ReadOnlyCollection<_> [| |]
  |> Array.binarySearchNearestSmaller 5 comparer true |> shouldEqual None

[<Test>]
let ``Binary searching for greater value works`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 5 comparer false |> shouldEqual (Some 3)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 2 comparer false |> shouldEqual (Some 2)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestGreater 6 comparer false |> shouldEqual None
  new ReadOnlyCollection<_> [| |]
  |> Array.binarySearchNearestGreater 5 comparer false |> shouldEqual None
    
[<Test>]
let ``Binary searching for smaller value works`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 5 comparer false |> shouldEqual (Some 2)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 6 comparer false |> shouldEqual (Some 2)
  new ReadOnlyCollection<_> [| 1; 2; 4; 6 |]
  |> Array.binarySearchNearestSmaller 0 comparer false |> shouldEqual None
  new ReadOnlyCollection<_> [| |]
  |> Array.binarySearchNearestSmaller 5 comparer false |> shouldEqual None
    
[<Test>]
let ``Binary searching for exact or greater value satisfies laws`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
    let input = new ReadOnlyCollection<_>(Array.sort input)
    match Array.binarySearchNearestGreater key comparer true input with
    | Some idx -> input.[idx] >= key
    | None -> Seq.forall (fun v -> v < key) input )

[<Test>]
let ``Binary searching for exact or smaller value satisfies laws`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
    let input = new ReadOnlyCollection<_>(Array.sort input)
    match Array.binarySearchNearestSmaller key comparer true input with
    | Some idx -> input.[idx] <= key
    | None -> Seq.forall (fun v -> v > key) input )

[<Test>]
let ``Binary searching for greater value satisfies laws`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
    let input = new ReadOnlyCollection<_>(input |> Seq.distinct |> Seq.sort |> Array.ofSeq)
    match Array.binarySearchNearestGreater key comparer false input with
    | Some idx -> input.[idx] > key
    | None -> Seq.forall (fun v -> v <= key) input )

[<Test>]
let ``Binary searching for smaller value satisfies laws`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
    let input = new ReadOnlyCollection<_>(input |> Seq.distinct |> Seq.sort |> Array.ofSeq)
    match Array.binarySearchNearestSmaller key comparer false input with
    | Some idx -> input.[idx] < key
    | None -> Seq.forall (fun v -> v >= key) input )

[<Test>]
let ``Seq.lastFew works on empty lists`` () = 
  Seq.lastFew 3 List.empty<int> |> List.ofSeq |> shouldEqual []

[<Test>]
let ``Seq.lastFew works on non-empty lists`` () = 
  Seq.lastFew 3 [ 1 .. 10 ]  |> List.ofSeq |> shouldEqual [ 8; 9; 10]

[<Test>]
let ``Seq.lastFew works on list with insufficient number of elements`` () = 
  Seq.lastFew 3 [ 9; 10 ]  |> List.ofSeq |> shouldEqual [ 9; 10]

[<Test>]
let ``Seq.startAndEnd works on shorter inputs`` () =
  Seq.startAndEnd 4 4 [ 1 .. 6 ] |> Array.ofSeq
  |> shouldEqual [| Choice1Of3 1; Choice1Of3 2; Choice1Of3 3; Choice1Of3 4; Choice1Of3 5; Choice1Of3 6|]
  Seq.startAndEnd 3 3 [ 1 .. 6 ] |> Array.ofSeq
  |> shouldEqual [| Choice1Of3 1; Choice1Of3 2; Choice1Of3 3; Choice1Of3 4; Choice1Of3 5; Choice1Of3 6|]

[<Test>]
let ``Seq.startAndEnd works on longer inputs`` () =
  Seq. startAndEnd 2 2 [ 1 .. 6 ] |> Array.ofSeq
  |> shouldEqual [| Choice1Of3 1; Choice1Of3 2; Choice2Of3(); Choice3Of3 5; Choice3Of3 6 |]
  Seq.startAndEnd 2 2 [ 1 .. 6 ] |> Array.ofSeq
  |> shouldEqual [| Choice1Of3 1; Choice1Of3 2; Choice2Of3(); Choice3Of3 5; Choice3Of3 6 |]

[<Test>]
let ``Seq.chunkedWhile works on simple input`` () =
  Seq.chunkedWhile (fun f t -> t - f < 10) [ 1; 4; 11; 12; 13; 15; 20; 25 ] |> Array.ofSeq
  |> shouldEqual [| [|1; 4|]; [|11; 12; 13; 15; 20|]; [|25|] |]

[<Test>]
let ``Seq.chunkedWhile works and does not lose values`` () =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let all = Seq.chunkedWhile (fun f t -> t - f < 10) input |> Seq.concat |> Array.ofSeq
    all = input )

[<Test>]
let ``Seq.windowedWhile works on simple input`` () =
  Seq.windowedWhile (fun f t -> t - f < 10) [ 1; 4; 11; 12; 13; 15; 20; 25 ] |> Array.ofSeq
  |> shouldEqual
      [| [|1; 4|]; [|4; 11; 12; 13|]; [|11; 12; 13; 15; 20|]; [|12; 13; 15; 20|];
          [|13; 15; 20|]; [|15; 20|]; [|20; 25|]; [|25|] |]

[<Test>]
let ``Seq.windowedWhile does not throw and does not lose values`` () =
  Check.QuickThrowOnFailure(fun (input:int[]) ->
    let all = Seq.windowedWhile (fun f t -> t - f < 10) input |> Seq.concat |> Array.ofSeq
    set all = set input )

[<Test>]
let ``Seq.windowedWithBounds can generate boundary at the beginning`` () =
  Seq.windowedWithBounds 3 Boundary.AtBeginning [ 1; 2; 3; 4 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Incomplete, [| 1 |]); DataSegment(Incomplete, [| 1; 2 |])
       DataSegment(Complete, [| 1; 2; 3 |]); DataSegment(Complete, [| 2; 3; 4 |]) |]

[<Test>]
let ``Seq.windowedWithBounds can skip boundaries`` () =
  Seq.windowedWithBounds 3 Boundary.Skip [ 1; 2; 3; 4 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [| 1; 2; 3 |]); DataSegment(Complete, [| 2; 3; 4 |]) |]

[<Test>]
let ``Seq.windowedWithBounds can generate boundary at the ending`` () =
  Seq.windowedWithBounds 3 Boundary.AtEnding [ 1; 2; 3; 4 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [| 1; 2; 3 |]); DataSegment(Complete, [| 2; 3; 4 |]) 
       DataSegment(Incomplete, [| 3; 4 |]); DataSegment(Incomplete, [| 4 |]) |]

[<Test>]
let ``Seq.chunkedWithBounds works when length is multiple of chunk size`` () =
  Seq.chunkedWithBounds 3 Boundary.AtBeginning [ 1 .. 9 ] |> Array.ofSeq 
  |> shouldEqual
    [| DataSegment(Complete, [|1; 2; 3|]); DataSegment(Complete, [|4; 5; 6|]); 
        DataSegment(Complete, [|7; 8; 9|]) |]
  
[<Test>]
let ``Seq.chunkedWithBounds generates incomplete chunk at beginning`` () =
  Seq.chunkedWithBounds 3 Boundary.AtBeginning [ 1 .. 10 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Incomplete, [|1|]); DataSegment(Complete, [|2; 3; 4|]); 
       DataSegment(Complete, [|5; 6; 7|]); DataSegment(Complete, [|8; 9; 10|]) |]

[<Test>]
let ``Seq.chunkedWithBounds can skip incomplete chunk at the beginning`` () =
  Seq.chunkedWithBounds 3 (Boundary.AtBeginning ||| Boundary.Skip) [ 1 .. 10 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [|2; 3; 4|]); DataSegment(Complete, [|5; 6; 7|]); 
       DataSegment(Complete, [|8; 9; 10|]) |]

[<Test>]
let ``Seq.chunkedWithBounds works when length is multiple of chunk size (2)`` () =
  Seq.chunkedWithBounds 3 Boundary.AtEnding [ 1 .. 9 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [|1; 2; 3|]); DataSegment(Complete, [|4; 5; 6|]); 
       DataSegment(Complete, [|7; 8; 9|]) |]
  
[<Test>]
let ``Seq.chunkedWithBounds can generate incomplete chunk at the end`` () =
  Seq.chunkedWithBounds 3 Boundary.AtEnding [ 1 .. 10 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [|1; 2; 3|]); DataSegment(Complete, [|4; 5; 6|]); 
       DataSegment(Complete, [|7; 8; 9|]); DataSegment(Incomplete, [| 10 |]) |]

[<Test>]
let ``Seq.chunkedWithBounds can skip incomplete chunk at the end`` () =
  Seq.chunkedWithBounds 3 (Boundary.AtEnding ||| Boundary.Skip) [ 1 .. 10 ] |> Array.ofSeq
  |> shouldEqual
    [| DataSegment(Complete, [|1; 2; 3|]); DataSegment(Complete, [|4; 5; 6|]); 
       DataSegment(Complete, [|7; 8; 9|]) |]

[<Test>]
let ``Seq.alignOrdered (union) satisfies basic conditions`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values & sorted inputs
    let a1 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a1))
    let a2 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a2))
    let keys, relocs = Seq.alignOrdered a1 a2 comparer false
    let r1, r2 = match relocs with [r1;r2] -> r1,r2 | _ -> failwith "Expected 2 items"

    // Check that: keys are sorted
    Array.sort keys = keys &&
    // Check that: all original keys are somewhere in new keys
    a1 |> Seq.forall (fun k1 -> Seq.exists ((=) k1) keys) &&
    a2 |> Seq.forall (fun k2 -> Seq.exists ((=) k2) keys) &&
    // Check that: there are as many relocations as values & no indices are duplicated
    r1.Length = a1.Count &&
    (Seq.length (Seq.distinctBy fst r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy snd r1)) = r1.Length &&
    r2.Length = a2.Count &&
    (Seq.length (Seq.distinctBy fst r2)) = r2.Length &&
    (Seq.length (Seq.distinctBy snd r2)) = r2.Length &&
    // Check that: relocations point to right keys
    r1 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a1.[int oidx]) &&
    r2 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a2.[int oidx]) )

[<Test>]
let ``Seq.alignUnordered (union) satisfies basic conditions`` () =
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values 
    let a1 = ReadOnlyCollection.ofSeq (Seq.distinct a1)
    let a2 = ReadOnlyCollection.ofSeq (Seq.distinct a2)
    let keys, relocs = Seq.alignUnordered a1 a2 false
    let r1, r2 = match relocs with [r1;r2] -> r1,r2 | _ -> failwith "Expected 2 items"

    // Check that: all original keys are somewhere in new keys
    a1 |> Seq.forall (fun k1 -> Seq.exists ((=) k1) keys) &&
    a2 |> Seq.forall (fun k2 -> Seq.exists ((=) k2) keys) &&
    // Check that: there are as many relocations as values & no indices are duplicated
    r1.Length = a1.Count &&
    (Seq.length (Seq.distinctBy fst r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy snd r1)) = r1.Length &&
    r2.Length = a2.Count &&
    (Seq.length (Seq.distinctBy fst r2)) = r2.Length &&
    (Seq.length (Seq.distinctBy snd r2)) = r2.Length &&
    // Check that: relocations point to right keys
    r1 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a1.[int oidx]) &&
    r2 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a2.[int oidx]) )

[<Test>]
let ``Seq.alignOrdered (intersection) satisfies basic conditions`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values & sorted inputs
    let a1 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a1))
    let a2 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a2))
    let keys, relocs = Seq.alignOrdered a1 a2 comparer true
    let r1, r2 = match relocs with [r1;r2] -> r1,r2 | _ -> failwith "Expected 2 items"

    let expectedKeys = Set.intersect (set a1) (set a2)

    // Check that: keys match expected keys according to F# set
    keys = Array.sort (Array.ofSeq expectedKeys) &&
    // Check that: relocation lengths match number of keys to be included in the output
    r1.Length = (a1 |> Seq.sumBy (fun k1 -> if expectedKeys.Contains(k1) then 1 else 0)) &&
    r2.Length = (a2 |> Seq.sumBy (fun k2 -> if expectedKeys.Contains(k2) then 1 else 0)) &&
    // Check that: relocation indices are unique
    (Seq.length (Seq.distinctBy fst r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy snd r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy fst r2)) = r2.Length &&
    (Seq.length (Seq.distinctBy snd r2)) = r2.Length &&
    // Check that: relocations point to right keys
    r1 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a1.[int oidx]) &&
    r2 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a2.[int oidx]) )

[<Test>]
let ``Seq.alignUnordered (intersection) satisfies basic conditions`` () =
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values 
    let a1 = ReadOnlyCollection.ofSeq (Seq.distinct a1)
    let a2 = ReadOnlyCollection.ofSeq (Seq.distinct a2)
    let keys, relocs = Seq.alignUnordered a1 a2 true
    let r1, r2 = match relocs with [r1;r2] -> r1,r2 | _ -> failwith "Expected 2 items"

    let expectedKeys = Set.intersect (set a1) (set a2)

    // Check that: keys match expected keys according to F# set
    set keys = expectedKeys &&
    // Check that: relocation lengths match number of keys to be included in the output
    r1.Length = (a1 |> Seq.sumBy (fun k1 -> if expectedKeys.Contains(k1) then 1 else 0)) &&
    r2.Length = (a2 |> Seq.sumBy (fun k2 -> if expectedKeys.Contains(k2) then 1 else 0)) &&
    // Check that: relocation indices are unique
    (Seq.length (Seq.distinctBy fst r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy snd r1)) = r1.Length &&
    (Seq.length (Seq.distinctBy fst r2)) = r2.Length &&
    (Seq.length (Seq.distinctBy snd r2)) = r2.Length &&
    // Check that: relocations point to right keys
    r1 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a1.[int oidx]) &&
    r2 |> Seq.forall (fun (nidx, oidx) -> keys.[int nidx] = a2.[int oidx]) )

[<Test>]
let ``Seq.alignAllOrdered behaves the same as Seq.alignOrdered`` () =
  let comparer = System.Collections.Generic.Comparer<int>.Default
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values & sorted inputs
    let a1 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a1))
    let a2 = ReadOnlyCollection.ofSeq (Seq.sort (Seq.distinct a2))
    Seq.alignAllOrdered [| a1; a2 |] comparer = Seq.alignOrdered a1 a2 comparer false )

[<Test>]
let ``Seq.alignAllUnordered behaves the same as Seq.alignUnordered`` () =
  Check.QuickThrowOnFailure(fun (a1:int[]) (a2:int[]) ->
    // Preprocess: we only want distinct values 
    let a1 = ReadOnlyCollection.ofSeq (Seq.distinct a1)
    let a2 = ReadOnlyCollection.ofSeq (Seq.distinct a2)
    Seq.alignAllUnordered [| a1; a2 |] = Seq.alignUnordered a1 a2 false )


[<Test>]
let ``Binomial heap can insert and remove minimum`` () =
  Check.QuickThrowOnFailure(fun (nums:int[]) ->
    let mutable h = BinomialHeap.empty
    // Check that we can insert all numbers and minimum works correctly
    for i in 0 .. nums.Length - 1 do 
      h <- BinomialHeap.insert (nums.[i]) h 
      if BinomialHeap.findMin h <> (Seq.min nums.[0 .. i]) then failwith "Min failed" 
    
    // Check that we can remove all elements and the one before is always smaller
    let mutable lastMin = Int32.MinValue
    for i in 0 .. nums.Length - 1 do
      let min, nh = BinomialHeap.removeMin h
      if min < lastMin then failwith "RemoveMin failed"
      h <- nh
      lastMin <- min )

[<Test>]
let ``Array.quickSelectInplace selects nth element when compared with sorted array`` () =
  Check.QuickThrowOnFailure(fun (input:float[]) ->
    let input = input |> Array.filter (Double.IsNaN >> not)
    for n in 0 .. input.Length - 1 do
      let nth = StatsInternal.quickSelectInplace n (Array.map id input)
      nth |> shouldEqual ((Array.sort input).[n]) )

// ------------------------------------------------------------------------------------------------
// Type conversions
// ------------------------------------------------------------------------------------------------

let values = 
  [ box 1uy; box 1y; box 1us; box 1s; box 1; box 1u; box 1L; 
    box 1UL; box 1.0M; box 1.0f; box 1.0; box "1"; box true ] 

[<Test>]
let ``Type conversion (flexible) can convert values to float and int`` () =
  for value in values do
    Convert.convertType<float> ConversionKind.Flexible value |> shouldEqual 1.0
    Convert.canConvertType<float> ConversionKind.Flexible value |> shouldEqual true
  for value in values do
    Convert.convertType<int> ConversionKind.Flexible value |> shouldEqual 1
    Convert.canConvertType<int> ConversionKind.Flexible value |> shouldEqual true

[<Test>]
let ``Type conversion (exact) can convert values of exact numeric types to float and int`` () =
  for value in values do
    let ty = value.GetType() 
    if ty = typeof<float> then
      Convert.convertType<float> ConversionKind.Safe value |> shouldEqual 1.0
      Convert.canConvertType<float> ConversionKind.Exact value |> shouldEqual true
  for value in values do
    let ty = value.GetType() 
    if ty = typeof<int> then
      Convert.convertType<int> ConversionKind.Exact value |> shouldEqual 1
      Convert.canConvertType<int> ConversionKind.Exact value |> shouldEqual true

[<Test>]
let ``Type conversion (exact) cannot convert values of non-exact numeric types to float and int`` () =
  for value in values do
    let ty = value.GetType() 
    if ty <> typeof<float> then
      (fun () -> Convert.convertType<float> ConversionKind.Exact value |> ignore) |> should throw typeof<InvalidCastException>
      Convert.canConvertType<float> ConversionKind.Exact value |> shouldEqual false
  for value in values do
    let ty = value.GetType() 
    if ty <> typeof<int> then
      (fun () -> Convert.convertType<int> ConversionKind.Exact value |> ignore) |> should throw typeof<InvalidCastException>
      Convert.canConvertType<int> ConversionKind.Exact value |> shouldEqual false

[<Test>]
let ``Type conversion (safe) can convert values of smaller numeric types to float and int`` () =
  for value in values do
    let ty = value.GetType() 
    if ty <> typeof<string> && ty <> typeof<bool> then
      Convert.convertType<float> ConversionKind.Safe value |> shouldEqual 1.0
      Convert.canConvertType<float> ConversionKind.Safe value |> shouldEqual true
  for value in values do
    let ty = value.GetType() 
    if ty <> typeof<string> && ty <> typeof<bool> && ty <> typeof<float> && ty <> typeof<decimal> && 
       ty <> typeof<float32> && ty <> typeof<float> && ty <> typeof<uint32> && ty <> typeof<int64> && ty <> typeof<uint64> then
      Convert.convertType<int> ConversionKind.Safe value |> shouldEqual 1
      Convert.canConvertType<int> ConversionKind.Safe value |> shouldEqual true

[<Test>]
let ``Type conversion (safe) cannot convert values of bigger or non-numeric types to float and int`` () =
  for value in values do
    let ty = value.GetType() 
    if ty = typeof<string> || ty = typeof<bool> then
      (fun () -> Convert.convertType<float> ConversionKind.Safe value |> ignore) |> should throw typeof<InvalidCastException>
      Convert.canConvertType<float> ConversionKind.Safe value |> shouldEqual false
  for value in values do
    let ty = value.GetType() 
    if ty = typeof<string> || ty = typeof<bool> || ty = typeof<float> || ty = typeof<decimal> || 
       ty = typeof<float32> || ty = typeof<float> || ty = typeof<uint32> || ty = typeof<int64> || ty = typeof<uint64> then
      (fun () -> Convert.convertType<int> ConversionKind.Safe value |> ignore) |> should throw typeof<InvalidCastException>
      Convert.canConvertType<int> ConversionKind.Safe value |> shouldEqual false
