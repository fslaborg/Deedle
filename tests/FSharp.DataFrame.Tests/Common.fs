namespace FSharp.DataFrame.Common.Tests

#if INTERACTIVE
#r "../../bin/FSharp.DataFrame.dll"
#r "../../packages/NUnit.2.6.2/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open FSharp.DataFrame
open FSharp.DataFrame.Internal

module MissingValues = 
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

module Array =
  [<Test>]
  let ``Array.dropRange drops inclusive range from an array`` () = 
    [| 1 .. 10 |] |> Array.dropRange 0 9 |> shouldEqual [| |]
    [| 1 .. 10 |] |> Array.dropRange 1 8 |> shouldEqual [| 1; 10 |]    

  [<Test>]
  let ``Binary searching for nearest greater value works`` () =
    let comparer = System.Collections.Generic.Comparer<int>.Default
    Array.binarySearchNearestGreater 5 comparer [| 1; 2; 4; 6 |] |> shouldEqual (Some 3)
    Array.binarySearchNearestGreater 6 comparer [| 1; 2; 4; 6 |] |> shouldEqual (Some 3)
    Array.binarySearchNearestGreater 7 comparer [| 1; 2; 4; 6 |] |> shouldEqual None
    Array.binarySearchNearestGreater 5 comparer [| |] |> shouldEqual None
    
  [<Test>]
  let ``Binary searching for nearest smaller value works`` () =
    let comparer = System.Collections.Generic.Comparer<int>.Default
    Array.binarySearchNearestSmaller 5 comparer [| 1; 2; 4; 6 |] |> shouldEqual (Some 2)
    Array.binarySearchNearestSmaller 6 comparer [| 1; 2; 4; 6 |] |> shouldEqual (Some 3)
    Array.binarySearchNearestSmaller 0 comparer [| 1; 2; 4; 6 |] |> shouldEqual None
    Array.binarySearchNearestSmaller 5 comparer [| |] |> shouldEqual None
    
  [<Test>]
  let ``Binary searching for nearest greater value satisfies laws`` () =
    let comparer = System.Collections.Generic.Comparer<int>.Default
    Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
      let input = Array.sort input
      match Array.binarySearchNearestGreater key comparer input with
      | Some idx -> input.[idx] >= key
      | None -> Seq.forall (fun v -> v < key) input )

  [<Test>]
  let ``Binary searching for nearest smaller value satisfies laws`` () =
    let comparer = System.Collections.Generic.Comparer<int>.Default
    Check.QuickThrowOnFailure(fun (input:int[]) (key:int) -> 
      let input = Array.sort input
      match Array.binarySearchNearestSmaller key comparer input with
      | Some idx -> input.[idx] <= key
      | None -> Seq.forall (fun v -> v > key) input )

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
      [| Incomplete [| 1 |]; Incomplete [| 1; 2 |]
         Complete [| 1; 2; 3 |]; Complete [| 2; 3; 4 |] |]

  [<Test>]
  let ``Seq.windowedWithBounds can skip boundaries`` () =
    Seq.windowedWithBounds 3 Boundary.Skip [ 1; 2; 3; 4 ] |> Array.ofSeq
    |> shouldEqual
      [| Complete [| 1; 2; 3 |]; Complete [| 2; 3; 4 |] |]

  [<Test>]
  let ``Seq.windowedWithBounds can generate boundary at the ending`` () =
    Seq.windowedWithBounds 3 Boundary.AtEnding [ 1; 2; 3; 4 ] |> Array.ofSeq
    |> shouldEqual
      [| Complete [| 1; 2; 3 |]; Complete [| 2; 3; 4 |] 
         Incomplete [| 3; 4 |]; Incomplete [| 4 |] |]

(*
  chunkedWithBounds 3 Boundary.AtBeginning [ 1 .. 9 ] |> Array.ofSeq =
    [| Complete [|1; 2; 3|]; Complete [|4; 5; 6|]; Complete [|7; 8; 9|] |]
  
  chunkedWithBounds 3 Boundary.AtBeginning [ 1 .. 10 ] |> Array.ofSeq =
    [| Incomplete [|1|]; Complete [|2; 3; 4|]; Complete [|5; 6; 7|]; Complete [|8; 9; 10|] |]

  chunkedWithBounds 3 (Boundary.AtBeginning ||| Boundary.Skip) [ 1 .. 10 ] |> Array.ofSeq =
    [| Complete [|2; 3; 4|]; Complete [|5; 6; 7|]; Complete [|8; 9; 10|] |]

  chunkedWithBounds 3 Boundary.AtEnding [ 1 .. 9 ] |> Array.ofSeq =
    [| Complete [|1; 2; 3|]; Complete [|4; 5; 6|]; Complete [|7; 8; 9|] |]
  
  chunkedWithBounds 3 Boundary.AtEnding [ 1 .. 10 ] |> Array.ofSeq =
    [| Complete [|1; 2; 3|]; Complete [|4; 5; 6|]; Complete [|7; 8; 9|]; Incomplete [| 10 |] |]

  chunkedWithBounds 3 (Boundary.AtEnding ||| Boundary.Skip) [ 1 .. 10 ] |> Array.ofSeq =
    [| Complete [|1; 2; 3|]; Complete [|4; 5; 6|]; Complete [|7; 8; 9|] |]
*)