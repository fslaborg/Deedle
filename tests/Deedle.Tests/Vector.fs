module Deedle.Tests.Vector

#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle
open Deedle.Internal
open Deedle.Addressing
open Deedle.Vectors

[<Test>]
let ``Equality on vectors works as expected`` () =
  let sample = Vector.ofValues [ 1 .. 10 ]
  sample |> shouldEqual sample

[<Test>]
let ``GetValue returns the value at the specified address for a dense vector`` () =
  let vec = Vector.ofValues [ 10; 20; 30 ]
  vec.GetValue(LinearAddress.ofInt 0) |> shouldEqual (OptionalValue 10)
  vec.GetValue(LinearAddress.ofInt 1) |> shouldEqual (OptionalValue 20)
  vec.GetValue(LinearAddress.ofInt 2) |> shouldEqual (OptionalValue 30)

[<Test>]
let ``GetValue returns missing for addresses of missing values in a sparse vector`` () =
  let vec = Vector.ofOptionalValues [ Some 1.0; None; Some 3.0 ]
  vec.GetValue(LinearAddress.ofInt 0) |> shouldEqual (OptionalValue 1.0)
  vec.GetValue(LinearAddress.ofInt 1) |> shouldEqual OptionalValue.Missing
  vec.GetValue(LinearAddress.ofInt 2) |> shouldEqual (OptionalValue 3.0)

[<Test>]
let ``Length reports the number of elements including missing values`` () =
  let vec = Vector.ofOptionalValues [ Some 1; None; Some 3; None ]
  (vec :> IVector).Length |> shouldEqual 4L

[<Test>]
let ``ElementType returns the underlying element type`` () =
  let vec = Vector.ofValues [ 1; 2; 3 ]
  (vec :> IVector).ElementType |> shouldEqual typeof<int>

[<Test>]
let ``Data returns a DenseList for a vector without missing values`` () =
  let vec = Vector.ofValues [ 1; 2; 3 ]
  match vec.Data with
  | VectorData.DenseList data -> data |> List.ofSeq |> shouldEqual [ 1; 2; 3 ]
  | _ -> failwith "Expected DenseList representation"

[<Test>]
let ``Data returns a SparseList for a vector with missing values`` () =
  let vec = Vector.ofOptionalValues [ Some 1; None; Some 3 ]
  match vec.Data with
  | VectorData.SparseList data ->
      data |> List.ofSeq |> shouldEqual [ OptionalValue 1; OptionalValue.Missing; OptionalValue 3 ]
  | _ -> failwith "Expected SparseList representation"

[<Test>]
let ``DataSequence preserves order and includes missing values`` () =
  let vec = Vector.ofOptionalValues [ Some 1; None; Some 3 ]
  vec.DataSequence |> List.ofSeq |> shouldEqual [ OptionalValue 1; OptionalValue.Missing; OptionalValue 3 ]

[<Test>]
let ``ObjectSequence boxes the underlying values`` () =
  let vec = Vector.ofOptionalValues [ Some 1; None; Some 3 ]
  (vec :> IVector).ObjectSequence |> List.ofSeq
  |> shouldEqual [ OptionalValue(box 1); OptionalValue.Missing; OptionalValue(box 3) ]

[<Test>]
let ``Select transforms values and skips missing values by default`` () =
  let vec = Vector.ofOptionalValues [ Some 1; None; Some 3 ]
  let result = vec.Select(fun v -> v * 10)
  result.DataSequence |> List.ofSeq
  |> shouldEqual [ OptionalValue 10; OptionalValue.Missing; OptionalValue 30 ]

[<Test>]
let ``Select can turn present values into missing values`` () =
  let vec = Vector.ofValues [ 1; 2; 3 ]
  let result = vec.Select(fun _ (v:OptionalValue<int>) -> if v.Value = 2 then OptionalValue.Missing else v)
  result.DataSequence |> List.ofSeq
  |> shouldEqual [ OptionalValue 1; OptionalValue.Missing; OptionalValue 3 ]

[<Test>]
let ``Convert applies the forward function to all values`` () =
  let vec = Vector.ofValues [ 1; 2; 3 ]
  let converted = vec.Convert((fun v -> float v * 2.0), (fun v -> int (v / 2.0)))
  converted.DataSequence |> List.ofSeq |> shouldEqual [ OptionalValue 2.0; OptionalValue 4.0; OptionalValue 6.0 ]

[<Test>]
let ``Vector CreateMissing builds a vector from OptionalValue array (C# style)`` () =
  let data = [| OptionalValue 1; OptionalValue.Missing; OptionalValue 3 |]
  let vec = Vector.CreateMissing<int>(data)
  vec.DataSequence |> List.ofSeq |> shouldEqual [ OptionalValue 1; OptionalValue.Missing; OptionalValue 3 ]

[<Test>]
let ``Vector CreateMissing builds a vector from Nullable sequence (C# style)`` () =
  let data : seq<Nullable<int>> = seq [ Nullable(1); Nullable(); Nullable(3) ]
  let vec = Vector.CreateMissing(data)
  vec.DataSequence |> List.ofSeq |> shouldEqual [ OptionalValue 1; OptionalValue.Missing; OptionalValue 3 ]

[<Test>]
let ``Double.NaN is turned into a missing value`` () =
  let actual = Vector.ofValues [ 1.0; Double.NaN; 10.1 ]
  let expected = Vector.ofOptionalValues [ Some 1.0; None; Some 10.1 ]
  actual |> shouldEqual expected

[<Test>]
let ``null of Nullable type is turned into a missing value`` () =
  let actual = Vector.ofValues [ Nullable(1.0); unbox null; Nullable(10.1) ]
  let expected = Vector.ofOptionalValues [ Some (Nullable 1.0); None; Some (Nullable 10.1) ]
  actual |> shouldEqual expected

[<Test>]
let ``Select method correctly turns Double.NaN into a missing value`` () =
  let actual = (Vector.ofValues [ 1.0 .. 10.0 ]).Select(fun v -> Double.NaN)
  let expected = Vector.ofOptionalValues [ for i in 1 .. 10 -> None ]
  actual |> shouldEqual expected
