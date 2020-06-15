#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.LinearAlgebra
#endif

open MathNet.Numerics.LinearAlgebra
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Math

let testMatrix =
    [ "Singular3x3",
      [ [ 1.0; 1.0; 2.0 ]
        [ 1.0; 1.0; 2.0 ]
        [ 1.0; 1.0; 2.0 ] ]
      "Square3x3",
      [ [ -1.1; -2.2; -3.3 ]
        [ 0.0; 1.1; 2.2 ]
        [ -4.4; 5.5; 6.6 ] ]
      "Square4x4",
      [ [ -1.1; -2.2; -3.3; -4.4 ]
        [ 0.0; 1.1; 2.2; 3.3 ]
        [ 1.0; 2.1; 6.2; 4.3 ]
        [ -4.4; 5.5; 6.6; -7.7 ] ]
      "Singular4x4",
      [ [ -1.1; -2.2; -3.3; -4.4 ]
        [ -1.1; -2.2; -3.3; -4.4 ]
        [ -1.1; -2.2; -3.3; -4.4 ]
        [ -1.1; -2.2; -3.3; -4.4 ] ]
      "Tall3x2",
      [ [ -1.1; -2.2 ]
        [ 0.0; 1.1 ]
        [ -4.4; 5.5 ] ]
      "Wide2x3",
      [ [ -1.1; -2.2; -3.3 ]
        [ 0.0; 1.1; 2.2 ] ]
      "Symmetric3x3",
      [ [ 1.0; 2.0; 3.0 ]
        [ 2.0; 2.0; 0.0 ]
        [ 3.0; 0.0; 3.0 ] ]]
    |> Seq.map (fun (name, x) -> name, x |> array2D |> DenseMatrix.ofArray2)

let testFrame =
  testMatrix
  |> Seq.map(fun (name, m) ->
    name, m |> Frame.ofMatrix [1..m.RowCount] [1..m.ColumnCount] )
  |> dict

[<TestCase("Singular3x3")>]
[<TestCase("Singular4x4")>]
[<TestCase("Wide2x3")>]
[<TestCase("Tall3x2")>]
let ``Frame can multiply with frame`` (name) =
  let frameA = testFrame.[name]
  let frameB = testFrame.[name].Transpose()
  let matrixC = frameA.Dot(frameB).ToMatrix()
  for i in [|0..matrixC.RowCount-1|] do
    for j in [|0..matrixC.ColumnCount-1|] do
      let s1 = frameA.GetRowAt<float>(i)
      let s2 = frameB.GetColumnAt<float>(j)
      s1.Dot(s2) |> should beWithin (matrixC.Item(i,j) +/- 1e-5)

[<TestCase("Singular3x3")>]
[<TestCase("Singular4x4")>]
[<TestCase("Wide2x3")>]
[<TestCase("Tall3x2")>]
let ``Frame can multiply with matrix`` (name) =
  let frameA = testFrame.[name]
  let matrixB = testFrame.[name].ToMatrix().Transpose()
  let matrixC = frameA.Dot(matrixB).ToMatrix()
  for i in [|0..matrixC.RowCount-1|] do
    for j in [|0..matrixC.ColumnCount-1|] do
      let s1 = frameA.GetRowAt<float>(i)
      let s2 = matrixB.Column(j)
      s1.Dot(s2) |> should beWithin (matrixC.Item(i,j) +/- 1e-5)

  let matrixD = matrixB.Dot(frameA)
  for i in [|0..matrixD.RowCount-1|] do
    for j in [|0..matrixD.ColumnCount-1|] do
      let s1 = matrixB.Row(i)
      let s2 = frameA.GetColumnAt<float>(j)
      s1.Dot(s2) |> should beWithin (matrixD.Item(i,j) +/- 1e-5)

[<Test>]
let ``Frame can multiply with series``() =
  let frame = testFrame.["Singular3x3"]
  let s = frame.[1]
  frame.Dot(s).Item(1) |> should beWithin (4.0 +/- 1e-5)
  s.Dot(frame).Item(1) |> should beWithin (3.0 +/- 1e-5)

[<Test>]
let ``Frame can multiply with vector``() =
  let frame = testFrame.["Singular3x3"]
  let x = DenseVector.ofArray [|1.0..3.0|]
  frame.Dot(x).Item(1) |> should beWithin (9.0 +/- 1e-5)
  x.Dot(frame).Item(1) |> should beWithin (6.0 +/- 1e-5)

[<Test>]
let ``Series can multiply with vector``() =
  let frame = testFrame.["Singular3x3"]
  let s = frame.[1]
  let x = DenseVector.ofArray [|1.0..3.0|]
  s.Dot(x) |> should beWithin (6.0 +/- 1e-5)
  x.Dot(s) |> should beWithin (6.0 +/- 1e-5)

[<Test>]
let ``Series can multiply with matrix``() =
  let frame = testFrame.["Singular3x3"]
  let s = frame.[1]
  let m = frame.ToMatrix()
  s.Dot(m).Item(1) |> should beWithin (3.0 +/- 1e-5)
  m.Dot(s).Item(1) |> should beWithin (4.0 +/- 1e-5)

[<Test>]
let ``Frame dot frame works on matching set of keys``() =
  let keys1 = ["A"; "B"; "C"]
  let keys3 = [1; 2; 3]
  let keys4 = [3; 1; 2]
  let x =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith keys1
    |> Frame.indexRowsWith keys3
  let y =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith keys4
    |> Frame.indexRowsWith keys1
  let expected1 = seq [3; 1; 2]
  let actual1 = x.Dot(y).ColumnKeys
  expected1 |> should equal actual1

[<Test>]
let ``Frame dot frame throws error on unmatched keys``() =
  let keys1 = ["A"; "B"; "C"]
  let keys2 = ["A"; "B"; "D"]
  let keys3 = [1; 2; 3]
  let keys4 = [3; 1; 2]
  let x =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith keys1
    |> Frame.indexRowsWith keys3
  let y =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith keys4
    |> Frame.indexRowsWith keys2
  (fun () -> x.Dot(y) |> ignore)
  |> should throw typeof<System.InvalidOperationException>

[<Test>]
let ``Frame dot series works on matching set of keys``() =
  let x =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith [0; 2; 1]
  let y = [1.; 2.; 4.] |> Series.ofValues
  let expectedKeys = seq [0; 1; 2]
  let actualKeys = x.Dot(y).Keys
  let expectedValue = x.Dot(y).Item(1)
  let actualValue = 9.
  expectedKeys |> should equal actualKeys
  expectedValue |> should beWithin (actualValue +/- 1e-5)

[<Test>]
let ``Frame dot series throws error on unmatched keys``() =
  let x =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith [0; 3; 1]
  let y = [1.; 2.; 4.] |> Series.ofValues
  (fun () -> x.Dot(y) |> ignore)
  |> should throw typeof<System.InvalidOperationException>

[<Test>]
let ``Series dot frame works on matching set of keys``() =
  let x = [1.; 2.; 4.] |> Series.ofValues
  let y =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith [0; 2; 1]
  let expectedKeys = seq [0; 1; 2]
  let actualKeys = x.Dot(y).Keys
  let expectedValue = x.Dot(y)
  let actualValue = [7.; 14.; 7.] |> Series.ofValues
  expectedKeys |> should equal actualKeys
  expectedValue |> should equal actualValue

[<Test>]
let ``Series dot frame throws error on unmatched keys``() =
  let x = [1.; 2.; 4.] |> Series.ofValues |> Series.indexWith [0; 3; 1]
  let y =
    testFrame.["Singular3x3"]
    |> Frame.indexColsWith [0; 2; 1]
  (fun () -> x.Dot(y) |> ignore)
  |> should throw typeof<System.InvalidOperationException>
