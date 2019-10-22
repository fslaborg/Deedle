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
    [| "Singular3x3",
       array2D
           [| [| 1.0; 1.0; 2.0 |]
              [| 1.0; 1.0; 2.0 |]
              [| 1.0; 1.0; 2.0 |] |]
       "Square3x3",
       array2D
           [| [| -1.1; -2.2; -3.3 |]
              [| 0.0; 1.1; 2.2 |]
              [| -4.4; 5.5; 6.6 |] |]
       "Square4x4",
       array2D
           [| [| -1.1; -2.2; -3.3; -4.4 |]
              [| 0.0; 1.1; 2.2; 3.3 |]
              [| 1.0; 2.1; 6.2; 4.3 |]
              [| -4.4; 5.5; 6.6; -7.7 |] |]
       "Singular4x4",
       array2D
           [| [| -1.1; -2.2; -3.3; -4.4 |]
              [| -1.1; -2.2; -3.3; -4.4 |]
              [| -1.1; -2.2; -3.3; -4.4 |]
              [| -1.1; -2.2; -3.3; -4.4 |] |]
       "Tall3x2",
       array2D
           [| [| -1.1; -2.2 |]
              [| 0.0; 1.1 |]
              [| -4.4; 5.5 |] |]
       "Wide2x3",
       array2D
           [| [| -1.1; -2.2; -3.3 |]
              [| 0.0; 1.1; 2.2 |] |]
       "Symmetric3x3",
       array2D
           [| [| 1.0; 2.0; 3.0 |]
              [| 2.0; 2.0; 0.0 |]
              [| 3.0; 0.0; 3.0 |] |] |]
    |> Array.map (fun (name, arr) -> name, DenseMatrix.ofArray2 (arr))

let testFrame =
  testMatrix
  |> Array.map(fun (name, m) ->
    name, m |> Frame.ofMatrix [1..m.RowCount] [1..m.ColumnCount] )
  |> dict

[<TestCase("Singular3x3")>]
[<TestCase("Singular4x4")>]
[<TestCase("Wide2x3")>]
[<TestCase("Tall3x2")>]
let ``Frame can multiply with frame`` (name:string) =
  let frameA = testFrame.[name]
  let frameB = testFrame.[name].Transpose()

  let matrixC = frameA.Dot(frameB)
  for i in [|0..matrixC.RowCount-1|] do
    for j in [|0..matrixC.ColumnCount-1|] do
      let s1 = frameA.GetRowAt<float>(i)
      let s2 = frameB.GetColumnAt<float>(j)
      s1.Dot(s2) |> should beWithin (matrixC.Item(i,j) +/- 1e-5)

[<TestCase("Singular3x3")>]
[<TestCase("Singular4x4")>]
[<TestCase("Wide2x3")>]
[<TestCase("Tall3x2")>]
let ``Frame can multiply with matrix``(name:string) =
  let frameA = testFrame.[name]
  let matrixB = testFrame.[name].ToMatrix().Transpose()

  let matrixC = frameA.Dot(matrixB)
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
  
let ``Series can multiply with vector``() =
  let frame = testFrame.["Singular3x3"]
  let x = DenseVector.ofArray [|1.0..3.0|]

  frame.[1].Dot(x) |> should beWithin (6.0 +/- 1e-5)
  x.Dot(frame.[1]) |> should beWithin (6.0 +/- 1e-5)

let ``Frame can multiply with vector``() =
  let frame = testFrame.["Singular3x3"]
  let x = DenseVector.ofArray [|1.0..3.0|]
  frame.Dot(x).Item(0) |> should beWithin (9.0 +/- 1e-5)  

