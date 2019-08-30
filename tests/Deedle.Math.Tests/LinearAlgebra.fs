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

open System.Collections.Generic
open MathNet.Numerics.LinearAlgebra
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Math

let testMatrix =
  [|
      "Singular3x3", array2D [| [| 1.0; 1.0; 2.0 |]; [| 1.0; 1.0; 2.0 |]; [| 1.0; 1.0; 2.0 |] |];
      "Square3x3", array2D [| [| -1.1; -2.2; -3.3 |]; [| 0.0; 1.1; 2.2 |]; [| -4.4; 5.5; 6.6 |] |];
      "Square4x4",  array2D [| [| -1.1; -2.2; -3.3; -4.4 |]; [| 0.0; 1.1; 2.2; 3.3 |]; [| 1.0; 2.1; 6.2; 4.3 |]; [| -4.4; 5.5; 6.6; -7.7 |] |];
      "Singular4x4",  array2D [| [| -1.1; -2.2; -3.3; -4.4 |]; [| -1.1; -2.2; -3.3; -4.4 |]; [| -1.1; -2.2; -3.3; -4.4 |]; [| -1.1; -2.2; -3.3; -4.4 |] |];
      "Tall3x2",  array2D [| [| -1.1; -2.2 |]; [| 0.0; 1.1 |]; [| -4.4; 5.5 |] |];
      "Wide2x3",  array2D [| [| -1.1; -2.2; -3.3 |]; [| 0.0; 1.1; 2.2 |] |];
      "Symmetric3x3",  array2D [| [| 1.0; 2.0; 3.0 |]; [| 2.0; 2.0; 0.0 |]; [| 3.0; 0.0; 3.0 |] |]
  |]
  |> Array.map(fun (name, arr) -> name, DenseMatrix.ofArray2(arr))

let testFrame =
  testMatrix
  |> Array.map(fun (name, m) -> name, m |> Frame.ofMatrix [1..m.RowCount] [1..m.ColumnCount] )
  |> dict

[<TestCase("Singular3x3")>]
[<TestCase("Singular4x4")>]
[<TestCase("Wide2x3")>]
[<TestCase("Tall3x2")>]
let ``Can Transpose And Multiply Frame With Frame`` (name:string) =
    let frameA = testFrame.[name]
    let frameB = testFrame.[name]
    let matrixC = frameA.Transpose().Dot(frameB)

    matrixC.RowCount |> shouldEqual frameA.RowCount
    matrixC.ColumnCount |> shouldEqual frameB.RowCount

    for i in [|0..matrixC.RowCount-1|] do
      for j in [|0..matrixC.ColumnCount-1|] do
        let s1 = frameA.GetRowAt<float>(i)
        let s2 = frameB.GetRowAt<float>(j)
        s1.Dot(s2).Item(0,0) |> should beWithin (matrixC.Item(i,j) +/- 1e-5)