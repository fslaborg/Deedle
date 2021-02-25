#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#load "Deedle.Math.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Math.Tests.PCA
#endif

open Deedle
open Deedle.Math
open MathNet.Numerics
open NUnit.Framework


// Ingredients dataset from https://www.mathworks.com/help/stats/pca.html
[<Test>]
let ``PCA result matches with example from MathWorks`` () =
  let row1 = [| 7; 26;  6; 60|] |> Series.ofValues
  let row2 = [| 1; 29; 15; 52|] |> Series.ofValues
  let row3 = [|11; 56;  8; 20|] |> Series.ofValues
  let row4 = [|11; 31;  8; 47|] |> Series.ofValues
  let row5 = [| 7; 52;  6; 33|] |> Series.ofValues
  let row6 = [|11; 55;  9; 22|] |> Series.ofValues
  let row7 = [| 3; 71; 17;  6|] |> Series.ofValues
  let row8 = [| 1; 31; 22; 44|] |> Series.ofValues
  let row9 = [| 2; 54; 18; 22|] |> Series.ofValues
  let rowA = [|21; 47;  4; 26|] |> Series.ofValues
  let rowB = [| 1; 40; 23; 34|] |> Series.ofValues
  let rowC = [|11; 66;  9; 12|] |> Series.ofValues
  let rowD = [|10; 68;  8; 12|] |> Series.ofValues
  let df =
    Frame.ofRows
      [
        (0, row1)
        (1, row2)
        (2, row3)
        (3, row4)
        (4, row5)
        (5, row6)
        (6, row7)
        (7, row8)
        (8, row9)
        (9, rowA)
        (10, rowB)
        (11, rowC)
        (12, rowD)
      ]
  let pca = PCA.pca df
  let eigenVectors = PCA.eigenVectors pca
  let compareSeqs (expected : float seq) (actual : float seq) =
    Seq.zip expected actual
    |> Seq.iter (fun (e,a) -> Assert.AreEqual(e, a, 1e-10))
  let roundPcs (xs : Series<'a, float>) =
    xs
    |> Series.mapValues (fun x -> System.Math.Round(x, 4))
    |> Series.values
  let pc1 =
    eigenVectors.["PC1"] |> roundPcs
  let expectedPc1 =
    [ -0.0678; -0.6785; 0.0290; 0.7309]
  do compareSeqs pc1 expectedPc1
  let pc2 =
    eigenVectors.["PC2"] |> roundPcs
  let expectedPc2 =
    [ -0.6460; -0.0200; 0.7553; -0.1085]
  do compareSeqs expectedPc2 pc2
  let pc3 =
    eigenVectors.["PC3"] |> roundPcs
  let expectedPc3 =
    [ 0.5673; -0.5440; 0.4036; -0.4684]
  do compareSeqs expectedPc3 pc3
  let pc4 =
    eigenVectors.["PC4"] |> roundPcs
  let expectedPc4 =
    [ 0.5062; 0.4933; 0.5156; 0.4844]
  do compareSeqs expectedPc4 pc4
  let expectedEigenValues =
    [517.7969; 67.4964; 12.4054; 0.2372]
  let actualEigenValues =
    PCA.eigenValues pca |> roundPcs
  // Eigenvalues and vectors are shown in decreasing order.
  do compareSeqs expectedEigenValues actualEigenValues
