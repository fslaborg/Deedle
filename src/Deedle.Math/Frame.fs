namespace Deedle.Math
open Deedle
open MathNet.Numerics.LinearAlgebra

module Frame =

  let ofMatrix (rows: 'R seq) (cols: 'C seq) (m: Matrix<'T>): Frame<'R, 'C> =
    m.ToArray()
    |> Frame.ofArray2D
    |> Frame.indexColsWith cols
    |> Frame.indexRowsWith rows

  let toMatrix (df: Frame<'R, 'C>): Matrix<float> =
    df
    |> Frame.toArray2D
    |> DenseMatrix.ofArray2

