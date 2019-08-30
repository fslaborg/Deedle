namespace Deedle.Math
open Deedle
open MathNet.Numerics.LinearAlgebra

/// Frame to matrix conversion
///
/// [category:Matrix conversions and operators]
type Frame =
  /// Convert matrix to frame
  ///
  static member ofMatrix (rows: 'R seq) (cols: 'C seq) (m: Matrix<'T>): Frame<'R, 'C> =
    m.ToArray()
    |> Frame.ofArray2D
    |> Frame.indexColsWith cols
    |> Frame.indexRowsWith rows

  /// Convert frame to matrix
  ///
  static member toMatrix (df: Frame<'R, 'C>): Matrix<float> =
    df
    |> Frame.toArray2D
    |> DenseMatrix.ofArray2