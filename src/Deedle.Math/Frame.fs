namespace Deedle.Math
open Deedle
open MathNet.Numerics.LinearAlgebra

/// Frame to matrix conversion
///
/// [category:Matrix conversions and operators]
type Frame =
  /// Convert matrix to frame
  ///
  static member ofMatrix (rowKeys:'R seq) (colKeys:'C seq) (m:Matrix<'T>) =
    m.ToArray()
    |> Frame.ofArray2D
    |> Frame.indexColsWith colKeys
    |> Frame.indexRowsWith rowKeys

  /// Convert frame to matrix
  ///
  static member toMatrix (df:Frame<'R, 'C>) =
    df
    |> Frame.toArray2D
    |> DenseMatrix.ofArray2

type Series =
  /// Convert vector to series
  ///
  static member ofVector (keys:'K seq) (v:Vector<float>) =
    (keys |> Array.ofSeq, v.ToArray()) ||> Array.zip |> series

  /// Convert series to vector
  ///
  static member toVector (s:Series<'K, float>) =
    s.Values |> Array.ofSeq |> DenseVector.ofArray
