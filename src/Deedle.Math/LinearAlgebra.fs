namespace Deedle.Math

open MathNet.Numerics.LinearAlgebra
open Deedle

/// Linear algebra on frame using MathNet.Numerics library.
///
/// [category:Linear Algebra]
type LinearAlgebra =
  /// Transpose.
  /// Performance is faster than generic Frame.transpose as it only applies to frame of float values
  ///
  static member transpose (df:Frame<'R, 'C>) =
    if df.IsEmpty then
      df |> Frame.transpose
    else
      df
      |> Frame.toMatrix
      |> Matrix.transpose
      |> Frame.ofMatrix df.ColumnKeys df.RowKeys

  /// Inverse
  ///
  static member inverse (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.inverse

  /// Pseudo-inverse of matrix
  ///
  static member pseudoInverse (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> fun x -> x.PseudoInverse()

  /// Conjugate
  ///
  static member conjugate (df:Frame<'R, 'C>) = 
    df
    |> Frame.toMatrix
    |> Matrix.conjugate

  /// Conjugate tranpose
  ///
  static member conjugateTranspose (df:Frame<'R, 'C>) = 
    df
    |> Frame.toMatrix
    |> Matrix.conjugateTranspose

  /// Norm
  ///
  static member norm (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.norm

  /// Norm of rows
  ///
  static member normRows (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.normRows

  /// Norm of columns
  ///
  static member normCols (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.normCols
  
  /// Matrix rank
  ///
  static member rank (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.rank

  /// Matrix trace
  ///
  static member trace (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.trace

  /// Matrix determinant
  ///
  static member determinant (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.determinant

  /// Matrix condition
  ///
  static member condition (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.condition

  /// Matrix nullity
  ///
  static member nullity (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.nullity

  /// Matrix kernel
  ///
  static member kernel (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.kernel

  /// Matrix range
  ///
  static member range (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.range

  /// Check whether it is symmetric matrix
  ///
  static member isSymmetric (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.symmetric

  /// Check whether it is Hermitian matrix
  ///
  static member isHermitian (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.hermitian

  /// Cholesky decomposition
  ///
  static member cholesky (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.cholesky

  /// LU decomposition
  ///
  static member lu (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.lu
  
  /// QR decomposition
  ///
  static member qr (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.qr

  /// SVD decomposition
  ///
  static member svd (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.svd

  /// Eigen values and eigen vectors of matrix
  ///
  static member eigen (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.eigen

/// Matrix conversions and operators between Frame and Series
///
/// [category:Matrix conversions and operators]
type Matrix =
  
  /// Convert frame to matrix
  ///
  /// [category: Conversions]
  static member ofFrame df =
    df |> Frame.toMatrix
  
  /// Convert matrix to frame
  ///
  /// [category: Conversions]
  static member toFrame (rows: 'R seq) (cols: 'C seq) (m:Matrix<float>) =
    m |> Frame.ofMatrix rows cols

  /// frame multiply matrix
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, m2:Matrix<float>) =
    let m1 = df |> Frame.toMatrix
    m1 * m2 |> Frame.ofMatrix df.RowKeys df.RowKeys
  
  /// matrix multiply frame
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Matrix<float>, df:Frame<'R, 'C>) =
    let m2 = df |> Frame.toMatrix
    m1 * m2

  /// vector multiply frame
  ///
  /// [category: Matrix multiplication]
  static member dot (v1:Vector<float>, df:Frame<'R, 'C>) =
    let m2 = df |> Frame.toMatrix
    v1 * m2

  /// frame multiply vector
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, v2:Vector<float>) =
    let m1 = df |> Frame.toMatrix
    m1 * v2 |> Series.ofVector df.RowKeys
  
  /// series multiply matrix
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'K, float>, m2:Matrix<float>) =
    (Series.toVector s) * m2
  
  /// matrix multiply series
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Matrix<float>, s:Series<'K, float>) =
    m1 * (Series.toVector s)

  /// vector multiply series
  ///
  /// [category: Matrix multiplication]
  static member dot (v1:Vector<float>, s:Series<'R, float>) =
    v1 * (Series.toVector s)

  /// series multiply vector
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'R, float>, v2:Vector<float>) =    
    (Series.toVector s) * v2

  /// frame multiply frame
  ///
  /// [category: Matrix multiplication]
  static member dot (df1:Frame<'R, 'C>, df2:Frame<'C, 'R>) =
    let set1 = df1.ColumnKeys |> Set.ofSeq
    let set2 = df2.RowKeys |> Set.ofSeq
    let common = Set.union set1 set2
    if common.Count > set1.Count || common.Count > set2.Count then
      invalidOp "Matrices are not aligned"
    let left = df1.Columns.[common]
    let right = df2.Rows.[common]
    let m1 = left |> Frame.toMatrix
    let m2 = right |> Frame.toMatrix
    m1 * m2 |> Frame.ofMatrix left.RowKeys df2.ColumnKeys
  
  /// frame multiply series
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, s:Series<'C, float>) =
    let set1 = df.ColumnKeys |> Set.ofSeq
    let set2 = s.Keys |> Set.ofSeq
    let common = Set.union set1 set2
    if common.Count > set1.Count || common.Count > set2.Count then
      invalidOp "Matrices are not aligned"
    let left = df.Columns.[common]
    let right = s.[common]
    let m1 = left |> Frame.toMatrix
    let v2 = right |> Series.toVector
    m1 * v2 |> Series.ofVector common
  
  /// series multiply frame
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'C, float>, df:Frame<'R, 'C>) =
    let set1 = s.Keys |> Set.ofSeq
    let set2 = df.ColumnKeys |> Set.ofSeq
    let common = Set.union set1 set2
    if common.Count > set1.Count || common.Count > set2.Count then
      invalidOp "Matrices are not aligned"
    let right = df.Columns.[common]
    let left = s.[common]
    let v1 = left |> Series.toVector
    let m2 = right |> Frame.toMatrix
    v1 * m2 |> Series.ofVector common

  /// series multiply series
  ///
  /// [category: Matrix multiplication]
  static member dot (s1:Series<'C, float>, s2:Series<'C, float>) =
    let v1 = Series.toVector s1
    let v2 = Series.toVector s2
    v1 * v2