namespace Deedle.Math

open MathNet.Numerics.LinearAlgebra
open Deedle

/// Linear algebra on frame using MathNet.Numerics library.
///
/// [category:Linear Algebra]
type LinearAlgebra =
  /// Convert frame into matrix and return transpose of matrix.
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

  /// Convert frame into matrix and return inverse of matrix
  ///
  static member inverse (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.inverse

  /// Convert frame into matrix and return pseudo-inverse of matrix
  ///
  static member pseudoInverse (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> fun x -> x.PseudoInverse()

  /// Convert frame into matrix and return conjugate of matrix
  ///
  static member conjugate (df:Frame<'R, 'C>) = 
    df
    |> Frame.toMatrix
    |> Matrix.conjugate

  /// Convert frame into matrix and return conjugate tranpose of matrix
  ///
  static member conjugateTranspose (df:Frame<'R, 'C>) = 
    df
    |> Frame.toMatrix
    |> Matrix.conjugateTranspose

  /// Convert frame into matrix and return norm of matrix
  ///
  static member norm (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.norm

  /// Convert frame into matrix and return norm of rows
  ///
  static member normRows (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.normRows

  /// Convert frame into matrix and return norm of columns
  ///
  static member normCols (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.normCols
  
  /// Convert frame into matrix and return rank of matrix
  ///
  static member rank (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.rank

  /// Convert frame into matrix and return trace of matrix
  ///
  static member trace (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.trace

  /// Convert frame into matrix and return determinant of matrix
  ///
  static member determinant (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.determinant

  /// Convert frame into matrix and return condition of matrix
  ///
  static member condition (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.condition

  /// Convert frame into matrix and return nullity of matrix
  ///
  static member nullity (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.nullity

  /// Convert frame into matrix and return kernel of matrix
  ///
  static member kernel (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.kernel

  /// Convert frame into matrix and return range of matrix
  ///
  static member range (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.range

  /// Convert frame into matrix and check whether it's symmetric matrix
  ///
  static member isSymmetric (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.symmetric

  /// Convert frame into matrix and check whether it's Hermitian matrix
  ///
  static member isHermitian (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.hermitian

  /// Convert frame into matrix and return cholesky decomposition of matrix
  ///
  static member cholesky (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.cholesky

  /// Convert frame into matrix and return LU decomposition of matrix
  ///
  static member lu (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.lu
  
  /// Convert frame into matrix and return QR decomposition of matrix
  ///
  static member qr (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.qr

  /// Convert frame into matrix and return SVD decomposition of matrix
  ///
  static member svd (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.svd

  /// Convert frame into matrix and return eigen values and eigen vectors of matrix
  ///
  static member eigen (df:Frame<'R, 'C>) =
    df
    |> Frame.toMatrix
    |> Matrix.eigen

type SeriesMatrixForm =
  | ByRow = 0
  | ByColumn = 1

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

  /// Convert series to matrix with correct dimension for multiplication
  ///
  /// [category: Conversions]
  static member ofSeries (s:Series<'K, float>, t:SeriesMatrixForm) =
    let x = s.Values |> Array.ofSeq
    match t with
    | SeriesMatrixForm.ByColumn -> x |> DenseMatrix.raw 1 x.Length
    | SeriesMatrixForm.ByRow -> x |> DenseMatrix.raw x.Length 1
    | _ -> invalidOp "Invalid matrix form option"

  /// Convert series to match matrix dimension for s multiply m operation
  ///
  /// [category: Conversions]
  static member ofSeries (s:Series<'K, float>, m:Matrix<float>) =
    if s.KeyCount = m.ColumnCount then
      Matrix.ofSeries(s, SeriesMatrixForm.ByColumn)
    elif s.KeyCount = m.RowCount then
      Matrix.ofSeries(s, SeriesMatrixForm.ByRow)
    else
      invalidOp "Mismatched Dimensions"

  /// Convert series to match matrix dimension for m multiply s operation
  ///
  /// [category: Conversions]
  static member ofSeries (m:Matrix<float>, s:Series<'K, float>) =
    if s.KeyCount = m.ColumnCount then
      Matrix.ofSeries(s, SeriesMatrixForm.ByRow)
    elif s.KeyCount = m.RowCount then
      Matrix.ofSeries(s, SeriesMatrixForm.ByColumn)
    else
      invalidOp "Mismatched Dimensions"

  /// Matrix multiplication of frame and matrix
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, m2:Matrix<float>) =
    let m1 = df |> Frame.toMatrix
    m1 * m2
  
  /// Matrix multiplication of matrix and frame
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Matrix<float>, df:Frame<'R, 'C>) =
    let m2 = df |> Frame.toMatrix
    m1 * m2

  /// Matrix multiplication of vector and frame
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Vector<float>, df:Frame<'R, 'C>) =
    let m2 = df |> Frame.toMatrix
    m1 * m2

  /// Matrix multiplication of frame and vector
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, m2:Vector<float>) =
    let m1 = df |> Frame.toMatrix
    m1 * m2
  
  /// Matrix multiplication of series and matrix
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'K, float>, m2:Matrix<float>) =
    let m1 = Matrix.ofSeries(s, m2)
    m1 * m2
  
  /// Matrix multiplication of matrix and series
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Matrix<float>, s:Series<'K, float>) =
    let m2 = Matrix.ofSeries(m1, s)
    m1 * m2

  /// Matrix multiplication of vector and series
  ///
  /// [category: Matrix multiplication]
  static member dot (m1:Vector<float>, s:Series<'R, float>) =
    let m2 = Matrix.ofSeries(s, SeriesMatrixForm.ByColumn)
    m1 * m2

  /// Matrix multiplication of series and vector
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'R, float>, m2:Vector<float>) =
    let m1 = Matrix.ofSeries(s, SeriesMatrixForm.ByColumn)
    m1 * m2

  /// Matrix multiplication of frame and frame
  ///
  /// [category: Matrix multiplication]
  static member dot (df1:Frame<'R, 'C>, df2:Frame<'C, 'R>) =
    let m1 = df1 |> Frame.toMatrix
    // Align keys in the same order
    let m2 = df2.Rows.[df1.ColumnKeys] |> Frame.toMatrix
    m1 * m2
  
  /// Matrix multiplication of frame and series
  ///
  /// [category: Matrix multiplication]
  static member dot (df:Frame<'R, 'C>, s:Series<'C, float>) =
    let m1 = df |> Frame.toMatrix
    let m2 = Matrix.ofSeries(m1, s)
    m1 * m2
  
  /// Matrix multiplication of series and frame
  ///
  /// [category: Matrix multiplication]
  static member dot (s:Series<'C, float>, df:Frame<'R, 'C>) =
    let m2 = df |> Frame.toMatrix
    let m1 = Matrix.ofSeries(s, m2)
    m1 * m2

  /// Matrix multiplication of series and series
  ///
  /// [category: Matrix multiplication]
  static member dot (s1:Series<'C, float>, s2:Series<'C, float>) =
    let m1 = s1.Values |> Array.ofSeq |> DenseMatrix.raw 1 s1.KeyCount
    let m2 = s2.Values |> Array.ofSeq |> DenseMatrix.raw s2.KeyCount 1
    m1 * m2