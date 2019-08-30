namespace Deedle.Math

open System.Runtime.CompilerServices
open MathNet.Numerics.LinearAlgebra
open Deedle

/// Matrix multiplications between frame and series/matrix
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type FrameExtensions =
  
  // Matrix multiplication of frame and frame
  // 
  [<Extension>]
  static member Dot(df:Frame<'C, 'R>, self:Frame<'R, 'C>) = 
    Matrix.dot(self, df)
  
  // Matrix multiplication of frame and series
  // Returns a matrix
  [<Extension>]
  static member Dot(s:Series<'C, float>, self:Frame<'R, 'C>) = 
    Matrix.dot(self, s)

  // Matrix multiplication of frame and matrix
  // 
  [<Extension>]
  static member Dot(m:Matrix<float>, self:Frame<'R, 'C>) = 
    Matrix.dot(self, m)

/// Matrix multiplications between series and frame/matrix
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type SeriesExtensions =
  // Matrix multiplication of series and frame
  // 
  [<Extension>]
  static member Dot(df:Frame<'R, 'C>, self:Series<'C, float>) = 
    Matrix.dot(self, df)

  // Matrix multiplication of series and matrix
  //
  [<Extension>]
  static member Dot(m:Matrix<float>, self:Series<'C, float>) = 
    Matrix.dot(self, m)

  // Matrix multiplication of series and series
  //
  [<Extension>]
  static member Dot(m:Series<'C, float>, self:Series<'C, float>) = 
    Matrix.dot(self, m)

/// Dot oprators between Matrix<float> and Frame/Series
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type MatrixExtensions =
  // Matrix multiplication of matrix and frame
  //
  [<Extension>]
  static member Dot(df:Frame<'R, 'C>, self:Matrix<float>) = 
    Matrix.dot(self, df)

  // Matrix multiplication of matrix and matrix
  //
  [<Extension>]
  static member Dot(m:Matrix<float>, self:Matrix<float>) = 
    self * m

  // Matrix multiplication of matrix and vector
  //
  [<Extension>]
  static member Dot(v:Vector<float>, self:Matrix<float>) = 
    self * v

  // Matrix multiplication of matrix and series
  //
  [<Extension>]
  static member Dot(s:Series<'K, float>, self:Matrix<float>) = 
    Matrix.dot(self, s)