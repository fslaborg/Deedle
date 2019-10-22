namespace Deedle.Math

open System.Runtime.CompilerServices
open MathNet.Numerics.LinearAlgebra
open Deedle

/// Extension of Frame<'R, 'C>
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type FrameExtensions =
  
  /// frame multiply frame
  /// 
  [<Extension>]
  static member Dot(df1:Frame<'C, 'R>, df2:Frame<'R, 'C>) = 
    Matrix.dot(df1, df2)

  /// frame multiply 
  /// 
  [<Extension>]
  static member Dot(df:Frame<'R, 'C>, s:Series<'C, float>) = 
    Matrix.dot(df, s)

  /// frame multiply matrix
  ///
  [<Extension>]
  static member Dot(df:Frame<'R, 'C>, m:Matrix<float>) = 
    Matrix.dot(df, m)

  /// frame multiply matrix
  ///
  [<Extension>]
  static member Dot(df:Frame<'R, 'C>, v:Vector<float>) = 
    Matrix.dot(df, v)

  /// Convert frame to matrix
  ///
  [<Extension>]
  static member ToMatrix (df:Frame<'R, 'C>) =
    Matrix.ofFrame df

/// Extension of Series<'C, float>
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type SeriesExtensions =
  /// series multiply frame
  /// 
  [<Extension>]
  static member Dot(s:Series<'C, float>, df:Frame<'R, 'C>) = 
    Matrix.dot(s, df)

  /// series multiply series
  ///
  [<Extension>]
  static member Dot(s1:Series<'C, float>, s2:Series<'C, float>) = 
    Matrix.dot(s1, s2)

  /// series multiply matrix
  ///
  [<Extension>]
  static member Dot(s:Series<'K, float>, m:Matrix<float>) = 
    Matrix.dot(s, m)

  /// series multiply matrix
  ///
  [<Extension>]
  static member Dot(s:Series<'K, float>, v:Vector<float>) = 
    Matrix.dot(s, v)

  /// Convert series to vector
  ///
  [<Extension>]
  static member ToVector (s:Series<'K, float>) =    
    s.Values |> Array.ofSeq |> DenseVector.ofArray

/// Extension of Matrix<float>
///
/// [category:Matrix conversions and operators]  
[<Extension>]
type MatrixExtensions =
  /// matrix multiply frame
  /// 
  [<Extension>]
  static member Dot(m:Matrix<float>, df:Frame<'R, 'C>) = 
    Matrix.dot(m, df)

  /// matrix multiply series
  ///
  [<Extension>]
  static member Dot(m:Matrix<float>, s:Series<'C, float>) = 
    Matrix.dot(m, s)

  /// matrix multiply matrix
  ///
  [<Extension>]
  static member Dot(m1:Matrix<float>, m2:Matrix<float>) = 
    m1 * m2

  /// matrix multiply matrix
  ///
  [<Extension>]
  static member Dot(m:Matrix<float>, v:Vector<float>) = 
    m * v

  /// Convert matrix to frame
  ///
  [<Extension>]
  static member ToFrame (m:Matrix<float>, rowKeys:'R seq, colKeys:'C seq) =
    m |> Frame.ofMatrix rowKeys colKeys

/// Extension of Vector<float>
///
/// [category:Vector conversions and operators]  
[<Extension>]
type VectorExtensions =
  /// vector multiply frame
  ///
  [<Extension>]
  static member Dot(v:Vector<float>, df:Frame<'R, 'C>) = 
    Matrix.dot(v, df)

  /// vector multiply series
  ///
  [<Extension>]
  static member Dot(v:Vector<float>, s:Series<'C, float>) = 
    Matrix.dot(v, s)

  /// vector multiply matrix
  ///
  [<Extension>]
  static member Dot(v:Vector<float>, m:Matrix<float>) = 
    v * m

  /// vector multiply vector
  ///
  [<Extension>]
  static member Dot(v:Vector<float>, m:Vector<float>) = 
    v * m

  /// Convert vector to series
  ///
  [<Extension>]
  static member ToSeries (v:Vector<float>, keys:'R seq) =
    v |> Series.ofVector keys
