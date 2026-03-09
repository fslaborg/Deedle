namespace Deedle.Math

open System
open Deedle
open MathNet.Numerics.LinearAlgebra

/// Financial analysis
///
/// [category:Financial Analysis]
type Finance =

  /// Exponentially weighted moving volatility on series
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmVol (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let x = x |> Series.dropMissing
    if x.KeyCount < 2 then
      x |> Series.mapValues(fun _ -> nan)
    else
      let init = x |> Stats.stdDev
      let data = x.Values |> Array.ofSeq
      let res = Array.zeroCreate x.KeyCount
      let mutable mean = data.[0]
      let mutable var = init * init
      res.[0] <- init
      for i in 1..x.KeyCount-1 do
        let prevMean = mean
        let curr = data.[i]
        mean <- (1.0 - alpha) * mean + alpha * curr
        var <- (1.0 - alpha) * var + alpha * (curr - prevMean) * (curr - prevMean)
        res.[i] <- Math.Sqrt(var)
      Series(x.Keys, res)

  /// Exponentially weighted moving volatility on frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmVol (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Finance.ewmVol(series, alpha = alpha))
    |> Frame.ofColumns

  /// Exponentially weighted moving variance on series
  static member ewmVar (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmVol(x, alpha = alpha)
    |> Series.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving variance on frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmVar (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmVol(df, alpha = alpha)
    |> Frame.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving covariance matrix
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmCovMatrix (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let nCol = df.ColumnCount
    let matrix = df |> Frame.toMatrix
    let res = Array.create df.RowCount (DenseMatrix.zero nCol nCol)
    let mutable meanVec = matrix.Row(0)
    for i in [|0..df.RowCount-1|] do
      if i = 0 then
        res.[i] <- Stats.covMatrix df
      else
        let prevMean = meanVec.Clone()
        let row = matrix.Row(i)
        meanVec <- (1.0 - alpha) * meanVec + alpha * row
        let dev = row - prevMean
        let inc = dev.ToColumnMatrix() * dev.ToRowMatrix()
        res.[i] <- (1. - alpha) * res.[i-1] + alpha * inc
    Series(df.RowKeys, res)

  /// Exponentially weighted moving covariance frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmCov (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCovMatrix(df, alpha = alpha)
    |> Series.mapValues (Frame.ofMatrix df.ColumnKeys df.ColumnKeys)

  /// Exponentially weighted moving correlation matrix
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmCorrMatrix (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCov(df, alpha = alpha)
    |> Series.mapValues (Stats.cov2Corr >> snd >> Matrix.ofFrame)

  /// Exponentially weighted moving correlation frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmCorr (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCov(df, alpha = alpha)
    |> Series.mapValues(fun v -> v |> Stats.cov2Corr |> snd)
