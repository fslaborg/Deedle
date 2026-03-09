namespace Deedle.Math

open System
open Deedle
open MathNet.Numerics.LinearAlgebra

/// Financial analysis
///
/// <category>Financial Analysis</category>
type Finance =

  /// Exponentially weighted moving volatility using standard deviation (mean-corrected).
  /// Tracks the EWM mean and computes volatility as the sqrt of the EWM variance of
  /// deviations from that mean.
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmVolStdDev (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
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

  /// Exponentially weighted moving volatility using standard deviation (mean-corrected)
  /// applied to each column of a frame.
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmVolStdDev (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Finance.ewmVolStdDev(series, alpha = alpha))
    |> Frame.ofColumns

  /// Exponentially weighted moving volatility using root mean square (no mean correction).
  /// Computes vol as sqrt(EWM(x²)), which equals ewmMean for strictly positive sequences.
  /// Appropriate for returns series that are already mean-centred (e.g. zero-mean returns).
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmVolRMS (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let x = x |> Series.dropMissing
    if x.KeyCount < 2 then
      x |> Series.mapValues(fun _ -> nan)
    else
      let data = x.Values |> Array.ofSeq
      let res = Array.zeroCreate x.KeyCount
      let mutable meanSq = data.[0] * data.[0]
      res.[0] <- Math.Abs(data.[0])
      for i in 1..x.KeyCount-1 do
        let curr = data.[i]
        meanSq <- (1.0 - alpha) * meanSq + alpha * curr * curr
        res.[i] <- Math.Sqrt(meanSq)
      Series(x.Keys, res)

  /// Exponentially weighted moving volatility using root mean square (no mean correction)
  /// applied to each column of a frame.
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmVolRMS (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Finance.ewmVolRMS(series, alpha = alpha))
    |> Frame.ofColumns

  /// Exponentially weighted moving volatility on series.
  ///
  /// <category>Exponentially Weighted Moving</category>
  [<Obsolete("ewmVol is deprecated. Use ewmVolRMS for the same root-mean-square behaviour, or ewmVolStdDev for mean-corrected standard deviation.")>]
  static member ewmVol (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    // Return to RiskMetrics: The Evolution of a Standard
    // https://www.msci.com/documents/10199/dbb975aa-5dc2-4441-aa2d-ae34ab5f0945
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let x = x |> Series.dropMissing
    if x.KeyCount < 2 then
      x |> Series.mapValues(fun _ -> nan)
    else
      let init = x |> Stats.stdDev
      let data = x.Values |> Array.ofSeq
      let res = Array.zeroCreate x.KeyCount
      for i in [|0..x.KeyCount-1|] do
        if i = 0 then
          res.[i] <- init
        else
          let prev = res.[i-1]
          let curr = data.[i]
          res.[i] <- Math.Sqrt((1. - alpha) * prev * prev + alpha * curr * curr)
      Series(x.Keys, res)

  /// Exponentially weighted moving volatility on frame.
  ///
  /// <category>Exponentially Weighted Moving</category>
  [<Obsolete("ewmVol is deprecated. Use ewmVolRMS for the same root-mean-square behaviour, or ewmVolStdDev for mean-corrected standard deviation.")>]
  static member ewmVol (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series ->
        // Inline original ewmVol series logic to avoid deprecation warning at call site
        let x = series |> Series.dropMissing
        if x.KeyCount < 2 then
          x |> Series.mapValues(fun _ -> nan)
        else
          let init = x |> Stats.stdDev
          let data = x.Values |> Array.ofSeq
          let res = Array.zeroCreate x.KeyCount
          for i in [|0..x.KeyCount-1|] do
            if i = 0 then
              res.[i] <- init
            else
              let prev = res.[i-1]
              let curr = data.[i]
              res.[i] <- Math.Sqrt((1. - alpha) * prev * prev + alpha * curr * curr)
          Series(x.Keys, res))
    |> Frame.ofColumns

  /// Exponentially weighted moving variance on series
  static member ewmVar (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmVolStdDev(x, alpha = alpha)
    |> Series.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving variance on frame
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmVar (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmVolStdDev(df, alpha = alpha)
    |> Frame.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving covariance matrix
  ///
  /// <category>Exponentially Weighted Moving</category>
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
  /// <category>Exponentially Weighted Moving</category>
  static member ewmCov (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCovMatrix(df, alpha = alpha)
    |> Series.mapValues (Frame.ofMatrix df.ColumnKeys df.ColumnKeys)

  /// Exponentially weighted moving correlation matrix
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmCorrMatrix (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCov(df, alpha = alpha)
    |> Series.mapValues (Stats.cov2Corr >> snd >> Matrix.ofFrame)

  /// Exponentially weighted moving correlation frame
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmCorr (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCov(df, alpha = alpha)
    |> Series.mapValues(fun v -> v |> Stats.cov2Corr |> snd)
