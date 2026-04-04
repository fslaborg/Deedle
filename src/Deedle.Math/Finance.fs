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

  /// Exponentially weighted moving covariance between two series (mean-corrected).
  /// Uses the same EWMA update rule as <c>ewmCovMatrix</c>: the EWM mean is tracked for
  /// each series and the cross-product of deviations from those means is accumulated.
  /// The two series are aligned on their shared keys; rows where either value is missing
  /// are dropped before computation. Initialised with the full-sample covariance.
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmCrossCov (x:Series<'R, float>, y:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let df =
      Frame.ofColumns(["x", x; "y", y])
      |> Frame.dropSparseRows
    if df.RowCount < 2 then
      Series(df.RowKeys, Array.create df.RowCount nan)
    else
      let keys  = df.RowKeys |> Array.ofSeq
      let xData = df.GetColumn<float>("x").Values |> Array.ofSeq
      let yData = df.GetColumn<float>("y").Values |> Array.ofSeq
      let n     = keys.Length
      let meanX0 = Array.average xData
      let meanY0 = Array.average yData
      let initCov =
        (Array.map2 (fun xi yi -> (xi - meanX0) * (yi - meanY0)) xData yData |> Array.sum)
        / float (n - 1)
      let res = Array.zeroCreate n
      let mutable meanX = xData.[0]
      let mutable meanY = yData.[0]
      let mutable cov   = initCov
      res.[0] <- initCov
      for i in 1..n-1 do
        let prevMeanX = meanX
        let prevMeanY = meanY
        let cx = xData.[i]
        let cy = yData.[i]
        meanX <- (1.0 - alpha) * meanX + alpha * cx
        meanY <- (1.0 - alpha) * meanY + alpha * cy
        cov   <- (1.0 - alpha) * cov + alpha * (cx - prevMeanX) * (cy - prevMeanY)
        res.[i] <- cov
      Series(keys, res)

  /// Exponentially weighted moving cross-volatility between two series.
  /// Defined as the signed square root of <c>ewmCrossCov</c>:
  /// <c>sign(cov) * sqrt(|cov|)</c>, so that squaring recovers the magnitude of the
  /// covariance and the sign indicates the direction of co-movement.
  /// The two series are aligned on their shared keys; rows where either value is
  /// missing are dropped before computation.
  ///
  /// <category>Exponentially Weighted Moving</category>
  static member ewmCrossVol (x:Series<'R, float>, y:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Finance.ewmCrossCov(x, y, alpha = alpha)
    |> Series.mapValues (fun cov -> float (Math.Sign cov) * Math.Sqrt(Math.Abs cov))
