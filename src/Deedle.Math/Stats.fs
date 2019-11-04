namespace Deedle.Math

open System
open Deedle
open MathNet.Numerics.LinearAlgebra
open MathNet.Numerics.Statistics

type StatsInternal =
  static member ewDecay(com, span, halfLife, alpha) =
    match com, span, halfLife, alpha with
    | _, _, _, Some a ->
      if a > 0. && a <= 1. then
        a
      else
        invalidArg "parameter" "alpha must be larger than 0 and smaller or equal to 1"
    | Some c, _, _, None ->
      if c >= 0. then
        1. / (1. + c)
      else
        invalidArg "parameter" "center of mass must be larger than or equal to 0"
    | None, Some s, _, None ->
      if s >= 1. then
        2. / (s + 1.)
      else
        invalidArg "parameter" "span must be larger than or equal to 1"
    | None, None, Some hl, None ->
      if hl > 0. then
        1. - Math.Exp(Math.Log(0.5) / hl)
      else
        invalidArg "parameter" "half life must be larger than 0"
    | _ -> invalidArg "parameter" "Unspecificed decay parameters"

/// Correlation method (Pearson or Spearman)
///
/// [category:Statistical Analysis]
type CorrelationMethod =
  /// Pearson correlation
  | Pearson = 0
  /// Spearman correlation
  | Spearman = 1

/// Statistical analysis using MathNet.Numerics
///
/// [category:Statistical Analysis]
type Stats =
  /// Exponentially weighted moving average on series
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmMean (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let data = 
      x
      |> Series.valuesAll
      |> Seq.map (Option.defaultValue nan)
      |> Array.ofSeq
    let res = Array.zeroCreate x.KeyCount
    for i in [|0..x.KeyCount-1|] do
      if i = 0 then
        res.[i] <- data.[0]
      else
        let prev = res.[i-1]
        let curr = data.[i]
        res.[i] <-
          if Double.IsNaN(prev) && Double.IsNaN(curr) then
            nan
          elif Double.IsNaN(prev) then
            curr
          elif Double.IsNaN(curr) then
            prev
          else
            (1. - alpha) * prev + alpha * curr
    Series(x.Keys, res)

  /// Exponentially weighted moving average on frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmMean (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Stats.ewmMean(series, alpha = alpha))
    |> Frame.ofColumns

  /// Exponentially weighted moving standard deviation on series
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmStdDev (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
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
          res.[i] <- (
              (1. - alpha) * res.[i-1] * res.[i-1] + 
              alpha * data.[i] * data.[i] )
              |> Math.Sqrt
      Series(x.Keys, res)

  /// Exponentially weighted moving standard deviation on frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmStdDev (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Stats.ewmStdDev(series, alpha = alpha))
    |> Frame.ofColumns

  /// Exponentially weighted moving variance on series
  static member ewmVariance (x:Series<'R, float>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Stats.ewmStdDev(x, alpha = alpha)
    |> Series.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving variance on frame
  ///
  /// [category: Exponentially Weighted Moving]
  static member ewmVariance (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Stats.ewmStdDev(df, alpha = alpha)
    |> Frame.mapValues(fun (v:float) -> v * v)

  /// Exponentially weighted moving covariance matrix
  /// 
  /// [category: Exponentially Weighted Moving]
  static member ewmCovMatrix (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    let nCol = df.ColumnCount
    let matrix = df |> Frame.toMatrix
    let res = Array.create df.RowCount (DenseMatrix.zero nCol nCol)
    for i in [|0..df.RowCount-1|] do
      if i = 0 then
        res.[i] <- Stats.covMatrix df
      else
        res.[i] <-
          let vector = matrix.Row(i)
          let inc = vector.ToColumnMatrix() * vector.ToRowMatrix()
          (1. - alpha) * res.[i-1] + alpha * inc
    Series(df.RowKeys, res)

  /// Exponentially weighted moving covariance frame 
  /// 
  /// [category: Exponentially Weighted Moving]
  static member ewmCov (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Stats.ewmCovMatrix(df, alpha = alpha)
    |> Series.mapValues (Frame.ofMatrix df.ColumnKeys df.ColumnKeys)
    
  /// Exponentially weighted moving correlation matrix 
  /// 
  /// [category: Exponentially Weighted Moving]
  static member ewmCorrMatrix (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Stats.ewmCov(df, alpha = alpha)
    |> Series.mapValues (Stats.cov2Corr >> snd >> Matrix.ofFrame)

  /// Exponentially weighted moving correlation frame 
  /// 
  /// [category: Exponentially Weighted Moving]
  static member ewmCorr (df:Frame<'R, 'C>, ?com, ?span, ?halfLife, ?alpha) =
    let alpha = StatsInternal.ewDecay(com, span, halfLife, alpha)
    Stats.ewmCov(df, alpha = alpha)
    |> Series.mapValues(fun v -> v |> Stats.cov2Corr |> snd)

  /// Convert covariance matrix to standard deviation series and correlation frame
  ///
  /// [category: Correlation and Covariance]
  static member cov2Corr (covFrame:Frame<'C, 'C>) =
    let cov = Matrix.ofFrame covFrame
    let keys = covFrame.RowKeys |> Array.ofSeq
    let stdDev = cov.Diagonal() |> Vector.map Math.Sqrt
    let dInv =
      stdDev
      |> DenseMatrix.ofDiag
      |> Matrix.inverse
    let stdDevSeries = stdDev.ToSeries(keys)
    let corrFrame = dInv * cov * dInv |> Frame.ofMatrix keys keys
    stdDevSeries, corrFrame    

  /// Convert standard deviation series and correlation frame to covariance frame
  ///
  /// [category: Correlation and Covariance]
  static member corr2Cov(sigmaSeries:Series<'C, float>, corrFrame:Frame<'C, 'C>) =
    let sigma = sigmaSeries.ToVector()
    let corr = corrFrame.ToMatrix()
    let keys = corrFrame.RowKeys
    let sigmaVector = sigma |> DenseMatrix.ofDiag
    sigmaVector * corr * sigmaVector |> Frame.ofMatrix keys keys

  /// Correlation matrix
  ///
  /// [category: Correlation and Covariance]
  static member corrMatrix (df:Frame<'R, 'C>, ?method:CorrelationMethod) =
    let method = defaultArg method CorrelationMethod.Pearson
    let arr =
      df
      |> Frame.toArray2D
      |> DenseMatrix.ofArray2
      |> fun x -> x.ToColumnArrays()
    match method with
    | CorrelationMethod.Pearson -> Correlation.PearsonMatrix arr
    | CorrelationMethod.Spearman -> Correlation.SpearmanMatrix arr
    | _ -> invalidArg "method" "Unknown correlation method"
  
  /// Correlation frame
  ///
  /// [category: Correlation and Covariance]
  static member corr (df:Frame<'R, 'C>, ?method:CorrelationMethod) =
    let method = defaultArg method CorrelationMethod.Pearson
    Stats.corrMatrix(df, method)
    |> Frame.ofMatrix df.ColumnKeys df.ColumnKeys

  static member correl (s1:Series<'K, float>, s2:Series<'K, float>, ?method:CorrelationMethod) =
    let method = defaultArg method CorrelationMethod.Pearson
    let df = [1, s1; 2, s2] |> Frame.ofColumns
    Stats.corr(df, method).GetColumnAt(1).GetAt(0)
    
  /// Covariance matrix
  ///
  /// [category: Correlation and Covariance]
  static member covMatrix (df:Frame<'R, 'C>) =
    // Treat nan as zero, the same as MATLAB
    let corr = Stats.corrMatrix(df) |> Matrix.map(fun x -> if Double.IsNaN x then 0. else x)
    let stdev = df |> Stats.stdDev |> Series.values |> Array.ofSeq
    let stdevDiag = DenseMatrix.ofDiagArray stdev
    stdevDiag * corr * stdevDiag

  /// Covariance frame
  ///
  /// [category: Correlation and Covariance]
  static member cov (df:Frame<'R, 'C>) =
    df
    |> Stats.covMatrix
    |> Frame.ofMatrix df.ColumnKeys df.ColumnKeys

  /// Quantile
  ///
  /// [category: Descriptive Statistics]
  static member inline quantile (series:Series<'R, 'V>, tau:float, ?definition:QuantileDefinition) =
    let definition = defaultArg definition QuantileDefinition.Excel
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> fun x -> Statistics.QuantileCustom(x, tau, definition)
  
  /// Ranks of Series
  ///
  /// [category: Descriptive Statistics]
  static member inline ranks (series:Series<'R, 'V>, ?rankDefinition:RankDefinition) =
    let rankDefinition = defaultArg rankDefinition RankDefinition.Average
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> fun x ->
      (series.Keys, Statistics.Ranks(x, rankDefinition))
      ||> Seq.zip
      |> Series.ofObservations
  
  /// Median of Series
  ///
  /// [category: Descriptive Statistics]
  static member inline median (series:Series<'R, 'V>) =
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> Statistics.Median

  /// Median of Frame
  ///
  /// [category: Descriptive Statistics]
  static member median (df:Frame<'R, 'C>) =
    df
    |> Frame.getNumericCols
    |> Series.mapValues Stats.median

  /// Quantile of Frame
  ///
  /// [category: Descriptive Statistics]
  static member quantile (df:Frame<'R, 'C>, tau:float, ?definition:QuantileDefinition) =
    let definition = defaultArg definition QuantileDefinition.Excel
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Stats.quantile(series, tau, definition))

  /// Ranks of Frame
  ///
  /// [category: Descriptive Statistics]
  static member ranks (df:Frame<'R, 'C>, ?rankDefinition:RankDefinition) =
    let rankDefinition = defaultArg rankDefinition RankDefinition.Average
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun v -> Stats.ranks(v, rankDefinition))
    |> Frame.ofColumns

  /// Moving standard deviation (parallel implementation)
  ///
  /// [category: Moving statistics]
  static member movingStdDevParallel window (df:Frame<'R, 'C>) =
    let rowKeys = df.RowKeys |> Array.ofSeq
    let len = rowKeys |> Array.length
    [|window..len|]
    |> Array.Parallel.map(fun i ->
      rowKeys.[i-1],
      df.Rows.[rowKeys.[i-window..i-1]] |> Stats.stdDev)   
    |> Frame.ofColumns
    |> LinearAlgebra.transpose
  
  /// Moving variance of frame (parallel implementation)
  ///
  /// [category: Moving statistics]
  static member movingVarianceParallel window (df:Frame<'R, 'C>) =
    let rowKeys = df.RowKeys |> Array.ofSeq
    let len = rowKeys |> Array.length
    [|window..len|]
    |> Array.Parallel.map(fun i ->
      rowKeys.[i-1],
      df.Rows.[rowKeys.[i-window..i-1]] |> Stats.variance)   
    |> Frame.ofColumns
    |> LinearAlgebra.transpose

  /// Moving covariance of frame (parallel implementation)
  ///
  /// [category: Moving statistics]
  static member movingCovarianceParallel window (df:Frame<'R, 'C>) =
    let rowKeys = df.RowKeys |> Array.ofSeq
    let len = rowKeys |> Array.length
    [|window..len|]
    |> Array.Parallel.map(fun i ->
      rowKeys.[i-1],
      df.Rows.[rowKeys.[i-window..i-1]]
      |> Stats.covMatrix )
    |> Series.ofObservations    