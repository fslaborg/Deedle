namespace Deedle.Math

open MathNet.Numerics.Statistics
open MathNet.Numerics.LinearAlgebra
open Deedle

type CorrelationMethod =
  | Pearson
  | Spearman

type Stats =

  static member corrMatrix (df:Frame<'R, 'C>, ?method:CorrelationMethod): Matrix<float> =
    let method = defaultArg method CorrelationMethod.Pearson
    let arr =
      df
      |> Frame.toArray2D
      |> DenseMatrix.ofArray2
      |> Matrix.transpose
      |> fun x -> x.ToRowArrays()
    match method with
    | CorrelationMethod.Pearson -> Correlation.PearsonMatrix arr
    | CorrelationMethod.Spearman -> Correlation.PearsonMatrix arr

  static member corrFrame (df:Frame<'R, 'C>, ?method:CorrelationMethod): Frame<'C, 'C> =
    let method = defaultArg method CorrelationMethod.Pearson
    Stats.corrMatrix(df, method)
    |> Frame.ofMatrix df.ColumnKeys df.ColumnKeys

  static member covMatrix (df:Frame<'R, 'C>): Matrix<float> =
    let corr = Stats.corrMatrix(df)
    let stdev = df |> Stats.stdDev |> Series.values |> Array.ofSeq
    let stdevDiag = DenseMatrix.ofDiagArray stdev
    stdevDiag.Multiply(corr).Multiply(stdevDiag) 

  static member covFrame (df:Frame<'R, 'C>): Frame<'C, 'C> =
    df
    |> Stats.covMatrix
    |> Frame.ofMatrix df.ColumnKeys df.ColumnKeys

  static member inline quantile (series:Series<'R, 'V>, tau:float): float =
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> fun x -> Statistics.Quantile(x, tau)

  static member inline quantileCustom (series:Series<'R, 'V>, tau:float, definition:QuantileDefinition): float =
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> fun x -> Statistics.QuantileCustom(x, tau, definition)

  static member inline ranks (series:Series<'R, 'V>): Series<'R, float> =
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> fun x ->
      Seq.zip series.Keys (Statistics.Ranks(x))
      |> Series.ofObservations

  static member inline median (series:Series<'R, 'V>): float =
    series.Values
    |> Seq.map float
    |> Array.ofSeq
    |> Statistics.Median

  static member median (df:Frame<'R, 'C>): Series<'C, float> =
    df
    |> Frame.getNumericCols
    |> Series.mapValues Stats.median

  static member quantile (df:Frame<'R, 'C>, tau:float): Series<'C, float> =
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Stats.quantile(series, tau))

  static member quantileCustom (df:Frame<'R, 'C>, tau:float, definition:QuantileDefinition): Series<'C, float> =
    df
    |> Frame.getNumericCols
    |> Series.mapValues(fun series -> Stats.quantileCustom(series, tau, definition))

  static member ranks (df:Frame<'R, 'C>): Frame<'R, 'C> =
    df
    |> Frame.getNumericCols
    |> Series.mapValues Stats.ranks
    |> Frame.ofColumns