namespace Deedle.Math

module PCA =
  open Deedle
  open MathNet.Numerics.LinearAlgebra
  open MathNet.Numerics.Statistics

  type t<'a> when 'a : equality =
    {
      EigenVectors : Frame<'a, string>
      EigenValues : Series<string, float>
    }

  /// <summary>
  /// The eigen values of the PCA transformation.
  /// </summary>
  /// <param name="pca">The PCA transformation.</param>
  let eigenValues pca =
    pca.EigenValues

  /// <summary>
  /// The eigen vectors of the PCA transformation.
  /// </summary>
  /// <param name="pca">The PCA transformation.</param>
  let eigenVectors pca =
    pca.EigenVectors

  let private normalizeSeriesUsing (mean : float) (stdDev : float) series =
    let normalizeValue x =
      (x - mean) / stdDev
    series
    |> Series.mapValues normalizeValue

  let private normalizeSeries (series: Series<'a,float>) =
    let mean = Stats.mean series
    let stdDev = Stats.stdDev series
    normalizeSeriesUsing mean stdDev series

  /// <summary>
  /// Normalizes the columns in the dataframe using a z-score.
  /// That is (X - mean) / (std. dev)
  /// </summary>
  /// <param name="df">The dataframe to normalize</param>
  let normalizeColumns (df:Frame<'a,'b>) =
    let normalizeColumn (k:'b) (row:ObjectSeries<'a>) =
      row.As<float>()
      |> normalizeSeries
    df
    |> Frame.mapCols normalizeColumn


  /// <summary>
  /// Computes the principal components from the data frame.
  /// The principal components are listed from PC1 .. PCn
  /// Where PC1 explains most of the variance.
  /// </summary>
  /// <param name="dataFrame">A PCA datatype that contains the eigen values and vectors.</param>
  let pca dataFrame =
    let factorization =
      dataFrame
      |> Stats.covMatrix
      |> Matrix.eigen

    let createPcNameForIndex n =
      sprintf "PC%d" (n + 1)

    let colKeyArray = dataFrame.ColumnKeys |> Seq.toArray

    let eigenValues =
      // eigen values are returned with least significant first.
      factorization.EigenValues
      |> Vector.map (fun x -> x.Real)
      |> Vector.toSeq
      |> Seq.rev
      |> Series.ofValues
      |> Series.mapKeys createPcNameForIndex
    let eigenVectors =
      // as eigen vectors match the eigen values, these also has to be reversed.
      factorization.EigenVectors
      |> Matrix.toColSeq
      |> Seq.rev
      |> Seq.mapi (fun i x -> (createPcNameForIndex i, Vector.toSeq x |> Series.ofValues))
      |> Frame.ofColumns
      |> Frame.mapRowKeys (fun i -> colKeyArray.[i])

    if eigenVectors.RowCount <> dataFrame.ColumnCount then
      failwith "Row count of eigen vectors does not match the input columns"
    {
      EigenValues = eigenValues
      EigenVectors = eigenVectors
    }