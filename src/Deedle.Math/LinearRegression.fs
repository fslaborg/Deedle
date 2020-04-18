namespace Deedle.Math

module rec LinearRegression =
  open Deedle
  open MathNet.Numerics

  [<Literal>]
  let interceptKey = "Intercept"

  module Fit =
    /// <summary>
    /// Represents a linear model fitted to specific values in a data frame.
    /// </summary>
    type t<'a> when 'a : equality =
      {
        InputFrame : Frame<'a,string>
        Coefficients : Series<string, float>
        FitIntercept : bool
        yKey : string
      }

    /// <summary>
    /// Construct a new fit.
    /// </summary>
    /// <param name="input">The data frame that served as input for the regression.</param>
    /// <param name="coeffs">The coefficients found by the linear regression</param>
    /// <param name="yKey">The y column used</param>
    /// <param name="fitIntercept">An option that specifies whether the intercept should be fitted.</param>
    let make input coeffs yKey fitIntercept =
      {
        InputFrame = input
        Coefficients = coeffs
        FitIntercept = fitIntercept
        yKey = yKey
      }

    let private computeXtXinverse (fit : t<'a>) =
      let addInterceptColumn (frame : Frame<'a, string>) =
        match fit.FitIntercept with
        | false -> frame
        | true ->
            let interceptSeries =
              frame.RowKeys
              |> Seq.map (fun rowKey -> (rowKey, 1.0))
              |> Series.ofObservations
            frame
            |> Frame.addCol interceptKey interceptSeries
      let computeXtXInverse (frame : Frame<'a, string>) =
        let columnKeys = frame.ColumnKeys
        let matrix = MathNet.Numerics.LinearAlgebra.Matrix.Build.DenseOfArray(Frame.toArray2D frame)
        (matrix.Transpose() * matrix).Inverse().ToArray()
        |> Frame.ofArray2D
        |> Frame.mapColKeys (fun i -> Seq.item i columnKeys)
        |> Frame.mapRowKeys (fun i -> Seq.item i columnKeys)
      fit.InputFrame
      |> Frame.filterCols (fun columnKey _ -> fit.Coefficients.ContainsKey(columnKey))
      |> addInterceptColumn
      |> computeXtXInverse

    /// <summary>
    /// The dataframe used for fitting the data.
    /// </summary>
    /// <param name="fit">The fit.</param>
    let input fit =
      fit.InputFrame

    /// <summary>
    /// The coefficients found by the linear regression.
    /// </summary>
    /// <param name="fit">The fit.</param>
    let coefficients fit =
      fit.Coefficients

    /// <summary>
    /// Whether or not the fit includes an estimate of the intercept.
    /// </summary>
    /// <param name="fit">The fit.</param>
    let fitIntercept fit =
      fit.FitIntercept

    /// <summary>
    /// Computes the fitted values from the linear model.
    /// </summary>
    /// <param name="fit">The fit.</param>
    let fittedValues fit =
      let fitIntercept = fit.FitIntercept
      let df = fit.InputFrame
      let coefficients = fit.Coefficients
      // TODO: Investigate whether we can simplify the code to pointwise multiply series.
      let fitRow (series : ObjectSeries<string>) =
        match fitIntercept with
        | false ->
            let terms = coefficients |> Series.map (fun k v -> v * series.GetAs<float>(k))
            terms.Sum()
        | true ->
            let terms =
              coefficients
              |> Series.filter (fun k _ -> k <> interceptKey)
              |> Series.map (fun k v -> v * series.GetAs<float>(k))
            terms.Sum() + coefficients.[interceptKey]
      df |> Frame.mapRowValues fitRow

    /// <summary>
    /// Computes the residuals of the regression (y - yHat)
    /// </summary>
    /// <param name="fit">The fit.</param>
    let residuals fit =
      let fitted = fittedValues fit
      fit.InputFrame.[fit.yKey] - fitted

    let yKey fit =
      fit.yKey

    module Summary =
      open System

      type t =
        {
          LinearFormula : string
          ResidualFiveVals : Series<string,float>
          TTable : Frame<string,string>
          RSquared : float
          AdjRSquared : float
        }

        override x.ToString () =
          let newLine = Environment.NewLine
          let formattedFiveVals = Frame.ofRows ["", x.ResidualFiveVals]
          (sprintf "Formula: %s" x.LinearFormula) + newLine +
          (formattedFiveVals.Format()) + newLine +
          (x.TTable.Format()) + newLine +
          (sprintf "R^2: %f, Adj. R^2: %f" x.RSquared x.AdjRSquared) + newLine


    let private computeToleranceFor xKey xKeys frame =
      let otherKeys = xKeys |> Series.keys |> Seq.except (Seq.singleton xKey)
      let fitXKey = ols otherKeys xKey false frame
      let actual = Frame.getCol xKey frame |> Series.values |> Seq.toArray
      let fitted = Fit.fittedValues fitXKey |> Series.values |> Seq.toArray
      1.0 - (GoodnessOfFit.RSquared (fitted, actual))

    let private computeSquaredErrorFor (stdErr : float) xKeys (frame : Frame<'a,string>) xKey =
      let otherKeys = xKeys |> Series.keys |> Seq.except (Seq.singleton xKey)
      let xValues =
        frame.[xKey]
        |> Series.values
      let mean = Statistics.Statistics.Mean xValues
      let ssx = xValues |> Seq.sumBy (fun x -> pown (x - mean) 2)
      if Seq.isEmpty otherKeys then
        let sumOfSquaredXResiduals =
          frame.[xKey] - (Stats.mean frame.[xKey])
          |> Series.mapValues (fun x -> pown x 2)
          |> Stats.sum
        sqrt ((pown stdErr 2) / sumOfSquaredXResiduals)
      else
        let tolerance = computeToleranceFor xKey xKeys frame
        stdErr / sqrt (ssx * tolerance)
    // Pr(>|t|)
    let private twoSidedtTest df tValue  =
      let tValue = abs tValue
      let rightTail = 1.0 - Distributions.StudentT.CDF(0.0, 1.0, df, tValue)
      let leftTail = Distributions.StudentT.CDF(0.0, 1.0, df, -tValue)
      leftTail + rightTail

    let summary (fit:t<'a>) =
      let residuals = fit |> (residuals >> Series.sort >> Series.values >> Seq.toArray)
      let fitted = fit |> (fittedValues >> Series.values >> Seq.toArray)
      let actual = fit.InputFrame.[fit.yKey] |> Series.values |> Seq.toArray
      let fiveVals = Statistics.SortedArrayStatistics.FiveNumberSummary residuals
      let keys = ["Min:"; "1Q:"; "Median:"; "3Q"; "Max:"]
      let namedFiveVals = Seq.zip keys fiveVals |> Series.ofObservations
      let xtxInv = Fit.computeXtXinverse fit
      let rhs = fit.Coefficients.Keys |> fun xs -> System.String.Join (" + ", xs)
      let nObs = fit.InputFrame.[fit.yKey] |> Series.countKeys |> float
      let nCoeffs = fit.Coefficients |> Series.countKeys
      let r2 = GoodnessOfFit.RSquared (fitted, actual)
      let stdErrorResiduals = GoodnessOfFit.StandardError (fitted, actual, nCoeffs)
      let stdErrors =
        fit.Coefficients
        |> Series.map (fun key _ -> stdErrorResiduals * sqrt xtxInv.[key].[key])
      let tValues = fit.Coefficients / stdErrors
      let tTestProbs = tValues |> Series.mapValues (twoSidedtTest (nObs - float nCoeffs))
      let tTable = [("Estimate:", fit.Coefficients); ("Std.Err.", stdErrors); ("t value", tValues); ("Pr(>|t|)", tTestProbs)] |> Frame.ofColumns
      {
        LinearFormula = fit.yKey.ToString() + " ~ " + rhs
        ResidualFiveVals = namedFiveVals
        TTable = tTable
        RSquared = r2
        AdjRSquared = 1.0 - (1.0 - r2) * (nObs - 1.0) / (nObs - float nCoeffs)
      } : Summary.t

  let private dataFrameContainsMissingValues columns df =
    let noOfColumns = Set.count columns
    Frame.countValues df
    |> Series.foldValues (fun a x -> a && x = noOfColumns) true

  let valuesToArray s = (Series.values >> Seq.toArray) s

  let private fitWithOutIntercept xs y names =
    let coefficients = Fit.multiDim false xs y
    Seq.zip names coefficients
    |> Series.ofObservations

  let private fitWithIntercept xs y names interceptKey =
    let coefficients = Fit.multiDim true xs y
    let interceptKeyAsSeq = Seq.singleton interceptKey
    let names' = Seq.append interceptKeyAsSeq names
    Seq.zip names' coefficients
    |> Series.ofObservations

  let private multiFitCleanDataFrame xCols yCol fitIntercept df =
    let xColArray = xCols |> Seq.toArray
    let y = df |> Frame.getCol yCol |> valuesToArray
    let rowToArray (series:ObjectSeries<string>) =
      xColArray
      |> Array.map series.GetAs<float>
    let xs =
      df |> Frame.mapRowValues rowToArray |> Series.values |> Seq.toArray
    if fitIntercept then
      fitWithIntercept xs y xColArray interceptKey
    else
      fitWithOutIntercept xs y xColArray

  let private interceptKeyIsEqualToColumnInDataFrame columns fitIntercept =
    if fitIntercept then
      Set.contains interceptKey columns
    else
      false

  /// <summary>
  /// Performs linear regression on the values in a dataframe.
  /// </summary>
  /// <param name="xCols">The column keys that constitutes the independent variables.</param>
  /// <param name="yCol">The column key that consitutes the dependent variable.</param>
  /// <param name="fitIntercept">An option type that specifies a key to use for the intercept in the result, if set to None, the fit will not produce an intercept.</param>
  /// <param name="df">The dataframe to perform the regression on</param>
  /// <returns>A series with column keys as keys, and regression coefficients as values.</returns>
  let ols (xCols : string seq) (yCol : string) (fitIntercept : bool) (df : Frame<'a, string>) =
    let columns = xCols |> Set.ofSeq |> Set.add yCol
    let df' = Frame.filterCols (fun k _ -> Set.contains k columns) df
    if dataFrameContainsMissingValues columns df then
      failwith "The dataframe you are attempting to do regression on contains missing values in the columns used for the regression. Consider dropping sparse rows."
    if interceptKeyIsEqualToColumnInDataFrame columns fitIntercept then
      failwith "The key specified for the intercept is equal to a column key in the dataframe, consider picking new key for your intercept or rename a column in the dataframe."
    let coefficients = multiFitCleanDataFrame xCols yCol fitIntercept df'
    Fit.make df coefficients yCol fitIntercept
