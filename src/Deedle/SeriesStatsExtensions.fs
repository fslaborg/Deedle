namespace Deedle

open System
open System.Runtime.CompilerServices

/// The type implements C# and F# extension methods that add numerical operations
/// to Deedle series.
///
/// <category>Frame and series operations</category>
[<Extension>]
type SeriesStatsExtensions =

  // ----------------------------------------------------------------------------------------------
  // Statistics
  // ----------------------------------------------------------------------------------------------

  /// Returns the sum of the elements of the series of float values.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Sum(series:Series<'K, 'V>) = Stats.sum series

  /// Returns the sum of the elements of the series of numeric values.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline NumSum(series:Series<'K, 'V>) = Stats.numSum series

  /// Returns the minimum of the values in a series. The result is an float value.
  /// When the series contains no values, the result is NaN.
  /// Throws a `FormatException` or an `InvalidCastException` if the value type of the series
  /// is not convertible to floating point number.
  ///
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Min(series: Series<'K, 'V>) = Stats.min series

  /// Returns the maximum of the values in a series. The result is an float value.
  /// When the series contains no values, the result is NaN.
  /// Throws a `FormatException` or an `InvalidCastException` if the value type of the series
  /// is not convertible to floating point number.
  ///
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Max(series: Series<'K, 'V>) = Stats.max series

  /// Returns the minimum of the values in a series. The result is an option value.
  /// When the series contains no values, the result is `None`.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline TryMin(series: Series<'K, 'V>) = Stats.tryMin series

  /// Returns the maximum of the values in a series. The result is an option value.
  /// When the series contains no values, the result is `None`.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline TryMax(series: Series<'K, 'V>) = Stats.tryMax series

  /// Returns the mean of the elements of the series.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Mean(series:Series<'K, 'V>) = Stats.mean series

  /// Returns the standard deviation of the elements of the series.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline StdDev(series:Series<'K, 'V>) = Stats.stdDev series

  /// Returns the skewness of the elements of the series.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Skewness(series:Series<'K, 'V>) = Stats.skew series

  /// Returns the kurtosis of the elements of the series.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Kurtosis(series:Series<'K, 'V>) = Stats.kurt series

  [<Extension>]
  [<Obsolete("Use StdDev instead")>]
  static member inline StandardDeviation(series:Series<'K, 'V>) = Stats.stdDev series

  /// Returns the median of the elements of the series.
  /// <category>Statistics</category>
  [<Extension>]
  static member inline Median(series:Series<'K, 'V>) = Stats.median series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the mean of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the means</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  [<Extension>]
  static member inline MeanLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
    Series.applyLevel groupSelector.Invoke Stats.mean series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the standard deviation of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the standard deviations</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  static member inline StdDevLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
    Series.applyLevel groupSelector.Invoke Stats.stdDev series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the median of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the medians</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  [<Extension>]
  static member inline MedianLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
      Series.applyLevel groupSelector.Invoke Stats.median series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the sums</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  [<Extension>]
  static member inline SumLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
    Series.applyLevel groupSelector.Invoke Stats.sum series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the smallest elements</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  [<Extension>]
  static member inline MinLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
    Series.applyLevelOptional groupSelector.Invoke Stats.tryMin series

  /// <summary>
  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group.
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  /// </summary>
  /// <param name="series">A series of values that are used to calculate the greatest elements</param>
  /// <param name="groupSelector">A delegate that returns a new group key, based on the key in the input series</param>
  /// <category>Statistics</category>
  [<Extension>]
  static member inline MaxLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) =
    Series.applyLevelOptional groupSelector.Invoke Stats.tryMax series

  /// <summary>
  /// Linearly interpolates an ordered series given a new sequence of keys.
  /// </summary>
  /// <param name="keys">Sequence of new keys that forms the index of interpolated results</param>
  /// <param name="keyDiff">A function representing "subtraction" between two keys</param>
  /// <category>Calculations, aggregation and statistics</category>
  [<Extension>]
  static member inline InterpolateLinear(series:Series<'K, 'V>, keys:'K seq, keyDiff:Func<'K,'K,float>): Series<'K,float> =
    series |> Stats.interpolateLinear keys (fun a b -> keyDiff.Invoke(a,b))

