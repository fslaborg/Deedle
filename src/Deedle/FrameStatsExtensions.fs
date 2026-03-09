namespace Deedle

open System
open System.Runtime.CompilerServices

/// <summary>
/// The type implements C# and F# extension methods that add numerical operations
/// to Deedle series. With a few exceptions, the methods are only available for
/// series containing floating-point values, that is <c>Series&lt;'K, float&gt;</c>.
/// </summary>
///
/// <category>Frame and series operations</category>
[<Extension>]
type FrameStatsExtensions =

  // ----------------------------------------------------------------------------------------------
  // Statistics
  // ----------------------------------------------------------------------------------------------

  /// <summary>
  /// For each numerical column, returns the sum of the values in the column.
  /// The function skips over missing values and `NaN` values. When there are no
  /// available values, the result is 0.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Sum(df:Frame<'R, 'C>) = Stats.sum df

  /// <summary>
  /// For each numerical column, returns the minimal values as a series.
  /// The function skips over missing and `NaN` values. When there are no values,
  /// the result is `NaN`.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Min(df:Frame<'R, 'C>) = Stats.min df

  /// <summary>
  /// For each numerical column, returns the maximal values as a series.
  /// The function skips over missing and `NaN` values. When there are no values,
  /// the result is `NaN`.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Max(df:Frame<'R, 'C>) = Stats.max df

  /// <summary>
  /// For each numerical column, returns the mean of the values in the column.
  /// The function skips over missing values and `NaN` values. When there are
  /// no available values, the result is NaN.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Mean(df:Frame<'R, 'C>) = Stats.mean df

  /// <summary>
  /// For each numerical column, returns the standard deviation of the values in the column.
  /// The function skips over missing values and `NaN` values. When there are less than 2 values,
  /// the result is NaN.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member StdDev(df:Frame<'R, 'C>) = Stats.stdDev df

  /// <summary>
  /// For each numerical column, returns the variance of the values in the column.
  /// The function skips over missing values and `NaN` values. When there are less
  /// than 2 values, the result is NaN.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Variance(df:Frame<'R, 'C>) = Stats.variance df

  /// <summary>
  /// For each numerical column, returns the skewness of the values in a series.
  /// The function skips over missing values and `NaN` values. When there are less than 3 values,
  /// the result is NaN.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Skewness(df:Frame<'R, 'C>) = Stats.skew df

  /// <summary>
  /// For each numerical column, returns the kurtosis of the values in a series.
  /// The function skips over missing values and `NaN` values. When there are less than 4 values,
  /// the result is NaN.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Kurtosis(df:Frame<'R, 'C>) = Stats.kurt df

  /// <summary>
  /// For each numerical column, returns the median of the values in the column.
  /// </summary>
  ///
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member Median(df:Frame<'R, 'C>) = Stats.median df

  /// <summary>
  /// For each column, returns the number of unique values.
  /// </summary>
  /// <category>Frame Statistics</category>
  [<Extension>]
  static member UniqueCount(df:Frame<'R, 'C>) = Stats.uniqueCount df
