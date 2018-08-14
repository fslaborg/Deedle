namespace Deedle 

open System
open System.Linq
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.InteropServices
open System.Runtime.CompilerServices
open Microsoft.FSharp.Quotations

open Deedle.Keys
open Deedle.Indices
open Deedle.Internal

/// The type implements C# and F# extension methods that add numerical operations 
/// to Deedle series. With a few exceptions, the methods are only available for 
/// series containing floating-point values, that is `Series<'K, float>`.
///
/// [category:Frame and series operations]
[<Extension>]
type SeriesStatsExtensions =

  // ----------------------------------------------------------------------------------------------
  // Statistics
  // ----------------------------------------------------------------------------------------------

  /// Returns the sum of the elements of the series of float values.
  /// [category:Statistics]
  [<Extension>]
  static member inline Sum(series:Series<'K, 'V>) = Stats.sum series

  /// Returns the sum of the elements of the series of numeric values.
  /// [category:Statistics]
  [<Extension>]
  static member inline NumSum(series:Series<'K, 'V>) = Stats.numSum series

  /// Returns the smallest of all elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Min(series:Series<'K, 'V>) = Stats.tryMin series |> Option.get

  /// Returns the greatest of all elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Max(series:Series<'K, 'V>) = Stats.tryMax series |> Option.get

  /// Returns the mean of the elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Mean(series:Series<'K, 'V>) = Stats.mean series

  /// Returns the standard deviation of the elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline StdDev(series:Series<'K, 'V>) = Stats.stdDev series

  /// Returns the skewness of the elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Skewness(series:Series<'K, 'V>) = Stats.skew series

  /// Returns the kurtosis of the elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Kurtosis(series:Series<'K, 'V>) = Stats.kurt series

  [<Extension>]
  [<Obsolete("Use StdDev instead")>]
  static member inline StandardDeviation(series:Series<'K, 'V>) = Stats.stdDev series

  /// Returns the median of the elements of the series.
  /// [category:Statistics]
  [<Extension>]
  static member inline Median(series:Series<'K, 'V>) = Stats.median series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the mean of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the means
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member inline MeanLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
    Series.applyLevel groupSelector.Invoke Stats.mean series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the standard deviation of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the standard deviations
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  static member inline StdDevLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
    Series.applyLevel groupSelector.Invoke Stats.stdDev series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the median of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the medians
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member inline MedianLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
      Series.applyLevel groupSelector.Invoke Stats.median series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the sum of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the sums
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member inline SumLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
    Series.applyLevel groupSelector.Invoke Stats.sum series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the smallest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the smallest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member inline MinLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
    Series.applyLevelOptional groupSelector.Invoke Stats.tryMin series

  /// Groups the elements of the input series in groups based on the keys
  /// produced by `groupSelector` and then returns a new series containing
  /// the greatest element of each group. 
  ///
  /// This operation is designed to be used with [hierarchical indexing](../features.html#indexing).
  ///
  /// ## Parameters
  ///  - `series` - A series of values that are used to calculate the greatest elements
  ///  - `groupSelector` - A delegate that returns a new group key, based on the key in the input series
  ///
  /// [category:Statistics]
  [<Extension>]
  static member inline MaxLevel(series:Series<'K1, 'V>, groupSelector:Func<'K1, 'K2>) = 
    Series.applyLevelOptional groupSelector.Invoke Stats.tryMax series

  /// Linearly interpolates an ordered series given a new sequence of keys. 
  ///
  /// ## Parameters
  ///  - `keys` - Sequence of new keys that forms the index of interpolated results
  ///  - `keyDiff` - A function representing "subtraction" between two keys
  ///
  /// [category:Calculations, aggregation and statistics]
  [<Extension>]
  static member inline InterpolateLinear(series:Series<'K, 'V>, keys:'K seq, keyDiff:Func<'K,'K,float>): Series<'K,float> = 
    series |> Stats.interpolateLinear keys (fun a b -> keyDiff.Invoke(a,b))   

