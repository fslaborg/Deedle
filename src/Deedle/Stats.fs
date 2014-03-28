namespace Deedle

open System
open System.Collections.Generic

open Deedle.Vectors
open MathNet.Numerics.Statistics

/// The `Stats` module contains functions for fast calculation of statistics over
/// and entire series as well as over a moving and an expanding window in a series. 
/// The three kinds of functions in this module are:
///
///  * **Standard** - functions such as `count`, `mean`, `kurt` etc. return the
///    statistics calculated over all values of a series. The calculation skips
///    over missing values (or `nan` values), so for example `mean` returns the
///    average of all _present_ values.
///
///  * **Moving window** means that the window has a fixed size and moves over the series.
///    In this case, the result of the statisitcs is always attached to the last key
///    of the window. The function names are prefixed with `moving`.
///
///  * **Expanding window** means that the window starts as a zero-element sized window
///    and expands as it moves over the series. In this case, statistics is calculated
///    for all values preceding the current key. This means that the result is attached
///    to the key _after_ the end of the window (prefix). The function names are prefixed
///    with `expanding`.
///
/// The resulting series has the same keys as the input series. When the window contains
/// no values, or contains missing values, different functions behave in different ways.
/// Statistics (e.g. mean) return missing value when any value is missing, while min/max
/// functions return the minimal/maximal element (skipping over missing values).
///
/// ## Remarks
///
/// The windowing functions in the `Stats` module support calculations over a fixed-size
/// windows specified by the size of the window. If you need more complex windowing 
/// behavior (such as window based on the distance between keys), different handling
/// of boundary, or chunking (calculation over adjacent chunks), you can use chunking and
/// windowing functions from the `Series` module such as `Series.windowSizeInto` or
/// `Series.chunkSizeInto`.
///
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Stats =

  // TODO: still to do, possibly: median, percentile, corr, cov


  // ------------------------------------------------------------------------------------
  // Implementation internals - moving window functionality
  // ------------------------------------------------------------------------------------  

  /// Apply transformation on series elements. The projection function `proj` always
  /// returns `float`, but may return `nan` to indicate that the value is not available.
  /// The resulting sequence should have the same number of values as the input sequence
  let inline internal applySeriesProj (proj: float opt seq -> float seq) (series:Series<'K, float>) : Series<'K, float> =
    let newData = 
      series.Vector.DataSequence 
      |> proj 
      |> Vector.ofValues
      
    Series(series.Index, newData, series.VectorBuilder, series.IndexBuilder)

  /// Helper for moving window calculations (adopted from `Seq.windowed` in F# code base)
  /// When calling `finit`, we do not copy the array - this is fine, because the function
  /// is internal (and the only use in `applyMovingSumsTransform` is correct)
  ///
  /// # Parameters
  ///   - `winSize` - The size of the window to create
  ///   - `finit` takes the first fully populated window array to an initial state
  ///   - `fupdate` takes the current state, incoming observation, 
  ///      and out-going observation to the next state
  ///   - `ftransf` takes the current state to the current output
  let internal movingWindowFn winSize finit fupdate ftransf (source: seq<_>) =
    seq {
       let arr = Array.zeroCreate winSize 
       let r = ref (winSize - 1)
       let i = ref 0 
       let isInit = ref false
       let state = ref Unchecked.defaultof<_>
       use e = source.GetEnumerator() 
       while e.MoveNext() do 
         let curr = e.Current
         let outg = arr.[!i]
         arr.[!i] <- curr
         i := (!i + 1) % winSize
         if !r = 0 then 
           if not !isInit then
             state := arr |> finit
             isInit := true
           else 
             state := fupdate !state curr outg
           yield !state |> ftransf
         else 
           r := (!r - 1)
           yield nan }

  /// When calculating moments, this record is used to keep track of the 
  /// count (`nobs`), sum of values (`sum`), sum of squares (`sum2`),
  /// sum of values to the power of 3 and 3 (`sum3` and `sum4`)
  type internal Sums = { nobs: float; sum: float; sump2: float; sump3: float; sump4: float }

  /// Given an initial array of values, calculate the initial `Sums` value
  /// (only required elements of `Sums` are calculated based on `moment`)
  let internal initSumsDense moment (init: float []) = 
    let count = init |> Array.length |> float
    let sum   = if moment < 1 then 0.0 else init |> Array.sum
    let sump2 = if moment < 2 then 0.0 else init |> Array.sumBy (fun x -> pown x 2)
    let sump3 = if moment < 3 then 0.0 else init |> Array.sumBy (fun x -> pown x 3)
    let sump4 = if moment < 4 then 0.0 else init |> Array.sumBy (fun x -> pown x 4)
    { nobs = count; sum = sum; sump2 = sump2; sump3 = sump3; sump4 = sump4 }
  
  /// Given an existing `state` of type `Sums`, new incoming element and
  /// an old outgoing element, update the sums value
  /// (only required elements of `Sums` are calculated based on `moment`)
  let internal updateSumsDense moment state curr outg =
    let sum   = if moment < 1 then 0.0 else state.sum + curr - outg
    let sump2 = if moment < 2 then 0.0 else state.sump2 + (pown curr 2) - (pown outg 2)
    let sump3 = if moment < 3 then 0.0 else state.sump3 + (pown curr 3) - (pown outg 3)
    let sump4 = if moment < 4 then 0.0 else state.sump4 + (pown curr 4) - (pown outg 4)
    { state with sum = sum; sump2 = sump2; sump3 = sump3; sump4 = sump4 }

  /// Pick only available values from the input array and call `initSumsDense`
  /// (no need to handle `nan` values, because those are returned as Missing by Deedle)
  let internal initSumsSparse moment (init: float opt []) =
    init 
    |> Array.choose OptionalValue.asOption
    |> initSumsDense moment

  /// Update `Sums` value using `updateSumsDense`, but handle the case
  /// when removing/adding value that is missing (`OptionalValue.Missing`)
  let internal updateSumsSparse moment state curr outg = 
    match curr, outg with
    | OptionalValue.Present x, OptionalValue.Present y -> updateSumsDense moment state x y
    | OptionalValue.Present x, OptionalValue.Missing   -> { updateSumsDense moment state x 0.0 with nobs = state.nobs + 1.0 }
    | OptionalValue.Missing,   OptionalValue.Present y -> { updateSumsDense moment state 0.0 y with nobs = state.nobs - 1.0 }
    | OptionalValue.Missing,   OptionalValue.Missing   -> state

  /// Apply moving window transformation based on `Sums` calculation. The `proj` function
  /// calculates the statistics from `Sums` value and the `moment` specifies which of the
  /// `Sums` properties are calculated during the processing.
  let internal applyMovingSumsTransform moment winSize (proj: Sums -> float) series =
    if winSize <= 0 then invalidArg "windowSize" "Window must be positive"
    let calcSparse = movingWindowFn winSize (initSumsSparse moment) (updateSumsSparse moment) proj
    applySeriesProj calcSparse series

  /// Calculate variance from `Sums`; requires `moment=2`
  let internal varianceSums s =
    let v = (s.nobs * s.sump2 - s.sum * s.sum) / (s.nobs * s.nobs - s.nobs) 
    if v < 0.0 then nan else v

  /// Calculate skewness from `Sums`; requires `moment=3`
  let internal skewSums s =
    let a = s.sum / s.nobs
    let b = s.sump2 / s.nobs - a * a
    let c = s.sump3 / s.nobs - a * a * a - 3.0 * a * b
    let r = b |> sqrt
    if b = 0.0 || s.nobs < 3.0 then nan 
    else (sqrt (s.nobs * (s.nobs - 1.0)) * c) / ((s.nobs - 2.0) * pown r 3)
  
  /// Calculate kurtsis from `Sums`; requires `moment=4`
  let internal kurtSums s =
    let a = s.sum / s.nobs
    let r = a * a
    let b = s.sump2 / s.nobs - r
    let r = r * a
    let c = s.sump3 / s.nobs - r - 3.0 * a * b
    let r = r * a
    let d = s.sump4 / s.nobs - r - 6.0 * b * a * a - 4.0 * c * a
    if b = 0.0 || s.nobs < 4.0 then nan 
    else 
      let k = (s.nobs * s.nobs - 1.0) * d / (b * b) - 3.0 * (pown (s.nobs - 1.0) 2)
      k / ((s.nobs - 2.0) * (s.nobs - 3.0))

  // ------------------------------------------------------------------------------------
  // Implementation internals - moving minimum and maximum
  // ------------------------------------------------------------------------------------
      
  /// O(n) moving min/max calculator
  ///
  /// Keeps double-ended queue of values sorted acording to the specified order,
  /// such that the front is the min/max value. During the iteration, new value is
  /// added to the end (and all values that are greater/smaller than the new value
  /// are removed before it is appended).
  let internal movingMinMaxHelper winSize cmp s = 
    seq {
      let i = ref 0
      let q = Deque()
      for v in s ->
        // invariant: all values in deque are strictly ascending (min) or descending (max)
        // invariant: all values in deque are in the current window (fst q.[i] > !i)
        i := !i + 1        
        match v with
        | OptionalValue.Present x -> 
          // remove from front any values that fell out of moving window
          while q.Count > 0 && !i >= fst q.[0] do q.RemoveFront() |> ignore
          // remove from back any values >= (min) or <= (max) compared to current obs
          while q.Count > 0 && (cmp (snd q.[q.Count - 1]) x) do q.RemoveBack() |> ignore
          // append new obs to back
          q.AddBack( (!i + winSize, x) )
          // return min/max value at front
          snd q.[0]
        | OptionalValue.Missing -> 
          if q.IsEmpty then nan else snd q.[0] }

  // ------------------------------------------------------------------------------------
  // Public - moving window functions
  // ------------------------------------------------------------------------------------

  /// Returns a series that contains counts over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is 0.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingCount")>]
  let movingCount size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 0 size (fun s -> s.nobs) series

  /// Returns a series that contains sums over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is 0.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingSum")>]
  let movingSum size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 1 size (fun s -> s.sum) series

  /// Returns a series that contains means over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is also missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingMean")>]
  let movingMean size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 1 size (fun s -> s.sum / s.nobs) series

  /// Returns a series that contains variance over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is also missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingVariance")>]
  let movingVariance size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 2 size varianceSums series

  /// Returns a series that contains standard deviations over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is also missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingStandardDeviation")>]
  let movingStdDev size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 2 size (varianceSums >> sqrt) series

  /// Returns a series that contains skewness over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is also missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingSkewness")>]
  let movingSkew size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 3 size skewSums series

  /// Returns a series that contains kurtosis over a moving window of the specified size.
  /// The first `size-1` elements of the returned series are always missing; if the 
  /// entire window contains missing values, the result is also missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingKurtosis")>]
  let movingKurt size (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 4 size kurtSums series 

  /// Returns a series that contains minimum over a moving window of the specified size.
  /// The first `size-1` elements are calculated using smaller windows spanning over `1 .. size-1` 
  /// values. If the entire window contains missing values, the result is missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingMinimum")>]
  let movingMin size (series:Series<'K, float>) : Series<'K, float> =
    applySeriesProj (movingMinMaxHelper size (>=)) series

  /// Returns a series that contains maximum over a moving window of the specified size.
  /// The first `size-1` elements are calculated using smaller windows spanning over `1 .. size-1` 
  /// values. If the entire window contains missing values, the result is missing.
  ///
  /// [category:Moving windows]
  [<CompiledName("MovingMaximum")>]
  let movingMax size (series:Series<'K, float>) : Series<'K, float> =
    applySeriesProj (movingMinMaxHelper size (<=)) series

  // ------------------------------------------------------------------------------------
  // Implementation internals - expanding windows
  // ------------------------------------------------------------------------------------

  /// Helper for expanding window calculations
  ///
  /// ## Parameters
  ///  - `initState` is the initial state of the computation
  //   - `fupdate` takes the current state and incoming observation to the next state
  //   - `ftransf` takes the current state to the current output
  let inline internal expandingWindowFn initState fupdate ftransf (source: seq<_>) =
    source 
    |> Seq.scan fupdate initState
    |> Seq.skip 1
    |> Seq.map ftransf 

  /// Represents the moments as calculated during online processing
  /// (`nobs` is the count, `sum` is the sum, `M1` to `M4` are moments)
  type internal Moments = { 
    nobs : float 
    sum  : float 
    M1   : float
    M2   : float 
    M3   : float
    M4   : float 
  }

  /// Updates the moments using the Knuth/Welford algorithm for online stats updating
  /// (See: http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm)
  let internal updateMoments state x =
    let { nobs = nobs; sum = sum; M1 = M1; M2 = M2; M3 = M3; M4 = M4 } = state
    let n1 = nobs
    let n = nobs + 1.0
    let delta = x - M1
    let delta_n = delta / n
    let delta_n2 = delta_n * delta_n
    let term1 = delta * delta_n * n1
    let M1 = M1 + delta_n
    let M4 = M4 + term1 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * M2 - 4.0 * delta_n * M3
    let M3 = M3 + term1 * delta_n * (n - 2.0) - 3.0 * delta_n * M2
    let M2 = M2 + term1
    let s = sum + x 
    { nobs = n; sum = s; M1 = M1; M2 = M2; M3 = M3; M4 = M4 }   

  /// Updates the moments using `updateMoments`, but skips over missing values
  let internal updateMomentsSparse state curr =     
    match curr with
    | OptionalValue.Present x -> updateMoments state x
    | OptionalValue.Missing   -> state

  /// Given a series, calculates expanding moments (using online `updateMoments`)
  /// The specified `proj` function is used to calculate the resulting value
  let internal applyExpandingMomentsTransform (proj: Moments -> float) series =
    let initMoments = {nobs = 0.0; sum = 0.0; M1 = 0.0; M2 = 0.0; M3 = 0.0; M4 = 0.0 }
    let calcSparse = expandingWindowFn initMoments updateMomentsSparse proj
    applySeriesProj calcSparse series

  /// Calculates minimum or maximum over an expanding window
  let internal expandingMinMaxHelper cmp s =
    seq {
      let m = ref nan
      for v in s ->        
        match v with
        | Some x -> 
          let mv = !m
          if System.Double.IsNaN(mv) || cmp mv x then m := x; x else mv
        | None -> !m }  

  // ------------------------------------------------------------------------------------
  // Public - expanding window functions
  // ------------------------------------------------------------------------------------

  /// Returns a series that contains counts over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys).
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingCount")>]
  let expandingCount (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.nobs) series

  /// Returns a series that contains sums over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains no values, the result is 0.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingSum")>]
  let expandingSum (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.sum) series

  /// Returns a series that contains means over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains no values, the result is missing.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingMean")>]
  let expandingMean (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.M1) series

  /// Returns a series that contains variance over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains fewer than 2 values, the result is missing.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingVariance")>]
  let expandingVariance (series:Series<'K, float>) : Series<'K, float> =
    let toVar w = 
      if w.nobs < 2.0 then nan
      else w.M2 / (w.nobs - 1.0)
    applyExpandingMomentsTransform toVar series

  /// Returns a series that contains standard deviation over expanding windows (the 
  /// value for a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains fewer than 2 values, the result is missing.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingStandardDeviation")>]
  let expandingStdDev (series:Series<'K, float>) : Series<'K, float> =
    let toStdDev w = 
      if w.nobs < 2.0 then nan
      else w.M2 / (w.nobs - 1.0) |> sqrt
    applyExpandingMomentsTransform toStdDev series

  /// Returns a series that contains skewness over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains fewer than 3 values, the result is missing.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingSkewness")>]
  let expandingSkew (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate    
    let toEstSkew w = 
      if w.nobs < 3.0 then nan else
        let adjust = (sqrt (w.nobs * (w.nobs - 1.0))) / (w.nobs - 2.0)
        adjust * (sqrt w.nobs) * w.M3 / (w.M2 ** 1.5) 
    applyExpandingMomentsTransform toEstSkew series

  /// Returns a series that contains kurtosis over expanding windows (the value for
  /// a given key is calculated from all elements with smaller keys); If the 
  /// entire window contains fewer than 4 values, the result is missing.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingKurtosis")>]
  let expandingKurt (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate
    let toEstKurt w = 
      if w.nobs < 4.0 then nan else
        let adjust p = (6.0 + p * (w.nobs + 1.0)) * (w.nobs - 1.0) / ((w.nobs - 2.0) * (w.nobs - 3.0))
        adjust ((w.nobs * w.M4) / (w.M2 * w.M2) - 3.0)
    applyExpandingMomentsTransform toEstKurt series

  /// Returns a series that contains minimum over an expanding window. The value
  /// for a key _k_ in the returned series is the minimum from all elements with
  /// smaller keys.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingMinimum")>]
  let expandingMin (series:Series<'K, float>) : Series<'K, float> =
    let minFn s v =
      match v with
      | OptionalValue.Present x -> if System.Double.IsNaN(s) then x else min x s
      | OptionalValue.Missing   -> s
    applySeriesProj ((Seq.scan minFn nan) >> (Seq.skip 1)) series

  /// Returns a series that contains maximum over an expanding window. The value
  /// for a key _k_ in the returned series is the maximum from all elements with
  /// smaller keys.
  ///
  /// [category:Expanding windows]
  [<CompiledName("ExpandingMaximum")>]
  let expandingMax (series:Series<'K, float>) : Series<'K, float> =
    let maxFn s v =
      match v with
      | OptionalValue.Present x -> if System.Double.IsNaN(s) then x else max x s
      | OptionalValue.Missing   -> s
    applySeriesProj ((Seq.scan maxFn nan) >> (Seq.skip 1)) series

  // ------------------------------------------------------------------------------------
  // Statistics calculated over the entire series
  // ------------------------------------------------------------------------------------

  /// Returns all values of a series as an array of `OptionalValue`s
  let internal valuesAllOpt (series:Series<_, _>) =
    series.Vector.DataSequence |> Array.ofSeq

  /// Calculates minimum or maximum using the specified function 'f'
  /// Returns None when there are no values or Some.
  let inline internal trySeriesExtreme f (series:Series<'K, 'V>) =
    let mutable res = Unchecked.defaultof<_>
    let mutable initialized = false
    for v in series.Vector.DataSequence do
      if v.HasValue then 
        res <- if initialized then f res v.Value else v.Value
        initialized <- true
    if initialized then Some res else None


  /// Returns the number of the values in a series. This excludes missing values
  /// and values created from `Double.NaN` etc.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Count")>]
  let inline count (series:Series<'K, 'V>) = 
    series.ValueCount

  /// Returns the sum of the values in a series. The function skips over missing values
  /// and `NaN` values. When there are no available values, the result is 0.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Sum")>]
  let inline sum (series:Series<'K, 'V>) = 
    series.Values |> Seq.sum 

  /// Returns the mean of the values in a series. The function skips over missing values
  /// and `NaN` values. When there are no available values, the result is NaN.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Mean")>]
  let mean (series:Series<'K, float>) =
    let sums = initSumsSparse 1 (valuesAllOpt series)
    sums.sum / sums.nobs

  /// Returns the variance of the values in a series. The function skips over missing values
  /// and `NaN` values. When there are less than 2 values, the result is NaN.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Variance")>]
  let variance (series:Series<'K, float>) =
    varianceSums (initSumsSparse 2 (valuesAllOpt series))

  /// Returns the standard deviation of the values in a series. The function skips over 
  /// missing values and `NaN` values. When there are less than 2 values, the result is NaN.
  ///
  /// [category:Standard statistics]
  [<CompiledName("StandardDeviation")>]
  let stdDev (series:Series<'K, float>) =
    sqrt (varianceSums (initSumsSparse 2 (valuesAllOpt series)))

  /// Returns the skewness of the values in a series. The function skips over missing 
  /// values and `NaN` values. When there are less than 3 values, the result is NaN.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Skewness")>]
  let skew (series:Series<'K, float>) =
    skewSums (initSumsSparse 3 (valuesAllOpt series))

  /// Returns the kurtosis of the values in a series. The function skips over missing 
  /// values and `NaN` values. When there are less than 4 values, the result is NaN.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Kurtosis")>]
  let kurt (series:Series<'K, float>) =
    kurtSums (initSumsSparse 4 (valuesAllOpt series))

  /// Returns the minimum of the values in a series. The result is option value.
  /// When the series contains no values, the result is `None`.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Minimum")>]
  let inline min (series:Series<'K, 'V>) = trySeriesExtreme min series

  /// Returns the maximum of the values in a series. The result is option value.
  /// When the series contains no values, the result is `None`.
  ///
  /// [category:Standard statistics]
  [<CompiledName("Maximum")>]
  let inline max (series:Series<'K, 'V>) = trySeriesExtreme max series

  /// Returns the median of the elements of the series.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("Median")>]
  let inline median (series:Series<'K, float>) = series.Values |> Statistics.Median

  // Cumulative functions

  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("CumCount")>]
  let inline cumCount (series:Series<'K, 'V>) = 
    let add a _ = a + 1
    series.ScanValues(Func<_,_,_>(add), 0)

  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("CumSum")>]
  let inline cumSum (series:Series<'K, 'V>) = 
    let add a b = a + b
    series.ScanValues(Func<_,_,_>(add), LanguagePrimitives.GenericZero)

  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("CumProd")>]
  let inline cumProd (series:Series<'K, 'V>) = 
    let mult a b = a * b
    series.ScanValues(Func<_,_,_>(mult), LanguagePrimitives.GenericOne)
 
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("CumMin")>]
  let inline cumMin (series:Series<'K, float>) = 
    series.ScanValues(Func<_,_,_>(LanguagePrimitives.GenericMinimum), System.Double.PositiveInfinity)        

  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("CumMax")>]
  let inline cumMax (series:Series<'K, float>) = 
    series.ScanValues(Func<_,_,_>(LanguagePrimitives.GenericMaximum), System.Double.NegativeInfinity)        

  /// Interpolates an ordered series given a new sequence of keys. The function iterates through
  /// each new key, and invokes a function on the current key, the nearest smaller and larger valid 
  /// observations from the series argument. The function must return a new valid float. 
  ///
  /// ## Parameters
  ///  - `keys` - Sequence of new keys that forms the index of interpolated results
  ///  - `f` - Function to do the interpolating
  ///
  /// [category:Calculations, aggregation and statistics]
  let interpolate keys f (series:Series<'K,'T>) =
    let liftedf k (prev:KeyValuePair<_,_> opt) (next:KeyValuePair<_,_> opt) =
      let t1 = prev |> OptionalValue.map (fun kvp -> kvp.Key, kvp.Value) |> OptionalValue.asOption
      let t2 = next |> OptionalValue.map (fun kvp -> kvp.Key, kvp.Value) |> OptionalValue.asOption
      f k t1 t2

    series.Interpolate(keys, Func<_,_,_,_>(liftedf))

  /// Linearly interpolates an ordered series given a new sequence of keys. 
  ///
  /// ## Parameters
  ///  - `keys` - Sequence of new keys that forms the index of interpolated results
  ///  - `keyDiff` - A function representing "subtraction" between two keys
  ///
  /// [category:Calculations, aggregation and statistics]
  let inline interpolateLinear keys (keyDiff:'K->'K->float) (series:Series<'K, float>) =
    let linearF k a b =
      match a, b with
      | Some x, Some y -> 
        if x = y then snd x 
        else (snd x) + (keyDiff k (fst x)) / (keyDiff (fst y) (fst x)) * (snd y - snd x)
      | Some x, _      -> snd x
      | _, Some y      -> snd y
      | _              -> raise <| new ArgumentException("Unexpected code path in interpolation")
    series |> interpolate keys linearF

  // ------------------------------------------------------------------------------------
  // Statistics calculated over the entire frames' float column series
  // ------------------------------------------------------------------------------------

  /// Returns the sums of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnSum")>]
  let colSum (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> sum)

  /// Returns the means of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnMean")>]
  let colMean (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> mean)

  /// Returns the means of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnMedian")>]
  let colMedian (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> median)

  /// Returns the standard deviations of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnStdDev")>]
  let colStdDev (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> stdDev)

  /// Returns the variance of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnVariance")>]
  let colVariance (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> variance)

  /// Returns the skewness of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnSkewness")>]
  let colSkews (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> skew)

  /// Returns the kurtosis of the float columns in a frame.
  /// [category:Calculations, aggregation and statistics]
  [<CompiledName("ColumnKurtosis")>]
  let colKurt (frame:Frame<'R, 'C>) = 
    frame.GetColumns<float>() |> Series.map (fun _ -> kurt)  