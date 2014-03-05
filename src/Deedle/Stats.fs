namespace Deedle

open Deedle.Vectors

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Stats =
  // -- Experimental fast sliding window logic 
  // - TODO: 
  //    -- still to do, possibly: median, percentile, min, max, corr, cov

  let internal applySeriesProj (denseProj:float seq -> float seq) (sparseProj:float option seq -> float option seq) 
    (series:Series<'K, float>) : Series<'K, float> =
    
    let makeOptionalSeries x = 
      let newVals = x |> Seq.map OptionalValue.asOption |> sparseProj
      Series(series.Index, Vector.ofOptionalValues newVals, series.VectorBuilder, series.IndexBuilder)

    let makeSeries x =
      let newVals = x |> denseProj
      Series(series.Index, Vector.ofValues newVals, series.VectorBuilder, series.IndexBuilder)

    match series.Vector.Data with
    | VectorData.DenseList x  -> makeSeries x
    | VectorData.SparseList x -> makeOptionalSeries x
    | VectorData.Sequence x   -> makeOptionalSeries x
    
  // Helps create a moving window calculation
  // nb. adopted from Seq.windowed in F# code base
  //   finit takes the first fully populated window array to an initial state
  //   fupdate takes the current state, incoming observation, and out-going observation to the next state
  //   ftransf takes the current state to the current output
  let internal movingWindowFn winSz finit fupdate ftransf noVal (source: seq<_>) =
    seq {
       let arr = Array.zeroCreate winSz 
       let r = ref (winSz - 1)
       let i = ref 0 
       let isInit = ref false
       let state = ref Unchecked.defaultof<_>
       use e = source.GetEnumerator() 
       while e.MoveNext() do 
         let curr = e.Current
         let outg = arr.[!i]
         arr.[!i] <- curr
         i := (!i+ 1 ) % winSz
         if !r = 0 then 
           if not !isInit then
             state := Array.copy arr |> finit
             isInit := true
           else 
             state := fupdate !state curr outg
           yield !state |> ftransf
         else 
           r := (!r - 1)
           yield noVal }

  type Sums = { nobs: float; sum: float; sump2: float; sump3: float; sump4: float }

  let internal initSumsDense moment (init: float []) = 
    let count = init |> Array.length |> float
    let sum   = if moment < 1 then 0.0 else init |> Array.sum
    let sump2 = if moment < 2 then 0.0 else init |> Array.sumBy (fun x -> pown x 2)
    let sump3 = if moment < 3 then 0.0 else init |> Array.sumBy (fun x -> pown x 3)
    let sump4 = if moment < 4 then 0.0 else init |> Array.sumBy (fun x -> pown x 4)
    { nobs = count; sum = sum; sump2 = sump2; sump3 = sump3; sump4 = sump4 }
  
  let internal updateSumsDense moment state curr outg =
    let sum   = if moment < 1 then 0.0 else state.sum + curr - outg
    let sump2 = if moment < 2 then 0.0 else state.sump2 + (pown curr 2) - (pown outg 2)
    let sump3 = if moment < 3 then 0.0 else state.sump3 + (pown curr 3) - (pown outg 3)
    let sump4 = if moment < 4 then 0.0 else state.sump4 + (pown curr 4) - (pown outg 4)
    { state with sum = sum; sump2 = sump2; sump3 = sump3; sump4 = sump4 }

  let internal initSumsSparse moment (init: float option []) =
    init |> Seq.filter Option.isSome |> Seq.map Option.get |> Seq.toArray |> (initSumsDense moment)

  let internal updateSumsSparse moment state curr outg = 
    match curr, outg with
    | Some x, Some y -> updateSumsDense moment state x y
    | Some x, None   -> { updateSumsDense moment state x 0.0 with nobs = state.nobs + 1.0 }
    | None, Some y   -> { updateSumsDense moment state 0.0 y with nobs = state.nobs - 1.0 }
    | None, None     -> state

  let internal checkWinSz winSz minObs =
    if winSz <= 0 then invalidArg "windowSize" "Window must be non-negative"
    if winSz < minObs then invalidArg "windowSize" "Window must be at least the size of minObs"

  let internal applyMovingStatsTransform moment winSz minObs (proj: Sums -> float) series =
    checkWinSz winSz minObs
    let filtProj s = if (int s.nobs) >= minObs then Some(proj s) else None
    let calcDense = movingWindowFn winSz (initSumsDense moment) (updateSumsDense moment) proj nan
    let calcSparse = movingWindowFn winSz (initSumsSparse moment) (updateSumsSparse moment) filtProj None
    applySeriesProj calcDense calcSparse series

  let internal variance s =
    let v = (s.nobs * s.sump2 - s.sum * s.sum) / (s.nobs * s.nobs - s.nobs) 
    if v < 0.0 then nan else v

  let internal skew s =
    let a = s.sum / s.nobs
    let b = s.sump2 / s.nobs - a * a
    let c = s.sump3 / s.nobs - a * a * a - 3.0 * a * b
    let r = b |> sqrt
    if b = 0.0 || s.nobs < 3.0 then nan 
    else (sqrt (s.nobs * (s.nobs - 1.0)) * c) / ((s.nobs - 2.0) * pown r 3)
  
  let internal kurt s =
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

  // moving window functions

  let movingCount winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 0 winSz minObs (fun s -> s.nobs) series

  let movingSum winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 1 winSz minObs (fun s -> s.sum) series

  let movingMean winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 1 winSz minObs (fun s -> s.sum / s.nobs) series

  let movingVariance winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 2 winSz minObs variance series

  let movingStdDev winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 2 winSz minObs (variance >> sqrt) series

  let movingSkew winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 3 winSz minObs skew series

  let movingKurt winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingStatsTransform 4 winSz minObs kurt series 

  // end moving window functions

  // Helps create an expanding window calculation
  //   winSz is the minimum window size
  //   fupdate takes the current state and incoming observation to the next state
  //   ftransf takes the current state to the current output
  let internal expandingWindowFn winSz initState fupdate ftransf (source: seq<_>) =
    source 
    |> Seq.scan fupdate initState 
    |> Seq.map ftransf 

  // Knuth/Welford algorithm for online stats updating
  // see: http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm            
  type Moments = { 
    nobs : float 
    sum  : float 
    M1   : float
    M2   : float 
    M3   : float
    M4   : float 
  }

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

  let internal updateMomentsSparse state curr =     
    match curr with
    | Some x -> updateMoments state x
    | None   -> state

  let internal applyExpandingStatsTransform minObs (proj: Moments -> float) series =
    if minObs < 1 then invalidArg "minObs" "minObs must be at least 1"
    let proj' s = if (int s.nobs) >= minObs then proj s else nan
    let filtProj s = if (int s.nobs) >= minObs then Some(proj s) else None
    let initState = {nobs = 0.0; sum = 0.0; M1 = 0.0; M2 = 0.0; M3 = 0.0; M4 = 0.0 }
    let calcDense = expandingWindowFn minObs initState updateMoments proj' 
    let calcSparse = expandingWindowFn minObs initState updateMomentsSparse filtProj
    applySeriesProj calcDense calcSparse series

  // expanding window functions

  let expandingCount minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingStatsTransform minObs (fun w -> w.nobs) series

  let expandingSum minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingStatsTransform minObs (fun w -> w.sum) series

  let expandingMean minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingStatsTransform minObs (fun w -> w.M1) series

  let expandingVariance minObs (series:Series<'K, float>) : Series<'K, float> =
    let toVar w = w.M2 / (w.nobs - 1.0)
    applyExpandingStatsTransform minObs toVar series

  let expandingStdDev minObs (series:Series<'K, float>) : Series<'K, float> =
    let toStdDev w = w.M2 / (w.nobs - 1.0) |> sqrt
    applyExpandingStatsTransform minObs toStdDev series

  let expandingSkew minObs (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate    
    let toEstSkew w = 
      if w.nobs < 3.0 then nan else
        let adjust = (sqrt (w.nobs * (w.nobs - 1.0))) / (w.nobs - 2.0)
        adjust * (sqrt w.nobs) * w.M3 / (w.M2 ** 1.5) 
    applyExpandingStatsTransform minObs toEstSkew series

  let expandingKurt minObs (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate
    let toEstKurt w = 
      if w.nobs < 4.0 then nan else
        let adjust p = (6.0 + p * (w.nobs + 1.0)) * (w.nobs - 1.0) / ((w.nobs - 2.0) * (w.nobs - 3.0))
        adjust ((w.nobs * w.M4) / (w.M2 * w.M2) - 3.0)
    applyExpandingStatsTransform minObs toEstKurt series

  // end expanding window functions

