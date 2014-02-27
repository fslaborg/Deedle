namespace Deedle

open Deedle.Vectors

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Stats =
  // -- Experimental window logic 
  // - TODO: 
  //    -- still to do, possibly: prod, skew, kurt, min, max, median, quantile, corr, cov

  let internal applySeriesProj keyProj (denseProj:float seq -> float seq) (sparseProj:float option seq -> float option seq) 
    (series:Series<'K, float>) : Series<'K, float> =
    
    let newKeys = series.Index.Keys |> keyProj
    
    let makeOptionalSeries x = 
      let newVals = x |> Seq.map OptionalValue.asOption |> sparseProj
      Series(Index.ofKeys newKeys, Vector.ofOptionalValues newVals, series.VectorBuilder, series.IndexBuilder)

    let makeSeries x =
      let newVals = x |> denseProj
      Series(Index.ofKeys newKeys, Vector.ofValues newVals, series.VectorBuilder, series.IndexBuilder)

    match series.Vector.Data with
    | VectorData.DenseList x  -> makeSeries x
    | VectorData.SparseList x -> makeOptionalSeries x
    | VectorData.Sequence x   -> makeOptionalSeries x
    
  // moving window functions:

  // Helps create a moving window calculation
  // nb. adopted from Seq.windowed in F# code base
  //   finit takes the first fully populated window array to an initial state
  //   fupdate takes the current state, incoming observation, and out-going observation to the next state
  //   ftransf takes the current state to the current output
  let internal movingWindowFn winSz finit fupdate ftransf (source: seq<_>) =
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
           r := (!r - 1) }

  type Sums = { nobs: float; sum: float; sumsq: float }

  let internal initSumsDense (init: float []) = 
    let count = init |> Array.length |> float
    let sum   = init |> Array.sum
    let sumsq = init |> Array.sumBy (fun x -> x * x)
    { nobs = count; sum = sum; sumsq = sumsq }
  
  let internal updateSumsDense state curr outg =
    let newsum   = state.sum + curr - outg
    let newsumsq = state.sumsq + curr * curr - outg * outg
    { state with sum = newsum; sumsq = newsumsq }

  let internal initSumsSparse (init: float option []) =
    init |> Seq.filter Option.isSome |> Seq.map Option.get |> Seq.toArray |> initSumsDense

  let internal updateSumsSparse state curr outg = 
    match curr, outg with
    | Some x, Some y -> updateSumsDense state x y
    | Some x, None   -> { updateSumsDense state x 0.0 with nobs = state.nobs + 1.0 }
    | None, Some y   -> { updateSumsDense state 0.0 y with nobs = state.nobs - 1.0 }
    | None, None     -> state

  let internal checkWinSz winSz minObs =
    if winSz <= 0 then invalidArg "windowSize" "Window must be non-negative"
    if winSz < minObs then invalidArg "windowSize" "Window must be at least the size of minObs"

  let internal applyMovingWindowTransform winSz minObs (proj: Sums -> float) series =
    checkWinSz winSz minObs
    let filtProj s = if (int s.nobs) > minObs then Some(proj s) else None
    let keyFn = Seq.skip (winSz - 1)
    let movingCountDense = movingWindowFn winSz initSumsDense updateSumsDense proj
    let movingCountSparse = movingWindowFn winSz initSumsSparse updateSumsSparse filtProj
    applySeriesProj keyFn movingCountDense movingCountSparse series

  // moving window functions

  let movingCount winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingWindowTransform winSz minObs (fun s -> s.nobs) series

  let movingSum winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingWindowTransform winSz minObs (fun s -> s.sum) series

  let movingMean winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingWindowTransform winSz minObs (fun s -> s.sum / s.nobs) series

  let movingVariance winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingWindowTransform winSz minObs (fun s -> (s.nobs * s.sumsq - s.sum * s.sum) / (s.nobs * s.nobs - s.nobs)) series

  let movingStdDev winSz minObs (series:Series<'K, float>) : Series<'K, float> =
    applyMovingWindowTransform winSz minObs (fun s -> sqrt <| (s.nobs * s.sumsq - s.sum * s.sum) / (s.nobs * s.nobs - s.nobs)) series

  // end moving window functions

  // Helps create an expanding window calculation
  //   minObs is the minimum number of observations 
  //   fupdate takes the current state and incoming observation to the next state
  //   ftransf takes the current state to the current output
  let internal expandingWindowFn minObs initState fupdate ftransf (source: seq<_>) =
    source |> Seq.scan fupdate initState |> Seq.map ftransf |> Seq.skip minObs
     
  // Knuth/Welford algorithm for online updating
  // ref: http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm            
  type Welford = { nobs: float; delta: float; mean: float; M2: float; sum: float }

  let internal updateWelford state curr =
    let { nobs = nobs; delta = delta; mean = mean; M2 = M2; sum = sum } = state
    let n = nobs + 1.0
    let delta = curr - mean
    let mean = mean + delta / n
    let M2 = M2 + delta * (curr - mean)
    let s = sum + curr 
    { nobs = n; delta = delta; mean = mean; M2 = M2; sum = s }   

  let internal updateWelfordSparse state curr =     
    match curr with
    | Some x -> updateWelford state x
    | None   -> state

  let internal applyExpandingWindowTransform minObs (proj: Welford -> float) series =
    if minObs < 1 then invalidArg "minObs" "minObs must be at least 1"
    let filtProj s = if (int s.nobs) > minObs then Some(proj s) else None
    let initState = {nobs = 0.0; delta = 0.0; mean = 0.0; M2 = 0.0; sum = 0.0 }
    let keyFn = Seq.skip (minObs - 1)
    let movingCountDense = expandingWindowFn minObs initState updateWelford proj
    let movingCountSparse = expandingWindowFn minObs initState updateWelfordSparse filtProj
    applySeriesProj keyFn movingCountDense movingCountSparse series

  // expanding window functions

  let expandingCount minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingWindowTransform minObs (fun w -> w.nobs) series

  let expandingSum minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingWindowTransform minObs (fun w -> w.sum) series

  let expandingMean minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingWindowTransform minObs (fun w -> w.mean) series

  let expandingVariance minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingWindowTransform minObs (fun w -> w.M2/(w.nobs - 1.0)) series

  let expandingStdDev minObs (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingWindowTransform minObs (fun w -> sqrt <| w.M2/(w.nobs - 1.0)) series

  // end expanding window functions

