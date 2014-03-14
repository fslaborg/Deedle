namespace Deedle

open Deedle.Vectors
open System.Collections.Generic

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]    
module Stats =
  // -- Experimental fast sliding window logic 
  // - TODO: 
  //    -- still to do, possibly: median, percentile, corr, cov

  let internal applySeriesProj (proj: float opt seq -> float seq) (series:Series<'K, float>) : Series<'K, float> =
    let newData = 
      series.Vector.DataSequence 
      |> proj 
      |> Vector.ofValues

    Series(series.Index, newData, series.VectorBuilder, series.IndexBuilder)

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
           r := (!r - 1)
           yield nan }

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

  let internal initSumsSparse moment (init: float opt []) =
    init 
    |> Seq.filter (function OptionalValue.Present _ -> true | OptionalValue.Missing -> false )
    |> Seq.map OptionalValue.get
    |> Seq.toArray 
    |> initSumsDense moment

  let internal updateSumsSparse moment state curr outg = 
    match curr, outg with
    | OptionalValue.Present x, OptionalValue.Present y -> updateSumsDense moment state x y
    | OptionalValue.Present x, OptionalValue.Missing   -> { updateSumsDense moment state x 0.0 with nobs = state.nobs + 1.0 }
    | OptionalValue.Missing,   OptionalValue.Present y -> { updateSumsDense moment state 0.0 y with nobs = state.nobs - 1.0 }
    | OptionalValue.Missing,   OptionalValue.Missing   -> state

  let internal applyMovingSumsTransform moment winSz (proj: Sums -> float) series =
    if winSz <= 0 then invalidArg "windowSize" "Window must be non-negative"
    let calcSparse = movingWindowFn winSz (initSumsSparse moment) (updateSumsSparse moment) proj
    applySeriesProj calcSparse series

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
      
  // O(n) moving min/max calculator
  let internal movingMinMaxHelper winSz cmp s = 
    seq {
      let i = ref 0
      let q = Deque()
      for v in s ->
        // invariant: all values in deque are strictly ascending (min) or descending (max)
        i := !i + 1        
        match v with
        | OptionalValue.Present x -> 
          // remove from front any values that fell out of moving window
          while q.Count > 0 && !i >= fst q.[0] do q.RemoveFront() |> ignore
          // remove from back any values >= (min) or <= (max) compared to current obs
          while q.Count > 0 && (cmp (snd q.[q.Count - 1]) x) do q.RemoveBack() |> ignore
          // append new obs to back
          q.AddBack( (!i + winSz, x) )
          // return min/max value at front
          snd q.[0]
        | OptionalValue.Missing -> 
          if q.IsEmpty then nan else snd q.[0] }

  // moving window functions

  let movingCount winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 0 winSz (fun s -> s.nobs) series

  let movingSum winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 1 winSz (fun s -> s.sum) series

  let movingMean winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 1 winSz (fun s -> s.sum / s.nobs) series

  let movingVariance winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 2 winSz variance series

  let movingStdDev winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 2 winSz (variance >> sqrt) series

  let movingSkew winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 3 winSz skew series

  let movingKurt winSz (series:Series<'K, float>) : Series<'K, float> =
    applyMovingSumsTransform 4 winSz kurt series 

  let movingMin winSz (series:Series<'K, float>) : Series<'K, float> =
    applySeriesProj (movingMinMaxHelper winSz (>=)) series

  let movingMax winSz (series:Series<'K, float>) : Series<'K, float> =
    applySeriesProj (movingMinMaxHelper winSz (<=)) series

  // end moving window functions

  // Helps create an expanding window calculation
  //   winSz is the minimum window size
  //   fupdate takes the current state and incoming observation to the next state
  //   ftransf takes the current state to the current output
  let internal expandingWindowFn initState fupdate ftransf (source: seq<_>) =
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
    | OptionalValue.Present x -> updateMoments state x
    | OptionalValue.Missing   -> state

  let internal applyExpandingMomentsTransform (proj: Moments -> float) series =
    let initMoments = {nobs = 0.0; sum = 0.0; M1 = 0.0; M2 = 0.0; M3 = 0.0; M4 = 0.0 }
    let calcSparse = expandingWindowFn initMoments updateMomentsSparse proj
    applySeriesProj calcSparse series

  let internal expandingMinMaxHelper cmp s =
    seq {
      let m = ref nan
      for v in s ->        
        match v with
        | Some x -> 
          let mv = !m
          if System.Double.IsNaN(mv) || cmp mv x then m := x; x else mv
        | None -> !m }  

  // expanding window functions

  let expandingCount (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.nobs) series

  let expandingSum (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.sum) series

  let expandingMean (series:Series<'K, float>) : Series<'K, float> =
    applyExpandingMomentsTransform (fun w -> w.M1) series

  let expandingVariance (series:Series<'K, float>) : Series<'K, float> =
    let toVar w = w.M2 / (w.nobs - 1.0)
    applyExpandingMomentsTransform toVar series

  let expandingStdDev (series:Series<'K, float>) : Series<'K, float> =
    let toStdDev w = w.M2 / (w.nobs - 1.0) |> sqrt
    applyExpandingMomentsTransform toStdDev series

  let expandingSkew (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate    
    let toEstSkew w = 
      if w.nobs < 3.0 then nan else
        let adjust = (sqrt (w.nobs * (w.nobs - 1.0))) / (w.nobs - 2.0)
        adjust * (sqrt w.nobs) * w.M3 / (w.M2 ** 1.5) 
    applyExpandingMomentsTransform toEstSkew series

  let expandingKurt (series:Series<'K, float>) : Series<'K, float> =
    // population -> sample estimate
    let toEstKurt w = 
      if w.nobs < 4.0 then nan else
        let adjust p = (6.0 + p * (w.nobs + 1.0)) * (w.nobs - 1.0) / ((w.nobs - 2.0) * (w.nobs - 3.0))
        adjust ((w.nobs * w.M4) / (w.M2 * w.M2) - 3.0)
    applyExpandingMomentsTransform toEstKurt series

  let expandingMin (series:Series<'K, float>) : Series<'K, float> =
    let minFn s v =
      match v with
      | OptionalValue.Present x -> if System.Double.IsNaN(s) then x else min x s
      | OptionalValue.Missing   -> s
    applySeriesProj (Seq.scan minFn nan) series

  let expandingMax (series:Series<'K, float>) : Series<'K, float> =
    let maxFn s v =
      match v with
      | OptionalValue.Present x -> if System.Double.IsNaN(s) then x else max x s
      | OptionalValue.Missing   -> s
    applySeriesProj (Seq.scan maxFn nan) series

  // end expanding window functions

