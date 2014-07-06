#if INTERACTIVE
#I "../../bin/"
#load "Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.VirtualFrame
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Virtual

type TrackingSource(lo, hi) = 
  member val AccessList = [] with get, set
  member val IsTracking = true with get, set
  interface VirtualVectorSource<int64> with
    member x.Length = hi - lo + 1L
    member x.ValueAt addr = 
      if x.IsTracking then x.AccessList <- (lo + addr) :: x.AccessList
      if addr % 3L = 0L then OptionalValue.Missing
      else OptionalValue(lo + addr)
    member x.GetSubVector(nlo, nhi) = 
      if nhi < nlo then invalidOp "hi < lo"
      elif nlo < 0L then invalidOp "lo < 0"
      elif nhi > hi then invalidOp "hi > max"
      else TrackingSource(lo+nlo, lo+nhi, IsTracking = x.IsTracking, AccessList = x.AccessList) :> _

[<Test>]
let ``Formatting accesses only printed values`` () =
  let src = TrackingSource(0L, 1000000000L)
  let series = Virtual.CreateOrdinalSeries(src)
  series.Format(3, 3) |> ignore
  src.AccessList |> shouldEqual [ 0L; 1L; 2L; 1000000000L-2L; 1000000000L-1L; 1000000000L ]

[<Test>]
let ``Counting keys does not evaluate the series`` () =
  let src = TrackingSource(0L, 1000000000L)
  let series = Virtual.CreateOrdinalSeries(src)
  series.KeyCount |> shouldEqual 1000000001
  src.AccessList |> shouldEqual []

[<Test>]
let ``Counting values does not evaluate the series`` () =
  let src = TrackingSource(0L, 10000000L, IsTracking=false)
  let series = Virtual.CreateOrdinalSeries(src)
  series.ValueCount |> shouldEqual 6666667

