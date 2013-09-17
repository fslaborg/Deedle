module FSharp.DataFrame.Tests.Series

#if INTERACTIVE
#I "../../bin"
#load "../../bin/FSharp.DataFrame.fsx"
#r "../../packages/NUnit.2.6.2/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open FSharp.DataFrame

[<Test>] 
let ``Series with the same data are considered equal``() = 
  let input = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  input |> shouldEqual input

[<Test>] 
let ``Series with different data are not considered equal``() = 
  let input1 = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 4 ]
  let input3 = Series.ofObservations [ 'a' => 1; 'b' => 2  ]
  input1 |> should notEqual input2
  input1 |> should notEqual input3

[<Test>]
let ``Series.diff and SeriesExtensions.Diff work on sample input``() =
  let input = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let expectedForward = Series.ofObservations [ 'a' => 2 ]
  let expectedBackward = Series.ofObservations [ 'c' => 2 ]
  input |> Series.diff -2 |> shouldEqual expectedBackward
  input |> Series.diff 2 |> shouldEqual expectedForward 
  SeriesExtensions.Diff(input, -2) |> shouldEqual expectedBackward
  SeriesExtensions.Diff(input, 2) |> shouldEqual expectedForward

[<Test>] 
let ``Union correctly unions series, prefering left or right values``() = 
  let input1 = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = Series.ofObservations [ 'c' => 1; 'd' => 4  ]
  let expectedL = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3; 'd' => 4 ]
  let expectedR = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 1; 'd' => 4 ]
  input1.Union(input2) |> shouldEqual expectedL
  input1.Union(input2, UnionBehavior.PreferRight) |> shouldEqual expectedR

[<Test>] 
let ``Union throws exception when behavior is exclusive and series overlap``() = 
  let input1 = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = Series.ofObservations [ 'c' => 1; 'd' => 4  ]
  (fun () -> input1.Union(input2, UnionBehavior.Exclusive) |> ignore) 
  |> should throw typeof<System.InvalidOperationException>

[<Test>] 
let ``Union combines series when behavior is exclusive and series do not overlap``() = 
  let input1 = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = Series.ofObservations [ 'd' => 4  ]
  let expected = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3; 'd' => 4 ]
  input1.Union(input2, UnionBehavior.Exclusive) |> shouldEqual expected