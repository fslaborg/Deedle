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

// ------------------------------------------------------------------------------------------------
// Construction & basics
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can create series with incomparable keys``() =
  let rnd1 = System.Random()
  let rnd2 = System.Random()
  let s = Series.ofObservations [rnd1 => 1; rnd2 => 2]
  s.[rnd1] |> shouldEqual 1

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

// ------------------------------------------------------------------------------------------------
// Operations - union, grouping, diff, etc.
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Series.diff and SeriesExtensions.Diff work on sample input``() =
  let input = Series.ofObservations [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let expectedForward = Series.ofObservations [ 'c' => 2 ]
  let expectedBackward = Series.ofObservations [ 'a' => -2 ]
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

[<Test>] 
let ``Grouping series with missing values works on sample input``() =
  let n = Series.ofNullables [Nullable(); Nullable(1); Nullable(); Nullable(2)]
  let actual = n |> Series.groupBy (fun k _ -> k % 2) 
  let expected = Series.ofObservations [ 1 => Series.ofObservations [1 => 1; 3 => 2]]
  actual |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Sampling
// ------------------------------------------------------------------------------------------------

let generate (dt:DateTime) (ts:TimeSpan) count =
  Seq.init count (fun i -> dt.Add(TimeSpan(ts.Ticks * int64 i)), i) |> Series.ofObservations

[<Test>]
let ``Series.sampleTime works when using forward direction`` () =
  let start = DateTime(2012, 2, 12)
  let expected = 
    Series.ofObservations
      [ start.AddHours(0.0) => 0;  start.AddHours(1.0) => 12
        start.AddHours(2.0) => 23; start.AddHours(3.0) => 34
        start.AddHours(4.0) => 45 ]
  generate start (TimeSpan.FromMinutes(5.37)) 50
  |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Forward Series.firstValue
  |> shouldEqual expected        

[<Test>]
let ``Series.sampleTime works when using backward direction`` () =
  let start = DateTime(2012, 2, 12)
  let expected = 
    Series.ofObservations
      [ start.AddHours(0.0) => 0;  start.AddHours(1.0) => 11
        start.AddHours(2.0) => 22; start.AddHours(3.0) => 33
        start.AddHours(4.0) => 44; start.AddHours(5.0) => 49 ]
  generate start (TimeSpan.FromMinutes(5.37)) 50
  |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Backward Series.lastValue
  |> shouldEqual expected        

[<Test>]
let ``Series.sampleInto works when using forward direction`` () =
  let start = DateTime(2012, 2, 12)
  generate start (TimeSpan.FromHours(5.37)) 20
  |> Series.sampleInto [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Forward (fun _ -> Series.firstValue)
  |> shouldEqual <| Series.ofObservations [ DateTime(2012, 2, 13) => 0; DateTime(2012, 2, 15) => 14 ]

[<Test>]
let ``Series.sampleInto works when using backward direction`` () =
  let start = DateTime(2012, 2, 12)
  generate start (TimeSpan.FromHours(5.37)) 20
  |> Series.sampleInto [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Backward (fun _ -> Series.lastValue)
  |> shouldEqual <| Series.ofObservations [ DateTime(2012, 2, 13) => 4; DateTime(2012, 2, 15) => 19 ]

[<Test>]
let ``Series.sample generates empty chunks for keys where there are no values`` () =
  let start = DateTime(2012, 2, 12)
  let keys = [ for d in 12 .. 20 -> DateTime(2012, 2, d) ]
  generate start (TimeSpan.FromHours(48.0)) 5
  |> Series.sample keys Direction.Forward
  |> Series.mapValues (fun s -> if s.IsEmpty then -1 else s.[s.KeyRange |> fst])
  |> shouldEqual <| Series.ofObservations (Seq.zip keys [0; -1; 1; -1; 2; -1; 3; -1; 4])

// ------------------------------------------------------------------------------------------------
// Indexing & slicing & related extensions
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``SeriesExtensions.EndAt works when the key is before, after or in range``() =
  let s = Series.ofObservations [ for i in 10.0 .. 20.0 -> i => int i ]
  SeriesExtensions.EndAt(s, 15.5).Values |> List.ofSeq |> shouldEqual [ 10 .. 15 ]
  SeriesExtensions.EndAt(s, 15.0).Values |> List.ofSeq |> shouldEqual [ 10 .. 15 ]
  SeriesExtensions.EndAt(s, 5.00).Values |> List.ofSeq |> shouldEqual [ ]
  SeriesExtensions.EndAt(s, 25.0).Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]

[<Test>]
let ``SeriesExtensions.StartAt works when the key is before, after or in range``() =
  let s = Series.ofObservations [ for i in 10.0 .. 20.0 -> i => int i ]
  SeriesExtensions.StartAt(s, 15.5).Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  SeriesExtensions.StartAt(s, 15.0).Values |> List.ofSeq |> shouldEqual [ 15 .. 20 ]
  SeriesExtensions.StartAt(s, 5.00).Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]
  SeriesExtensions.StartAt(s, 25.0).Values |> List.ofSeq |> shouldEqual [ ]

[<Test>]
let ``Slicing of ordered series works when using inexact keys (below, inside, above) key range``() =
  let s = Series.ofObservations [ for i in 10.0 .. 20.0 -> i => int i ]
  s.[15.5 .. 20.0].Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.[5.50 .. 20.0].Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]
  s.[15.5 .. 25.0].Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.[15.5 .. 18.5].Values |> List.ofSeq |> shouldEqual [ 16 .. 18 ]

[<Test>]
let ``Slicing of ordered series works when keys are out of series key range``() =
  let s = Series.ofObservations [ for i in 10.0 .. 20.0 -> i => int i ]
  s.[0.0 .. 5.0].Values |> List.ofSeq |> shouldEqual []
  s.[25.0 .. 35.0].Values |> List.ofSeq |> shouldEqual []
  s.[20.0 .. 5.0].Values |> List.ofSeq |> shouldEqual []
