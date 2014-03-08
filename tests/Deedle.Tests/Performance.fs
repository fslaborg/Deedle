#if INTERACTIVE
#I "../../bin"
#load "Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#r "../../packages/FSharp.Data.1.1.10/lib/net40/FSharp.Data.dll"
#r "../Deedle.PerfTest.Core/bin/Debug/Deedle.PerfTest.Core.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Performance
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.PerfTest.Core

// ------------------------------------------------------------------------------------------------
// Performance tests that are run & evaluated automatically against a baseline
// ------------------------------------------------------------------------------------------------

let generateFrame colKeys rowCount rowOffset = 
  let rnd = Random(0)
  [ for c in colKeys do
      let s = series [ for i in 0 .. rowCount - 1 -> rowOffset + i => rnd.NextDouble() ]
      yield c.ToString() => s ] |> frame

// Generate sample input data for testing
let frame20x10000 = generateFrame (Seq.map string "ABCDEFGHIJKLMNOPQRSTUV") 10000 0
let frames10x1000 =
  [ for i in 0 .. 9 -> generateFrame (Seq.map string "ABCDEFGHIJ") 1000 (i * 1000) ]
let array1M = Array.init 1000000 id
let series1M = Series.ofValues array1M

// Load sample data sets
let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + @"\..\Performance\data\Titanic.csv")

// ------------------------------------------------------------------------------------------------

[<Test;PerfTest>]
let ``Numerical operators on 20x10k frame``() =
  let add = frame20x10000 + frame20x10000
  let mul = frame20x10000 * frame20x10000
  let div = frame20x10000 / frame20x10000
  let sub = frame20x10000 - frame20x10000
 
  let testVal = frame20x10000?D.GetAt(55)
  ( add?D.GetAt(55) = 2.0 * testVal &&
    mul?D.GetAt(55) = pown testVal 2 &&
    div?D.GetAt(55) = 1.0 &&
    sub?D.GetAt(55) = 0.0 )
  |> shouldEqual true

[<Test;PerfTest(Iterations=10)>]
let ``Building a large (1M items) series from two arrays``() =
  let s = Series(array1M, array1M)
  s.KeyCount |> shouldEqual 1000000

[<Test;PerfTest(Iterations=5)>]
let ``Append 10 medium-size (1000) frames (by repeatedly calling Append)``() =
  let appended = frames10x1000 |> Seq.reduce (Frame.append)
  appended.RowCount |> shouldEqual 10000

[<Test;PerfTest(Iterations=100)>]
let ``Calculate survival rate for Titanic based on gender (using groupRowsBy & applyLevel)``() =   
  let bySex = titanic |> Frame.groupRowsByString "Sex"
  let survivedBySex = bySex.Columns.["Survived"].As<bool>()
  let survivals = 
    survivedBySex
    |> Series.applyLevel Pair.get1Of2 (fun sr -> 
        sr.Values |> Seq.countBy id |> series)
    |> Frame.ofRows
    |> Frame.indexColsWith ["Survived"; "Died"]
  survivals?Total <- 
    bySex
    |> Frame.applyLevel Pair.get1Of2 Series.countKeys

  // Verify that we get the expected results
  let actual = round (survivals?Survived / survivals?Total * 100.0)
  let expected = series ["male" => 81.0; "female" => 26.0]
  actual |> shouldEqual expected

  let actual = round (survivals?Died / survivals?Total * 100.0)
  let expected = series ["male" => 19.0; "female" => 74.0]
  actual |> shouldEqual expected

[<Test;PerfTest(Iterations=1)>]
let ``Realign a 1M element series according to a specified key array`` () =
  let newKeys = [|1 .. 1000000|]
  let actual = series1M |> Series.realign newKeys

  // Verify the results  
  actual.TryGet(1).HasValue |> shouldEqual true
  actual.TryGet(0).HasValue |> shouldEqual false
  actual.FirstKey() |> shouldEqual 1


(*
TODO: https://github.com/BlueMountainCapital/Deedle/issues/142#issuecomment-33587885

let genRandomNumbers count =
    let rnd = System.Random()
    List.init count (fun _ -> (float <| rnd.Next()) / (float Int32.MaxValue))

let data : Frame<int,string> = frame [] |> Frame.indexRowsWith [1..1000000]
data?S <- [for i in (genRandomNumbers 1000000) -> if i > 0.5 then "x" else "y" ]
data?N <- [1 .. 1000000]


data
|> Frame.groupRowsByString "S"
|> Frame.applyLevel fst (fun r -> 
    let df = Frame.ofRows r
    df - Series.mean df?N )
|> Frame.collapseRows
|> Frame.mapRowKeys snd


let gped = data |> Frame.groupRowsUsing (fun k r -> r.GetAs<string>("S"))
let gavg = gped |> Frame.meanLevel fst

gped 
|> Frame.nest
|> Series.zipAlign JoinKind.Left Lookup.Exact gavg?N
|> Series.mapValues (fun v -> (fst v) |> OptionalValue.asOption, (snd v) |> OptionalValue.asOption)
|> Series.mapValues (function 
   | Some avg, Some fr -> fr - avg
   | _                 -> frame [])
|> Frame.unnest
|> Frame.mapRowKeys snd


let r = genRandomNumbers 1000000
let x = [for i in r -> if i > 0.5 then "x" else "y" ]
let s = Series.ofValues(x)
let n = Series.ofValues(r)
let f = Frame.ofColumns ["s" => s]
f?n <- n

This code takes more than a minute (vast upward variance depending on how many gen2 GC's happen)
let means = f |> Frame.groupRowsByString "s" |> Frame.applyLevel fst (fun r -> 
  let df = Frame.ofRows r
  Series.mean df?n )


Whereas this code takes less than three seconds fairly consistently
let means = f.GroupRowsWith x |> Series.mapValues (fun f -> f?n |> Series.mean)


/// -------------------
// also, the function where AppendN makes things faster is: collapseFrameSeries 
// So add a test tracking that.

// The following should go from 3.8 sec to almost no-op
// ~3.8sec
for i in 0 .. 100 do
  frame20x10000.Columns
  |> Frame.ofColumns
  |> ignore



// Converting huge series to its own type via columns should be no-op
let df = frame [ "A" => Series.ofValues [for i in 0 .. 1000000 -> float i ] ]
df.Columns.["A"].As<float>() |> ignore

(with conversion, it will take some time)



  let s = series [ for i in 0 .. 1000000 -> i, float i ]
  // 95ms (was 7286ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 100 -> i * 10000 ] Direction.Forward (fun _ s -> Series.mean s) |> ignore
  // 100ms (was 9224ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 100 -> i * 10000 ] Direction.Backward (fun _ s -> Series.mean s) |> ignore
  // 190ms (was 7547ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 1000 -> i * 1000 ] Direction.Forward (fun _ s -> Series.mean s) |> ignore
  // 190ms (was 9294ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 1000 -> i * 1000 ] Direction.Backward (fun _ s -> Series.mean s) |> ignore
  // 440ms (was 8215ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 10000 -> i * 100 ] Direction.Forward (fun _ s -> Series.mean s) |> ignore
  // 430ms (was 9934ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 10000 -> i * 100 ] Direction.Backward (fun _ s -> Series.mean s) |> ignore
  // 4150ms (was 15853ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 100000 -> i * 10 ] Direction.Forward (fun _ s -> Series.mean s) |> ignore
  // 5000ms (was 20695ms)
  for i in 0 .. 9 do
    s |> Series.resampleInto [for i in 0 .. 100000 -> i * 10 ] Direction.Backward (fun _ s -> Series.mean s) |> ignore

*)

