#if INTERACTIVE
#I "../../bin"
#load "Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#r "../../packages/FSharp.Data.1.1.10/lib/net40/FSharp.Data.dll"
#r "../Deedle.Tests.PerfTool/bin/Debug/Deedle.Tests.PerfTool.exe"
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
