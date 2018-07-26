#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#r "../PerformanceTools/bin/net45/Deedle.PerfTest.Core.dll"
#else
module Deedle.Tests.Performance
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.PerfTest

// ------------------------------------------------------------------------------------------------
// Load data and generate frames
// ------------------------------------------------------------------------------------------------

let generateFrame colKeys rowCount rowOffset = 
  let rnd = Random(0)
  [ for c in colKeys do
      let s = series [ for i in 0 .. rowCount - 1 -> rowOffset + i => rnd.NextDouble() ]
      yield c.ToString() => s ] |> frame

// Generate sample input data for testing
let frame20x10000 = generateFrame (Seq.map string "ABCDEFGHIJKLMNOPQRST") 10000 0
let frames10x1000 =
  [ for i in 0 .. 9 -> generateFrame (Seq.map string "ABCDEFGHIJ") 1000 (i * 1000) ]
let frame1000x1000 = generateFrame [ 0 .. 999 ] 1000 0
let frame100x10000WithNans = 
  [ for i in 0 .. 100 ->
      i => series [ for j in 0 .. 10000 -> j => if j%10=0 then nan else float j ]  ] |> frame

// Generate series of various lengths
let rnd = Random(0)
let array1M = Array.init 1000000 (fun i -> i, rnd.NextDouble())
let array10k = Array.init 10000 (fun i -> i, rnd.NextDouble())
let series1M = series array1M
let series10k = series array10k

// Frame with 'Key' of type string and 'Value' of type float
let frameTwoCol1M = 
  let rnd = Random(0)
  Array.init 100000 (fun i -> (if rnd.Next(2) = 0 then "x" else "y"), float i)
  |> Frame.ofRecords
  |> Frame.indexColsWith ["Key"; "Value"]

// Load sample data sets
let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../Performance/data/titanic.csv")

// ------------------------------------------------------------------------------------------------
// Compatibility
// ------------------------------------------------------------------------------------------------

#if BELOW_0_9_13
module Stats = 
  let mean (s:Series<_, float>) = Series.mean s
  let stdDev (s:Series<_, float>) = Series.sdv s

type Deedle.Series<'K, 'V when 'K : equality> with 
  member s1.Merge(s2) = s1.Append(s2)
#endif

// ------------------------------------------------------------------------------------------------
// Performance tests
// ------------------------------------------------------------------------------------------------

#if BELOW_0_9_13
#else
#if BELOW_1_1_1
#else
type ITitanicRow =
  abstract PassengerId : int
  abstract Survived : bool
  abstract Pclass : int
  abstract Name : string

type ITitanicFloatPClassRow =
  abstract Pclass : float

[<Test; PerfTest(Iterations=50)>]
let ``Sum column using typed rows`` () = 
  let rows = titanic.GetRowsAs<ITitanicRow>()
  for j in 0 .. 10 do 
    let mutable c = 0
    for i in fst rows.KeyRange .. snd rows.KeyRange do
      c <- c + rows.[i].Pclass 
    c |> shouldEqual 2057

[<Test; PerfTest(Iterations=50)>]
let ``Sum column using typed rows with conversion`` () = 
  let rows = titanic.GetRowsAs<ITitanicFloatPClassRow>() 
  for j in 0 .. 10 do 
    let mutable c = 0.0
    for i in fst rows.KeyRange .. snd rows.KeyRange do
      c <- c + rows.[i].Pclass 
    c |> shouldEqual 2057.0
#endif
#endif

[<Test; PerfTest(Iterations=5)>]
let ``Sum column using untyped rows`` () = 
  let rows = titanic.Rows
  for j in 0 .. 10 do
    let mutable c = 0
    for i in fst rows.KeyRange .. snd rows.KeyRange do
      c <- c + titanic.Rows.[i].GetAs<int>("Pclass")
    c |> shouldEqual 2057

[<Test; PerfTest(Iterations=10)>]
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

[<Test; PerfTest(Iterations=10)>]
let ``Numerical operators on 100x10k frame and series`` () =
  let f1 = frame [ for i in 0 .. 100 -> i => series10k ]
  let res = f1 - series10k 
  unbox<float> (res.Rows.[100].[100]) |> shouldEqual 0.0

[<Test; PerfTest(Iterations=10)>]
let ``Numerical operators on 10x10k frame and series`` () =
  let f1 = frame [ for i in 0 .. 10 -> i => series10k ]
  let res = f1 - series10k 
  unbox<float> (res.Rows.[100].[5]) |> shouldEqual 0.0

[<Test;PerfTest(Iterations=10)>]
let ``Building a large (1M items) series from two arrays``() =
  let s = Series(array1M, array1M)
  s.KeyCount |> shouldEqual 1000000

[<Test;PerfTest(Iterations=50)>]
let ``Titanic survival rate based on gender (groupRowsBy)``() =   
  let bySex = titanic |> Frame.groupRowsByString "Sex"
  let survivedBySex = bySex.Columns.["Survived"].As<bool>()
  let survivals = 
    survivedBySex
    |> Series.applyLevel Pair.get1Of2 (fun sr -> 
        sr.Values |> Seq.countBy id |> series)
    |> Frame.ofRows
    |> Frame.indexColsWith ["Survived"; "Died"]
#if BELOW_0_9_13
  survivals?Total <- 
    bySex
    |> Frame.applyLevel Pair.get1Of2 Series.countKeys
#else
  survivals?Total <- 
    bySex.Rows
    |> Series.applyLevel Pair.get1Of2 Series.countKeys
#endif

  // Verify that we get the expected results
  let actual = round (survivals?Survived / survivals?Total * 100.0)
  let expected = series ["male" => 81.0; "female" => 26.0]
  actual |> shouldEqual expected

  let actual = round (survivals?Died / survivals?Total * 100.0)
  let expected = series ["male" => 19.0; "female" => 74.0]
  actual |> shouldEqual expected

#if BELOW_0_9_13
#else
[<Test;PerfTest(Iterations=50)>]
let ``Titanic survival rate based on gender (pivotTable)``() =   
  let survivals = 
    titanic 
    |> Frame.pivotTable 
        (fun _ row -> row.GetAs<string>("Sex"))
        (fun _ row -> row.GetAs<bool>("Survived")) 
        (fun df -> df.RowCount)
    |> Frame.indexColsWith ["Survived";"Died"]
  survivals?Total <- survivals?Died + survivals?Survived

  // Verify that we get the expected results
  let actual = round (survivals?Survived / survivals?Total * 100.0)
  let expected = series ["male" => 81.0; "female" => 26.0]
  actual |> shouldEqual expected

  let actual = round (survivals?Died / survivals?Total * 100.0)
  let expected = series ["male" => 19.0; "female" => 74.0]
  actual |> shouldEqual expected
#endif

[<Test; PerfTest(Iterations=10)>]
let ``Realign 1M series according to a key array`` () =
  let newKeys = [|1 .. 1000000|]
  let actual = series1M |> Series.realign newKeys

  // Verify the results  
  actual.TryGet(1).HasValue |> shouldEqual true
  actual.TryGet(0).HasValue |> shouldEqual false
#if BELOW_0_9_13
  actual.KeyRange |> fst |> shouldEqual 1
#else
  Series.firstKey actual |> shouldEqual 1
#endif

[<Test; PerfTest(Iterations=10)>]
let ``Group by column and subtract group averages`` () =
  // https://github.com/fslaborg/Deedle/issues/142#issuecomment-33587885
  let grouped = frameTwoCol1M |> Frame.groupRowsByString "Key"
  let diffs = 
    #if BELOW_0_9_13
    grouped
    |> Frame.applyLevel fst (fun r -> 
        let df = Frame.ofRows r
        df - Series.mean df?Value )
    |> Frame.collapseRows
    #else
    grouped.Rows
    |> Series.applyLevel fst (fun r -> 
        let df = Frame.ofRows r
        df - Stats.mean df?Value |> Frame.mapRowKeys snd )
    |> Frame.unnest
    #endif
    |> Frame.mapRowKeys snd
  let sdv = int (diffs?Value |> Stats.stdDev)
  sdv |> shouldEqual 28867

[<Test; PerfTest(Iterations=10)>]
let ``Get frames columns and re-create frame`` () =
  for i in 0 .. 100 do
    let df = frame20x10000.Columns |> Frame.ofColumns
    df.ColumnCount |> shouldEqual 20

[<Test; PerfTest(Iterations=4)>]
let ``Accessing float series via object series`` () =
  for i in 0 .. 20 do 
    let df = frame [ "A" => series1M ]
    df.Columns.["A"].As<float>().[0] |> shouldEqual (series1M.[0])

[<PerfTest(Iterations=10)>]
let ``Resample 1M series using 10000 blocks``() =
  let keys = [for i in 0 .. 100 -> i * 10000 ]
  series1M |> Series.resampleInto keys Direction.Forward (fun _ s -> Stats.mean s) |> ignore

[<PerfTest(Iterations=10)>]
let ``Resample 1M series using 10000 blocks (forward)``() =
  let keys = [for i in 0 .. 100 -> i * 10000 ]
  series1M |> Series.resampleInto keys Direction.Forward (fun _ s -> Stats.mean s) |> ignore

[<PerfTest(Iterations=10)>]
let ``Resample 1M series using 10000 blocks (backward)``() =
  let keys = [for i in 0 .. 100 -> i * 10000 ]
  series1M |> Series.resampleInto keys Direction.Backward (fun _ s -> Stats.mean s) |> ignore

[<PerfTest(Iterations=10)>]
let ``Resample 1M series using 100 blocks (forward)``() =
  let keys = [for i in 0 .. 10000 -> i * 100 ]
  series1M |> Series.resampleInto keys Direction.Forward (fun _ s -> Stats.mean s) |> ignore

[<PerfTest(Iterations=10)>]
let ``Resample 1M series using 100 blocks (backward)``() =
  let keys = [for i in 0 .. 10000 -> i * 100 ]
  series1M |> Series.resampleInto keys Direction.Backward (fun _ s -> Stats.mean s) |> ignore

[<Test; PerfTest(Iterations=6)>]
let ``Shift 20x10k frame by offset 10`` () =
  let res = frame20x10000 |> Frame.shift 10
  res.Rows.[100].As<float>() |> shouldEqual (frame20x10000.Rows.[90].As<float>())

[<Test; PerfTest(Iterations=6)>]
let ``Diff 20x10k frame by offset 10`` () =
  let res = frame20x10000 |> Frame.diff 10
  let test = (frame20x10000.Rows.[100].As<float>() - frame20x10000.Rows.[90].As<float>())
  res.Rows.[100].As<float>() |> shouldEqual test

#if BELOW_0_9_13
#else
[<Test; PerfTest(Iterations=10)>]
let ``Take 500k elements from a 1M element series`` () =
  let res = series1M |> Series.take 500000
  res.KeyCount |> shouldEqual 500000
#endif

[<Test; PerfTest(Iterations=10)>]
let ``Stack values of a 1000x1000 frame`` () = 
  let df = frame1000x1000 |> Frame.stack
  df.RowCount |> shouldEqual 1000000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 10 frames of size 1k (repeated Merge)``() =
#if BELOW_0_9_13
  let appended = frames10x1000 |> Seq.reduce (Frame.append)
#else
  let appended = frames10x1000 |> Seq.reduce (Frame.merge)
#endif
  appended.RowCount |> shouldEqual 10000
  
let s1 = series [ for i in 0000001 .. 0300000 -> i => float i ]
let s2 = series [ for i in 1000001 .. 1300000 -> i => float i ]
let s3 = series [ for i in 2000001 .. 2300000 -> i => float i ]
let s4 = series [ for i in 3000001 .. 3300000 -> i => float i ]
let s5 = series [ for i in 4000001 .. 4300000 -> i => float i ]
let s6 = series [ for i in 5000001 .. 5300000 -> i => float i ]

let r1 = series [ for i in 0300000 .. -1 .. 0000001 -> i => float i ] 
let r2 = series [ for i in 1300000 .. -1 .. 1000001 -> i => float i ] 
let r3 = series [ for i in 2300000 .. -1 .. 2000001 -> i => float i ] 
let r4 = series [ for i in 3300000 .. -1 .. 3000001 -> i => float i ] 
let r5 = series [ for i in 4300000 .. -1 .. 4000001 -> i => float i ] 
let r6 = series [ for i in 5300000 .. -1 .. 5000001 -> i => float i ] 

[<Test;PerfTest(Iterations=10)>]
let ``Merge 3 ordered 300k long series (repeating Merge)`` () =
  s1.Merge(s2).Merge(s3).KeyCount |> shouldEqual 900000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 6 ordered 300k long series (repeating Merge)`` () =
  s1.Merge(s2).Merge(s3).Merge(s4).Merge(s5).Merge(s6).KeyCount |> shouldEqual 1800000

#if BELOW_0_9_13
#else
[<Test;PerfTest(Iterations=10)>]
let ``Merge 3 ordered 300k long series (single Merge)`` () =
  s1.Merge(s2, s3).KeyCount |> shouldEqual 900000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 6 ordered 300k long series (single Merge)`` () =
  s1.Merge(s2, s3, s4, s5, s6).KeyCount |> shouldEqual 1800000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 1000 ordered 1k long series (single Merge)`` () =
  let series1000of1000 = 
    [ for i in 1 .. 1000 ->
        series [ for j in 1000000*i+1 .. 1000000*i+1000 -> j => float j ] ]
  (Series.mergeAll series1000of1000).KeyCount |> shouldEqual 1000000
#endif

[<Test;PerfTest(Iterations=10)>]
let ``Merge 3 unordered 300k long series (repeating Merge)`` () =
  r1.Merge(r2).Merge(r3).KeyCount |> shouldEqual 900000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 6 unordered 300k long series (repeating Merge)`` () =
  r1.Merge(r2).Merge(r3).Merge(r4).Merge(r5).Merge(r6).KeyCount |> shouldEqual 1800000

#if BELOW_0_9_13
#else
[<Test;PerfTest(Iterations=10)>]
let ``Merge 3 unordered 300k long series (single Merge)`` () =
  r1.Merge(r2, r3).KeyCount |> shouldEqual 900000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 6 unordered 300k long series (single Merge)`` () =
  r1.Merge(r2, r3, r4, r5, r6).KeyCount |> shouldEqual 1800000

[<Test;PerfTest(Iterations=5)>]
let ``Merge 1000 unordered 1k long series (single Merge)`` () =
  let series1000of1000 = 
    [ for i in 1 .. 1000 ->
        series [ for j in 1000000*i+1000 .. -1 .. 1000000*i+1 -> j => float j ] ]
  (Series.mergeAll series1000of1000).KeyCount |> shouldEqual 1000000
#endif 

[<Test;PerfTest(Iterations=10)>]
let ``Fill forward missing values in 100x10k frame`` () =
  let filled = frame100x10000WithNans |> Frame.fillMissing Direction.Forward 
  filled.Rows.[10000].GetAs<float>(50) |> shouldEqual 9999.0

[<Test;PerfTest(Iterations=5)>]
let ``Creating small (10) series from array`` () =
  let vs = Array.init 10 (fun i -> i, float i)
  let a = Array.init 100000 (fun _ -> series vs)
  a.[5].KeyCount |> shouldEqual 10
  a.[5].[5] |> shouldEqual 5.0

[<Test;PerfTest(Iterations=5)>]
let ``Creating large (10k) series from array`` () =
  let vs = Array.init 10000 (fun i -> i, float i)
  let a = Array.init 100 (fun _ -> series vs)
  a.[5].KeyCount |> shouldEqual 10000
  a.[5].[5000] |> shouldEqual 5000.0


let smallRowSparseFrame = 
  frame [ for k in ["A"; "B"; "C"] ->
            k => Series.ofValues [ for i in 0 .. 9 -> if i%2=0 then float i else nan ] ]
let bigRowSparseFrame = 
  frame [ for k in ['A' .. 'Z'] ->
            k => Series.ofValues [ for i in 0 .. 10000 -> if i%2=0 then float i else nan ] ]
let smallColSparseFrame = 
  frame [ for k in ["A"; "B"; "C"] ->
            k => Series.ofValues [ for i in 0 .. 9 -> if i%2=0||k<>"B" then float i else nan ] ]
let bigColSparseFrame = 
  frame [ for k in ['A' .. 'Z'] ->
            k => Series.ofValues [ for i in 0 .. 10000 -> if (int k)%3<>1 then float i else nan ] ]

[<Test;PerfTest(Iterations=10)>]
let ``Drop sparse rows from a small frame`` () =
  let a = Array.init 100 (fun _ -> smallRowSparseFrame |> Frame.dropSparseRows)
  a.[0].RowCount |> shouldEqual 5

[<Test;PerfTest(Iterations=15)>]
let ``Drop sparse rows from a large frame`` () =
  let a = bigRowSparseFrame |> Frame.dropSparseRows
  a.RowCount |> shouldEqual 5001

#if BELOW_0_9_13
#else
#if BELOW_1_1_1
#else
[<Test;PerfTest(Iterations=10)>]
let ``Drop sparse columns from a small frame`` () =
  let a = Array.init 1000 (fun _ -> smallColSparseFrame |> Frame.dropSparseCols) 
  a.[0].ColumnCount |> shouldEqual 2

[<Test;PerfTest(Iterations=20)>]
let ``Drop sparse columns from a large frame`` () =
  let a = bigColSparseFrame |> Frame.dropSparseCols
  a.ColumnCount |> shouldEqual 18
#endif
#endif