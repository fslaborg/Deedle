module Main
#if INTERACTIVE
#I "..\\bin"
#load "DataFrame.fsx"
#endif

open System
open System.Linq.Expressions
open Deedle.Tests
open Deedle

// ------------------------------------------------------------------------------------------------
// Simple "test runner" for running tests in your IDE with just Run 
// (useful for performance analysis in VS)
// ------------------------------------------------------------------------------------------------

// Set console color temporarilly
let colored color = 
  let prev = Console.ForegroundColor
  Console.ForegroundColor <- color
  { new IDisposable with
      member x.Dispose() = Console.ForegroundColor <- prev }

// Measure how long does a test take
let timed k f = 
  let times = ResizeArray<_>()
  for i in 1 .. k do
    let sw = System.Diagnostics.Stopwatch.StartNew()
    let res = f()
    times.Add(float sw.ElapsedMilliseconds)
    printfn "%dms (Average: %dms)" sw.ElapsedMilliseconds (int64 (Seq.average times))
    res

// Measure how long does a test take
let timedFormat f fmt arg = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn fmt sw.ElapsedMilliseconds arg
  res

// Find all referenced tests
let testAll () =
  let asm = System.Reflection.Assembly.Load("Deedle.Tests")
  let tests =
    [ for typ in asm.GetTypes() do
        for mi in typ.GetMethods() do
          if mi.GetCustomAttributes(typeof<NUnit.Framework.TestAttribute>, false).Length <> 0 then
            yield typ.Name, mi ]
    |> Seq.groupBy fst

  // Run all tests and measure time
  for unit, tests in tests do
    ( use _n = colored ConsoleColor.White
      printfn "\nTESTING: %s" unit )
    for _, test in tests do 
      let call = Expression.Lambda<Action>(Expression.Call(null, test)).Compile()
      try 
        use _n = colored ConsoleColor.Green
        timedFormat call.Invoke " - [%dms] %s" test.Name
      with e -> 
        ( use _n = colored ConsoleColor.Red
          printfn " - %s (FAILED)" test.Name )
        ( use _n = colored ConsoleColor.Gray
          printfn "%A\n" e )

// ------------------------------------------------------------------------------------------------
// Run all tests, or run a single test
// ------------------------------------------------------------------------------------------------

open Deedle

let testOne() =
  //let d1 = Array.init 1000000 float
  //let d2 = Array.init 1000000 float

//  let s = series [ for k in 0 .. 100000 -> k => float k ]
(*
  let genRandomNumbers count =
    let rnd = System.Random()
    List.init count (fun _ -> (float <| rnd.Next()) / (float Int32.MaxValue))
  let r = genRandomNumbers 1000000
  let n = Series.ofValues(r)
  let f = Frame.ofColumns ["n" => n]
  *)

  let s1 = series [ for i in 0000001 .. 0100000 -> i => float i ]
  let s3 = series [ for i in 1000001 .. 1100000 -> i => float i ]
  let s4 = series [ for i in 2000001 .. 2100000 -> i => float i ]
  let s2 = series [ for i in 3000001 .. 3100000 -> i => float i ]

  //Deedle.Internal.Seq.useBinomial <- true

  // 570, 270
  printfn "Append few"
  timed 5 (fun _ -> 
    s1.Merge(s2, s3, s4) |> ignore
  )

  printfn "Append lots"
  timed 5 (fun _ -> 
      let mkSeries objId = Series([for j in 1..60 -> (objId, j)], [1..60])
      let h::tl = [1..1000] |> List.map mkSeries  
      h.Merge(tl |> Array.ofSeq) |> ignore
  )
//  s |> Series.chunkInto 100 Series.mean |> ignore
//  s |> Series.chunkWhileInto (fun k1 k2 -> k2 - k1 < 100) Series.mean |> ignore
//  s |> Series.windowWhileInto (fun k1 k2 -> k2 - k1 < 100) Series.mean |> ignore

  //let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + @"\..\..\docs\content\data\Titanic.csv")
  //let msft = Frame.ReadCsv(__SOURCE_DIRECTORY__ + @"\..\..\docs\content\data\stocks\msft.csv")
  //let rows = ResizeArray<_>()
  //let df = msft |> Frame.sortRowsByKey

  //let df = frame [ for c in ["A";"B";"C";"D";"E"] -> c => series [ for i in 0 .. 500000 -> i => float i  ]]
  
  (*
  let size = 1000000
  let rnd = Random()

  for count in 10 .. 30 .. 300 do
    let ss = Array.init count (fun _ -> ResizeArray<_>())
    for i in 1 .. size do
      ss.[rnd.Next(ss.Length)].Add( (i, float i) )
    let sser = Array.map series ss

    Deedle.Internal.Seq.useBinomial <- false
    printfn "Normal (%d):" count
    timed 2 (fun _ ->
      Series.appendN sser |> ignore )

    Deedle.Internal.Seq.useBinomial <- true
    printfn "Binomial (%d):" count
    timed 2 (fun _ ->
      Series.appendN sser |> ignore )

    printfn ""
    *)


  //timed 1 (fun () ->


       //let nada = s |> Series.windowInto 100 Series.mean

       //let nada = s |> Series.windowInto 100 Series.mean |> ignore

       //rows.Add(f.RowsDense)

      
      // 37 sec, 41 sec, 42 sec, 42 sec ~~~> sub 1sec
      //let mkSeries objId = Series([for j in 1..60 -> (objId, j)], [1..60])
      //let h::tl = [1..2000] |> List.map mkSeries  
      //h.Append(tl |> Array.ofSeq) |> ignore

      // 520ms // 1 sec
      //s1.Append(s2, s3) |> ignore

      // 926ms
      //s1.Append(s2, s3, s4, s5) |> ignore

      // 1930ms
      //s1.Append(s2, s3, s4, s5, s6, s7, s8) |> ignore

    //  )

      //df?A |> Series.windowInto 5 (fun _ -> 0.0) |> ignore // 1.2sec ~

      //df |> Frame.shift 1 |> ignore // 3.2s
      //df |> Frame.diff 1 |> ignore // 3.0s

      // #time 

      // 0.95
      //s1.Append(s2, s3, s4) |> ignore
      // 2.44
      //s1.Append(s2).Append(s3) |> ignore
      // 4.35
      //s1.Append(s2).Append(s3).Append(s4) |> ignore
      // 6.80
      //s1.Append(s2).Append(s3).Append(s4).Append(s5) |> ignore

      //s1.Append(s2, s3, s4, s5)


      ///let nada2 = s |> Series.windowSizeInto (5, Boundary.Skip) (DataSegment.data >> Series.mean)
  //    () 
       (*
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
        let summary = 
              [ "Survived (%)" => survivals?Survived / survivals?Total * 100.0
                "Died (%)" => survivals?Died/ survivals?Total * 100.0 ] |> frame
                *)
      //CSharp.Tests.DynamicFrameTests.CanAddSeriesDynamically()
      //CSharp.Tests.DynamicFrameTests.CanGetColumnDynamically()
//      Tests.Frame.``Can group 10x5k data frame by row of type string and nest it (in less than a few seconds)``()
//      Series(d1, d2).[300000.0 .. 600000.0] |> Series.filter (fun k _ -> true) |> Series.mean
//      |> ignore

    //)

//do testAll()
do testOne()

(*



let s1 = series [ for i in 0000001 .. 0300000 -> i => float i ]
let s2 = series [ for i in 1000001 .. 1300000 -> i => float i ]
let s3 = series [ for i in 2000001 .. 2300000 -> i => float i ]
let s4 = series [ for i in 3000001 .. 3300000 -> i => float i ]
let s5 = series [ for i in 4000001 .. 4300000 -> i => float i ]
let s6 = series [ for i in 5000001 .. 5300000 -> i => float i ]
let s7 = series [ for i in 6000001 .. 6300000 -> i => float i ]
let s8 = series [ for i in 7000001 .. 7300000 -> i => float i ]

let ss = 
  [ for i in 1 .. 64 ->
      series [ for j in 1000000*i+1 .. 1000000*i+10000 -> j => float j ] ]
// #time 

// 0.95 ~> 0.38
s1.Append(s2) |> ignore
// 2.44 ~> 1.00
s1.Append(s2).Append(s3) |> ignore
// 4.35 ~> 1.75
s1.Append(s2).Append(s3).Append(s4) |> ignore
// 6.80 ~> 2.69
s1.Append(s2).Append(s3).Append(s4).Append(s5) |> ignore

s1.Append(s2).Append(s3).Append(s4).Append(s5).Append(s6).Append(s7).Append(s8) |> ignore

// 15s
s1.Append(Array.ofSeq ss) |> ignore
// 29s
ss |> List.fold (fun (s:Series<_, _>) v -> s.Append(v)) s1 |> ignore

// Original in a new context
//
// 1.25
// 3.18
// 5.62
// 8.57

// Using sorted dictionary ( / better version with simpler "returnUsingAlignedSequences" )
//
// 1.35 / 1.19
// 3.30 / 3.10
// 5.95 / 5.60
// 9.65 / 8.90

// Using fancy general function
//
// 1.75
// 3.95
// 8.15
// 13.25

// 0.8 ~> 0.60
s1.Append(s2, s3) |> ignore
// 1.2 ~> 0.76
s1.Append(s2, s3, s4) |> ignore
// 1.8 ~> 1.0
s1.Append(s2, s3, s4, s5) |> ignore
// 3.55 ~> 1.8
s1.Append(s2, s3, s4, s5, s6, s7, s8) |> ignore

let r1 = series [ for i in 0000001 .. 0300000 -> i => float i ] |> Series.rev
let r2 = series [ for i in 1000001 .. 1300000 -> i => float i ] |> Series.rev
let r3 = series [ for i in 2000001 .. 2300000 -> i => float i ] |> Series.rev
let r4 = series [ for i in 3000001 .. 3300000 -> i => float i ] |> Series.rev 
let r5 = series [ for i in 4000001 .. 4300000 -> i => float i ] |> Series.rev
let r6 = series [ for i in 5000001 .. 5300000 -> i => float i ] |> Series.rev

// #time 

// 1.05
r1.Append(r2) |> ignore
// 2.62 ~> 2.82
r1.Append(r2).Append(r3) |> ignore
// 4.82 ~> 5.15
r1.Append(r2).Append(r3).Append(r4) |> ignore
// 7.55 ~> 8.10
r1.Append(r2).Append(r3).Append(r4).Append(r5) |> ignore

// 1.90
r1.Append(r2, r3) |> ignore
// 2.95
r1.Append(r2, r3, r4) |> ignore
// 4.10
r1.Append(r2, r3, r4, r5) |> ignore



*)
