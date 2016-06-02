#if INTERACTIVE
#time
#I "../../bin"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module Main
#endif

open System
open System.Linq.Expressions
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

let rnd = System.Random(0)
(*
let s0 = series <| Array.init 10000 (fun i -> i => rnd.NextDouble())
let f0 = frame [ for i in 0 .. 10 -> i => s0 ]
let f1 = frame [ for i in 0 .. 100 -> i => s0 ]

let s1 = series <| Array.init 1000000 (fun i -> i*2 => rnd.NextDouble())
let s2 = series <| Array.init 1000000 (fun i -> i*2+1 => rnd.NextDouble())

let ss1 = series <| Array.init 100 (fun i -> i*2 => rnd.NextDouble())
let ss2 = series <| Array.init 100 (fun i -> i*2+1 => rnd.NextDouble())
*)
let testOne() =      
  let file = "c:/temp/test.csv"
  //System.IO.File.WriteAllLines(file, Array.create 1000000 "20160114,ABC,acc12345,entity llc,Joe Doe,default,port1,FWD,ABC.TO,CAD")
  timed 1 (fun () ->
    let df = 
      Frame.ReadCsv(file, hasHeaders = false, schema = "report_date,source_system,account,legal_entity,trader,strategy,portfolio,security_type,security,currency")
    ()  
  )
  //Console.ReadLine() |> ignore
(*
  // 970 ~~> 457
  printfn "Creating lots of small series"
  timed 10 (fun () ->
    let vs = [ for i in 0 .. 10 -> i => float i ]
    Array.init 100000 (fun _ -> series vs) |> ignore
  )
  // 196 ~~> 122
  printfn "Creating few large series"
  timed 10 (fun () ->
    let vs = [ for i in 0 .. 10000 -> i => float i ]
    Array.init 100 (fun _ -> series vs) |> ignore
  )
*)
(*
  timed 5 (fun () ->
    s1.ZipInner(s1)
    |> ignore
  )
  timed 5 (fun () ->
    s1.ZipInner(s1)
    |> ignore
  )
*)
  // 1288ms
  // 1286ms

(*
  timed 10 (fun () ->
    s1.Select(fun kvp -> kvp.Value * 2.0) |> ignore

    // Selection
    // Sorting

    // 390
  )

  timed 10 (fun () ->
    for i in 0 .. 5000 do 
      ss1.Select(fun kvp -> kvp.Value * 2.0) |> ignore

    // 177
  )
*)
  ()  
//  Deedle.Tests.Frame.``Applying (+) on frame & series introduces missing values``()

(*
  printfn "Slow KeyCount"
  timed 5 (fun () ->
    f1.Rows.KeyCount |> ignore
  )
  printfn "Fast KeyCount"
  timed 5 (fun () ->
    f1.FastRows.KeyCount |> ignore
  )
  
  printfn "Slow iterate"
  timed 5 (fun () ->
    f1.Rows |> Series.mapValues (fun r -> r.GetAs<float>(5)) |> ignore
  )
  printfn "Fast iterate"
  timed 5 (fun () ->
    f1.FastRows |> Series.mapValues (fun r -> r.GetAs<float>(5)) |> ignore
  )
*)

  (*
  timed 5 (fun () ->

    ()
  
    // 75 ms ~> 40 ms
    //f0 + s0 |> ignore

    // 750 ms ~> 360 ms
    //f1 + s0 |> ignore

    // 315ms
    //for i in 0 .. 1000 do (s0 |> Stats.movingMin 100 |> ignore)

    // 310ms
    //s1 |> Stats.movingMin 100 |> ignore

    // 190ms
    //s1 |> Stats.expandingMin |> ignore
    // 1950ms
    //s2 |> Stats.expandingMin |> ignore

    // 325ms
    //s1 |> Stats.movingMin 100000 |> ignore

    // 320ms
    //s1 |> Stats.movingMin 500000 |> ignore

    // 3250ms
    //s2 |> Stats.movingMin 100 |> ignore
    ()

  )
  *)

//do testAll()
do testOne()