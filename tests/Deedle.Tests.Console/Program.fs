#if INTERACTIVE
#time
#I "../../bin"
#load "Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
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

//let s0 = series <| Array.init 1000 (fun i -> i => rnd.NextDouble())
let s1 = series <| Array.init 1000000 (fun i -> i => rnd.NextDouble())
//let s2 = series <| Array.init 10000000 (fun i -> i => rnd.NextDouble())

let testOne() =      
  timed 1 (fun () ->

    // 315ms
    //for i in 0 .. 1000 do (s0 |> Stats.movingMin 100 |> ignore)

    // 310ms
    s1 |> Stats.movingMin 100 |> ignore

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

//do testAll()
do testOne()
