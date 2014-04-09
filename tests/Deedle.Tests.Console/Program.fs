#if INTERACTIVE
#time
#I "../../bin"
#load "Deedle.fsx"
#else
module Main
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
  let ordFrames =
    [| for i in 0 .. 500 ->
        [ for col in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ->
            col => series [  for j in 0 .. 100 -> (i, j) => 1.0 ] ] |> frame |]
  
  let unordFrames =
    [| for i in 0 .. 500 ->
        [ for col in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ->
            col => series [  for j in 0 .. 100 -> (i, 100-j) => 1.0 ] ] |> frame |]
      
  timed 1 (fun () ->
    
    // 0.32
    ordFrames.[0 .. 20] |> Frame.mergeAll |> ignore
    // 1.75
    ordFrames.[0 .. 50] |> Frame.mergeAll |> ignore

    // 0.32
    unordFrames.[0 .. 20] |> Frame.mergeAll |> ignore
    // 2.0
    unordFrames.[0 .. 50] |> Frame.mergeAll |> ignore

    // 0.40
    unordFrames |> Frame.mergeAll |> ignore

    
    let df1 = frame [ "A" => series [1=>1.0] ]
    let df2 = frame [ "B" => series [2=>1.0] ]
    
    df1.Merge(df2) |> ignore
    ()

  )

//do testAll()
do testOne()
