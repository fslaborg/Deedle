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
let timed f = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn "%dms" sw.ElapsedMilliseconds
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

  let s = series [ for k in 0 .. 100000 -> k => float k ]

//  s |> Series.chunkInto 100 Series.mean |> ignore
//  s |> Series.chunkWhileInto (fun k1 k2 -> k2 - k1 < 100) Series.mean |> ignore
//  s |> Series.windowWhileInto (fun k1 k2 -> k2 - k1 < 100) Series.mean |> ignore

  //let titanic = Frame.ReadCsv(__SOURCE_DIRECTORY__ + @"\..\..\docs\content\data\Titanic.csv")
  for i in 1 .. 5 do
    timed(fun () ->
       //let nada = s |> Series.windowInto 100 Series.mean

       let nada = s |> Series.windowInto 100 Series.mean |> ignore

       ///let nada2 = s |> Series.windowSizeInto (5, Boundary.Skip) (DataSegment.data >> Series.mean)
       ()
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
      //CSharp.Tests.DynamicFrameTests.CanGetSeriesDynamically()
//      Tests.Frame.``Can group 10x5k data frame by row of type string and nest it (in less than a few seconds)``()
//      Series(d1, d2).[300000.0 .. 600000.0] |> Series.filter (fun k _ -> true) |> Series.mean
//      |> ignore

    )

//do testAll()
do testOne()

