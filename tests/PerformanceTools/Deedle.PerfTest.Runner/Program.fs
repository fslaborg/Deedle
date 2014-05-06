module Deedle.PerfTest.Runner

open System
open System.IO
open System.Linq.Expressions
open System.Reflection
open System.Diagnostics

// ------------------------------------------------------------------------------------------------
// Resolving and running performance tests
// ------------------------------------------------------------------------------------------------

/// Performance tests is a list of method infos with number of iterations
type PerfTests = list<string * Action * int>

/// Find all performance tests in a specified directory
/// (we use reflection to avoid library version mismatch)
let getPerformanceTests dir : PerfTests = 
  [ for library in Directory.GetFiles(dir, "*.dll") do
      let asm = Assembly.LoadFrom(library)
      for typ in asm.GetTypes() do 
        for mi in typ.GetMethods() do
          let attrs = mi.GetCustomAttributes()
          let perfAttrs = attrs |> Seq.choose (function
            | attr when attr.GetType().Name = "PerfTestAttribute" -> 
                let iter = attr.GetType().GetProperty("Iterations").GetValue(attr)
                Some (unbox<int> iter)
            | _ -> None)
          match List.ofSeq perfAttrs with
          | [iters] -> 
              let f = Expression.Lambda<Action>(Expression.Call(mi)).Compile()
              yield mi.Name, f, iters
          | _ -> () ] 

/// Run the specified tests and output time to the console
/// in a CSV format for furhter analysis
let evalPerformance (tests:PerfTests) =
  for name, f, iter in tests do 
    let sw = Stopwatch.StartNew()
    printfn "RUNNING: %s" name
    for i = 1 to iter * 10 do f.Invoke()
    printfn "DONE: %d" sw.ElapsedMilliseconds
  printfn "COMPLETED"
    
// ------------------------------------------------------------------------------------------------
// Usage:
//   perftest-runner.exe C:\directory\with\binaries\to\test
// ------------------------------------------------------------------------------------------------

[<EntryPoint>]
let main argv = 
  match argv with
  | [| dir |] -> getPerformanceTests dir |> evalPerformance
  | _ -> printfn "Error - directory with libraries not specified!"
  0