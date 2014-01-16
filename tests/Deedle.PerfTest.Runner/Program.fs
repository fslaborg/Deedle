module Deedle.PerfTest.Runner

open System
open System.IO
open System.Linq.Expressions
open System.Reflection
open Deedle.PerfTest.Core

// ------------------------------------------------------------------------------------------------
// Resolving and running performance tests
// ------------------------------------------------------------------------------------------------

/// Performance tests is a list of method infos with number of iterations
type PerfTests = list<MethodInfo * int>

/// Find all performance tests in a specified directory
/// (we use reflection to avoid library version mismatch)
let getPerformanceTests dir : PerfTests = 
  [ for library in Directory.GetFiles(dir, "*.dll") do
      printfn "%A" library
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
          | [iters] -> yield mi, iters
          | _ -> () ] 

let buildFunction mi = 
  ()  

let evalPerformance (tests:PerfTests) =
  for it in tests do printfn "%A" it

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