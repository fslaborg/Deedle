module Deedle.PerfTest.Runner
(*
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
*)

    
open System
open System.Reflection


open Microsoft.FSharp.Compiler.SimpleSourceCodeServices
open System.IO


open System
open System.IO
open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

let scs = SimpleSourceCodeServices()

(* 
let references = 
  [ @"C:\Tomas\Public\Deedle\packages\FsCheck.0.9.1.0\lib\net40-Client\FsCheck.dll"
    @"C:\Tomas\Public\Deedle\packages\FSharp.Data.2.0.5\lib\net40\FSharp.Data.dll"
    @"C:\Tomas\Public\Deedle\packages\MathNet.Numerics.3.0.0-beta01\lib\net40\MathNet.Numerics.dll"
    @"C:\Tomas\Public\Deedle\packages\NUnit.2.6.3\lib\nunit.framework.dll"
    @"C:\Tomas\Public\Deedle\tests\PerformanceTools\bin\Deedle.PerfTest.Core.dll" ]
*)

let sources = 
  [ @"C:\Tomas\Public\Deedle\tests\Common\FsUnit.fs" ]

let references = 
  [ yield! Directory.GetFiles(@"C:\Tomas\Public\Deedle\tests\Performance\builds\0.9.13", "*.dll")
    yield @"C:\Tomas\Public\Deedle\tests\PerformanceTools\bin\Deedle.PerfTest.Core.dll" ]

let compile () = 
  let file = @"C:\Tomas\Public\Deedle\tests\Deedle.PerfTests\Performance.fs"
  let tempDll = Path.GetTempFileName() + ".dll"
  let commandLine = 
    [| yield "fsc.exe"
       yield "-a"
       yield! [ "-o"; tempDll ]
       for ref in references do 
          if ref.Contains("FSharp.Data") |> not then
            yield! [ "-r"; ref ]
       for src in sources do yield src
       yield file |]
  let errors1, exitCode1 = scs.Compile(commandLine)
  errors1 |> Seq.iter (printfn "%A\n")



type private Foo() =
  inherit MarshalByRefObject()
  member x.Bar() = 
    //let fsi = 
    [| for a in AppDomain.CurrentDomain.GetAssemblies() -> a.FullName |]

let test () =
  let domain = AppDomain.CreateDomain("PerfTest")
  try
    let f = domain.CreateInstanceFromAndUnwrap(Assembly.GetExecutingAssembly().Location, typeof<Foo>.FullName) :?> Foo
    printfn "New app domain"
    f.Bar() |> Seq.iter (printfn "%s")

    printfn "\n\nCurrent app domain"
    (new Foo()).Bar() |> Seq.iter (printfn "%s")
  finally
    AppDomain.Unload domain

// ------------------------------------------------------------------------------------------------
// Usage:
//   perftest-runner.exe C:\directory\with\binaries\to\test
// ------------------------------------------------------------------------------------------------

[<EntryPoint>]
let main argv = 
  compile ()

  //match argv with
  //| [| dir |] -> test() // getPerformanceTests dir |> evalPerformance
  //| _ -> printfn "Error - directory with libraries not specified!"
  0