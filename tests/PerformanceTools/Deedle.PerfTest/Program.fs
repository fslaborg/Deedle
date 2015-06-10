namespace Deedle

open System
open System.IO
open System.Reflection
open System.Diagnostics

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

module Utils =
  let (@@) a b = Path.Combine(a, b)

  // Set console color temporarilly
  let colored color = 
    let prev = Console.ForegroundColor
    Console.ForegroundColor <- color
    { new IDisposable with
        member x.Dispose() = Console.ForegroundColor <- prev }

// ------------------------------------------------------------------------------------------------
// Compile performance tests against the references in a specified folder
// ------------------------------------------------------------------------------------------------
open Utils

module internal Compiler = 
  open FSharp.Data
  open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

  type PerfConfig = XmlProvider<"perf.config">

  let scs = SimpleSourceCodeServices()
  let programFiles = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86)
  let microsoft =  programFiles @@ "Reference Assemblies" @@ "Microsoft"

  /// Given a list of source files & directory with references 
  /// (for a specific version of the tested library), compile!
  let compile sources refpath = 
    let tempDll = Path.GetTempFileName() + ".dll"
    let config = PerfConfig.Load(refpath @@ "perf.config")
    let refLibs = Directory.GetFiles(refpath, "*.dll")

    let commandLine = seq {
      // Compile as library and save it in temp directory
      yield "fsc.exe"
      yield "-a"
      yield! [ "-o"; tempDll ]

      yield "--nowarn:52"
      yield "--noframework"
      yield! [ "-r"; microsoft @@ "FSharp" @@ ".NETFramework" @@ "v4.0" @@ "4.3.0.0" @@ "FSharp.Core.dll"]
      yield! [ "-r"; microsoft @@"Framework" @@ ".NETFramework" @@ "v4.5" @@ "mscorlib.dll" ]
      yield! [ "-r"; microsoft @@"Framework" @@ ".NETFramework" @@ "v4.5" @@ "System.dll" ]
      yield! [ "-r"; microsoft @@"Framework" @@ ".NETFramework" @@ "v4.5" @@ "System.Core.dll" ]
      yield! [ "-r"; microsoft @@"Framework" @@ ".NETFramework" @@ "v4.5" @@ "System.Data.dll" ]

      // Generate "--define" argument for all symbols
      for def in config.Defines do
        yield "--define:" + def.Symbol

      // Generate "-r" argument for all libraries to be referenced
      for ref in config.References do
        for lib in refLibs do 
          if lib.Contains(ref.Name) then yield! ["-r"; lib]
      // Generate "-r" argument for Deedle.PerfTest.Core (with the attribute)
      let asm = typeof<Deedle.PerfTest.PerfTestAttribute>.Assembly
      yield! ["-r"; asm.Location]

      // Specify all the sources
      for src in sources do yield src }

    // Compile, report possible errors and reutrn the DLL
    ( use c = colored ConsoleColor.Yellow
      printfn "\nCompiling: %s" (Path.GetFileName(refpath)) )
    printfn "%s" (String.concat " " commandLine)

    let errors1, exitCode1 = scs.Compile(Array.ofSeq commandLine)
    if exitCode1 > 0 then
      use c = colored ConsoleColor.Red
      errors1 |> Seq.iter (printfn "%A\n") 
      failwith "Compilation failed!"
    
    tempDll

// ------------------------------------------------------------------------------------------------
// Resolving and running performance tests
// ------------------------------------------------------------------------------------------------
open Deedle
open System.Linq.Expressions

/// Runner that is created & executed in a separate AppDomain
type private PerfRunner() =
  inherit MarshalByRefObject()

  /// Find all performance tests in a specified directory
  /// (we use reflection to avoid library version mismatch)
  let getPerformanceTests (asm:Assembly) : list<string * Action * int> = 
    [ for typ in asm.GetTypes() do 
        for mi in typ.GetMethods() do
          let attrs = mi.GetCustomAttributes()
          let perfAttrs = attrs |> Seq.choose (function
            | :? Deedle.PerfTest.PerfTestAttribute as pe ->
                Some (pe.Iterations)
            | _ -> None)
          match List.ofSeq perfAttrs with
          | [iters] -> 
              let f = Expression.Lambda<Action>(Expression.Call(mi)).Compile()
              yield mi.Name, f, iters
          | _ -> () ] 

  /// Run the specified tests and output time to the console
  /// in a CSV format for furhter analysis
  let evalPerformance (tests:list<string * Action * int>) =
    [ for name, f, iter in tests do 
        printf " * %s" name
        f.Invoke()
        let times = 
          [ for i in 1 .. iter -> 
              printf "."
              let sw = Stopwatch.StartNew()
              f.Invoke()
              float sw.ElapsedMilliseconds ]
        let mean = Seq.average times
        let sdv = sqrt ((times |> Seq.sumBy (fun v -> pown (v - mean) 2)) / (float iter))
        for time in times do yield name, time
        printfn " %s (%f+/-%fms)" name mean sdv ]
    
  /// Evaluate performance for the specified library
  member x.Run(library) = 
    Assembly.LoadFrom(library) 
    |> getPerformanceTests
    |> evalPerformance 

  /// Eavluate performance of a single library and return list of tests & times
  static member RunTests refpath library =
    ( use c = colored ConsoleColor.Yellow
      printfn "\nRunning perf tests: %s" (Path.GetFileName(refpath)) )
    // Create new AppDomain and call the PerfRunner
    // (Resolve references in the 'refpath' folder)
    let ads = AppDomainSetup(ApplicationBase=refpath)
    let domain = AppDomain.CreateDomain("PerfTest_" + Path.GetFileName(refpath), null, ads)
    try
      let loc = Assembly.GetExecutingAssembly().Location
      let f = domain.CreateInstanceFromAndUnwrap(loc, typeof<PerfRunner>.FullName) :?> PerfRunner
      f.Run(library)
    finally
      AppDomain.Unload domain

// ------------------------------------------------------------------------------------------------
// Entry point - parse command line & run it!
// ------------------------------------------------------------------------------------------------

type PerfTests = 
  /// Run performance testing.
  /// 
  /// ## Parameters
  ///  - `binRoot` - Specifies a directory with binaries. There should be one sub-directory
  ///    for each version, containing `perf.config` file. 
  ///  - `sources` - List of source files containing tests (and other required code)
  ///  - `outFile` - Specifies the file name where the resulting output is stored
  ///
  static member Run(binRoot, sources, outFile:string) =
    let versions = Directory.GetDirectories(binRoot)
    let compiled = versions |> Array.map (Compiler.compile sources)
    let results = 
      [ for ver, compiled in Array.zip versions compiled do
          for test, time in PerfRunner.RunTests ver compiled do
            yield Path.GetFileName ver, test, time ]
      |> Frame.ofRecords
      |> Frame.indexColsWith [ "Version"; "Test"; "Time" ]
    results.SaveCsv(outFile)

    use c = colored ConsoleColor.Yellow
    printfn "\nDone. Cleaning temp files"
    for library in compiled do File.Delete(library)