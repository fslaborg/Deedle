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
      for def in config.Defines.Defines do
        yield "--define:" + def.Symbol

      // Generate "-r" argument for all libraries to be referenced
      for ref in config.Reference.References do
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
        f.Invoke()
        let times = 
          [ for i in 1 .. iter -> 
              let sw = Stopwatch.StartNew()
              f.Invoke()
              float sw.ElapsedMilliseconds ]
        let mean = Seq.average times
        let sdv = sqrt ((times |> Seq.sumBy (fun v -> pown (v - mean) 2)) / (float iter))
        yield name, float mean, float sdv
        printfn " * %s (%f+/-%fms)" name mean sdv ]
    
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
// Generate 
// ------------------------------------------------------------------------------------------------

module internal Formatter =
  let generateChart data = 
    let fnum (v:float) = Math.Round(v, 2)
    let chartData =
      [ for dir, tests in data do
          for name, time, sdv in tests do
            yield sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%f, \"valuelo\":%f, \"valuehi\":%f}" dir name (fnum time) (fnum (time - sdv)) (fnum (time + sdv)) ]
      |> String.concat ", "
    let root = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
    let templ = Path.Combine(root, "template.html")
    File.ReadAllText(templ).Replace("***DATA***", chartData)

// ------------------------------------------------------------------------------------------------
// Entry point - parse command line & run it!
// ------------------------------------------------------------------------------------------------

type PerfTests = 
  /// Run performance testing.
  /// 
  /// ## Parameters
  ///  - `binRoot` - Specifies a directory with binaries. There should be one sub-directory
  ///    for each version, containing `perf.config` file. One of the directories should have
  ///    `baseline` in the name (to be used as the baseline)
  ///  - `sources` - List of source files containing tests (and other required code)
  ///  - `outFile` - Specifies the file name where the resulting output is stored
  ///
  static member Run(binRoot, sources, outFile) =
    // Get directories with libraries & find a baseline directory
    let baseline, versions =
      Directory.GetDirectories(binRoot)
      |> List.ofSeq
      |> List.partition (fun dir -> Path.GetFileName(dir).ToLower().Contains("baseline"))
  
    let versions = 
      match baseline with
      | [baseline] -> baseline::versions
      | _ -> failwith "Expected a single sub-folder with baseline in the name."

    let compiled = versions |> List.map (Compiler.compile sources)
    let results = (versions, compiled) ||> List.map2 PerfRunner.RunTests
    let data = List.zip (List.map Path.GetFileName versions) results
    let html = Formatter.generateChart data
    File.WriteAllText(outFile, html)

    use c = colored ConsoleColor.Yellow
    printfn "\nDone. Cleaning temp files"
    for library in compiled do File.Delete(library)

module Main = 
  [<EntryPoint>]
  let main argv = 
    let binRoot, outFile = 
      match List.ofSeq argv with 
      | bin::out::_ -> bin, out
      | _ ->
        System.Console.WriteLine
         ( "Directory with binaries or output not specified. The expected usage is:\n\n" +
           "   perftest C:\\your\\directory C:\\outfile.html \n\n" +
           "where 'C:\\your\\directory' contains a number of sub-directories\n" +
           "with different versions of the assemblies that you want to measure.\n" +
           "There should be at least one directory with 'baseline' in the name\n" +
           "which is used as the baseline for the comparison." ) 
        exit -1

    let sources = 
      [ @"C:\Tomas\Public\Deedle\tests\Common\FsUnit.fs"
        @"C:\Tomas\Public\Deedle\tests\Deedle.PerfTests\Performance.fs" ]
  
    PerfTests.Run(binRoot, sources, outFile)
    0