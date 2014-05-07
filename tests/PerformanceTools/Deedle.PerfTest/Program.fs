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
      yield! [ "-r"; @"C:\Program Files (x86)\Reference Assemblies\Microsoft\FSharp\.NETFramework\v4.0\4.3.0.0\FSharp.Core.dll"]
      yield! [ "-r"; @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\mscorlib.dll" ]
      yield! [ "-r"; @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.dll" ]
      yield! [ "-r"; @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Core.dll" ]
      yield! [ "-r"; @"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.dll" ]

      // Generate "-r" argument for all libraries to be referenced
      for ref in config.Reference.References do
        for lib in refLibs do 
          if lib.Contains(ref.Name) then yield! ["-r"; lib]
      // Generate "-r" argument for Deedle.PerfTest.Core (with the attribute)
      let asm = typeof<Deedle.PerfTest.PerfTestAttribute>.Assembly
      yield! ["-r"; asm.Location]

      // Specify all the sources
      for src in sources do yield src }

    ( use c = colored ConsoleColor.Yellow
      printfn "Compiling %s\n%s" (Path.GetFileName(refpath)) (String.concat " " commandLine) )
    let errors1, exitCode1 = scs.Compile(Array.ofSeq commandLine)
    ( use c = colored ConsoleColor.Red
      errors1 |> Seq.iter (printfn "%A\n") )

// ------------------------------------------------------------------------------------------------
(*
/// Matches a string that starts with a given prefix & returns the rest
let (|StartsWith|_|) prefix (s:string) = 
  if s.StartsWith(prefix) then Some(s.Substring(prefix.Length)) else None

/// Run the perftest-runner on the specified folder & collect results
let evalPerformance folder = 
  let fg = Console.ForegroundColor
  Console.ForegroundColor <- ConsoleColor.Yellow
  printfn "\nEvaluating folder: %s" (Path.GetFileName(folder))
  Console.ForegroundColor <- fg

  // Agent that reads all performance results from the standard output
  let rows = ResizeArray<_>()
  let readerAgent = MailboxProcessor<string>.Start(fun inbox ->
    let rec waiting () = inbox.Scan(function
      | StartsWith "RUNNING: " name -> Some(running name)
      | _ -> None)
    and running name = inbox.Scan(function
      | StartsWith "DONE: " time -> Some(async { 
          printfn " - %s (%dms)" name (int time)
          rows.Add(name, float time)
          return! waiting() })
      | _ -> None)
    waiting ())

  let ps = 
    ProcessStartInfo
      ("perftest-runner.exe", folder, RedirectStandardOutput=true, UseShellExecute=false)
  let p = Process.Start(ps) 
  p.OutputDataReceived.Add(fun e -> readerAgent.Post(e.Data))
  p.BeginOutputReadLine()
  p.WaitForExit()
  Path.GetFileName(folder), rows :> seq<_>
*)
// ------------------------------------------------------------------------------------------------
// Generate 
// ------------------------------------------------------------------------------------------------
(*
let generateChart data = 
  let chartData =
    [ for dir, tests in data do
        for name, time in tests do
          yield sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%f}" dir name time ]
    |> String.concat ", "
  let root = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
  let templ = Path.Combine(root, "template.html")
  File.ReadAllText(templ).Replace("***DATA***", chartData)
*)
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
  static member Run(binRoot, sources) =
    // Get directories with libraries & find a baseline directory
    let baseline, versions =
      Directory.GetDirectories(binRoot)
      |> List.ofSeq
      |> List.partition (fun dir -> Path.GetFileName(dir).ToLower().Contains("baseline"))
  
    let baseline, versions = 
      match baseline with
      | [baseline] -> baseline, versions
      | _ -> failwith "Expected a single sub-folder with baseline in the name."

    printfn "%A\n%A" baseline versions
    baseline::versions |> List.iter (Compiler.compile sources)
    //let results = baseline::versions |> List.map evalPerformance
    //let html = generateChart results
    //File.WriteAllText(outFile, html)

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
  
    PerfTests.Run(binRoot, sources)        
    0