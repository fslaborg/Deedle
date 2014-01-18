module Deedle.PerfTest.Main

open System
open System.IO
open System.Reflection
open System.Diagnostics

// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// Generate 
// ------------------------------------------------------------------------------------------------

let generateChart data = 
  let chartData =
    [ for dir, tests in data do
        for name, time in tests do
          yield sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%f}" dir name time ]
    |> String.concat ", "
  let root = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
  let templ = Path.Combine(root, "template.html")
  File.ReadAllText(templ).Replace("***DATA***", chartData)

// ------------------------------------------------------------------------------------------------
// Entry point - parse command line & run it!
// ------------------------------------------------------------------------------------------------

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

  // Get directories with libraries & find a baseline directory
  let baseline, versions =
    Directory.GetDirectories(binRoot)
    |> List.ofSeq
    |> List.partition (fun dir -> Path.GetFileName(dir).ToLower().Contains("baseline"))
  
  let baseline, versions = 
    match baseline with
    | [baseline] -> baseline, versions
    | _ -> failwith "Expected just a single sub-folder with baseline in the name."

  let results = baseline::versions |> List.map evalPerformance
  let html = generateChart results
  File.WriteAllText(outFile, html)
  0