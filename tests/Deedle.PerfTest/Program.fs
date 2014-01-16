module Deedle.PerfTest.Main

open System.IO
open System.Reflection

// ------------------------------------------------------------------------------------------------
// Entry point - parse command line & run it!
// ------------------------------------------------------------------------------------------------

[<EntryPoint>]
let main argv = 
  let binRoot = 
    match List.ofSeq argv with 
    | bin::_ -> bin
    | _ ->
      System.Console.WriteLine
       ( "Directory with binaries not specified. The expected usage is:\n\n" +
         "   perftool C:\\your\\directory\n\n" +
         "where 'C:\\your\\directory' contains a number of sub-directories\n" +
         "with different versions of the assemblies that you want to measure.\n" +
         "There should be at least one directory with 'baseline' in the name\n" +
         "which is used as the baseline for the comparison." ) 
      exit -1

  // Get directories with libraries & find a baseline directory
  let baseline, versions =
    Directory.GetDirectories(binRoot)
    |> List.ofSeq
    |> List.map (fun dir -> dir, -1)
    |> List.partition (fun (dir, _) -> Path.GetFileName(dir).ToLower().Contains("baseline"))
  
  let baseline, versions = 
    match baseline with
    | [_, baseline] -> baseline, List.map snd versions
    | _ -> failwith "Expected just a single sub-folder with baseline in the name."

  //evalPerformance baseline
  0