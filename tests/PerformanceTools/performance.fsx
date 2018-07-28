#I "../../bin/net45"
#I "../../packages/FAKE/tools"
#r "Deedle.dll"
#r "FakeLib.dll"
#r "bin/Deedle.PerfTest.dll"
#if INTERACTIVE_IN_VS
#load "Deedle.fsx" // Does not work when invoked via FAKE
#endif
open System
open System.IO
open Fake
open Deedle

let baseline = "1.0.0"
let builds = __SOURCE_DIRECTORY__ @@ "../Performance/builds/"
let outFile = __SOURCE_DIRECTORY__ @@ "../Performance/output.csv"
let sources = 
  [ __SOURCE_DIRECTORY__ @@ "../Common/FsUnit.fs"
    __SOURCE_DIRECTORY__ @@ "../Deedle.PerfTests/Performance.fs" ]


Target "RunTests" (fun _ -> 
  PerfTests.Run(builds, sources, outFile)
)

Target "GenerateAbsChart" (fun _ -> 
  let tests = Frame.ReadCsv(outFile)

  let aggregated = 
    tests
    |> Frame.groupRowsByString "Test"
    |> Frame.groupRowsByString "Version"
    |> Frame.getCol "Time"
    |> Series.applyLevel (fun (v,(t,_)) -> v, t) Stats.mean
    |> Series.mapKeys(fun (v, t) -> t, v)
    |> Series.sortByKey

  let chartData =
    [ for (test, version), value in aggregated |> Series.observations ->
        sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%.0f}" version test value ]
    |> String.concat ", "

  let templ = __SOURCE_DIRECTORY__ @@ "template-abs.html"
  let outHtml = __SOURCE_DIRECTORY__ @@ "../Performance/output-abs.html"
  File.WriteAllText(outHtml, File.ReadAllText(templ).Replace("***DATA***", chartData))
)

Target "GenerateRelChart" (fun _ -> 
  let tests = Frame.ReadCsv(outFile)

  let aggregated : Series<_, float> = 
    let avgs = tests.PivotTable<string, string, _>("Version", "Test", fun df -> df.GetColumn("Time") |> Stats.mean)  
    let baseline = avgs.Rows.[baseline].As<float>()
    avgs.Rows
    |> Series.mapValues (fun row -> row.As<float>() / baseline * 100.0)
    |> Frame.ofRows
    |> Frame.stack
    |> Frame.indexRowsUsing (fun row -> row.GetAs<string>("Column"), row.GetAs<string>("Row"))
    |> Frame.sortRowsByKey
    |> Frame.getCol "Value"

  let chartData =
    [ for (test, version), value in aggregated |> Series.observations ->
        sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%.2f}" version test value ]
    |> String.concat ", "

  let templ = __SOURCE_DIRECTORY__ @@ "template-rel.html"
  let outHtml = __SOURCE_DIRECTORY__ @@ "../Performance/output-rel.html"
  File.WriteAllText(outHtml, File.ReadAllText(templ).Replace("***DATA***", chartData))
)

Target "OpenChart" (fun _ ->
  let outHtml1 = __SOURCE_DIRECTORY__ @@ "../Performance/output-abs.html"
  let outHtml2 = __SOURCE_DIRECTORY__ @@ "../Performance/output-rel.html"
  System.Diagnostics.Process.Start(outHtml1) |> ignore
  System.Diagnostics.Process.Start(outHtml2) |> ignore
)

"RunTests" ==> "GenerateRelChart" ==> "GenerateAbsChart" ==> "OpenChart"
RunTargetOrDefault "OpenChart"