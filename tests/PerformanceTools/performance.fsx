#I "../../bin"
#I "../../packages/FAKE/tools"
#load "Deedle.fsx"
#r "FakeLib.dll"
#r "bin/Deedle.PerfTest.dll"
open System
open System.IO
open Fake
open Deedle

let builds = __SOURCE_DIRECTORY__ @@ "../Performance/builds/"
let outFile = __SOURCE_DIRECTORY__ @@ "../Performance/output.csv"
let outHtml = Path.ChangeExtension(outFile, "html")
let sources = 
  [ __SOURCE_DIRECTORY__ @@ "../Common/FsUnit.fs"
    __SOURCE_DIRECTORY__ @@ "../Deedle.PerfTests/Performance.fs" ]

Target "RunTests" (fun _ -> 
  PerfTests.Run(builds, sources, outFile)
)

Target "GenerateChart" (fun _ -> 
  let tests = Frame.ReadCsv(outFile)

  let aggregated = 
    tests
    |> Frame.groupRowsByString "Test"
    |> Frame.groupRowsByString "Version"
    |> Frame.getCol "Time"
    |> Series.applyLevel (fun (v,(t,_)) -> v, t) (fun s -> 
        Stats.count s, Stats.mean s, Stats.stdDev s)
    |> Frame.ofRecords
    |> Frame.indexColsWith ["Count"; "Mean"; "StdDev"]

  let aggregated = 
    let avgs = tests.PivotTable<string, string, _>("Version", "Test", fun df -> df.GetColumn("Time") |> Stats.mean)  
    let baseline = avgs.Rows.["0.9.12-baseline"].As<float>()
    let res = 
      avgs.Rows
      |> Series.mapValues (fun row -> row.As<float>() / baseline * 100.0)
      |> Frame.ofRows
      |> Frame.stack
      |> Frame.indexRowsUsing (fun row -> row.GetAs<string>("Row"), row.GetAs<string>("Column"))
      |> Frame.sortRows "Value" 
    res.RenameColumn("Value", "Mean")
    res.AddColumn("StdDev", res.GetColumn<float>("Mean"))
    res

  let chartData =
    [ for (version, test), row in aggregated.Rows |> Series.observations ->
        let v, sdv = row.GetAs<float>("Mean"), row.GetAs<float>("StdDev")
        sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%.1f, \"valuelo\":%.0f, \"valuehi\":%.0f}" version test v (v-sdv) (v+sdv) ]
    |> String.concat ", "

  let templ = __SOURCE_DIRECTORY__ @@ "template.html"
  File.WriteAllText(outHtml, File.ReadAllText(templ).Replace("***DATA***", chartData))
)

Target "OpenChart" (fun _ ->
  System.Diagnostics.Process.Start(outHtml)
  |> ignore
)

"RunTests" ==> "GenerateChart" ==> "OpenChart"
RunTargetOrDefault "OpenChart"