#r "../../packages/FAKE/tools/FakeLib.dll"
#load "../../bin/Deedle.fsx"
#r "bin/Deedle.PerfTest.dll"
open System
open System.IO
open Fake
open Deedle

let builds = __SOURCE_DIRECTORY__ @@ "../Performance/builds/"
let outFile = __SOURCE_DIRECTORY__ @@ "../Performance/output.csv"
let sources = 
  [ __SOURCE_DIRECTORY__ @@ "../Common/FsUnit.fs"
    __SOURCE_DIRECTORY__ @@ "../Deedle.PerfTests/Performance.fs" ]
  
PerfTests.Run(builds, sources, outFile)

let tests = Frame.ReadCsv(outFile)

tests.PivotTable<string, string, _>("Version", "Test", fun df ->
  df.GetColumn("Time") |> Stats.mean,
  df.GetColumn("Time") |> Stats.stdDev,
  df.GetColumn("Time") |> Stats.count
)

let aggregated = 
  tests
  |> Frame.groupRowsByString "Test"
  |> Frame.groupRowsByString "Version"
  |> Frame.getCol "Time"
  |> Series.applyLevel (fun (v,(t,_)) -> v, t) (fun s -> 
      Stats.count s, Stats.mean s, Stats.stdDev s)
  |> Frame.ofRecords
  |> Frame.indexColsWith ["Count"; "Mean"; "StdDev"]

let chartData =
  [ for (version, test), row in aggregated.Rows |> Series.observations ->
      let v, sdv = row.GetAs<float>("Mean"), row.GetAs<float>("StdDev")
      sprintf "{\"dir\":\"%s\", \"test\":\"%s\", \"value\":%.0f, \"valuelo\":%.0f, \"valuehi\":%.0f}" version test v (v-sdv) (v+sdv) ]
  |> String.concat ", "

let templ = __SOURCE_DIRECTORY__ @@ "template.html"
let outHtml = Path.ChangeExtension(outFile, "html")
File.WriteAllText(outHtml, File.ReadAllText(templ).Replace("***DATA***", chartData))
