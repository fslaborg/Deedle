// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I @"tools/FAKE/tools"
#r @"tools/FAKE/tools/FakeLib.dll"

open System
open System.IO
open Fake 
open Fake.AssemblyInfoFile
open Fake.Git

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let files includes = 
  { BaseDirectories = [__SOURCE_DIRECTORY__]
    Includes = includes
    Excludes = [] } |> Scan

// Information about the project to be used at NuGet and in AssemblyInfo files
let project = "FSharp.DataFrame"
let authors = ["Blue Mountain Capital"]
let summary = "Easy to use F# library for data manipulation and scientific programming"
let description = """
  The F# DataFrame library (FSharp.DataFrame.dll) implements an efficient and robust 
  data frame and series structures for manipulating with structured data. It supports
  handling of missing values, aggregations, grouping, joining, statistical functions and
  more. For frames and series with ordered indices (such as time series), automatic
  alignment is also available. """

let tags = "F# fsharp data frame series statistics science"

// Read additional information from the release notes document
let releaseNotes, version = 
    let lastItem = File.ReadLines "RELEASE_NOTES.md" |> Seq.last
    let firstDash = lastItem.IndexOf('-')
    ( lastItem.Substring(firstDash + 1 ).Trim(), 
      lastItem.Substring(0, firstDash).Trim([|'*'|]).Trim() )

// --------------------------------------------------------------------------------------
// Generate assembly info files with the right version & up-to-date information

Target "AssemblyInfo" (fun _ ->
  let fileName = "src/Common/AssemblyInfo.fs"
  CreateFSharpAssemblyInfo fileName
      [ Attribute.Title project
        Attribute.Product project
        Attribute.Description summary
        Attribute.Version version
        Attribute.FileVersion version] 
)

// --------------------------------------------------------------------------------------
// Clean build results

Target "Clean" (fun _ ->
    CleanDirs ["bin"]
)

Target "CleanDocs" (fun _ ->
    CleanDirs ["docs"]
)

// --------------------------------------------------------------------------------------
// Build library (builds Visual Studio solution, which builds multiple versions
// of the runtime library & desktop + Silverlight version of design time library)

Target "Build" (fun _ ->
    (files ["FSharp.DataFrame.sln"; "FSharp.DataFrame.Tests.sln"])
    |> MSBuildRelease "" "Rebuild"
    |> ignore
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = sprintf "packages/NUnit.Runners.%s/Tools" nunitVersion

    ActivateFinalTarget "CloseTestRunner"

    (files ["tests/FSharp.DataFrame.Tests/bin/Release/FSharp.DataFrame.Tests*.dll"])
    |> NUnit (fun p ->
        { p with
            ToolPath = nunitPath
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    // Format the description to fit on a single line (remove \r\n and double-spaces)
    let description = description.Replace("\r", "").Replace("\n", "").Replace("  ", " ")
    let nugetPath = ".nuget/nuget.exe"
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = project
            Summary = summary
            Description = description
            Version = version
            ReleaseNotes = releaseNotes
            Tags = tags
            OutputPath = "bin"
            ToolPath = nugetPath
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey"
            Dependencies = [] })
        "nuget/FSharp.DataFrame.nuspec"
)

// --------------------------------------------------------------------------------------
// Generate the documentation

Target "JustGenerateDocs" (fun _ ->
    executeFSI "tools" "build.fsx" [] |> ignore
)

Target "GenerateDocs" DoNothing
"CleanDocs" ==> "JustGenerateDocs" ==> "GenerateDocs"

// --------------------------------------------------------------------------------------
// Release Scripts

(*
Target "ReleaseDocs" (fun _ ->
    DeleteDir "gh-pages"
    Repository.clone "" "https://github.com/fsharp/FSharp.Data.git" "gh-pages"
    Branches.checkoutBranch "gh-pages" "gh-pages"
    CopyRecursive "docs" "gh-pages" true |> printfn "%A"
    CommandHelper.runSimpleGitCommand "gh-pages" (sprintf """commit -a -m "Update generated documentation for version %s""" version) |> printfn "%s"
    Branches.push "gh-pages"
)

Target "UpdateBinaries" (fun _ ->
    DeleteDir "release"
    Repository.clone "" "https://github.com/fsharp/FSharp.Data.git" "release"
    Branches.checkoutBranch "release" "release"
    CopyRecursive "bin" "release/bin" true |> printfn "%A"
    CommandHelper.runSimpleGitCommand "release" (sprintf """commit -a -m "Update binaries for version %s""" version) |> printfn "%s"
    Branches.push "release"
)

Target "Release" DoNothing
"GenerateDocs" ==> "UpdateDocs"
"UpdateDocs" ==> "Release"
"NuGet" ==> "Release"
"UpdateBinaries" ==> "Release"
*)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "All" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "GenerateDocs"
  ==> "RunTests"
  ==> "All"

RunTargetOrDefault "All"
