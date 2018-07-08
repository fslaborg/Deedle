// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"
open System
open Fake 
open Fake.Git
open Fake.ReleaseNotesHelper
open Fake.AssemblyInfoFile
open Fake.Testing

// --------------------------------------------------------------------------------------
// Information about the project to be used at NuGet and in AssemblyInfo files
// --------------------------------------------------------------------------------------

let project = "Deedle"
let authors = ["BlueMountain Capital";"FsLab"]
let summary = "Easy to use .NET library for data manipulation and scientific programming"
let description = """
  Deedle implements an efficient and robust frame and series data structures for 
  manipulating with structured data. It supports handling of missing values, 
  aggregations, grouping, joining, statistical functions and more. For frames and 
  series with ordered indices (such as time series), automatic alignment is also 
  available. """
let tags = "F# fsharp deedle dataframe series statistics data science"

let rpluginProject = "Deedle.RPlugin"
let rpluginSummary = "Easy to use .NET library for data manipulation with R project integration"
let rpluginDescription = """
  This package installs core Deedle package, together with an R type provider plugin 
  which makes it possible to pass data frames and time series between R and Deedle"""
let rpluginTags = "R RProvider"

let gitHome = "https://github.com/fslaborg"
let gitName = "Deedle"

let nunitRunnerPath = "packages/NUnit.ConsoleRunner/tools/nunit3-console.exe"

// --------------------------------------------------------------------------------------
// The rest of the code is standard F# build script 
// --------------------------------------------------------------------------------------

// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md")

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
  let fileName = "src/Deedle/Common/AssemblyInfo.fs"
  CreateFSharpAssemblyInfo fileName
      [ Attribute.Title project
        Attribute.Product project
        Attribute.Description summary
        Attribute.Version release.AssemblyVersion
        Attribute.FileVersion release.AssemblyVersion] 
)

// --------------------------------------------------------------------------------------
// Clean build results

Target "Clean" (fun _ ->
    CleanDirs ["bin"; "temp" ]

    CleanDirs [ "tests/Deedle.CSharp.Tests/bin" ]
    CleanDirs [ "tests/Deedle.RPlugin.Tests/bin" ]
    CleanDirs [ "tests/Deedle.Tests/bin" ]
    CleanDirs [ "tests/Deedle.Tests.Console/bin" ]
)

Target "CleanDocs" (fun _ ->
    CleanDirs ["docs/output"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->
    !! (project + ".sln")
      |> MSBuildRelease "" "Rebuild"
      |> Log "AppBuild-Output: "

    !! ("tests/PerformanceTools.sln")
      |> MSBuildRelease "" "Rebuild"
      |> Log "AppBuild-Output: "
  
    !! (project + ".Tests.sln")
      |> MSBuildRelease "" "Rebuild"
      |> Log "AppBuild-Output: "
)

Target "BuildCore" (fun _ ->
    !! (project + ".Core.sln")
      |> MSBuildRelease "" "Rebuild"
      |> Log "AppBuild-Output: "
  )

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    ActivateFinalTarget "CloseTestRunner"

    !! "tests/Deedle.*Tests/bin/Release/Deedle*Tests*.dll"
    |> NUnit3 (fun p ->
        { p with
            ToolPath = nunitRunnerPath
            ShadowCopy = false
            TimeOut = TimeSpan.FromMinutes 20.})
    )

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    // Format the description to fit on a single line (remove \r\n and double-spaces)
    let description = description.Replace("\r", "").Replace("\n", "").Replace("  ", " ")
    let rpluginDescription = rpluginDescription.Replace("\r", "").Replace("\n", "").Replace("  ", " ")
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = project
            Summary = summary
            Description = description
            Version = release.NugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey" })
        ("nuget/" + project + ".nuspec")
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = rpluginProject
            Summary = rpluginSummary
            Description = description + "\n\n" + rpluginDescription
            Version = release.NugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            Dependencies = 
              [ "Deedle", release.NugetVersion
                "R.NET.Community", GetPackageVersion "packages" "R.NET.Community"
                "R.NET.Community.FSharp", GetPackageVersion "packages" "R.NET.Community.FSharp"
                "RProvider", GetPackageVersion "packages" "RProvider" ]
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey" })
        ("nuget/Deedle.RPlugin.nuspec")
)

// --------------------------------------------------------------------------------------
// Generate the documentation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

// --------------------------------------------------------------------------------------
// Release Scripts

Target "ReleaseDocs" (fun _ ->
    Repository.clone "" (gitHome + "/" + gitName + ".git") "temp/gh-pages"
    Branches.checkoutBranch "temp/gh-pages" "gh-pages"
    CopyRecursive "docs/output" "temp/gh-pages" true |> printfn "%A"
    CommandHelper.runSimpleGitCommand "temp/gh-pages" "add ." |> printfn "%s"
    // let cmd = sprintf """commit -a -m "Update generated documentation for version %s""" release.NugetVersion
    let cmd = sprintf """commit -a -m "Fix http url to https for web page rendering"""  
    CommandHelper.runSimpleGitCommand "temp/gh-pages" cmd |> printfn "%s"
    Branches.push "temp/gh-pages"
)

Target "ReleaseBinaries" (fun _ ->
    Repository.clone "" (gitHome + "/" + gitName + ".git") "temp/release"
    Branches.checkoutBranch "temp/release" "release"
    
    // Delete old files and copy in new files
    !! "temp/release/*" |> DeleteFiles
    "temp/release/bin" |> CleanDir
    CopyRecursive "bin" "temp/release/bin" true |> printfn "%A"
    !! "temp/release/bin/*.nupkg" |> DeleteFiles
    "temp/release/bin/Deedle.fsx" |> MoveFile "temp/release"
    "temp/release/bin/RProvider.fsx" |> MoveFile "temp/release"

    CommandHelper.runSimpleGitCommand "temp/release" "add bin/*" |> printfn "%s"
    let cmd = sprintf """commit -a -m "Update binaries for version %s""" release.NugetVersion
    CommandHelper.runSimpleGitCommand "temp/release" cmd |> printfn "%s"
    Branches.push "temp/release"
)

Target "TagRelease" (fun _ ->
    // Concatenate notes & create a tag in the local repository
    let notes = (String.concat " " release.Notes).Replace("\n", ";").Replace("\r", "")
    let tagName = "v" + release.NugetVersion
    let cmd = sprintf """tag -a %s -m "%s" """ tagName notes
    CommandHelper.runSimpleGitCommand "." cmd |> printfn "%s"

    // Find the main remote (fslaborg GitHub)
    let _, remotes, _ = CommandHelper.runGitCommand "." "remote -v"
    let main = remotes |> Seq.find (fun s -> s.Contains("(push)") && s.Contains("fslaborg/Deedle"))
    let remoteName = main.Split('\t').[0]
    Fake.Git.Branches.pushTag "." remoteName tagName
)

Target "Release" DoNothing

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "All" DoNothing
Target "AllCore" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "All" 

"AssemblyInfo"
  ==> "BuildCore"
  ==> "AllCore"

"RunTests" ==> "All"
"RunTests" ==> "AllCore"

"All" ==> "NuGet" ==> "Release"
"All" 
  ==> "CleanDocs"
  ==> "GenerateDocs"
  ==> "ReleaseDocs"
  ==> "ReleaseBinaries"
  ==> "TagRelease"
  ==> "Release"

RunTargetOrDefault "All"
