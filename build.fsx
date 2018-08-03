// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"
open System
open System.IO
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

let deedleExcelProject = "Deedle.Excel"
let deedleExcelSummary = "Deedle integration with Excel"
let deedleExcelDescription = """
  This package installs the core Deedle package, NetOffice.Excel, and a Deedle extension
  which makes it possible to send Deedle Frames to Excel."""
let deedleExcelTags = "Excel"

let gitHome = "https://github.com/fslaborg"
let gitName = "Deedle"




Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let desiredSdkVersion = "2.1.302"
let mutable sdkPath = None
let getSdkPath() = (defaultArg sdkPath "dotnet")

printfn "Desired .NET SDK version = %s" desiredSdkVersion
printfn "DotNetCli.isInstalled() = %b" (DotNetCli.isInstalled())
let useMsBuildToolchain = environVar "USE_MSBUILD" <> null

if DotNetCli.isInstalled() then 
    let installedSdkVersion = DotNetCli.getVersion()
    printfn "The installed default .NET SDK version reported by FAKE's 'DotNetCli.getVersion()' is %s" installedSdkVersion
    if installedSdkVersion <> desiredSdkVersion then
        match environVar "CI" with 
        | null -> 
            if installedSdkVersion > desiredSdkVersion then 
                printfn "*** You have .NET SDK version '%s' installed, assuming it is compatible with version '%s'" installedSdkVersion desiredSdkVersion 
            else
                printfn "*** You have .NET SDK version '%s' installed, we expect at least version '%s'" installedSdkVersion desiredSdkVersion 
        | _ -> 
            printfn "*** The .NET SDK version '%s' will be installed (despite the fact that version '%s' is already installed) because we want precisely that version in CI" desiredSdkVersion installedSdkVersion
            sdkPath <- Some (DotNetCli.InstallDotNetSDK desiredSdkVersion)
else
    printfn "*** The .NET SDK version '%s' will be installed (no other version was found by FAKE helpers)" desiredSdkVersion 
sdkPath <- Some (DotNetCli.InstallDotNetSDK desiredSdkVersion)

// Read release notes & version info from RELEASE_NOTES.md
let release = 
    File.ReadLines "RELEASE_NOTES.md" 
    |> ReleaseNotesHelper.parseReleaseNotes

let bindir = "./bin"

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

Target "Clean" <| fun () ->
    // have to clean netcore output directories because they corrupt the full-framework outputs
    seq {
        yield bindir
        yield! !!"**/bin"
        yield! !!"**/obj"
    } |> CleanDirs

Target "CleanDocs" (fun _ ->
    CleanDirs ["docs/output"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

let testProjs = 
    [ "tests/Deedle.Tests/Deedle.Tests.fsproj" 
      "tests/Deedle.CSharp.Tests/Deedle.CSharp.Tests.csproj" 
      "tests/Deedle.Documentation.Tests/Deedle.Documentation.Tests.fsproj"
      "tests/Deedle.PerfTests/Deedle.PerfTests.fsproj"
      "tests/Deedle.RPlugin.Tests/Deedle.RPlugin.Tests.fsproj"  ]

let testCoreProjs = 
    [ "tests/Deedle.Tests/Deedle.Tests.fsproj" 
      "tests/Deedle.CSharp.Tests/Deedle.CSharp.Tests.csproj" 
      "tests/Deedle.Documentation.Tests/Deedle.Documentation.Tests.fsproj"
      "tests/Deedle.PerfTests/Deedle.PerfTests.fsproj" ]      

let buildProjs =
    [ "src/Deedle/Deedle.fsproj"
      "src/Deedle.RProvider.Plugin/Deedle.RProvider.Plugin.fsproj"
      "src/Deedle.Excel/Deedle.Excel.fsproj" ]

let buildCoreProjs =
    [ "src/Deedle/Deedle.fsproj"    
      "src/Deedle.Excel/Deedle.Excel.fsproj" ]

Target "Build" <| fun () ->
    if useMsBuildToolchain then
        buildProjs |> Seq.iter (fun proj -> 
            DotNetCli.Restore  (fun p -> { p with Project = proj; ToolPath =  getSdkPath() }))

        buildProjs |> Seq.iter (fun proj ->
            let projName = System.IO.Path.GetFileNameWithoutExtension proj
            MSBuildReleaseExt null ["SourceLinkCreate", "true"] "Build" [proj]
            |> Log (sprintf "%s-Output:\t" projName))
    else
        buildProjs |> Seq.iter (fun proj -> 
        DotNetCli.RunCommand (fun p -> { p with ToolPath = getSdkPath() }) (sprintf "build -c Release \"%s\" /p:SourceLinkCreate=true" proj))

Target "BuildCore" <| fun () ->
    if useMsBuildToolchain then
        buildCoreProjs |> Seq.iter (fun proj -> 
            DotNetCli.Restore  (fun p -> { p with Project = proj; ToolPath =  getSdkPath() }))

        buildCoreProjs |> Seq.iter (fun proj ->
            let projName = System.IO.Path.GetFileNameWithoutExtension proj
            MSBuildReleaseExt null ["SourceLinkCreate", "true"] "Build" [proj]
            |> Log (sprintf "%s-Output:\t" projName))
    else
        buildCoreProjs |> Seq.iter (fun proj -> 
        DotNetCli.RunCommand (fun p -> { p with ToolPath = getSdkPath() }) (sprintf "build -c Release \"%s\" /p:SourceLinkCreate=true" proj))    



// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "BuildTests" <| fun () ->
    for testProj in testProjs do 
        if useMsBuildToolchain then
            DotNetCli.Restore (fun p -> { p with Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"] })
            MSBuildRelease null "Build" [testProj] |> Log "BuildTests.DesignTime-Output: "
        else
            DotNetCli.Build (fun p -> { p with Configuration = "Release"; Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"]; })

Target "BuildCoreTests" <| fun () ->
    for testProj in testCoreProjs do 
        if useMsBuildToolchain then
            DotNetCli.Restore (fun p -> { p with Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"] })
            MSBuildRelease null "Build" [testProj] |> Log "BuildTests.DesignTime-Output: "
        else
            DotNetCli.Build (fun p -> { p with Configuration = "Release"; Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"]; })

Target "RunTests" <| fun () ->
    let nunitRunnerPath = "packages/NUnit.ConsoleRunner/tools/nunit3-console.exe"
    if useMsBuildToolchain then
        ActivateFinalTarget "CloseTestRunner"
        (!! "tests/Deedle.*Tests/bin/Release/net45/Deedle*Tests*.dll" ++ 
            "tests/Deedle.*Tests/bin/Release/net461/Deedle*Tests*.dll")
        |> NUnit3 (fun p ->
            { p with
                ToolPath = nunitRunnerPath
                ShadowCopy = false })
    else
        for testProj in testCoreProjs do 
            DotNetCli.Test (fun p -> { p with Configuration = "Release"; Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"] })
        ActivateFinalTarget "CloseTestRunner"
        !! "tests/Deedle.RPlugin.Tests/bin/Release/net45/Deedle*Tests*.dll"
        |> NUnit3 (fun p ->
            { p with
                ToolPath = nunitRunnerPath
                ShadowCopy = false })

Target "RunCoreTests" <| fun () ->
    let nunitRunnerPath = "packages/NUnit.ConsoleRunner/tools/nunit3-console.exe"
    if useMsBuildToolchain then    
        ActivateFinalTarget "CloseTestRunner"
        (!! "tests/Deedle.*Tests/bin/Release/net45/Deedle*Tests*.dll" ++ 
            "tests/Deedle.*Tests/bin/Release/net461/Deedle*Tests*.dll")
        |> NUnit3 (fun p ->
            { p with
                ToolPath = nunitRunnerPath
                ShadowCopy = false })
    else
        for testProj in testCoreProjs do 
            DotNetCli.Test (fun p -> { p with Configuration = "Release"; Project = testProj; ToolPath = getSdkPath(); AdditionalArgs=["/v:n"] })

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit3-console.exe"
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
        ("nuget/Deedle.nuspec")
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
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = deedleExcelProject
            Summary = deedleExcelSummary
            Description = description + "\n\n" + deedleExcelDescription
            Version = release.NugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags + " " + deedleExcelTags
            OutputPath = "bin"            
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey" })
        ("nuget/Deedle.Excel.nuspec")
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
    let cmd = sprintf """commit -a -m "Update generated documentation for version %s""" release.NugetVersion
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

"BuildTests" ==> "All"
"RunTests" ==> "All"
"BuildCoreTests" ==> "AllCore"
"RunCoreTests" ==> "AllCore"

"All" ==> "NuGet" ==> "Release"
"All" 
  ==> "CleanDocs"
  ==> "GenerateDocs"
  ==> "ReleaseDocs"
  ==> "ReleaseBinaries"
  ==> "TagRelease"
  ==> "Release"

RunTargetOrDefault "AllCore"