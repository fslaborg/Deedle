// --------------------------------------------------------------------------------------
// Builds the documentation from `.fsx` and `.md` files in the 'docs/content' directory
// (the generated documentation is stored in the 'docs/output' directory)
// --------------------------------------------------------------------------------------

// Binaries that have XML documentation (in a corresponding generated XML file)
let referenceBinaries = [ "Deedle.dll" ]
// Web site location for the generated documentation
let website = "https://fslab.org/Deedle"

// Specify more information about your project
let info =
  [ "project-name", "Deedle"
    "project-author", "BlueMountain Capital, FsLab.Org"
    "project-summary", "Easy to use .NET library for data manipulation and scientific programming"
    "project-github", "http://github.com/fslaborg/Deedle"
    "project-nuget", "https://nuget.org/packages/Deedle" ]

// --------------------------------------------------------------------------------------
// For typical project, no changes are needed below
// --------------------------------------------------------------------------------------

#load "formatters.fsx"
#load "../../.fake/build.fsx/intellisense.fsx"
#if !FAKE
let execContext = Fake.Core.Context.FakeExecutionContext.Create false "generate.fsx" []
Fake.Core.Context.setExecutionContext (Fake.Core.Context.RuntimeContext.Fake execContext)
#endif
open Fake.Core
open System.IO
open Fake.IO.FileSystemOperators
open Fake.IO
open FSharp.Formatting.Razor

// When called from 'build.fsx', use the public project URL as <root>
// otherwise, use the current 'output' directory.
#if RELEASE
let root = website
#else
let root = "file://" + (__SOURCE_DIRECTORY__ @@ "../output")
#endif

// Paths with template/source/output locations
let bin        = __SOURCE_DIRECTORY__ @@ "../../bin/net45"
let content    = __SOURCE_DIRECTORY__ @@ "../content"
let output     = __SOURCE_DIRECTORY__ @@ "../output"
let files      = __SOURCE_DIRECTORY__ @@ "../files"
let templates  = __SOURCE_DIRECTORY__ @@ "templates"
let formatting = __SOURCE_DIRECTORY__ @@ "../../packages/FSharp.Formatting/"
let docTemplate = formatting @@ "templates/docpage.cshtml"

// Where to look for *.csproj templates (in this order)
let layoutRoots =
  [ templates; formatting @@ "templates"
    formatting @@ "templates/reference" ]

// Copy static files and CSS + JS from F# Formatting
let copyFiles () =
  Shell.copyRecursive files output true |> Trace.logItems "Copying file: "
  Directory.ensure (output @@ "content")
  Shell.copyRecursive (formatting @@ "styles") (output @@ "content") true 
    |> Trace.logItems "Copying styles and scripts: "

// Based on https://github.com/fsprojects/ProjectScaffold/pull/135
let references =
  if Environment.isMono then
    // Workaround compiler errors in Razor-ViewEngine
    let d = RazorEngine.Compilation.ReferenceResolver.UseCurrentAssembliesReferenceResolver()
    let loadedList = d.GetReferences () |> Seq.map (fun r -> r.GetFile()) |> Seq.cache
    // We replace the list and add required items manually as mcs doesn't like duplicates...
    let getItem name = loadedList |> Seq.find (fun l -> l.Contains name)
    [ 
      (getItem "FSharp.Core").Replace("4.3.0.0", "4.4.1.0")
      Path.GetFullPath(formatting @@ "../FSharp.Compiler.Service/lib/net45/FSharp.Compiler.Service.dll")
      Path.GetFullPath(formatting @@ "lib/net40/System.Web.Razor.dll")
      Path.GetFullPath(formatting @@ "lib/net40/RazorEngine.dll")
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.Literate.dll")
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.CodeFormat.dll")
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.Markdown.dll")      
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.Formatting.Common.dll")
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.Formatting.Razor.dll")
      Path.GetFullPath(formatting @@ "lib/net40/FSharp.MetadataFormat.dll") ]
    |> Some
  else None

// Build API reference from XML comments
let buildReference () =
  Shell.cleanDir (output @@ "reference")
  for lib in referenceBinaries do
    RazorMetadataFormat.Generate
      ( bin @@ lib, output @@ "reference", layoutRoots, 
        parameters = ("root", root)::info,
        sourceRepo = "https://github.com/fslaborg/Deedle/tree/master/",
        sourceFolder = __SOURCE_DIRECTORY__.Substring(0, __SOURCE_DIRECTORY__.Length - "\docs\tools".Length),
        ?assemblyReferences = references )

// Build documentation from `fsx` and `md` files in `docs/content`
let buildDocumentation () =
  Shell.copyFile content (__SOURCE_DIRECTORY__ @@ "../../RELEASE_NOTES.md")
  let fsiEvaluator = Formatters.createFsiEvaluator root output "#.####"
  let subdirs = Directory.EnumerateDirectories(content, "*", SearchOption.AllDirectories)
  for dir in Seq.append [content] subdirs do
    let sub = if dir.Length > content.Length then dir.Substring(content.Length + 1) else "."
    RazorLiterate.ProcessDirectory
      ( dir, docTemplate, output @@ sub, replacements = ("root", root)::info,
        layoutRoots = layoutRoots, fsiEvaluator = fsiEvaluator, generateAnchors = true,
        ?assemblyReferences = references )

// Generate
copyFiles()
buildDocumentation()
buildReference()