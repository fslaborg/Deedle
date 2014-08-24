// --------------------------------------------------------------------------------------
// Test that the documentation is generated correctly withtout F# errors 
// --------------------------------------------------------------------------------------
#if INTERACTIVE
#I "../../packages/FSharp.Formatting.2.4.21/lib/net40"
#I "../../packages/RazorEngine.3.3.0/lib/net40/"
#r "../../packages/Microsoft.AspNet.Razor.2.0.30506.0/lib/net40/System.Web.Razor.dll"
#r "../../packages/FSharp.Compiler.Service.0.0.59/lib/net40/FSharp.Compiler.Service.dll"
#r "RazorEngine.dll"
#r "FSharp.Literate.dll"
#r "FSharp.CodeFormat.dll"
#r "FSharp.MetadataFormat.dll"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#load "../Common/FsUnit.fs"
#else
module FSharp.Data.Tests.DocumentationTests
#endif

open FsUnit
open NUnit.Framework
open System
open System.IO
open System.Net
open System.Reflection
open FSharp.Literate
open FSharp.CodeFormat

// Initialization of the test - lookup the documentation files,
// create temp folder for the output and load the F# compiler DLL
let (@@) a b = Path.Combine(a, b)
let template = __SOURCE_DIRECTORY__ @@ "../../tools/template.html"
let sources = __SOURCE_DIRECTORY__ @@ "../../docs/content"
let output = Path.GetTempPath() @@ "Deedle.Docs"
if Directory.Exists(output) then Directory.Delete(output, true)
do Directory.CreateDirectory(output) |> ignore


/// Process a specified file in the documentation folder and return 
/// the total number of unexpected errors found (print them to the output too)
let processFile file =
  printfn "Processing '%s'" file

  let dir = Path.GetDirectoryName(Path.Combine(output, file))
  if not (Directory.Exists(dir)) then Directory.CreateDirectory(dir) |> ignore

  let evaluationErrors = ResizeArray()
#if INTERACTIVE
  let fsiEvaluator = FsiEvaluator()
  fsiEvaluator.EvaluationFailed |> Event.add evaluationErrors.Add
  let literateDoc = Literate.ParseScriptFile(Path.Combine(sources, file), fsiEvaluator = fsiEvaluator)
#else
  let literateDoc = Literate.ParseScriptFile(Path.Combine(sources, file)) 
#endif
  Seq.append
    (literateDoc.Errors 
     |> Seq.choose (fun (SourceError(startl, endl, kind, msg)) ->
       if msg <> "Multiple references to 'mscorlib.dll' are not permitted" then
         Some <| sprintf "%A %s (%s)" (startl, endl) msg file
       else None))
    (evaluationErrors |> Seq.map (fun x -> x.ToString()))
  |> String.concat "\n"

// ------------------------------------------------------------------------------------
// Core API documentation
// ------------------------------------------------------------------------------------

let docFiles = 
  seq { for sub in [ "."; ] do
          for file in Directory.EnumerateFiles(sources @@ sub, "*.fsx") do
            yield sub + "/" + Path.GetFileName(file) }

#if INTERACTIVE
for file in docFiles do 
    printfn "%s" (processFile file)
#else

[<Test>]
[<TestCaseSource "docFiles">]
let ``Documentation generated correctly `` file = 
  let errors = processFile file
  if errors <> "" then
    Assert.Fail("Found errors when processing file '" + file + "':\n" + errors)

#endif
