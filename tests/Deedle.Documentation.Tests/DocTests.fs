// --------------------------------------------------------------------------------------
// Test that the documentation is generated correctly withtout F# errors 
// --------------------------------------------------------------------------------------
#if INTERACTIVE
#I "../../packages/FSharp.Formatting/lib/net40"
#I "../../packages/RazorEngine/lib/net40/"
#r "../../packages/FSharp.Compiler.Service/lib/net40/FSharp.Compiler.Service.dll"
#r "System.Web.Razor.dll"
#r "RazorEngine.dll"
#r "FSharp.Literate.dll"
#r "FSharp.CodeFormat.dll"
#r "FSharp.MetadataFormat.dll"
#r "../../packages/NUnit/lib/nunit.framework.dll"
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

/// Represents evaluation or compilation error returned by `processFile`
type TestError =
  | EvaluationFailed of FsiEvaluationFailedInfo
  | CompileError of string * SourceError
  override x.ToString() = 
    match x with
    | EvaluationFailed(err) ->
        sprintf "%s: %A\nSource:\n%s\n\nError:%s" (defaultArg err.File "unknown") err.Exception.Message err.Text err.StdErr
    | CompileError(file, SourceError(startl, endl, kind, msg)) -> 
        sprintf "%s (%d:%d-%d:%d): %O - %s" file (fst startl) (snd startl) (fst endl) (snd endl) kind msg

/// Process a specified file in the documentation folder and return 
/// the total number of unexpected errors found (print them to the output too)
let processFile file =
  printfn "Processing '%s'" file
  let dir = Path.GetDirectoryName(Path.Combine(output, file))
  if not (Directory.Exists(dir)) then Directory.CreateDirectory(dir) |> ignore

  // Process the file and capture evaluation errors
  let evaluationErrors = ResizeArray()
  let fsiEvaluator = FsiEvaluator(fsiObj = FsiEvaluatorConfig.CreateNoOpFsiObject())
  fsiEvaluator.EvaluationFailed |> Event.add evaluationErrors.Add
  let literateDoc = Literate.ParseScriptFile(Path.Combine(sources, file), fsiEvaluator = fsiEvaluator)

  // Return compile & evaluation errors
  [ for (SourceError(startl, endl, kind, msg)) as err in literateDoc.Errors do
      if msg <> "Multiple references to 'mscorlib.dll' are not permitted" then
        yield CompileError(file, err)
    for err in evaluationErrors do
      yield EvaluationFailed(err) ]

// ------------------------------------------------------------------------------------
// Core API documentation
// ------------------------------------------------------------------------------------

let docFiles = 
  seq { for sub in [ "."; ] do
          for file in Directory.EnumerateFiles(sources @@ sub, "*.fsx") do
            yield sub + "/" + Path.GetFileName(file) }

#if INTERACTIVE
for file in docFiles do 
    let errors = processFile file
    errors |> Seq.map (sprintf "%O") |> String.concat "\n" |> printfn "%s"
#else

[<Test>]
[<TestCaseSource "docFiles">]
let ``Documentation generated correctly `` (file:string) = 
  let errors = 
    // WORKAROUND: The R type provider fails on Travis because it does not have R installed
    // (This should be removed once we close #91 in RProvider)
    if file.Contains("rinterop.fsx") && Type.GetType("Mono.Runtime") <> null then []
    else processFile file

  let errors =  
    // WORKAROUND: The R type provider does not seem to work in the NUnit context 
    // (it gives "System.Security.SecurityException : Type System.Runtime.Remoting.ObjRef 
    // and the types derived from it (such as System.Runtime.Remoting.ObjRef) are not permitted 
    // to be deserialized at this security level.) so ignore expected errors...
    if file.Contains("rinterop.fsx") then
      errors |> List.filter (function
        | CompileError(_, SourceError(_, _, _, msg)) ->
            not (msg.Contains("'datasets' is not defined") || msg.Contains("'base' is not defined") || 
              msg.Contains("'zoo' is not defined") || msg.Contains("'R' is not defined"))
        | EvaluationFailed _ -> false )
 
    elif (file.Contains("series.fsx") || file.Contains("tutorial.fsx")) && Type.GetType("Mono.Runtime") <> null then
      // WORKAROUND: FSharp.Charting which is used in some examples does not work on Mono
      // and so the evaluation fails for various reasons - ignore that for now
      // (and use Foogle.Charts instead in the future!)
      errors |> List.filter (function
        | CompileError _ -> true
        | EvaluationFailed _ -> false )

    else errors 

  if errors <> [] then
    let errors = errors |> Seq.map (sprintf "%O") |> String.concat "\n"
    Assert.Fail("Found errors when processing file '" + file + "':\n" + errors)

#endif
