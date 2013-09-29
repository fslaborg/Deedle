// --------------------------------------------------------------------------------------
// Builds the documentation from FSX files in the 'samples' directory
// (the documentation is stored in the 'docs' directory)
// --------------------------------------------------------------------------------------

#I "../packages/FSharp.Formatting.1.0.15/lib/net40"
#load "../packages/FSharp.Formatting.1.0.15/literate/literate.fsx"
#r "FSharp.MetadataFormat.dll"
open System.IO
open FSharp.Literate
open FSharp.MetadataFormat

let (++) a b = Path.Combine(a, b)
let project  = __SOURCE_DIRECTORY__ ++ "../"
let template = __SOURCE_DIRECTORY__ ++ "template.html"
let sources  = __SOURCE_DIRECTORY__ ++ "../samples"
let output   = __SOURCE_DIRECTORY__ ++ "../docs"

// When running locally, you can use your path
let root = @"file://C:\dev\FSharp.DataFrame\docs"
//let root = @"file://C:\Tomas\Projects\FSharp.DataFrame\docs"
//let root = "http://tpetricek.github.io/FSharp.DataFrame/hidden-doc-link"

let buildReference () = 
  // Build the API reference documentation
  if not (Directory.Exists(output ++ "reference")) then  
    Directory.CreateDirectory(output ++ "reference") |> ignore
  MetadataFormat.Generate
    ( project ++ "bin" ++ "FSharp.DataFrame.dll", 
      output ++ "reference", project ++ "tools" ++ "reference" )

let build () =
  // Copy all sample data files to the "data" directory
  let copy = [ sources ++ "data", output ++ "data"
               sources ++ "../tools/content", output ++ "content"
               sources ++ "../tools/content/images", output ++ "content/images" ]
  for source, target in copy do
    if Directory.Exists target then Directory.Delete(target, true)
    Directory.CreateDirectory target |> ignore
    for fileInfo in DirectoryInfo(source).EnumerateFiles() do
        fileInfo.CopyTo(target ++ fileInfo.Name) |> ignore

  // Generate HTML from all FSX files in samples & subdirectories
  for sub in [ "." ] do
    Literate.ProcessDirectory
      ( sources ++ sub, template, output ++ sub, 
        replacements = [ "root", root ] )

// Generate 
build()
buildReference()