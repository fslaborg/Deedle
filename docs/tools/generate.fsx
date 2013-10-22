// --------------------------------------------------------------------------------------
// Builds the documentation from FSX files in the 'samples' directory
// (the documentation is stored in the 'docs' directory)
// --------------------------------------------------------------------------------------

#I "../../packages/RazorEngine.3.3.0/lib/net40/"
#I "../../packages/FSharp.Formatting.2.0.2/lib/net40"

#r "FSharp.Literate.dll"
#r "FSharp.CodeFormat.dll"
#r "FSharp.MetadataFormat.dll"
open System.IO
open FSharp.Literate
open FSharp.MetadataFormat

let (++) a b = Path.Combine(a, b)
let project  = __SOURCE_DIRECTORY__ ++ "../../"
let template = __SOURCE_DIRECTORY__ ++ "template.html"
let sources  = __SOURCE_DIRECTORY__ ++ "../content"
let output   = __SOURCE_DIRECTORY__ ++ "../output"

// When running locally, you can use your path
//let root = @"file://C:/dev/FSharp.DataFrame/docs"
//let root = @"file://C:/Tomas/Projects/FSharp.DataFrame/docs"
let root = "http://BlueMountainCapital.github.io/Deedle"

let buildReference () = 
  // Build the API reference documentation
  if not (Directory.Exists(output ++ "reference")) then  
    Directory.CreateDirectory(output ++ "reference") |> ignore
  MetadataFormat.Generate
    ( project ++ "bin" ++ "Deedle.dll", 
      output ++ "reference", project ++ "docs" ++ "tools" ++ "reference" )

let build () =
  // Copy all sample data files to the "data" directory
  let copy = [ sources ++ "data", output ++ "data"
               sources ++ "../../packages/FSharp.Formatting.2.0.2/literate/content", output ++ "content"
               sources ++ "../files/images", output ++ "images" ]
  for source, target in copy do
    if Directory.Exists target then Directory.Delete(target, true)
    Directory.CreateDirectory target |> ignore
    for fileInfo in DirectoryInfo(source).EnumerateFiles() do
        fileInfo.CopyTo(target ++ fileInfo.Name) |> ignore

  // Generate HTML from all FSX files in samples & subdirectories
  for sub in [ "."; "samples" ] do
    Literate.ProcessDirectory
      ( sources ++ sub, template, output ++ sub, 
        replacements = [ "root", root ] )

// Generate 
build()
buildReference()