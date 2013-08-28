#I "lib/net40"
#load "literate/literate.fsx"
open FSharp.Literate

let build source =
  let file = __SOURCE_DIRECTORY__ + "\\..\\samples\\" + source
  let template = __SOURCE_DIRECTORY__ + "\\template.html"
  Literate.ProcessScriptFile(file, template)

build "Tutorial.fsx"