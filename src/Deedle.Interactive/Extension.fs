namespace Deedle.Interactive

open System
open System.Threading.Tasks
open Microsoft.DotNet.Interactive
open Microsoft.DotNet.Interactive.Formatting
open Deedle
open Deedle.Internal


type FormatterKernelExtension() =

  interface IKernelExtension with
    member _.OnLoadAsync _ =
      Formatter.Register<IFrameFormattable>(
        Action<_, _>
          (fun item (writer: IO.TextWriter) ->
            writer.Write(item |> Deedle.Interactive.Formatters.frameToHtmlTable)),
        "text/html"
      )
      Task.CompletedTask
