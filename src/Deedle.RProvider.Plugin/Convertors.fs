namespace Deedle.RProvider.Plugin

open Deedle
open RProvider
open System.ComponentModel.Composition

[<Export(typedefof<IConvertToR<Frame<_,_>>>)>]
[<Export(typeof<IDefaultConvertFromR>)>]
type Class1() = 
    member this.X = "F#"
