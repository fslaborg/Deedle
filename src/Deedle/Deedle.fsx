#nowarn "211"
// Try including various folders where Deedle might be (version updated by FAKE)
#I "../../bin"
#I "../bin"
#I "bin"
#I "lib"
#I "../packages/Deedle.1.0.6/lib/net40"
#I "../../packages/Deedle.1.0.6/lib/net40"
#I "../../../packages/Deedle.1.0.6/lib/net40"
// Also reference path with FSharp.Data.DesignTime.dll
#I "../FSharp.Data.2.0.14/lib/net40/"
// Reference Deedle
#r "Deedle.dll"

do fsi.AddPrinter(fun (printer:Deedle.Internal.IFsiFormattable) -> "\n" + (printer.Format()))
open Deedle
