#nowarn "211"
// Standard NuGet or Paket location
#I "."
#I "lib/netstandard2.0"

// Try various folders that people might like
#I "bin/netstandard2.0"
#I "../bin/netstandard2.0"
#I "../../bin/netstandard2.0"
#I "lib"
// Reference Deedle
#r "Deedle.dll"

do fsi.AddPrinter(fun (printer:Deedle.Internal.IFsiFormattable) -> "\n" + (printer.Format()))
open Deedle
