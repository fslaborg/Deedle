#nowarn "211"
// Standard NuGet or Paket location
#I "."
#I "lib/net45"

// Try various folders that people might like
#I "bin/net45"
#I "../bin/net45"
#I "../../bin/net45"
#I "lib"
// Reference Deedle
#r "Deedle.dll"

do fsi.AddPrinter(fun (printer:Deedle.Internal.IFsiFormattable) -> "\n" + (printer.Format()))
open Deedle
