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

frame [
    "C1" => series ["R1" => 1; "R2" => 2]
    "C2" => series ["R1" => 3; "R2" => 4]
    "C3" => series ["R1" => 5; "R2" => 6]
    "C4" => series ["R1" => 7; "R2" => 8]
    "C5" => series ["R1" => 9; "R2" => 10]
]

frame [
    for i in 0 .. 100 ->
        i => series [for ii in 0 .. 100 -> ii => ii]
]

series ["R1" => 9; "R2" => 10]

series [for ii in 0 .. 100 -> ii => if ii % 10 = 0 then nan else float ii]
