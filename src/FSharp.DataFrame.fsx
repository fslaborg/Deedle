#nowarn "211"
#I "../bin"
#I "../../packages/FSharp.DataFrame.0.9.2-beta/lib/net40"
#r "FSharp.DataFrame.dll"
module FsiAutoShow = 
  fsi.AddPrinter(fun (printer:FSharp.DataFrame.Internal.IFsiFormattable) -> "\n" + (printer.Format()))
