#r "FSharp.DataFrame.dll"
do fsi.AddPrinter(fun (printer:FSharp.DataFrame.Internal.IFsiFormattable) -> "\n" + (printer.Format()))
