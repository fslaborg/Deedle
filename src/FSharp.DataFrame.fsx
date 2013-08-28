#r "FSharp.Data.dll"
#r "FSharp.DataFrame.dll"
do fsi.AddPrinter(fun (printer:FSharp.DataFrame.Common.IFormattable) -> "\n" + (printer.Format()))