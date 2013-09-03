module Main
#if INTERACTIVE
#I "..\\bin"
#load "DataFrame.fsx"
#endif

open FSharp.DataFrame
open System
let timed f = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn "%dms" sw.ElapsedMilliseconds
  res

do
  let msftCsv = Frame.ReadCsv(@"C:\Users\tpetricek\Downloads\msft.csv") //http://ichart.finance.yahoo.com/table.csv?s=MSFT")
  let fbCsv = Frame.ReadCsv(@"C:\Users\tpetricek\Downloads\fb.csv") //"http://ichart.finance.yahoo.com/table.csv?s=FB")
  let msftDate = timed (fun _ -> fbCsv.WithRowIndex<DateTime>("Date"))
  ()