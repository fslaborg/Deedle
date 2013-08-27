module Main
#if INTERACTIVE
#I "..\\bin"
#load "DataFrame.fsx"
#endif

open System
open FSharp.DataFrame

let timed f = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn "%dms" sw.ElapsedMilliseconds
  res

let dateRange (first:System.DateTime) count = 
  seq { for i in 0 .. (count - 1) -> first.AddDays(float i) }

let rand count = 
  let rnd = System.Random()
  seq { for i in 0 .. (count - 1) -> rnd.NextDouble() }

[<EntryPoint>]
let main argv = 
  let tsOld = Series(dateRange (DateTime(2000,1,1)) 1000, rand 1000)
  let tsNew = Series(dateRange (DateTime(2000,1,1)) 1000, rand 1000)
  let df = Frame(["sierrats"; "olympus";"sierrats2"; "olympus2"], [tsOld; tsNew; tsOld; tsNew])
  let str = timed (fun () ->
      ((df :> Common.IFormattable).Format())
    )
  0
