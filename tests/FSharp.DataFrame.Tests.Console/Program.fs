module Main
#if INTERACTIVE
#I "..\\bin"
#load "DataFrame.fsx"
#endif

open System
open FSharp.DataFrame.Tests
open FSharp.DataFrame

let timed f = 
  let sw = System.Diagnostics.Stopwatch.StartNew()
  let res = f()
  printfn "%dms" sw.ElapsedMilliseconds
  res

do
  for i in 1 .. 6 do
    timed(fun () -> 
      //Tests.Frame.``Can perform numerical operation with a scalar on data frames``()
      //Tests.Frame.``Can perform pointwise numerical operations on two frames`` ()
      Tests.Frame.``Can perform pointwise numerical operations on two frames``()
    )

