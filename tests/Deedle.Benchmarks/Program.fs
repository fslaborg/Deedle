module Deedle.Benchmarks.Main

open BenchmarkDotNet.Running
open Deedle.Benchmarks

[<EntryPoint>]
let main argv =
    BenchmarkSwitcher
        .FromAssembly(typeof<FrameBenchmarks>.Assembly)
        .Run(argv)
    |> ignore
    0
