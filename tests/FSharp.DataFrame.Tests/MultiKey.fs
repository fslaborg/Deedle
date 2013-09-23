module FSharp.DataFrame.Tests.MultiKey

#if INTERACTIVE
#r "../../bin/FSharp.DataFrame.dll"
#r "../../packages/NUnit.2.6.2/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open FSharp.DataFrame
open FSharp.DataFrame.Internal

let sampleKey = MultiKey('a', MultiKey("hi", 1)) :> ICustomKey<_>

[<Test>]
let ``Sample multi-level key matches templates with holes``() =
  sampleKey.Matches(Level2Of3 "hi") |> shouldEqual true
  sampleKey.Matches(Level3Of3 1) |> shouldEqual true
  sampleKey.Matches(Level1Of3 'a') |> shouldEqual true

[<Test>]
let ``Sample multi-level key does not match templates with other values``() =
  sampleKey.Matches(Level2Of3 "!hi") |> shouldEqual false
  sampleKey.Matches(Level3Of3 10) |> shouldEqual false
  sampleKey.Matches(Level1Of3 '!') |> shouldEqual false
