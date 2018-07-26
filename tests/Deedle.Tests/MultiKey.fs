module Deedle.Tests.MultiKey

#if INTERACTIVE
#I "../../bin/net45"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle
open Deedle.Internal

let sampleKey1 = 'a', "hi", 1
let sampleKey2 = 'a', ("hi", 1)

[<Test>]
let ``Sample multi-level key matches templates with holes``() =
  (Lookup1Of3 'a').Matches(sampleKey1) |> shouldEqual true
  (Lookup2Of3 "hi").Matches(sampleKey1) |> shouldEqual true
  (Lookup3Of3 1).Matches(sampleKey1) |> shouldEqual true
  (Lookup1Of3 'a').Matches(sampleKey2) |> shouldEqual true
  (Lookup2Of3 "hi").Matches(sampleKey2) |> shouldEqual true
  (Lookup3Of3 1).Matches(sampleKey2) |> shouldEqual true

[<Test>]
let ``Sample multi-Lookup key does not match templates with other values``() =
  (Lookup1Of3 '!').Matches(sampleKey1) |> shouldEqual false
  (Lookup2Of3 "hi!").Matches(sampleKey1) |> shouldEqual false
  (Lookup3Of3 999).Matches(sampleKey1) |> shouldEqual false
  (Lookup1Of3 '!').Matches(sampleKey2) |> shouldEqual false
  (Lookup2Of3 "hi!").Matches(sampleKey2) |> shouldEqual false
  (Lookup3Of3 999).Matches(sampleKey2) |> shouldEqual false
