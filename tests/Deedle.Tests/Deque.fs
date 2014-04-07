#if INTERACTIVE
#load "../../bin/Deedle.fsx"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#r "../../packages/MathNet.Numerics.3.0.0-alpha8/lib/net40/MathNet.Numerics.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Deque
#endif

open System
open System.Linq
open System.Collections.Generic
open System.Globalization
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle

[<Test>]
let ``empty deque is empty`` () =
  let d = Deque()

  d.IsEmpty |> shouldEqual true
  
[<Test>]
let ``can add single element`` () =
  let d = Deque()

  d.Add(1)
  
  d.IsEmpty |> shouldEqual false 
  d.Count |> shouldEqual 1

[<Test>]
let ``can add elements`` () =
  let d = Deque()

  d.Add(1)
  d.Add(1)
  d.Add(2)
  d.Add(3)
  d.Add(4)
  
  d.IsEmpty |> shouldEqual false 
  d.Count |> shouldEqual 5

[<Test>]
let ``can add elements and remove from front`` () =
  let d = Deque()

  for x in 1..2049 do
      d.Add(x.ToString())
 
  d.IsEmpty |> shouldEqual false 
  d.Count |> shouldEqual 2049

  for x in 1..2049 do
      d.RemoveFirst() |> shouldEqual (x.ToString())

  d.IsEmpty |> shouldEqual true
  d.Count |> shouldEqual 0


[<Test>]
let ``can add elements and remove from back`` () =
  let d = Deque()

  for x in 1..2049 do
      d.Add(x)
 
  d.IsEmpty |> shouldEqual false 
  d.Count |> shouldEqual 2049

  for x in 1..2049 do
      d.RemoveLast() |> shouldEqual (2049-x+1)

  d.IsEmpty |> shouldEqual true
  d.Count |> shouldEqual 0


[<Test>]
let ``can add elements and interate`` () =
  let d = Deque()

  for x in 1..2049 do
      d.Add(x)
 
  let l = ref 1
  for x in d do
    x |> shouldEqual !l
    l := x + 1

[<Test>]
let ``can add elements and convert to list`` () =
  let d = Deque()

  for x in 1..2049 do
      d.Add(x)
 
  d |> Seq.toList |> shouldEqual [1..2049]