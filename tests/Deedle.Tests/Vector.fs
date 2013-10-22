module Deedle.Tests.Vector

#if INTERACTIVE
#r "../../bin/Deedle.dll"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#endif

open System
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle
open Deedle.Internal

[<Test>]
let ``Equality on vectors works as expected`` () = 
  let sample = Vector.ofValues [ 1 .. 10 ]
  sample |> shouldEqual sample

[<Test>]
let ``Double.NaN is turned into a missing value`` () = 
  let actual = Vector.ofValues [ 1.0; Double.NaN; 10.1 ]
  let expected = Vector.ofOptionalValues [ Some 1.0; None; Some 10.1 ]
  actual |> shouldEqual expected

[<Test>]
let ``null of Nullable type is turned into a missing value`` () = 
  let actual = Vector.ofValues [ Nullable(1.0); unbox null; Nullable(10.1) ]
  let expected = Vector.ofOptionalValues [ Some (Nullable 1.0); None; Some (Nullable 10.1) ]
  actual |> shouldEqual expected

[<Test>]
let ``Select method correctly turns Double.NaN into a missing value`` () = 
  let actual = (Vector.ofValues [ 1.0 .. 10.0 ]).Select(fun v -> Double.NaN)
  let expected = Vector.ofOptionalValues [ for i in 1 .. 10 -> None ]
  actual |> shouldEqual expected
