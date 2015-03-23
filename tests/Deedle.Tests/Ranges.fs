#if INTERACTIVE
#I "../../bin/"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Ranges
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Ranges
open Deedle.Indices.Virtual

// ------------------------------------------------------------------------------------------------
// Ranges
// ------------------------------------------------------------------------------------------------

let rng = Ranges.create [| (10L, 19L); (30L, 39L); (50L, 59L) |]

[<Test>]
let ``Restricting ranges using offset from beginning or end`` () =
  let res = rng |> Ranges.restrictRanges (RangeRestriction.Start(15L))
  res.Ranges |> shouldEqual [| (10L, 19L); (30L, 34L) |]
  let res = rng |> Ranges.restrictRanges (RangeRestriction.End(15L))
  res.Ranges |> shouldEqual [| (35L, 39L); (50L, 59L) |]

[<Test>]
let ``Restricting ranges using fixed restriction`` () =
  let res = rng |> Ranges.restrictRanges (RangeRestriction.Fixed(10L, 35L))
  res.Ranges |> shouldEqual [| (10L, 19L); (30L, 35L) |]
  let res = rng |> Ranges.restrictRanges (RangeRestriction.Fixed(35L, 59L))
  res.Ranges |> shouldEqual [| (35L, 39L); (50L, 59L) |]

[<Test>]
let ``Merging ranges joins ranges`` () = 
  let rng1 = Ranges.create [| (10L, 19L); (30L, 39L); (50L, 59L) |]
  let rng2 = Ranges.create [| (20L, 29L); (60L, 69L) |]
  let res = Ranges.combine [rng1; rng2]
  res.Ranges |> shouldEqual [| (10L, 39L); (50L, 69L) |]

[<Test>]
let ``Merging overlapping ranges fails`` () = 
  let rng1 = Ranges.create [| (10L, 19L); (30L, 39L); (50L, 59L) |]
  let rng2 = Ranges.create [| (20L, 29L); (45L, 69L) |]
  let rng3 = Ranges.create [| (19L, 20L) |]
  (fun _ -> Ranges.combine [rng1; rng2] |> ignore)
  |> shouldThrow<System.InvalidOperationException>
  (fun _ -> Ranges.combine [rng1; rng2] |> ignore)
  |> shouldThrow<System.InvalidOperationException>

[<Test>]
let ``Lookup around element inside the range works as expected`` () =
  rng |> Ranges.lookup 35L Lookup.Exact (fun _ _ -> true) 
  |> shouldEqual <| OptionalValue( (35L, 15L) )
  rng |> Ranges.lookup 35L Lookup.Greater (fun _ _ -> true) 
  |> shouldEqual <| OptionalValue( (36L, 16L) )
  rng |> Ranges.lookup 35L Lookup.Smaller (fun _ _ -> true)
  |> shouldEqual <| OptionalValue( (34L, 14L) )
  rng |> Ranges.lookup 35L Lookup.ExactOrGreater (fun k _ -> k > 51L)
  |> shouldEqual <| OptionalValue( (52L, 22L) )
  rng |> Ranges.lookup 35L Lookup.ExactOrSmaller (fun k _ -> k < 15L)
  |> shouldEqual <| OptionalValue( (14L, 4L) )

[<Test>]
let ``Lookup using key that is outside of the key range works as expected`` () =
  rng |> Ranges.lookup 1L Lookup.ExactOrGreater (fun _ _ -> true)
  |> shouldEqual <| OptionalValue( (10L, 0L) )
  rng |> Ranges.lookup 1L Lookup.ExactOrSmaller (fun _ _ -> true)
  |> shouldEqual OptionalValue.Missing
  rng |> Ranges.lookup 100L Lookup.ExactOrGreater (fun _ _ -> true)
  |> shouldEqual OptionalValue.Missing
  rng |> Ranges.lookup 100L Lookup.ExactOrSmaller (fun _ _ -> true)
  |> shouldEqual <| OptionalValue( (59L, 29L) )

[<Test>]
let ``Lookup using key that is between two parts of a range works as expected`` () =
  rng |> Ranges.lookup 25L Lookup.ExactOrGreater (fun _ _ -> true)
  |> shouldEqual <| OptionalValue( (30L, 10L) )
  rng |> Ranges.lookup 25L Lookup.ExactOrSmaller (fun _ _ -> true)
  |> shouldEqual <| OptionalValue( (19L, 9L) )
  rng |> Ranges.lookup 25L Lookup.Exact (fun _ _ -> true)
  |> shouldEqual <| OptionalValue.Missing
        
[<Test>]
let ``Getting key range works on sample input`` () =
  Ranges.keyRange rng |> shouldEqual (10L, 59L)

[<Test>]
let ``Key of address & address of key works on sample inputs`` () =
  Ranges.keyOfAddress 9L rng |> shouldEqual 19L
  Ranges.keyOfAddress 10L rng |> shouldEqual 30L
  Ranges.keyOfAddress 29L rng |> shouldEqual 59L
  Ranges.addressOfKey 19L rng |> shouldEqual 9L
  Ranges.addressOfKey 30L rng |> shouldEqual 10L
  Ranges.addressOfKey 59L rng |> shouldEqual 29L

[<Test>]
let ``Key of address & address of key fail as expected on sample inputs`` () =
  (fun () -> Ranges.keyOfAddress -1L rng |> ignore) 
  |> should throw typeof<System.IndexOutOfRangeException>
  (fun () -> Ranges.keyOfAddress 30L rng |> ignore) 
  |> should throw typeof<System.IndexOutOfRangeException>
  Ranges.addressOfKey 9L rng |> shouldEqual Ranges.invalid
  Ranges.addressOfKey 20L rng |> shouldEqual Ranges.invalid
  Ranges.addressOfKey 60L rng |> shouldEqual Ranges.invalid

[<Test>]
let ``Getting all keys from address returns expected keys`` () =
  [| for a in 0L .. 29L -> Ranges.keyOfAddress a rng |]
  |> shouldEqual <| Array.concat [ [| 10L .. 19L |]; [| 30L .. 39L |]; [| 50L .. 59L |] ]

[<Test>]
let ``Getting all keys using Range.keys returns expected keys`` () =
  Ranges.keys rng |> Array.ofSeq
  |> shouldEqual <| Array.concat [ [| 10L .. 19L |]; [| 30L .. 39L |]; [| 50L .. 59L |] ]