#if INTERACTIVE
#load "../../bin/Deedle.fsx"
#r "../../packages/NUnit/lib/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Series
#endif

open System
open System.Linq
open System.Collections.Generic
open System.Globalization
open FsUnit
open FsCheck
open NUnit.Framework

open Deedle

// ------------------------------------------------------------------------------------------------
// Indexing and accessing values
// ------------------------------------------------------------------------------------------------

let unordered = series [ 3 => "hi"; 2 => "bye"; 1 => "ciao"; 5 => "nazdar" ]
let sortedByKey = series [ 1 => "ciao"; 2 => "bye"; 3 => "hi"; 5 => "nazdar" ]
let sortedByVal = series [ 2 => "bye"; 1 => "ciao"; 3 => "hi"; 5 => "nazdar" ]

let ordered = series [ 1 => "hi"; 2 => "bye"; 3 => "ciao"; 5 => "nazdar" ]
let missing = series [ 1 => "hi"; 2 => null; 3 => "ciao"; 5 => "nazdar" ]
let usCulture = CultureInfo.GetCultureInfo("en-us")
let parseDateUSA s = DateTime.Parse(s,usCulture)

let ascending = series [ 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
let descending = series [ 3 => 3.0; 2 => 2.0; 1 => 1.0 ]
let randomOrder = series [ 2 => 2.0; 3 => 3.0; 1 => 1.0 ]

let ascendingMissing = series [ 0 => nan; 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
let descendingMissing = series [ 0 => nan; 3 => 3.0; 2 => 2.0; 1 => 1.0 ]
let randomOrderMissing = series [ 2 => 2.0; 3 => 3.0; 0 => nan; 1 => 1.0 ]

[<Test>]  
let ``Can access elements in ordered and unordered series`` () =
  unordered.[3] |> shouldEqual "hi"
  ordered.[3] |> shouldEqual "ciao"

[<Test>]  
let ``Accessing missing value or using out of range key throws`` () =
  (fun () -> missing.[2] |> ignore) |> should throw typeof<MissingValueException>
  (fun () -> missing.[7] |> ignore) |> should throw typeof<KeyNotFoundException>

[<Test>]  
let ``Can access elements by address`` () =
  unordered.GetAt(0) |> shouldEqual "hi"
  ordered.GetAt(0) |> shouldEqual "hi"
  missing.TryGetAt(1).HasValue |> shouldEqual false

[<Test>]  
let ``Can lookup previous and next elements (inclusively) in ordered series`` () =
  ordered.Get(4, Lookup.ExactOrGreater) |> shouldEqual "nazdar"
  ordered.Get(4, Lookup.ExactOrSmaller) |> shouldEqual "ciao"

[<Test>]  
let ``Can lookup previous and next elements (exclusively) in ordered series`` () =
  ordered.Get(3, Lookup.Greater) |> shouldEqual "nazdar"
  ordered.Get(3, Lookup.Smaller) |> shouldEqual "bye"

[<Test>]
let ``Can get first and last key of an ordered series`` () =
  ordered |> Series.firstKey |> shouldEqual 1
  ordered |> Series.lastKey |> shouldEqual 5
  
[<Test>]
let ``Can get first and last key of an unordered series`` () =
  unordered |> Series.firstKey |> shouldEqual 3
  unordered |> Series.lastKey |> shouldEqual 5

[<Test>]
let ``Can get key range of an ordered series`` () =
  ordered.KeyRange |> shouldEqual (1, 5)

// ------------------------------------------------------------------------------------------------
// Value conversions
// ------------------------------------------------------------------------------------------------

[<Test>]  
let ``Should be able to convert bool to nullable bool``() = 
  let s = (frame ["A" => series [ 1 => true]]).Rows.[1]
  s.GetAs<Nullable<bool>>("A") |> shouldEqual (Nullable true)

// ------------------------------------------------------------------------------------------------
// Construction & basics
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can create series with incomparable keys``() =
  let rnd1 = System.Random()
  let rnd2 = System.Random()
  let s = series [rnd1 => 1; rnd2 => 2]
  s.[rnd1] |> shouldEqual 1

[<Test>] 
let ``Series with the same data are considered equal``() = 
  let input = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  input |> shouldEqual input

[<Test>] 
let ``Series with different data are not considered equal``() = 
  let input1 = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = series [ 'a' => 1; 'b' => 2; 'c' => 4 ]
  let input3 = series [ 'a' => 1; 'b' => 2  ]
  input1 |> should notEqual input2
  input1 |> should notEqual input3

[<Test>] 
let ``Should throw when there are duplicate keys (on access)`` () =
  let actual =
    try let s = series [ 42 => "A"; 42 => "B" ] in ignore(s.Get(42)); ""
    with e -> e.Message
  actual |> should contain "42"

[<Test>] 
let ``Should throw when there are duplicate keys (on formatting)`` () =
  let actual =
    try let s = series [ 42 => "A"; 42 => "B" ] in ignore(s.Format()); ""
    with e -> e.Message
  actual |> should contain "42"

// ------------------------------------------------------------------------------------------------
// Chunking and windowing functions
// ------------------------------------------------------------------------------------------------

// Generate series with letters for testing
let letters n = series [ for k in 0 .. n - 1 -> k => char ((int 'A') + k) ]

[<Test>]
let ``Series.windowInto works correctly on sample input`` () =
  let actual = letters 10 |> Series.windowInto 8 (fun s -> new String(Array.ofSeq s.Values))
  let expected = series [7 => "ABCDEFGH"; 8 => "BCDEFGHI"; 9 => "CDEFGHIJ"]
  actual |> shouldEqual expected

[<Test>]
let ``Series.windowSizeInto with AtBeginning boundary works correctly on sample input`` () =
  let actual = letters 5 |> Series.windowSizeInto (4, Boundary.AtBeginning) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [0 => "A"; 1 => "AB"; 2 => "ABC"; 3 => "ABCD"; 4 => "BCDE" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.windowSizeInto with AtEnding boundary works correctly on sample input`` () =
  let actual = letters 5 |> Series.windowSizeInto (4, Boundary.AtEnding) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [0 => "ABCD"; 1 => "BCDE"; 2 => "CDE"; 3 => "DE"; 4 => "E" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkInto works correctly on sample input`` () =
  let actual = letters 10 |> Series.chunkInto 4 (fun s -> new String(Array.ofSeq s.Values))
  let expected = series [0 => "ABCD"; 4 => "EFGH" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtBeginning boundary works correctly on sample input`` () =
  let actual = letters 10 |> Series.chunkSizeInto (4, Boundary.AtBeginning) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [1 => "AB"; 5 => "CDEF"; 9 => "GHIJ" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtEnding boundary works correctly on sample input`` () =
  let actual = letters 10 |> Series.chunkSizeInto (4, Boundary.AtEnding) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [0 => "ABCD"; 4 => "EFGH"; 8 => "IJ" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtBeginning boundary works correctly when boundary is empty`` () =
  let actual = letters 8 |> Series.chunkSizeInto (4, Boundary.AtBeginning) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [3 => "ABCD"; 7 => "EFGH" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtEnding boundary works correctly when boundary is empty`` () =
  let actual = letters 8 |> Series.chunkSizeInto (4, Boundary.AtEnding) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [0 => "ABCD"; 4 => "EFGH" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtBeginning & Skip boundary works correctly on sample input`` () =
  let actual = letters 10 |> Series.chunkSizeInto (4, Boundary.AtBeginning ||| Boundary.Skip) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [5 => "CDEF"; 9 => "GHIJ" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkSizeInto with AtEnding & Skip boundary works correctly on sample input`` () =
  let actual = letters 10 |> Series.chunkSizeInto (4, Boundary.AtEnding ||| Boundary.Skip) (fun s -> new String(Array.ofSeq s.Data.Values))
  let expected = series [0 => "ABCD"; 4 => "EFGH" ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.chunkWhileInto works on sample input`` () =
  let s = series [ 01 => 01; 10 => 10; 11 => 11; 14 => 14; 21 => 21] 
  let actual = s |> Series.chunkWhileInto (fun k1 k2 -> k1/10 = k2/10) (Series.values >> List.ofSeq)
  let expected = series [ 1 => [1]; 10 => [10;11;14]; 21 => [21]]
  actual |> shouldEqual expected

[<Test>]
let ``Series.windowWhileInto works on sample input`` () =
  let s = series [ 01 => 01; 10 => 10; 11 => 11; 14 => 14; 21 => 21] 
  let actual = s |> Series.windowWhileInto (fun k1 k2 -> k1/10 = k2/10) (Series.values >> List.ofSeq)
  let expected = series [ 1 => [1]; 10 => [10;11;14]; 11 => [11;14]; 14 => [14]; 21 => [21]]
  actual |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Numerics
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can perform numerical operations on series of floats`` () =
  let sf = series [ 1 => 1.0; 2 => nan; 3 => 3.0 ]
  (-sf).[3] |> shouldEqual -3.0
  (sf * sf).[3] |> shouldEqual 9.0
  (sf * sf).[3] |> shouldEqual 9.0
  (sf + sf).[3] |> shouldEqual 6.0
  (sf - sf).[3] |> shouldEqual 0.0
  (sf / sf).[3] |> shouldEqual 1.0

[<Test>]
let ``Can perform numerical operations on series of integers`` () =
  let sn = series [ 1 => 1; 2 => 2; 3 => 3 ]
  (-sn).[3] |> shouldEqual -3
  (sn * sn).[3] |> shouldEqual 9
  (sn * sn).[3] |> shouldEqual 9
  (sn + sn).[3] |> shouldEqual 6
  (sn - sn).[3] |> shouldEqual 0
  (sn / sn).[3] |> shouldEqual 1

// ------------------------------------------------------------------------------------------------
// Operations - union, grouping, diff, etc.
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Series.diff and SeriesExtensions.Diff work on sample input``() =
  let input = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let expectedForward = series [ 'c' => 2 ]
  let expectedBackward = series [ 'a' => -2 ]
  input |> Series.diff -2 |> shouldEqual expectedBackward
  input |> Series.diff 2 |> shouldEqual expectedForward 
  SeriesExtensions.Diff(input, -2) |> shouldEqual expectedBackward
  SeriesExtensions.Diff(input, 2) |> shouldEqual expectedForward

[<Test>]
let ``Series.diff and Series.shift correctly return empty series`` () =
  let empty : Series<int, float> = series [] 
  empty |> Series.diff 1 |> shouldEqual <| series []
  empty |> Series.shift 1 |> Series.countKeys |> shouldEqual 0
  empty |> Series.shift -1 |> Series.countKeys |> shouldEqual 0
  
  let single = series [ 1 => 1.0 ]
  single |> Series.shift 2 |> Series.countKeys |> shouldEqual 0
  single |> Series.shift -2 |> Series.countKeys |> shouldEqual 0
  single |> Series.diff -2 |> shouldEqual <| series []

[<Test>]
let ``Series.diff correctly handles missing values``() =  
  let s = Series.ofValues [ 0.0; Double.NaN; Double.NaN; 0.0; 2.0 ]
  let actual1 = s |> Series.diff -1 |> Series.observationsAll |> List.ofSeq
  actual1 |> shouldEqual [(0, None); (1, None); (2, None); (3, Some -2.0)]
  let actual2 = s |> Series.diff 1 |> Series.observationsAll |> List.ofSeq
  actual2 |> shouldEqual [(1, None); (2, None); (3, None); (4, Some 2.0)]

[<Test>]
let ``Series.shift works correctnly on sample input``() =
  let input = series [ 'a' => 1.0; 'b' => 2.0; 'c' => nan; 'd' => 3.0;  ]
  let actual1 = input |> Series.shift 1
  actual1 |> shouldEqual <| series [ 'b' => 1.0; 'c' => 2.0; 'd' => nan ]
  let actual2 = input |> Series.shift -1
  actual2 |> shouldEqual <| series [ 'a' => 2.0; 'b' => nan; 'c' => 3.0 ]

[<Test>] 
let ``Union correctly unions series, prefering left or right values``() = 
  let input1 = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = series [ 'c' => 1; 'd' => 4  ]
  let expectedL = series [ 'a' => 1; 'b' => 2; 'c' => 3; 'd' => 4 ]
  let expectedR = series [ 'a' => 1; 'b' => 2; 'c' => 1; 'd' => 4 ]
  input1.Merge(input2, UnionBehavior.PreferLeft) |> shouldEqual expectedL
  input1.Merge(input2, UnionBehavior.PreferRight) |> shouldEqual expectedR
  
  let s1 = series [ 1 => nan; 2 => 1.0]
  let s2 = series [ 1 => 1.0 ]  
  s1.Merge(s2) |> shouldEqual (series [ 1 => 1.0; 2 => 1.0] )

[<Test>] 
let ``Union throws exception when behavior is exclusive and series overlap``() = 
  let input1 = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = series [ 'c' => 1; 'd' => 4  ]
  (fun () -> input1.Merge(input2, UnionBehavior.Exclusive) |> ignore) 
  |> should throw typeof<System.InvalidOperationException>

[<Test>] 
let ``Union combines series when behavior is exclusive and series do not overlap``() = 
  let input1 = series [ 'a' => 1; 'b' => 2; 'c' => 3 ]
  let input2 = series [ 'd' => 4  ]
  let expected = series [ 'a' => 1; 'b' => 2; 'c' => 3; 'd' => 4 ]
  input1.Merge(input2, UnionBehavior.Exclusive) |> shouldEqual expected

[<Test>] 
let ``Grouping series with missing values works on sample input``() =
  let n = Series.ofNullables [Nullable(); Nullable(1); Nullable(); Nullable(2)]
  let actual = n |> Series.groupBy (fun k _ -> k % 2) 
  let expected = series [ 1 => series [1 => 1; 3 => 2]]
  actual |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Fill missing values
// ------------------------------------------------------------------------------------------------

let generate (dt:DateTime) (ts:TimeSpan) count =
  Seq.init count (fun i -> dt.Add(TimeSpan(ts.Ticks * int64 i)), i) |> series

[<Test>]
let ``Can do simple fill forward``() =
  let n = Series.ofValues [ 0.0; Double.NaN; 1.0; Double.NaN; 2.0 ]
  let actual = n |> Series.fillMissing Direction.Forward
  actual |> shouldEqual (Series.ofValues [0.0; 0.0; 1.0; 1.0; 2.0])

[<Test>]
let ``Can do simple fill backward``() =
  let n = Series.ofValues [ 0.0; Double.NaN; 1.0; Double.NaN; 2.0 ]
  let actual = n |> Series.fillMissing Direction.Backward
  actual |> shouldEqual (Series.ofValues [0.0; 1.0; 1.0; 2.0; 2.0 ])

[<Test>]
let ``Can do fill inside backward``() =
  let n = Series.ofValues [ Double.NaN; 1.0; Double.NaN; 2.0; Double.NaN ]
  let actual = n |> Series.fillMissingInside Direction.Backward
  actual |> shouldEqual (Series.ofValues [Double.NaN; 1.0; 2.0; 2.0; Double.NaN ])

[<Test>]
let ``Can do fill inside forward``() =
  let n = Series.ofValues [ Double.NaN; 1.0; Double.NaN; 2.0; Double.NaN ]
  let actual = n |> Series.fillMissingInside Direction.Forward
  actual |> shouldEqual (Series.ofValues [Double.NaN; 1.0; 1.0; 2.0; Double.NaN ])

[<Test>]
let ``Fill inside corner cases work``() =
  let s = Series.ofValues [ Double.NaN; 1.0; Double.NaN]
  let actual = s |> Series.fillMissingInside Direction.Forward
  actual |> shouldEqual s

  let s = Series.ofValues [ Double.NaN ]
  let actual = s |> Series.fillMissingInside Direction.Forward
  actual |> shouldEqual s

  let s = Series.ofValues [ 1.0; Double.NaN ]
  let actual = s |> Series.fillMissingInside Direction.Forward
  actual |> shouldEqual s

  let s = Series.ofValues [ Double.NaN; 1.0 ]
  let actual = s |> Series.fillMissingInside Direction.Forward
  actual |> shouldEqual s

  let s = series [ 2.0 => Double.NaN; 1.0 => 1.0 ]
  (fun () -> s |> Series.fillMissingInside Direction.Forward |> ignore) |> should throw typeof<System.InvalidOperationException>
  
[<Test>]
let ``Can fill missing values in a specified range``() =
  let ts = generate DateTime.Today (TimeSpan.FromDays(1.0)) 20
  let tsmiss = ts |> Series.mapValues (fun v -> if v % 3 = 0 then Double.NaN else float v)
  let range = DateTime.Today.AddDays(5.0), DateTime.Today.AddDays(10.5)
  let tsfill = tsmiss |> Series.fillMissingBetween range Direction.Forward
  tsfill.KeyCount |> shouldEqual tsmiss.KeyCount
  tsfill.ValueCount |> should (be greaterThan) tsmiss.ValueCount
  tsfill.KeyCount |> should (be greaterThan) tsfill.ValueCount

[<Test>]
let ``Can perform linear interpolation``() =
  let s = series [ 0 => 0.0; 2 => 2.0; 4 => 4.0]
  let i = s |> Stats.interpolateLinear [0;1;2;3;4] (fun a b -> float <| a - b)
  let e = series [ 0 => 0.0; 1 => 1.0; 2 => 2.0; 3 => 3.0; 4 => 4.0]
  i |> shouldEqual e
  
// ------------------------------------------------------------------------------------------------
// Sorting and reindexing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can order series``() =
  let ord = unordered |> Series.sortByKey
  ord |> shouldEqual sortedByKey

[<Test>]
let ``Can sort series``() =
  let ord1 = randomOrder |> Series.sort
  ord1 |> shouldEqual ascending

  let ord2 = randomOrder |> Series.sortBy (fun v -> -v)
  ord2 |> shouldEqual descending

  let ord3 = randomOrder |> Series.sortWith (fun a b -> 
    if a < b then -1 else if a = b then 0 else 1)
  ord3 |> shouldEqual ascending

  let ord4 = randomOrderMissing |> Series.sort
  ord4 |> shouldEqual ascendingMissing
  
  let ord5 = randomOrderMissing |> Series.sortBy (fun v -> -v)
  ord5 |> shouldEqual descendingMissing

  let ord6 = randomOrderMissing |> Series.sortWith (fun a b -> 
    if a < b then -1 else if a = b then 0 else 1)
  ord6 |> shouldEqual ascendingMissing

[<Test>]
let ``Series.indexWith does not leak previous values through the value vector`` () =
  let s = series [ 1 => 1.0; 2 => 2.0 ]
  s |> Series.indexWith [1] |> Series.reduceValues (+) |> shouldEqual 1.0

[<Test>]
let ``Series.indexWith returns series that can be correctly tested for equality`` () =
  let s = series [ 1 => 1.0; 2 => 2.0 ]
  s |> Series.indexWith [1] |> shouldEqual <| series [ 1=>1.0 ]
  s |> Series.indexWith [1;2;3] |> shouldEqual <| series [ 1=>1.0; 2=>2.0; 3=>nan ]

// ------------------------------------------------------------------------------------------------
// Sampling and lookup
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Sample by time - get value at or just before specified time`` () = 
  let input = (generate (DateTime(2011, 12, 27)) (TimeSpan.FromHours(11.23)) 20) + 1
  let expected = 
    [ "12/27/2011 3:00:00 PM" => 2; "12/28/2011 3:00:00 PM" => 4;  "12/29/2011 3:00:00 PM" => 6;
      "12/30/2011 3:00:00 PM" => 8; "12/31/2011 3:00:00 PM" => 10; "1/1/2012 3:00:00 PM" => 13;
      "1/2/2012 3:00:00 PM" => 15;  "1/3/2012 3:00:00 PM" => 17;   "1/4/2012 3:00:00 PM" => 19
      "1/5/2012 3:00:00 PM" => 20 ] 
    |> series |> Series.mapKeys parseDateUSA

  let start = DateTime(2011, 12, 27).AddHours(15.0)
  let actual = SeriesExtensions.Sample(input, start, TimeSpan.FromDays(1.0), Direction.Backward)
  actual |> shouldEqual expected

[<Test>]
let ``Resample uniform - get the latest available value for each date (TestDaySampling)`` () = 
  let input = (generate (DateTime(2011, 12, 2)) (TimeSpan.FromHours(5.23)) 20)
  let expected = 
    [ "12/2/2011" => 4;  "12/3/2011" => 9; "12/4/2011" => 13;
      "12/5/2011" => 18; "12/6/2011" => 19 ] 
    |> series |> Series.mapKeys parseDateUSA
  let actual = SeriesExtensions.ResampleUniform(input, (fun (dt:DateTime) -> dt.Date), (fun dt -> dt.AddDays(1.0)))
  actual |> shouldEqual expected

[<Test>]
let ``Resample uniform with exact lookup`` () = 
  let input = series [ 0 => 1.0; 1 => 2.0; 4 => 5.0; 5 => 6.0 ]
  let expected = series [ 0 => 1.0; 1 => 2.0; 2 => nan; 3 => nan; 4 => 5.0; 5 => 6.0 ]
  let actual = SeriesExtensions.ResampleUniform(input, Func<_,_>(id), (fun x -> x + 1), Lookup.Exact)
  actual |> shouldEqual expected

[<Test>]
let ``Sample by time span - get the first available sample for each minute (TestMinuteSampling)`` () =
  let input = (generate (DateTime(2011, 12, 2)) (TimeSpan.FromSeconds(2.5)) 50)
  let expected = 
    [ "12/2/2011 12:00:00 AM" => 0; "12/2/2011 12:01:00 AM" => 24; 
      "12/2/2011 12:02:00 AM" => 48; "12/2/2011 12:03:00 AM" => 49 ]
    |> series |> Series.mapKeys parseDateUSA

  let actual = SeriesExtensions.Sample(input, TimeSpan.FromMinutes(1.0))
  actual |> shouldEqual expected

[<Test>]
let ``Sample by time span - get the last available previous value for every hour (TestDownSampling)`` () =
  let input = generate (DateTime(2012, 2, 12)) (TimeSpan.FromMinutes(5.37)) 50
  let expected = 
    [ "2/12/2012 12:00:00 AM" => 0;  "2/12/2012 1:00:00 AM" => 11
      "2/12/2012 2:00:00 AM" => 22; "2/12/2012 3:00:00 AM" => 33
      "2/12/2012 4:00:00 AM" => 44; "2/12/2012 5:00:00 AM" => 49 ]
    |> series |> Series.mapKeys parseDateUSA
  let actual = input |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Backward Series.lastValue
  actual |> shouldEqual expected        

[<Test>]
let ``Sample by time span does not crash on empty series`` () =
  let input = generate (DateTime(2012, 2, 12)) (TimeSpan.FromMinutes(5.37)) 0
  let actual = input |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Backward Series.lastValue
  actual |> shouldEqual <| series []

[<Test>]
let ``Sample by keys - get the nearest previous key or <missing> (TestExplicitTimeSamples)`` () =
  let input = (generate (DateTime(2012, 01, 01)) (TimeSpan.FromDays(3.0)) 15) + 1
  let dateSampels = 
    [ DateTime(2011, 12, 20); DateTime(2012, 01, 05); DateTime(2012, 01, 08);
      DateTime(2012, 01, 19); DateTime(2012, 01, 29) ]
  let expected = 
    [ "12/20/2011" => Double.NaN; "1/5/2012" => 2.0;
      "1/8/2012" => 3.0; "1/19/2012" => 7.0; "1/29/2012" => 10.0 ]
    |> series |> Series.mapKeys parseDateUSA |> Series.mapValues int
  let actual = input.GetItems(dateSampels, Lookup.ExactOrSmaller)
  actual |> shouldEqual expected

[<Test>]
let ``Resample uniform - select value of nearest previous key or fill with earlier (TestForwardFillSampling)`` () =
  let input = 
    [ "5/25/2012", 1.0; "5/26/2012", 2.0; "5/29/2012", 5.0; "5/30/2012", 6.0 ]
    |> series |> Series.mapKeys parseDateUSA 
  let expected = 
    [ "5/25/2012", 1.0; "5/26/2012", 2.0; "5/27/2012", 2.0;
      "5/28/2012", 2.0; "5/29/2012", 5.0; "5/30/2012", 6.0 ]
    |> series |> Series.mapKeys parseDateUSA 
  let actual = SeriesExtensions.ResampleUniform(input, (fun (dt:DateTime) -> dt.Date), (fun dt -> dt.AddDays(1.0)))
  actual |> shouldEqual expected

[<Test>]
let ``Series.sampleTime works when using forward direction`` () =
  let start = DateTime(2012, 2, 12)
  let input = generate start (TimeSpan.FromMinutes(5.37)) 50
  let expected = 
    series
      [ start.AddHours(0.0) => 0;  start.AddHours(1.0) => 12
        start.AddHours(2.0) => 23; start.AddHours(3.0) => 34
        start.AddHours(4.0) => 45 ]
  let actual = input |> Series.sampleTimeInto (TimeSpan(1,0,0)) Direction.Forward Series.firstValue
  actual |> shouldEqual expected        

[<Test>]
let ``Series.resampleInto works when using forward direction`` () =
  let start = DateTime(2012, 2, 12)
  let input = generate start (TimeSpan.FromHours(5.37)) 20
  let actual = 
    input 
    |> Series.resampleInto [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Forward (fun _ -> Series.firstValue)
  let expected = series [ DateTime(2012, 2, 13) => 0; DateTime(2012, 2, 15) => 14 ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resampleInto works when using backward direction`` () =
  let start = DateTime(2012, 2, 12)
  generate start (TimeSpan.FromHours(5.37)) 20
  |> Series.resampleInto [ DateTime(2012, 2, 13); DateTime(2012, 2, 15) ] Direction.Backward (fun _ -> Series.lastValue)
  |> shouldEqual <| series [ DateTime(2012, 2, 13) => 4; DateTime(2012, 2, 15) => 19 ]

[<Test>]
let ``Series.resample generates empty chunks for keys where there are no values`` () =
  let start = DateTime(2012, 2, 12)
  let keys = [ for d in 12 .. 20 -> DateTime(2012, 2, d) ]
  generate start (TimeSpan.FromHours(48.0)) 5
  |> Series.resample keys Direction.Forward
  |> Series.mapValues (fun s -> if s.IsEmpty then -1 else s.[s.KeyRange |> fst])
  |> shouldEqual <| series (Seq.zip keys [0; -1; 1; -1; 2; -1; 3; -1; 4])

[<Test>]
let ``Can create minute samples over one year of items``() =
  let input = generate DateTime.Today (TimeSpan.FromDays(30.0)) 12
  let sampl = SeriesExtensions.Sample(input, TimeSpan.FromMinutes(1.0))
  let dict = sampl |> Series.observations |> dict
  dict.[DateTime.Today.AddDays(5.0)] |> shouldEqual 0
  dict.Count |> should be (greaterThan 100000)

[<Test>]
let ``Series.resample works in forward direction with keys in range`` () =
  let s = series [ for i in 1 .. 10 -> i, i ]
  let actual = s |> Series.resampleInto [2;4;7] Direction.Forward (fun k s -> List.ofSeq s.Values)
  let expected = series [2 => [1;2;3]; 4 => [4;5;6]; 7 => [7;8;9;10]]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works in backward direction with keys in range`` () =
  let s = series [ for i in 1 .. 10 -> i, i ]
  let actual = s |> Series.resampleInto [2;4;7] Direction.Backward (fun k s -> List.ofSeq s.Values)
  let expected = series [2 => [1;2]; 4 => [3;4]; 7 => [5;6;7;8;9;10]]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works in forward direction when key is not in range`` () =
  let s = series [ for i in 1 .. 10 -> if i >= 4 then i + 1, i + 1 else i, i ]
  let actual = s |> Series.resampleInto [2;4;7] Direction.Forward (fun k s -> List.ofSeq s.Values)
  let expected = series [2 => [1;2;3]; 4 => [5;6]; 7 => [7;8;9;10;11]]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works in backward direction when key is not in range`` () =
  let s = series [ for i in 1 .. 10 -> if i >= 4 then i + 1, i + 1 else i, i ]
  let actual = s |> Series.resampleInto [2;4;7] Direction.Backward (fun k s -> List.ofSeq s.Values)
  let expected = series [2 => [1;2]; 4 => [3]; 7 => [5;6;7;8;9;10;11]]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works in forward direction with keys mapping to empty groups`` () =
  let s = series [ 1 => 1; 3 => 3; 4 => 4; 5 => 5 ]
  let actual = s |> Series.resampleInto [1;2;3;5;6] Direction.Forward (fun k s -> List.ofSeq s.Values)
  let expected = series [1 => [1]; 2 => []; 3 => [3;4]; 5 => [5]; 6 => [] ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works in backward direction with keys mapping to empty groups`` () =
  let s = series [ 1 => 1; 3 => 3; 4 => 4; 5 => 5 ]
  let actual = s |> Series.resampleInto [1;2;3;5;6] Direction.Backward (fun k s -> List.ofSeq s.Values)
  let expected = series [1 => [1]; 2 => []; 3 => [3]; 5 => [4;5]; 6 => [] ]
  actual |> shouldEqual expected

[<Test>]
let ``Series.resample works for very large number of keys`` () =
  let input = series [for m in 1 .. 12 -> DateTime.Today.AddMonths(m) => float m ] 
  let keys = [for m in 0.0 .. 100000.0 -> DateTime.Today.AddMinutes(m) ]

  let actual = input |> Series.resample keys Direction.Forward 
  actual.KeyCount |> shouldEqual 100001
  let actual = input |> Series.resample keys Direction.Backward 
  actual.KeyCount |> shouldEqual 100001

// ------------------------------------------------------------------------------------------------
// Indexing & slicing & related extensions
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``SeriesExtensions.EndAt works when the key is before, after or in range``() =
  let s = series [ for i in 10.0 .. 20.0 -> i => int i ]
  s.EndAt(15.5).Values |> List.ofSeq |> shouldEqual [ 10 .. 15 ]
  s.EndAt(15.0).Values |> List.ofSeq |> shouldEqual [ 10 .. 15 ]
  s.EndAt(5.00).Values |> List.ofSeq |> shouldEqual [ ]
  s.EndAt(25.0).Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]

[<Test>]
let ``SeriesExtensions.StartAt works when the key is before, after or in range``() =
  let s = series [ for i in 10.0 .. 20.0 -> i => int i ]
  s.StartAt(15.5).Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.StartAt(15.0).Values |> List.ofSeq |> shouldEqual [ 15 .. 20 ]
  s.StartAt(5.00).Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]
  s.StartAt(25.0).Values |> List.ofSeq |> shouldEqual [ ]

[<Test>]
let ``SeriesExtensions.After works when the key is before, after or in range``() =
  let s = series [ for i in 10.0 .. 20.0 -> i => int i ]
  s.After(15.5).Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.After(15.0).Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.After(5.00).Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]
  s.After(25.0).Values |> List.ofSeq |> shouldEqual [ ]

[<Test>]
let ``Slicing of ordered series works when using inexact keys (below, inside, above) key range``() =
  let s = series [ for i in 10.0 .. 20.0 -> i => int i ]
  s.[15.5 .. 20.0].Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.[5.50 .. 20.0].Values |> List.ofSeq |> shouldEqual [ 10 .. 20 ]
  s.[15.5 .. 25.0].Values |> List.ofSeq |> shouldEqual [ 16 .. 20 ]
  s.[15.5 .. 18.5].Values |> List.ofSeq |> shouldEqual [ 16 .. 18 ]

[<Test>]
let ``Slicing of ordered series works when keys are out of series key range``() =
  let s = series [ for i in 10.0 .. 20.0 -> i => int i ]
  s.[0.0 .. 5.0].Values |> List.ofSeq |> shouldEqual []
  s.[25.0 .. 35.0].Values |> List.ofSeq |> shouldEqual []
  s.[20.0 .. 5.0].Values |> List.ofSeq |> shouldEqual []

// ------------------------------------------------------------------------------------------------
// Ziping
// ------------------------------------------------------------------------------------------------
  
let a =
  [ DateTime(2013,9,9) => 1.0; // no matching point in b
    DateTime(2013,9,10) => 2.0; // no matching point in b
    DateTime(2013,9,11) => 3.0;
    DateTime(2013,9,12) => 4.0; ]  // no matching point in b
    |> series

let b = 
  [ DateTime(2013,9,8) => 8.0; // no matching point in a
    DateTime(2013,9,11) => 11.0 ] |> series


let lift2 f a b = 
    match a, b with
    | Some x, Some y -> Some(f x y)
    | _              -> None

[<Test>]
let ``ZipInto correctly zips series with missing values and custom operation``() =
  let res = (a, b) ||> Series.zipInto (fun l r -> (l**2.0) * r)
  res.GetAt(0) |> shouldEqual (99.0)

[<Test>]
let ``ZipAlignInto correctly left-aligns and zips series with nearest smaller option``() =
  let res = (a, b) ||> Series.zipAlignInto JoinKind.Left Lookup.ExactOrSmaller (lift2 (fun l r -> (l**2.0) * r))
  res.GetAt(0) |> shouldEqual 8.0
  res.GetAt(1) |> shouldEqual 32.0
  res.GetAt(2) |> shouldEqual 99.0
  res.GetAt(3) |> shouldEqual (16.0 * 11.0)

[<Test>]
let ``ZipAlignInto correctly left-aligns and zips series with nearest greater option``() =
  let res = (a, b) ||> Series.zipAlignInto JoinKind.Left Lookup.ExactOrGreater (lift2 (fun l r -> (l**2.0) * r))
  res.GetAt(0) |> shouldEqual 11.0
  res.GetAt(1) |> shouldEqual 44.0
  res.GetAt(2) |> shouldEqual 99.0
  res.TryGetAt(3) |> shouldEqual OptionalValue.Missing


[<Test>]
let ``ZipAlignInto correctly right-aligns and zips series with nearest smaller option``() =
  let res = (b, a) ||> Series.zipAlignInto JoinKind.Right Lookup.ExactOrSmaller (lift2 (fun l r -> (l**2.0) * r)) 
  res.GetAt(0) |> shouldEqual ((8.0 ** 2.0) * 1.0)
  res.GetAt(1) |> shouldEqual ((8.0 ** 2.0) * 2.0)
  res.GetAt(2) |> shouldEqual ((11.0 ** 2.0) * 3.0)
  res.GetAt(3) |> shouldEqual ((11.0 ** 2.0) * 4.0)


[<Test>]
let ``ZipAlignInto correctly right-aligns and zips series with nearest greater option``() =
  let res = (b, a) ||> Series.zipAlignInto JoinKind.Right Lookup.ExactOrGreater (lift2 (fun l r -> (l**2.0) * r))
  res.GetAt(0) |> shouldEqual ((11.0 ** 2.0) * 1.0)
  res.GetAt(1) |> shouldEqual ((11.0 ** 2.0) * 2.0)
  res.GetAt(2) |> shouldEqual ((11.0 ** 2.0) * 3.0)
  res.TryGetAt(3) |> shouldEqual OptionalValue.Missing


[<Test>]
let ``Can zip series with lookup and skip over missing values ``() =
  // join with lookup is not skipping over NaN values
  let l = [ 1 => 1.0;  2 => 2.0;        3 => 3.0;        4 => 4.0;  ] |> series
  let r = [ 1 => 10.0; 2 => Double.NaN; 3 => Double.NaN; 4 => 40.0; ] |> series

  let res1 = l.Zip(r, JoinKind.Left, Lookup.ExactOrSmaller)
  res1.GetAt(0) |> shouldEqual (OptionalValue 1.0, OptionalValue 10.0)
  res1.GetAt(1) |> shouldEqual (OptionalValue 2.0, OptionalValue 10.0) // second values is missing instead of 10
  res1.GetAt(2) |> shouldEqual (OptionalValue 3.0, OptionalValue 10.0) // second values is missing instead of 10
  res1.GetAt(3) |> shouldEqual (OptionalValue 4.0, OptionalValue 40.0)

  let res2 = l.Zip(r, JoinKind.Left, Lookup.ExactOrGreater)
  res2.GetAt(0) |> shouldEqual (OptionalValue 1.0, OptionalValue 10.0)
  res2.GetAt(1) |> shouldEqual (OptionalValue 2.0, OptionalValue 40.0) // second values is missing instead of 40
  res2.GetAt(2) |> shouldEqual (OptionalValue 3.0, OptionalValue 40.0) // second values is missing instead of 40
  res2.GetAt(3) |> shouldEqual (OptionalValue 4.0, OptionalValue 40.0)


[<Test>]
let ``Can left-zip two empty series`` () =
  let s1 = series ([] : list<int * int>)
  let s2 = s1.Zip(s1, JoinKind.Left)
  s2 |> shouldEqual (series [])

[<Test>]
let ``zipInner should not break alignment of the index (#233)``() = 
  let x = series [ 0 => nan; 1 => nan; 2 => 1.0; 3 => 2.0 ] 
  let y = series [ 0 => nan; 1 => nan; 2 => 3.0; 3 => 4.0 ]
  let expected = 
    [ 0 => None; 1 => None; 2 => Some(1.0, 3.0); 3 => Some(2.0, 4.0) ]
    |> Series.ofOptionalObservations 
  Series.zipInner x y |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Appending and joining
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can append two sample series`` () =
  let inputs = Array.init 1000 (fun i -> i => int (10.0 * sin (float i)))
  let ar1, ar2 = Array.partition (fun (_, v) -> v%2 = 0) inputs
  let actual = (series ar1).Merge(series ar2) 
  actual.Index.IsOrdered |> shouldEqual true
  actual |> shouldEqual (series inputs)

[<Test>]
let ``Can append 10 sample ordered series (by appending them one by one)`` () =
  let samples = [ for i in 0 .. 9 -> series [ for j in 0 .. 99 -> 10*j + i => i * j ] ]
  let expected = series [ for i in 0 .. 9 do for j in 0 .. 99 -> 10*j + i => i * j ] |> Series.sortByKey
  let actual = samples |> Seq.reduce Series.merge
  actual.Index.IsOrdered |> shouldEqual true
  actual |> shouldEqual expected

[<Test>]
let ``Can append 10 sample unordered series (by appending them one by one)`` () =
  let samples = [ for i in 0 .. 9 -> series [ for j in 99 .. -1 .. 0 -> 10*j + i => i * j ] ]
  let expected = series [ for i in 0 .. 9 do for j in 99 .. -1 .. 0 -> 10*j + i => i * j ] 
  let actual = samples |> Seq.reduce Series.merge
  actual.Index.IsOrdered |> shouldEqual false
  actual |> shouldEqual expected

[<Test>]
let ``Can append 10 sample ordered series`` () =
  let samples = [ for i in 0 .. 9 -> series [ for j in 0 .. 99 -> 10*j + i => i * j ] ]
  let expected = series [ for i in 0 .. 9 do for j in 0 .. 99 -> 10*j + i => i * j ] |> Series.sortByKey
  let actual = samples.Head.Merge(Array.ofSeq samples.Tail)
  actual.Index.IsOrdered |> shouldEqual true
  actual |> shouldEqual expected

[<Test>]
let ``Can append 10 sample unordered series`` () =
  let samples = [ for i in 0 .. 9 -> series [ for j in 99 .. -1 .. 0 -> 10*j + i => i * j ] ]
  let expected = series [ for i in 0 .. 9 do for j in 99 .. -1 .. 0 -> 10*j + i => i * j ] 
  let actual = samples.Head.Merge(Array.ofSeq samples.Tail)
  actual.Index.IsOrdered |> shouldEqual false
  actual |> shouldEqual expected

[<Test>]
let ``Can correctly append over 150 series`` () =
  let rnd = new Random(0)
  let values = Array.init 300 (fun _ -> new ResizeArray<_>())
  for i in 0 .. 10000 do values.[rnd.Next(300)].Add( (i, float i) )

  let ss = values |> Array.map series
  let actual = ss.[0].Merge(ss.[1 ..]) 
  actual |> shouldEqual <| series [ for i in 0 .. 10000 -> i => float i ] 

// ------------------------------------------------------------------------------------------------
// take, takeLast, skip, skipLast
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can take N elements from front and back`` () =
  let s = series [ for i in 1 .. 100 -> i => float i]

  Series.take 2 s |> shouldEqual <| series [1 => 1.0; 2 => 2.0]
  Series.take 100 s |> shouldEqual <| s
  Series.take 0 s |> shouldEqual <| series []

  Series.takeLast 2 s |> shouldEqual <| series [99 => 99.0; 100 => 100.0]
  Series.takeLast 100 s |> shouldEqual <| s
  Series.takeLast 0 s |> shouldEqual <| series []

[<Test>]
let ``Can skip N elements from front and back`` () =
  let s = series [ for i in 1 .. 100 -> i => float i]

  Series.skip 98 s |> shouldEqual <| series [99 => 99.0; 100 => 100.0]
  Series.skip 100 s |> shouldEqual <| series []
  Series.skip 0 s |> shouldEqual <| s

  Series.skipLast 98 s |> shouldEqual <| series [1 => 1.0; 2 => 2.0]
  Series.skipLast 100 s |> shouldEqual <| series []
  Series.skipLast 0 s |> shouldEqual <| s

[<Test>]
let ``Can restrict series range using a custom range restriction`` () =
  let s = series [ for i in 1 .. 100 -> i => float i]
  let indices = [ 0L<Addressing.address> .. 3L<Addressing.address> .. 99L<Addressing.address> ]
  let rangeRestr =
    { new IRangeRestriction<Addressing.Address> with
        member x.Count = int64 (Seq.length indices)
      interface seq<Addressing.Address> with 
        member x.GetEnumerator() = (indices :> seq<_>).GetEnumerator() 
      interface System.Collections.IEnumerable with
        member x.GetEnumerator() = (indices :> seq<_>).GetEnumerator() :> _ } 

  let rs = s.GetAddressRange(RangeRestriction.Custom(rangeRestr))
  rs.KeyCount |> shouldEqual 34
  rs.Keys |> List.ofSeq |> shouldEqual [ 1 .. 3 .. 100 ]

// ------------------------------------------------------------------------------------------------
// Misc
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``TryMap can catch errors`` () =
  let res = series ["a" => 0; "b" => 2; "c" => 3] 
            |> Series.tryMap (fun _ x -> 1 / x) 
  res |> Series.tryErrors |> Series.countKeys |> shouldEqual 1
  res |> Series.trySuccesses |> Series.countKeys |> shouldEqual 2

[<Test>]
let ``Realign works and isn't terribly slow`` () =
  let arr1 = [|0 .. 1000000|]
  let arr2 = [|1 .. 1000001|]
  let s1 = Array.zip arr1 arr1 |> series
  let s2 = s1.Realign(arr2)
  s2.Keys |> Seq.toArray |> shouldEqual arr2


[<Test>]
let ``Masking works as expected`` () =
  let s = series [ 0 => 1.0; 1 => nan; 2 => 3.0 ]
  let t = series [ 1 => 5.0; 2 => 3.0; 4 => 4.0 ]
  (t |> Series.withMissingFrom s) |> shouldEqual (series [ 1 => nan; 2 => 3.0; 4 => 4.0 ])
