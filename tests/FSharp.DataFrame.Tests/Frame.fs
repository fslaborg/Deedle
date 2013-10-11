#if INTERACTIVE
#I "../../bin"
#load "../../bin/FSharp.DataFrame.fsx"
#r "../../packages/NUnit.2.6.2/lib/nunit.framework.dll"
#r "../../packages/FsCheck.0.9.1.0/lib/net40-Client/FsCheck.dll"
#load "../Common/FsUnit.fs"
#else
module FSharp.DataFrame.Tests.Frame
#endif

open System
open System.Collections.Generic
open FsUnit
open FsCheck
open NUnit.Framework
open FSharp.DataFrame

// ------------------------------------------------------------------------------------------------
// Input and output (CSV files)
// ------------------------------------------------------------------------------------------------

let msft() = 
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", inferRows=10) 
  |> Frame.indexRowsDate "Date"

[<Test>]
let ``Can read MSFT data from CSV file`` () =
  let df = msft()
  df.RowKeys |> Seq.length |> shouldEqual 6527
  df.ColumnKeys |> Seq.length |> shouldEqual 7

[<Test>]
let ``Can save MSFT data as CSV file and read it afterwards (with default args)`` () =
  let file = System.IO.Path.GetTempFileName()
  let expected = msft()
  expected.SaveCsv(file)
  let actual = Frame.ReadCsv(file) |> Frame.indexRowsDate "Date"
  actual |> shouldEqual expected

[<Test>]
let ``Can save MSFT data as CSV file and read it afterwards (with custom format)`` () =
  let file = System.IO.Path.GetTempFileName()
  let expected = msft()
  expected.DropSeries("Date")
  expected.SaveCsv(file, ["Date"], separator=';', culture=System.Globalization.CultureInfo.GetCultureInfo("cs-CZ"))
  let actual = 
    Frame.ReadCsv(file, separators=";", culture="cs-CZ")
    |> Frame.indexRowsDate "Date" |> Frame.dropCol "Date"
  actual |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Indexing and accessing values
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Applying numerical operation to frame does not affect non-numeric series`` () =
  let df = msft() * 2.0
  let actual = df.GetSeries<DateTime>("Date").GetAt(0).Date 
  actual |> shouldEqual (DateTime(2012, 1, 27))
  
[<Test>]
let ``Can perform numerical operation with a scalar on data frames`` () =
  let df = msft() 

  (df * 2.0)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) * 2.0)
  (df / 2.0)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) / 2.0)
  (df + 2.0)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) + 2.0)
  (df - 2.0)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) - 2.0)
  (2.0 * df)?Open.GetAt(66) |> shouldEqual (2.0 * df?Open.GetAt(66))
  (2.0 + df)?Open.GetAt(66) |> shouldEqual (2.0 + df?Open.GetAt(66))
  (2.0 - df)?Open.GetAt(66) |> shouldEqual (2.0 - df?Open.GetAt(66))
  (2.0 / df)?Open.GetAt(66) |> shouldEqual (2.0 / df?Open.GetAt(66))
  
  (df / 2)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) / 2.0)
  (df * 2)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) * 2.0)
  (df + 2)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) + 2.0)
  (df - 2)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) - 2.0)
  (2 * df)?Open.GetAt(66) |> shouldEqual (2.0 * df?Open.GetAt(66))
  (2 + df)?Open.GetAt(66) |> shouldEqual (2.0 + df?Open.GetAt(66))
  (2 - df)?Open.GetAt(66) |> shouldEqual (2.0 - df?Open.GetAt(66))
  (2 / df)?Open.GetAt(66) |> shouldEqual (2.0 / df?Open.GetAt(66))

[<Test>]
let ``Can perform numerical operation with a series on data frames`` () =
  let df = msft() 

  let opens = df?Open
  (df * opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) * opens.GetAt(66))
  (df / opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) / opens.GetAt(66))
  (df + opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) + opens.GetAt(66))
  (df - opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) - opens.GetAt(66))
  (opens * df)?Open.GetAt(66) |> shouldEqual (opens.GetAt(66) * df?Open.GetAt(66))
  (opens + df)?Open.GetAt(66) |> shouldEqual (opens.GetAt(66) + df?Open.GetAt(66))
  (opens - df)?Open.GetAt(66) |> shouldEqual (opens.GetAt(66) - df?Open.GetAt(66))
  (opens / df)?Open.GetAt(66) |> shouldEqual (opens.GetAt(66) / df?Open.GetAt(66))
  
  let opens = int $ df?Open 
  (df * opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) * float (opens.GetAt(66)))
  (df / opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) / float (opens.GetAt(66)))
  (df + opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) + float (opens.GetAt(66)))
  (df - opens)?Open.GetAt(66) |> shouldEqual (df?Open.GetAt(66) - float (opens.GetAt(66)))
  (opens * df)?Open.GetAt(66) |> shouldEqual (float (opens.GetAt(66)) * df?Open.GetAt(66))
  (opens + df)?Open.GetAt(66) |> shouldEqual (float (opens.GetAt(66)) + df?Open.GetAt(66))
  (opens - df)?Open.GetAt(66) |> shouldEqual (float (opens.GetAt(66)) - df?Open.GetAt(66))
  (opens / df)?Open.GetAt(66) |> shouldEqual (float (opens.GetAt(66)) / df?Open.GetAt(66))
  
[<Test>]
let ``Can perform pointwise numerical operations on two frames`` () =
  let df1 = msft() |> Frame.orderRows
  let df2 = df1 |> Frame.shift 1
  let opens1 = df1?Open
  let opens2 = df2?Open

  (df2 - df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) - opens1.GetAt(66))
  (df2 + df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) + opens1.GetAt(66))
  (df2 * df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) * opens1.GetAt(66))
  (df2 / df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) / opens1.GetAt(66))



// ------------------------------------------------------------------------------------------------
// Operations - join, align
// ------------------------------------------------------------------------------------------------

let dates =
  [ DateTime(2013,9,9) => 0.0;
    DateTime(2013,9,10) => 1.0;
    DateTime(2013,9,11) => 2.0 ] |> series

let times = 
  [ DateTime(2013,9,9, 9, 31, 59) => 0.5
    DateTime(2013,9,10, 9, 31, 59) => 1.5
    DateTime(2013,9,11, 9, 31, 59) => 2.5 ] |> series

let daysFrame = [ "Days" => dates ] |> Frame.ofColumns
let timesFrame = [ "Times" => times ] |> Frame.ofColumns

[<Test>]
let ``Can left-align ordered frames - nearest smaller returns missing if no smaller value exists``() =
  // every point in timesFrames is later than in daysFrame, there is no point in times 
  // smaller than the first point in days, therefore first value in "Times" column must be missing
  // after left join with NearestSmaller option
  let daysTimesPrevL = 
      (daysFrame, timesFrame) 
      ||> Frame.align JoinKind.Left Lookup.NearestSmaller

  daysTimesPrevL?Times.TryGetAt(0) |> shouldEqual OptionalValue.Missing
  daysTimesPrevL?Times.TryGetAt(1) |> shouldEqual (OptionalValue 0.5)
  daysTimesPrevL?Times.TryGetAt(2) |> shouldEqual (OptionalValue 1.5)


[<Test>]
let ``Can left-align ordered frames - nearest greater always finds greater value`` () =
  // every point in timesFrames is later than in daysFrame, 
  // all values in Times must be as in original series
  let daysTimesNextL = 
      (daysFrame, timesFrame) 
      ||> Frame.align JoinKind.Left Lookup.NearestGreater
        
  daysTimesNextL?Times.TryGetAt(0) |> shouldEqual (OptionalValue 0.5)
  daysTimesNextL?Times.TryGetAt(1) |> shouldEqual (OptionalValue 1.5)
  daysTimesNextL?Times.TryGetAt(2) |> shouldEqual (OptionalValue 2.5)

[<Test>]
let ``Can right-align ordered frames - nearest smaller always finds smaller value``() =
  // every point in timesFrames is later than in daysFrame, 
  // all values in Days must be as in original series
  let daysTimesPrevR = 
      (daysFrame, timesFrame) 
      ||> Frame.align JoinKind.Right Lookup.NearestSmaller
  
  daysTimesPrevR?Days.TryGetAt(0) |> shouldEqual (OptionalValue 0.0)
  daysTimesPrevR?Days.TryGetAt(1) |> shouldEqual (OptionalValue 1.0)
  daysTimesPrevR?Days.TryGetAt(2) |> shouldEqual (OptionalValue 2.0)

[<Test>]
let ``Can right-align ordered frames - nearest greater returns missing if no greater value exists`` () =
  // every point in timesFrames is later than in daysFrame, 
  // last point in Days must be missing after joining
  let daysTimesNextR = 
      (daysFrame, timesFrame) 
      ||> Frame.align JoinKind.Right Lookup.NearestGreater
  
  daysTimesNextR?Days.TryGetAt(0) |> shouldEqual (OptionalValue 1.0)
  daysTimesNextR?Days.TryGetAt(1) |> shouldEqual (OptionalValue 2.0)
  daysTimesNextR?Days.TryGetAt(2) |> shouldEqual OptionalValue.Missing


  
// ------------------------------------------------------------------------------------------------
// Operations - zip
// ------------------------------------------------------------------------------------------------

// company A has common and preferred stocks, company B only common
// company A trades in US, company B common shares trade in US, while B prefs trade in Israel/GCC (e.g. B comm is ADR, B pref only local)
// company B did a 1:2 split on Sep-14

// Prices
let pxA =
  [ DateTime(2013,9,10) => 100.0;
    DateTime(2013,9,11) => 101.0;
    DateTime(2013,9,12) => 101.0; // Sat - // for US keep Sat and Sun values until Series.Join supports lookup option with full outer join
    DateTime(2013,9,13) => 101.0; // Sun - // 
    DateTime(2013,9,14) => 102.0; 
    DateTime(2013,9,15) => 103.0; 
    DateTime(2013,9,16) => 104.0;] 
    |> series

let pxB =
  [ DateTime(2013,9,10) => 200.0;
    DateTime(2013,9,11) => 200.0; // Fri
    DateTime(2013,9,12) => 200.0; // Sat
    DateTime(2013,9,13) => 201.0; 
    DateTime(2013,9,14) => 101.0; 
    DateTime(2013,9,15) => 101.5; 
    DateTime(2013,9,16) => 102.0;] 
    |> series

let pxCommons = 
  [ "A" => pxA;
    "B" => pxB;] 
    |> Frame.ofColumns

let pxBpref =
  [ DateTime(2013,9,10) => 20.0;
    //DateTime(2013,9,11) => 20.0; // Fri - // not traded
    //DateTime(2013,9,12) => 20.0; // Sat - // omit these values to illustrate how lookup works in Frame.zipAlignInto
    DateTime(2013,9,13) => 21.0; 
    DateTime(2013,9,14) => 22.0; 
    DateTime(2013,9,15) => 23.0; 
    DateTime(2013,9,16) => 24.0;] 
    |> series

let pxPrefs = 
  [ "B" => pxBpref;] 
    |> Frame.ofColumns

// Shares outstanding
let sharesA =
  [ DateTime(2012,12,31) => 10.0;] 
    |> series

let sharesB =
  [ DateTime(2012,12,31) => 20.0;
    DateTime(2013,9,14) => 40.0; ] // split
    |> series

let sharesCommons = 
  [ "A" => sharesA;
    "B" => sharesB;] 
    |> Frame.ofColumns


let sharesBpref =
  [ DateTime(2012,12,31) => 20.0; ]
    |> series

let sharesPrefs = 
  [ "B" => sharesBpref;] 
    |> Frame.ofColumns

// Net debt forecast 2013
let ndA =
  [ DateTime(2013,12,31) => 100.0;] 
    |> series

let ndB =
  [ DateTime(2013,12,31) => 1000.0; ]
    |> series

let netDebt = 
  [ "A" => ndA;
    "B" => ndB;] 
    |> Frame.ofColumns


[<Test>]
let ``Can zip-align frames with inner-join left-join nearest-smaller options`` () =
  
  let mktcapA = 
    (pxA, sharesA)
    ||> Series.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Left Lookup.NearestSmaller
  
  let mktcapB = 
    (pxB, sharesB)
    ||> Series.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Left Lookup.NearestSmaller
  
  // calculate stock mktcap 
  let mktCapCommons = 
    (pxCommons, sharesCommons)
    ||> Frame.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Inner JoinKind.Left Lookup.NearestSmaller
  
  mktCapCommons?A.GetAt(0) |> shouldEqual 1000.0
  mktCapCommons?A.GetAt(1) |> shouldEqual 1010.0
  mktCapCommons?A.GetAt(2) |> shouldEqual 1010.0
  mktCapCommons?A.GetAt(3) |> shouldEqual 1010.0
  mktCapCommons?A.GetAt(4) |> shouldEqual 1020.0
  mktCapCommons?A.GetAt(5) |> shouldEqual 1030.0
  mktCapCommons?A.GetAt(6) |> shouldEqual 1040.0

  mktCapCommons?B.GetAt(0) |> shouldEqual 4000.0
  mktCapCommons?B.GetAt(1) |> shouldEqual 4000.0
  mktCapCommons?B.GetAt(2) |> shouldEqual 4000.0
  mktCapCommons?B.GetAt(3) |> shouldEqual 4020.0
  mktCapCommons?B.GetAt(4) |> shouldEqual 4040.0
  mktCapCommons?B.GetAt(5) |> shouldEqual 4060.0
  mktCapCommons?B.GetAt(6) |> shouldEqual 4080.0





[<Test>]
let ``Can zip-align frames with different set of columns`` () =
  
  // calculate stock mktcap 
  let mktCapCommons = 
    (pxCommons, sharesCommons)
    ||> Frame.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Inner JoinKind.Left Lookup.NearestSmaller
  // calculate stock mktcap for prefs
  let mktCapPrefs = 
    (pxPrefs, sharesPrefs)
    ||> Frame.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Inner JoinKind.Left Lookup.NearestSmaller
  // calculate company mktcap 
  let mktCap = 
    (mktCapCommons, mktCapPrefs)
    ||> Frame.zipAlignInto (fun (l:float) r -> l+r) JoinKind.Left JoinKind.Left Lookup.NearestSmaller
  
  mktCap?A.GetAt(0) |> shouldEqual 1000.0
  mktCap?A.GetAt(1) |> shouldEqual 1010.0
  mktCap?A.GetAt(2) |> shouldEqual 1010.0
  mktCap?A.GetAt(3) |> shouldEqual 1010.0
  mktCap?A.GetAt(4) |> shouldEqual 1020.0
  mktCap?A.GetAt(5) |> shouldEqual 1030.0
  mktCap?A.GetAt(6) |> shouldEqual 1040.0

  mktCap?B.GetAt(0) |> shouldEqual 4400.0
  mktCap?B.GetAt(1) |> shouldEqual 4400.0
  mktCap?B.GetAt(2) |> shouldEqual 4400.0
  mktCap?B.GetAt(3) |> shouldEqual 4440.0
  mktCap?B.GetAt(4) |> shouldEqual 4480.0
  mktCap?B.GetAt(5) |> shouldEqual 4520.0
  mktCap?B.GetAt(6) |> shouldEqual 4560.0



[<Test>]
let ``Can zip-align frames with inner-join left-join nearest-greater options`` () =
    // calculate stock mktcap 
  let mktCapCommons = 
    (pxCommons, sharesCommons)
    ||> Frame.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Inner JoinKind.Left Lookup.NearestSmaller
  // calculate stock mktcap for prefs
  let mktCapPrefs = 
    (pxPrefs, sharesPrefs)
    ||> Frame.zipAlignInto (fun (l:float) r -> l*r) JoinKind.Inner JoinKind.Left Lookup.NearestSmaller
  // calculate company mktcap 
  let mktCap = 
    (mktCapCommons, mktCapPrefs)
    ||> Frame.zipAlignInto (fun (l:float) r -> l+r) JoinKind.Left JoinKind.Left Lookup.NearestSmaller
  
  // calculate enterprice value
  let ev = 
    (mktCap, netDebt)
    ||> Frame.zipAlignInto (fun (l:float) r -> l+r) JoinKind.Inner JoinKind.Left Lookup.NearestGreater // net debt is at the year end
  
  ev?A.GetAt(0) |> shouldEqual 1100.0
  ev?A.GetAt(1) |> shouldEqual 1110.0
  ev?A.GetAt(2) |> shouldEqual 1110.0
  ev?A.GetAt(3) |> shouldEqual 1110.0
  ev?A.GetAt(4) |> shouldEqual 1120.0
  ev?A.GetAt(5) |> shouldEqual 1130.0
  ev?A.GetAt(6) |> shouldEqual 1140.0

  ev?B.GetAt(0) |> shouldEqual 5400.0
  ev?B.GetAt(1) |> shouldEqual 5400.0
  ev?B.GetAt(2) |> shouldEqual 5400.0
  ev?B.GetAt(3) |> shouldEqual 5440.0
  ev?B.GetAt(4) |> shouldEqual 5480.0
  ev?B.GetAt(5) |> shouldEqual 5520.0
  ev?B.GetAt(6) |> shouldEqual 5560.0
