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
// Indexing and accessing values
// ------------------------------------------------------------------------------------------------

let msft() = 
  Frame.readCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", true, 10) 
  |> Frame.indexRowsDate "Date"

[<Test>]
let ``Can read MSFT data from CSV file`` () =
  let df = msft()
  df.RowKeys |> Seq.length |> shouldEqual 6527
  df.ColumnKeys |> Seq.length |> shouldEqual 7

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
// Operations - join, ...
// ------------------------------------------------------------------------------------------------


[<Test>]
let ``Can perform left and right join with nearest smaller or greater option`` () =
    let missingValue = OptionalValue.Missing
    
    let dates  = Series.ofObservations(
      [ DateTime(2013,9,9) => 0.0
        DateTime(2013,9,10) => 1.0
        DateTime(2013,9,11) => 2.0 ])
    let times  = Series.ofObservations(
      [ DateTime(2013,9,9, 9, 31, 59) => 0.5
        DateTime(2013,9,10, 9, 31, 59) => 1.5
        DateTime(2013,9,11, 9, 31, 59) => 2.5 ])
    
    let daysFrame = [ "Days" => dates ] |> Frame.ofColumns
    let timesFrame = [ "Times" => times ] |> Frame.ofColumns
  
    // every point in timesFrames is later than in daysFrame, there is no point in times 
    // smaller than the first point in days, therefore first value in "Times" column must be missing
    // after left join with NearestSmaller option
    let daysTimesPrevL = 
        (daysFrame, timesFrame) 
        ||> Frame.align JoinKind.Left Lookup.NearestSmaller
  
    daysTimesPrevL?Times.TryGetAt(0) |> shouldEqual missingValue
    daysTimesPrevL?Times.TryGetAt(1) |> shouldEqual (OptionalValue.ofOption(Some 0.5))
    daysTimesPrevL?Times.TryGetAt(2) |> shouldEqual (OptionalValue.ofOption(Some 1.5))


    // every point in timesFrames is later than in daysFrame, 
    // all values in Times must be as in original series
    let daysTimesNextL = 
        (daysFrame, timesFrame) 
        ||> Frame.align JoinKind.Left Lookup.NearestGreater
  
    daysTimesNextL?Times.TryGetAt(0) |> shouldEqual (OptionalValue.ofOption(Some 0.5))
    daysTimesNextL?Times.TryGetAt(1) |> shouldEqual (OptionalValue.ofOption(Some 1.5))
    daysTimesNextL?Times.TryGetAt(2) |> shouldEqual (OptionalValue.ofOption(Some 2.5))


    // every point in timesFrames is later than in daysFrame, 
    // all values in Days must be as in original series
    let daysTimesPrevR = 
        (daysFrame, timesFrame) 
        ||> Frame.align JoinKind.Right Lookup.NearestSmaller
  
    daysTimesPrevR?Days.TryGetAt(0) |> shouldEqual (OptionalValue.ofOption(Some 0.0))
    daysTimesPrevR?Days.TryGetAt(1) |> shouldEqual (OptionalValue.ofOption(Some 1.0))
    daysTimesPrevR?Days.TryGetAt(2) |> shouldEqual (OptionalValue.ofOption(Some 2.0))

    // every point in timesFrames is later than in daysFrame, 
    // last point in Days must be missing after joining
    let daysTimesNextR = 
        (daysFrame, timesFrame) 
        ||> Frame.align JoinKind.Right Lookup.NearestGreater
  
    daysTimesNextR?Days.TryGetAt(0) |> shouldEqual (OptionalValue.ofOption(Some 1.0))
    daysTimesNextR?Days.TryGetAt(1) |> shouldEqual (OptionalValue.ofOption(Some 2.0))
    daysTimesNextR?Days.TryGetAt(2) |> shouldEqual missingValue