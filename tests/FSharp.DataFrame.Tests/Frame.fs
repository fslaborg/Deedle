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
  df.CountRows() |> shouldEqual 6527
  df.CountColumns() |> shouldEqual 7

[<Test>]
let ``Applying numerical operation to frame does not affect non-numeric series`` () =
  let df = msft() * 2.0
  let actual = df.GetSeries<DateTime>("Date").GetAt(0).Value.Date 
  actual |> shouldEqual (DateTime(2012, 1, 27))
  
[<Test>]
let ``Can perform numerical operation with a scalar on data frames`` () =
  let df = msft() 

  (df * 2.0)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value * 2.0)
  (df / 2.0)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value / 2.0)
  (df + 2.0)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value + 2.0)
  (df - 2.0)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value - 2.0)
  (2.0 * df)?Open.GetAt(66).Value |> shouldEqual (2.0 * df?Open.GetAt(66).Value)
  (2.0 + df)?Open.GetAt(66).Value |> shouldEqual (2.0 + df?Open.GetAt(66).Value)
  (2.0 - df)?Open.GetAt(66).Value |> shouldEqual (2.0 - df?Open.GetAt(66).Value)
  (2.0 / df)?Open.GetAt(66).Value |> shouldEqual (2.0 / df?Open.GetAt(66).Value)
  
  (df / 2)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value / 2.0)
  (df * 2)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value * 2.0)
  (df + 2)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value + 2.0)
  (df - 2)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value - 2.0)
  (2 * df)?Open.GetAt(66).Value |> shouldEqual (2.0 * df?Open.GetAt(66).Value)
  (2 + df)?Open.GetAt(66).Value |> shouldEqual (2.0 + df?Open.GetAt(66).Value)
  (2 - df)?Open.GetAt(66).Value |> shouldEqual (2.0 - df?Open.GetAt(66).Value)
  (2 / df)?Open.GetAt(66).Value |> shouldEqual (2.0 / df?Open.GetAt(66).Value)

[<Test>]
let ``Can perform numerical operation with a series on data frames`` () =
  let df = msft() 

  let opens = df?Open
  (df * opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value * opens.GetAt(66).Value)
  (df / opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value / opens.GetAt(66).Value)
  (df + opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value + opens.GetAt(66).Value)
  (df - opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value - opens.GetAt(66).Value)
  (opens * df)?Open.GetAt(66).Value |> shouldEqual (opens.GetAt(66).Value * df?Open.GetAt(66).Value)
  (opens + df)?Open.GetAt(66).Value |> shouldEqual (opens.GetAt(66).Value + df?Open.GetAt(66).Value)
  (opens - df)?Open.GetAt(66).Value |> shouldEqual (opens.GetAt(66).Value - df?Open.GetAt(66).Value)
  (opens / df)?Open.GetAt(66).Value |> shouldEqual (opens.GetAt(66).Value / df?Open.GetAt(66).Value)
  
  let opens = int $ df?Open 
  (df * opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value * float (opens.GetAt(66).Value))
  (df / opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value / float (opens.GetAt(66).Value))
  (df + opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value + float (opens.GetAt(66).Value))
  (df - opens)?Open.GetAt(66).Value |> shouldEqual (df?Open.GetAt(66).Value - float (opens.GetAt(66).Value))
  (opens * df)?Open.GetAt(66).Value |> shouldEqual (float (opens.GetAt(66).Value) * df?Open.GetAt(66).Value)
  (opens + df)?Open.GetAt(66).Value |> shouldEqual (float (opens.GetAt(66).Value) + df?Open.GetAt(66).Value)
  (opens - df)?Open.GetAt(66).Value |> shouldEqual (float (opens.GetAt(66).Value) - df?Open.GetAt(66).Value)
  (opens / df)?Open.GetAt(66).Value |> shouldEqual (float (opens.GetAt(66).Value) / df?Open.GetAt(66).Value)
  
[<Test>]
let ``Can perform pointwise numerical operations on two frames`` () =
  let df1 = msft() |> Frame.orderRows
  let df2 = df1 |> Frame.shift 1
  let opens1 = df1?Open
  let opens2 = df2?Open

  (df2 - df1)?Open.GetAt(66).Value |> shouldEqual (opens2.GetAt(66).Value - opens1.GetAt(66).Value)
  (df2 + df1)?Open.GetAt(66).Value |> shouldEqual (opens2.GetAt(66).Value + opens1.GetAt(66).Value)
  (df2 * df1)?Open.GetAt(66).Value |> shouldEqual (opens2.GetAt(66).Value * opens1.GetAt(66).Value)
  (df2 / df1)?Open.GetAt(66).Value |> shouldEqual (opens2.GetAt(66).Value / opens1.GetAt(66).Value)
