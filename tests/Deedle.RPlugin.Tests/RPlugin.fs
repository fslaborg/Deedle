#if INTERACTIVE
#load "../../bin/Deedle.fsx"
#r "../../bin/RProvider.dll"
#r "../../bin/RDotNet.dll"
#r "../../packages/NUnit.2.6.3/lib/nunit.framework.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.RPlugin.Tests
#endif

open Deedle
open RProvider
open RProvider.``base``
open RProvider.datasets
open RProvider.zoo
open FsUnit
open NUnit.Framework

[<Test>]
let ``Can roundtrip data frame (mtcars) between Deedle and R`` () =
  // Get the 'mtcars' data set from R & check that we got something
  let cars1 : Frame<string, string> = R.mtcars.GetValue()
  cars1.RowCount |> should (be greaterThan) 30
  cars1.ColumnCount |> should (be greaterThan) 10

  // Assign the data frame to 'cars' variable and read it back
  R.assign("cars", cars1) |> ignore
  let cars2 = R.get("cars").GetValue()

  // Check that the data frames are the same
  cars1 |> shouldEqual cars2

[<Test>]
let ``Can roundtrip data frames with missing values`` () =
  //
  // NOTE: R.NET always returns data as floats, so we only test this for floats
  // but when R.NET is fixed, it should work for strings & ints too
  //
  let df1 = frame [ "Nums" => series [ 1 => 10.0; 2 => nan; 4 => 15.5 ] ]
  let df2 : Frame<int, string> = R.as_data_frame(df1).GetValue()
  df1 |> shouldEqual df2

[<Test>]
let ``Can pass strings in data frame to R``() = 
  let df = frame [ "Names" => series [ 1 => "Tomas" ] ]
  R.as_data_frame(df).Print() |> should contain "Tomas"

[<Test>]
let ``Can roundtrip time series (EuStockMarkets$FTSE) between Deedle and R`` () =
  let ftseStr = R.parse(text="EuStockMarkets[,\"FTSE\"]")
  let ftse1 : Series<float, float> = R.eval(ftseStr).GetValue()
  R.assign("ft", ftse1) |> ignore
  let ftse2 = R.get("ft").GetValue<Series<float, float>>()
  ftse1 |> shouldEqual ftse2

[<Test>]
let ``Can roundtrip time series with date times between Deedle and R`` () =
  let rnd = System.Random()
  // NOTE: This does not work for "DateTime.Now" because R uses less precise representation
  let ts = series [ for i in 0.0 .. 100.0 -> System.DateTime.Today.AddHours(i), rnd.NextDouble() ]
  R.assign("x", ts) |> ignore
  R.get("x").GetValue<Series<System.DateTime, float>>() 
  |> shouldEqual ts

