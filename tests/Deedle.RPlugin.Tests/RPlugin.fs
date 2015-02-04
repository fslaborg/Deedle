#if INTERACTIVE
#load "../../bin/Deedle.fsx"
#r "../../bin/RProvider.dll"
#r "../../bin/RProvider.Runtime.dll"
#r "../../bin/RDotNet.dll"
#r "../../packages/NUnit/lib/nunit.framework.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.RPlugin.Tests
#endif

open Deedle
open RProvider
open RProvider.stats
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
let ``Can get frame containing subset of columns and pass it to R``() =
  let mtcars : Frame<string, string> = R.mtcars.GetValue()
  let mtcars' = mtcars.Columns.[["vs";"am";"gear";"carb"]]
  R.as_data_frame(mtcars').GetValue() |> shouldEqual mtcars'

[<Test>]
let ``Can pass numerical stacked frames to R``() = 
  let df1 = frame [ 10 => series [ 1 => 1.0 ]; 11 => series [ 2 => 2.0] ] |> Frame.stack
  R.assign("df", df1) |> ignore
  let df2 : Frame<int, string> = R.get("df").GetValue()
  df1 |> shouldEqual df2

[<Test>]
let ``Can roundtrip data frames with missing values to R`` () =
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
  let ts = series [ for i in 0.0 .. 100.0 -> System.DateTime(2014,1,1).AddHours(i), rnd.NextDouble() ]
  R.assign("x", ts) |> ignore
  R.get("x").GetValue<Series<System.DateTime, float>>() 
  |> shouldEqual ts

[<Test>] 
let ``Can manipulate columns of a frame and pass it to R`` () =
  let cars1 : Frame<string, string> = R.mtcars.GetValue()

  let recreated = cars1.Columns |> Frame.ofColumns
  R.assign("x1", recreated) |> ignore
  R.get("x1").GetValue<_>() |> shouldEqual cars1

  let filtered = cars1.Columns.[ ["mpg"; "cyl"; "disp"] ]
  R.assign("x2", filtered) |> ignore
  R.get("x2").GetValue<_>() |> shouldEqual filtered

  let take3 = cars1.Columns |> Series.take 3 |> Frame.ofColumns
  R.assign("x3", take3) |> ignore
  R.get("x3").GetValue<_>() |> shouldEqual take3

[<Test>]
let ``Can pass data frame containing decimals to R (#90)``() = 
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/sample.csv", inferRows=10) 
  // The data frame has been loaded with decimals
  let types = df.GetFrameData().Columns |> Seq.map (fun (t, _) -> t.Name) |> List.ofSeq
  types |> shouldEqual ["Int32"; "Decimal"; "Decimal"; "Decimal"; "Decimal"; "Decimal"]
  // We can pass it to R and read it back
  // and the value we get is "reasonably same" as the original
  let rframe = df |> R.as_data_frame
  let actual = rframe.GetValue<Frame<int, string>>() 
  round (actual * 100.0) |> shouldEqual (round (df * 100.0))

[<Test>]
let ``Can index rows ordinally and pass the result to R (#146)`` () =
  let df = Frame.ofRowKeys([0; 1])
  df?col <- [10.0; 20.0]
  let df2 = df |> Frame.indexRowsOrdinally 

  R.assign("df", df2) |> ignore
  R.get("df").GetValue<Frame<int, string>>() |> shouldEqual df2

[<Test>]
let ``Filling missing values preserves type of columns (and allows R interop)`` () =
  let df1 = frame [ "A" => series [0 => 0.0; 1 => nan]]
  let df2 = df1 |> Frame.fillMissing Direction.Forward
  let df3 = R.as_data_frame(df2).GetValue<Frame<int, string>>()
  df3 |> shouldEqual df2

[<Test>]
let ``Can get the result of R.cor as a Deedle frame (#212)`` () =
  let df = R.cor(R.mtcars).GetValue<Frame<string, string>>()
  let actual = (round df?mpg).Values |> List.ofSeq
  let expected = [1.; -1.; -1.; -1.; 1.; -1.; 0.; 1.; 1.; 0.; -1.]
  actual |> shouldEqual expected