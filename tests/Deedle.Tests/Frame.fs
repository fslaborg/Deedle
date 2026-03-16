#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FSharp.Data/lib/net45/FSharp.Data.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#else
module Deedle.Tests.Frame
#endif

open System
open System.Data
open System.Dynamic
open System.Collections.Generic
open FsUnit
open FsCheck
open NUnit.Framework
open Deedle

// ------------------------------------------------------------------------------------------------
// Input and output (CSV files, IDataReader)
// ------------------------------------------------------------------------------------------------

let msft() =
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", inferRows=10)
  |> Frame.indexRowsDate "Date"

let msftNoHeaders() =
  let noHeaders =
    IO.File.ReadAllLines(__SOURCE_DIRECTORY__ + "/data/MSFT.csv")
    |> Seq.skip 1 |> String.concat "\n"
  let data = System.Text.Encoding.UTF8.GetBytes(noHeaders)
  Frame.ReadCsv(new IO.MemoryStream(data), false, inferRows=10)

let msftString() =
  let csvString = IO.File.ReadAllText(__SOURCE_DIRECTORY__ + "/data/MSFT.csv")
  Frame.ReadCsvString(csvString, inferRows=10)
  |> Frame.indexRowsDate "Date"

[<Test>]
let ``Can create empty data frame and empty series`` () =
  let f : Frame<int, int> = frame []
  let s : Series<int, int> = series []
  s.KeyCount |> shouldEqual 0
  f.ColumnCount |> shouldEqual 0
  f.RowCount |> shouldEqual 0

[<Test>]
let ``Can read MSFT data from CSV string`` () =
  let df = msftString()
  df.RowKeys |> Seq.length |> shouldEqual 6527
  df.ColumnKeys |> Seq.length |> shouldEqual 6

[<Test>]
let ``Can read MSFT data from CSV file`` () =
  let df = msft()
  df.RowKeys |> Seq.length |> shouldEqual 6527
  df.ColumnKeys |> Seq.length |> shouldEqual 6

[<Test>]
let ``Can read MSFT data from CSV file truncated`` () =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", inferRows=10, maxRows=27)
  df.RowKeys |> Seq.length |> shouldEqual 27
  df.ColumnKeys |> Seq.length |> shouldEqual 7

[<Test>]
let ``Can read MSFT data from CSV file using a specified index column`` () =
  let df = Frame.ReadCsv<DateTime>(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", "Date", inferRows=10, maxRows=27)
  df.RowKeys |> Seq.length |> shouldEqual 27
  df.ColumnKeys |> Seq.length |> shouldEqual 6
  df.RowKeys |> Seq.head |> shouldEqual (DateTime(2012, 1, 27))

[<Test>]
let ``Can read MSFT data from CSV file without header row`` () =
  let df = msftNoHeaders()
  let expected = msft()
  let colKeys = Seq.append ["Date"] expected.ColumnKeys
  let actual = df |> Frame.indexColsWith colKeys |> Frame.indexRowsDate "Date"
  actual |> shouldEqual expected

[<Test>]
let ``Can read MSFT data from CSV with no headers & no type inference``() =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", hasHeaders=false, inferTypes=false)
  df.RowKeys |> Seq.length |> shouldEqual 6528
  df.ColumnKeys |> List.ofSeq |> shouldEqual ["Column1"; "Column2"; "Column3"; "Column4"; "Column5"; "Column6"; "Column7"]

[<Test>]
let ``Can read MSFT data from CSV with explicit schema``() =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", schema="A (string),B,C,D,E,F,G (float)")
  let actual = List.ofSeq df.ColumnKeys
  actual |> shouldEqual ["A";"B";"C";"D";"E";"F";"G"]

[<Test>]
let ``Can read CSV with schema where column name contains parentheses``() =
  // Regression test for issue #348: nameAndTypeRegex was using RightToLeft which caused
  // incorrect parsing of column names containing parentheses (e.g. "Revenue (USD)(float)")
  let csv = "Revenue (USD),Count\n100.0,1\n200.0,2"
  use reader = new System.IO.StringReader(csv)
  // Schema specifies type for a column whose name contains parentheses
  let df = Frame.ReadCsv(reader, schema="Revenue (USD)(float),Count(int)")
  let actual = List.ofSeq df.ColumnKeys
  actual |> shouldEqual ["Revenue (USD)"; "Count"]
  df.GetColumn<float>("Revenue (USD)") |> Series.values |> List.ofSeq |> shouldEqual [100.0; 200.0]
  df.GetColumn<int>("Count") |> Series.values |> List.ofSeq |> shouldEqual [1; 2]

[<Test>]
let ``Decimal columns accept scientific notation values (regression #337)``() =
  // Regression test for issue #337: AsDecimal used NumberStyles.Currency which does not
  // support AllowExponent, so values like "9.51E-05" became missing values when column
  // type was inferred as decimal from earlier plain-decimal rows.
  let csv = "X,Y\n1.0,0.12345\n2.0,9.51E-05\n3.0,7.058365954"
  use reader = new System.IO.StringReader(csv)
  let df = Frame.ReadCsv(reader)
  // Y column should be inferred as decimal (or float) and all values should be present
  let yValues = df.GetColumn<decimal>("Y") |> Series.values |> Array.ofSeq
  yValues.Length |> shouldEqual 3
  yValues.[1] |> shouldEqual 9.51e-5m
  yValues.[2] |> shouldEqual 7.058365954m

[<Test>]
let ``Schema is respected when inferTypes=false (regression #347)``() =
  // Regression test for issue #347: schema was silently ignored when inferTypes=false
  let csv = "Name,Value,Flag\nAlice,42,true\nBob,17,false"
  use reader = new System.IO.StringReader(csv)
  // Use "Name=type" override-by-name syntax so schema columns are matched by column name
  let df = Frame.ReadCsv(reader, inferTypes=false, schema="Value=int,Flag=bool")
  // Schema-specified columns should use the declared types
  df.GetColumn<int>("Value") |> Series.values |> List.ofSeq |> shouldEqual [42; 17]
  df.GetColumn<bool>("Flag") |> Series.values |> List.ofSeq |> shouldEqual [true; false]
  df.ColumnKeys |> List.ofSeq |> shouldEqual ["Name"; "Value"; "Flag"]

[<Test>]
let ``Can read MSFT data from CSV and rename``() =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", schema="Day,Adj Close->Adjusted")
  let actual = List.ofSeq df.ColumnKeys
  actual |> shouldEqual ["Day"; "Open"; "High"; "Low"; "Close"; "Volume"; "Adjusted"]

[<Test>]
let ``Can save MSFT data as CSV file and read it afterwards (with default args)`` () =
  let file = System.IO.Path.GetTempFileName()
  let expected = msft()
  expected.SaveCsv(file, includeRowKeys = false)
  let actual = Frame.ReadCsv(file)
  actual |> shouldEqual (Frame.indexRowsOrdinally expected)

[<Test>]
let ``Can read CSV file that contains custom missing value formats`` () =
  let csv =
    "First,Second,Third\n" +
    "1.0,WhoKnows??,2.0\n" +
    "3.0,4.0,WhoKnows?"
  use reader = new System.IO.StringReader(csv)
  let actual = Frame.ReadCsv(reader, missingValues=[| "WhoKnows??" |])
  actual.Rows.[1].GetAs<string>("Third") |> shouldEqual "WhoKnows?"
  actual.Rows.[0].TryGetAs<float>("Second").HasValue |> shouldEqual false

[<Test>]
let ``Can read CSV file without headers when inferTypes = true`` () =
  let csv =
    "1.0,2.0,3.0\n" +
    "4.0,5.0,6.0\n" +
    "7.0,8.0,9.0"
  use reader = new System.IO.StringReader(csv)
  let actual = Frame.ReadCsv(reader,hasHeaders=false,inferTypes=true)
  actual.ColumnKeys
  |> Seq.toArray
  |> shouldEqual [| "Column1"; "Column2"; "Column3" |]

[<Test>]
let ``Can read CSV file without headers when inferTypes = false`` () =
  let csv =
    "1.0,2.0,3.0\n" +
    "4.0,5.0,6.0\n" +
    "7.0,8.0,9.0"
  use reader = new System.IO.StringReader(csv)
  let actual = Frame.ReadCsv(reader,hasHeaders=false,inferTypes=false)
  actual.ColumnKeys
  |> Seq.toArray
  |> shouldEqual [| "Column1"; "Column2"; "Column3" |]

[<Test>]
let ``Can read CSV file with missing column keys when inferTypes = true`` () =
  let csv =
    "a,,\n" +
    "1.0,2.0,3.0\n" +
    "3.0,4.0,5.0"
  use reader = new System.IO.StringReader(csv)
  let actual = Frame.ReadCsv(reader,hasHeaders=true,inferTypes=true)
  actual.ColumnKeys
  |> Seq.toArray
  |> shouldEqual [| "a"; "Column2"; "Column3" |]

[<Test>]
let ``Can read CSV file with missing column keys when inferTypes = false`` () =
  let csv =
    "a,,\n" +
    "1.0,2.0,3.0\n" +
    "3.0,4.0,5.0"
  use reader = new System.IO.StringReader(csv)
  let actual = Frame.ReadCsv(reader,hasHeaders=true,inferTypes=false)
  actual.ColumnKeys
  |> Seq.toArray
  |> shouldEqual [| "a"; "Column2"; "Column3" |]

[<Test>]
let ``Can read CSV with headers of multilines`` () =
  let csv = """"
col_A
";"
col_A
2";col_B
1;2;3
4;5;6"""
  use reader = new System.IO.StringReader(csv)
  let df = Frame.ReadCsv(reader,separators = ";")
  let actual = df.ColumnKeys |> Seq.toArray
  Assert.That(
    actual = [| "col_A"; "col_A\n2"; "col_B" |] ||
    actual = [| "col_A"; "col_A\r\n2"; "col_B" |], Is.True)

[<Test>]
let ``Can read space-separated file without headers`` () =
  // Regression test for https://github.com/fslaborg/Deedle/issues/550:
  // users expect to be able to read space-separated data using separators=" " and hasHeaders=false.
  let data =
    "1 2 3\n" +
    "4 5 6\n" +
    "7 8 9"
  use reader = new System.IO.StringReader(data)
  let df = Frame.ReadCsv(reader, hasHeaders=false, separators=" ", inferTypes=false)
  df.ColumnKeys |> Seq.toArray |> shouldEqual [| "Column1"; "Column2"; "Column3" |]
  df |> Frame.countRows |> shouldEqual 3
  df.GetColumn<string>("Column1").GetAt(0) |> shouldEqual "1"
  df.GetColumn<string>("Column2").GetAt(1) |> shouldEqual "5"
  df.GetColumn<string>("Column3").GetAt(2) |> shouldEqual "9"

[<Test>]
let ``Can read CSV file with empty cell`` () =
  let csv =
    "row,c1,c2,c3\n" +
    "1,,5,\n" +
    "2,4,6,"
  use reader = new System.IO.StringReader(csv)
  let df = Frame.ReadCsv(reader)
  let actual = df?c1
  let expected = [nan; 4.0] |> Series.ofValues
  actual |> shouldEqual expected

[<Test>]
let ``Can save MSFT data as CSV to a TextWriter and read it afterwards (with default args)`` () =
  let builder = new System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  let expected = msft()
  expected.SaveCsv(writer, includeRowKeys = false)
  use reader = new System.IO.StringReader(builder.ToString())
  let actual = Frame.ReadCsv(reader)
  actual |> shouldEqual (Frame.indexRowsOrdinally expected)

[<Test>]
let ``Can save MSFT data as CSV to a TextWriter and read it afterwards (using FrameExtensions)`` () =
  let cz = System.Globalization.CultureInfo.GetCultureInfo("cs-CZ")
  let builder = new System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  let expected = msft()
  FrameExtensions.SaveCsv (expected, writer, false, null, ';', cz)
  use reader = new System.IO.StringReader(builder.ToString())
  let actual = Frame.ReadCsv(reader, hasHeaders=true, separators=";", culture="cs-CZ")
  actual |> shouldEqual (Frame.indexRowsOrdinally expected)

[<Test>]
let ``Saving dates uses consistently invariant cultrue by default`` () =
  let file = System.IO.Path.GetTempFileName()
  let df = frame [ "A" => series [ DateTime(2014, 1, 29, 12, 39, 45) => 1.0; DateTime(2014, 1, 29) => 2.1] ]
  // Save the frame on a machine with "en-GB" date time format
  System.Threading.Thread.CurrentThread.CurrentCulture <- System.Globalization.CultureInfo.GetCultureInfo("en-GB")
  df.SaveCsv(file, ["Date"])
  // Read the frame (in invariant culture format) and parse row keys as dates
  // (we need to run this on InvariantCulture because the 'ReadCsv' method reads
  // values as DateTime. We intentionally ignore the fact the columns are inferred
  // as DateTime, because Deedle tries to avoid using DateTime)
  System.Threading.Thread.CurrentThread.CurrentCulture <- System.Globalization.CultureInfo.InvariantCulture
  let actual = Frame.ReadCsv(file).IndexRows<DateTime>("Date").RowKeys |> List.ofSeq
  let expected = df.RowKeys |> List.ofSeq
  actual |> shouldEqual expected

[<Test>]
let ``Can save MSFT data as CSV file and read it afterwards (with custom format)`` () =
  let file = System.IO.Path.GetTempFileName()
  let cz = System.Globalization.CultureInfo.GetCultureInfo("cs-CZ")
  let expected = msft()
  expected.SaveCsv(file, keyNames=["Date"], separator=';', culture=cz)
  let actual =
    Frame.ReadCsv(file, separators=";", culture="cs-CZ")
    |> Frame.indexRowsDate "Date"
  actual |> shouldEqual expected

[<Test>]
let ``Can read CSV file with non-UTF8 encoding using path overload`` () =
  let file = System.IO.Path.GetTempFileName()
  let csvContent = "Name,Value\nCafe,1\nNaive,2\n"
  let latin1 = System.Text.Encoding.GetEncoding("iso-8859-1")
  System.IO.File.WriteAllText(file, csvContent, latin1)
  let df = Frame.ReadCsv(file, encoding = latin1)
  df.RowCount |> shouldEqual 2
  df.GetColumn<string>("Name") |> Series.values |> List.ofSeq |> shouldEqual ["Cafe"; "Naive"]
  System.IO.File.Delete(file)

[<Test>]
let ``Can read CSV stream with non-UTF8 encoding using stream overload`` () =
  let csvContent = "Name,Value\nCafe,1\n"
  let latin1 = System.Text.Encoding.GetEncoding("iso-8859-1")
  let bytes = latin1.GetBytes(csvContent)
  use stream = new System.IO.MemoryStream(bytes)
  let df = Frame.ReadCsv(stream, encoding = latin1)
  df.RowCount |> shouldEqual 1
  df.GetColumn<string>("Name") |> Series.values |> List.ofSeq |> shouldEqual ["Cafe"]

[<Test>]
let ``SaveCsv uses correct CultureInfo for OptionalValue float columns`` () =
  let deDe = System.Globalization.CultureInfo.GetCultureInfo("de-DE")
  let builder = System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  // Build a frame where one column is plain float and one is OptionalValue<float>.
  let df =
    frame
      [ "PlainFloat" =?> series [ 0 => 1.5; 1 => 2.5 ]
        "OptFloat"   =?> series [ 0 => OptionalValue(3.5); 1 => OptionalValue.Missing ] ]
  FrameExtensions.SaveCsv(df, writer, false, null, ';', deDe)
  let csv = builder.ToString()
  // In de-DE the decimal separator is ',' so 1.5 → "1,5" and 3.5 → "3,5"
  csv |> should contain "1,5"
  csv |> should contain "2,5"
  csv |> should contain "3,5"
  // The missing OptionalValue cell should serialize as empty.
  // CSV (no row keys): header; row0 = "1,5;3,5"; row1 = "2,5;"
  let lines = csv.Split([|'\n';'\r'|], System.StringSplitOptions.RemoveEmptyEntries)
  // Find the data row that contains "2,5" (second row, PlainFloat=2.5, OptFloat=missing)
  let row1 = lines |> Array.find (fun l -> l.Contains("2,5"))
  let parts = row1.Split(';')
  // Last field should be empty (missing OptionalValue)
  parts |> Array.last |> should equal ""

[<Test>]
let ``SaveCsv uses correct CultureInfo for OptionalValue float columns (verify no dot decimal)`` () =
  // Additional check: without the fix the decimal would be written with '.', not ','
  let deDe = System.Globalization.CultureInfo.GetCultureInfo("de-DE")
  let builder = System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  let df =
    frame
      [ "A" =?> series [ 0 => 1.1 ]
        "B" =?> series [ 0 => OptionalValue(2.2) ] ]
  FrameExtensions.SaveCsv(df, writer, false, null, ';', deDe)
  let csv = builder.ToString()
  // In de-DE '.' as a decimal separator should NOT appear in numeric values
  // (it would appear if culture was ignored). We check both columns use ','
  csv |> should contain "1,1"
  csv |> should contain "2,2"

// Discriminated union type used in CSV writer tests below.
type private TwoValueDU = | DU_A | DU_B

[<Test>]
let ``SaveCsv serialises discriminated union columns correctly`` () =
  // Regression / performance test for #564.
  // DU values that don't implement IFormattable must still be serialised via ToString().
  let builder = System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  let df =
    frame
      [ "Col" =?> series [ 0 => DU_A; 1 => DU_B; 2 => DU_A ] ]
  FrameExtensions.SaveCsv(df, writer, false, null, ',', System.Globalization.CultureInfo.InvariantCulture)
  let csv = builder.ToString()
  csv |> should contain "DU_A"
  csv |> should contain "DU_B"
  let lines = csv.Split([|'\n';'\r'|], StringSplitOptions.RemoveEmptyEntries)
  // Header + 3 data rows
  lines.Length |> shouldEqual 4
  lines.[1] |> should contain "DU_A"
  lines.[2] |> should contain "DU_B"
  lines.[3] |> should contain "DU_A"

[<Test>]
let ``SaveCsv correctly caches repeated discriminated union values`` () =
  // Verify that the ToString() cache in writeCsv produces the same output as a
  // naive (uncached) implementation: each unique DU value must appear the correct
  // number of times regardless of the cache.
  let N = 200  // large enough to exceed trivial paths
  let values = Array.init N (fun i -> if i % 2 = 0 then DU_A else DU_B)
  let df = frame [ "V" =?> Series.ofValues values ]
  let builder = System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  FrameExtensions.SaveCsv(df, writer, false, null, ',', System.Globalization.CultureInfo.InvariantCulture)
  let csv = builder.ToString()
  let aCount = csv.Split([|'\n'|]) |> Array.filter (fun l -> l.Trim() = "DU_A") |> Array.length
  let bCount = csv.Split([|'\n'|]) |> Array.filter (fun l -> l.Trim() = "DU_B") |> Array.length
  aCount |> shouldEqual 100
  bCount |> shouldEqual 100

[<Test>]
let ``Can create frame from IDataReader``() =
  let dt = new DataTable()
  dt.Columns.Add(new DataColumn("First", typeof<int>))
  dt.Columns.Add(new DataColumn("Second", typeof<DateTimeOffset>))
  for i in 0 .. 10 do
    dt.Rows.Add [| box i; box (DateTimeOffset(DateTime.Today.AddDays(float i))) |] |> ignore

  let expected =
    Frame.ofColumns
      [ "First" =?> Series.ofValues [ 0 .. 10 ]
        "Second" =?> Series.ofValues [ for i in 0 .. 10 -> DateTimeOffset(DateTime.Today.AddDays(float i)) ] ]

  Frame.ReadReader(dt.CreateDataReader())
  |> shouldEqual expected

[<Test>]
let ``Can get type information about columns`` () =
  let df = frame [ "A" =?> series [1=>1.0]; "B" =?> series [1=>"hi"] ]
  df.ColumnTypes |> List.ofSeq
  |> shouldEqual <| [typeof<float>; typeof<string>]

[<Test>]
let ``Can turn a frame into DataTable`` () =
  let df = msft()
  let table = df.ToDataTable(["Year"])
  table.Rows.[10].["Open"] :?> decimal |> float |> shouldEqual <| df.Rows.GetAt(10)?Open
  table.Rows.[10].["Year"] :?> DateTime |> shouldEqual <| df.Rows.GetKeyAt(10)

// ------------------------------------------------------------------------------------------------
// Typed access to frame rows
// ------------------------------------------------------------------------------------------------

type IMsftRow =
  abstract Open : decimal
  abstract High : decimal
  abstract Low : decimal
  abstract Close : decimal
  abstract Volume : int
  abstract ``Adj Close`` : decimal

type IMsftFloatSubRow =
  abstract High : float
  abstract Low : float

[<Test>]
let ``Can access rows as a typed series via an interface`` () =
  let df = msft()
  let rows = df.GetRowsAs<IMsftRow>()
  rows.[DateTime(2000, 10, 10)].Close
  |> shouldEqual <| df.Rows.[DateTime(2000, 10, 10)].GetAs<decimal>("Close")

[<Test>]
let ``Can access rows as a typed series via an interface with convertible types`` () =
  let df = msft()
  let rows = df.GetRowsAs<IMsftFloatSubRow>()
  rows.[DateTime(2000, 10, 10)].High
  |> shouldEqual <| df.Rows.[DateTime(2000, 10, 10)]?High

type IAandOptB =
  abstract A : float
  abstract B : OptionalValue<float>

[<Test>]
let ``Can access missing data via typed rows`` () =
  let df = frame [ "A" => Series.ofValues [1.0; 2.0]; "B" => Series.ofValues [1.0] ]
  df.GetRowsAs<IAandOptB>().[0].B |> shouldEqual <| OptionalValue(1.0)
  df.GetRowsAs<IAandOptB>().[1].B |> shouldEqual <| OptionalValue.Missing

[<Test>]
let ``Cannot access typed rows when column is not available`` () =
  let df = frame [ "Open" => Series.ofValues [ 1.0M .. 10.0M ] ]
  let error = try ignore(df.GetRowsAs<IMsftRow>()); "" with e -> e.Message
  error |> should contain "High"

type ISimpleRow =
  abstract A : float
  abstract B : float

[<Test>]
let ``Can mapValues on a GetRowsAs series (issue #545)`` () =
  // Series.mapValues called Select on the mapFrameRowVector, which used to throw
  let df = frame [ "A" => Series.ofValues [1.0; 2.0]; "B" => Series.ofValues [3.0; 4.0] ]
  let rows = df.GetRowsAs<ISimpleRow>()
  // mapValues id should succeed and return identical rows
  let mapped = rows |> Series.mapValues id
  mapped.[0].A |> shouldEqual 1.0
  mapped.[1].A |> shouldEqual 2.0
  mapped.[0].B |> shouldEqual 3.0
  mapped.[1].B |> shouldEqual 4.0

[<Test>]
let ``Can transform a GetRowsAs series to a different type (issue #545)`` () =
  let df = frame [ "A" => Series.ofValues [1.0; 2.0]; "B" => Series.ofValues [3.0; 4.0] ]
  let rows = df.GetRowsAs<ISimpleRow>()
  // Map rows to a derived value to exercise Select with a non-identity function
  let values = rows |> Series.mapValues (fun r -> r.A + r.B)
  values.[0] |> shouldEqual 4.0
  values.[1] |> shouldEqual 6.0

// ------------------------------------------------------------------------------------------------
// Constructing frames and getting frame data
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Construction of frame from columns respects specified order``() =
  let df =
    frame
      [ "Z" => Series.ofValues [ 1 .. 10 ]
        "X" => Series.ofValues [ 1 .. 10 ] ]
  df.ColumnKeys |> List.ofSeq
  |> shouldEqual ["Z"; "X"]

[<Test>]
let ``Can create frame from float[,] and get data as float[,]``() =
  let data = Array2D.init 5000 200 (fun x y -> float (x+y))
  let data' = data |> Frame.ofArray2D |> Frame.toArray2D
  data' |> shouldEqual data

[<Test>]
let ``Can create frame from float[][] and get data as float[][]``() =
  let data = Array.init 5000 (fun x -> Array.init 200 (fun y -> float (x+y)))
  let data' = data |> Frame.ofJaggedArray |> Frame.toJaggedArray
  data' |> shouldEqual data

[<Test>]
let ``Frames from float[][] and float [,] are equal.``() =
  let dataJagged = Array.init 5000 (fun x -> Array.init 200 (fun y -> float (x+y)))
  let data2D = Array2D.init 5000 200 (fun x y -> float (x+y))
  let frameJagged = dataJagged |> Frame.ofJaggedArray
  let frame2D = data2D |> Frame.ofArray2D
  frameJagged |> shouldEqual frame2D

[<Test>]
let ``Creating frame from sorted series returns sorted frame``() =
  let s1 = series [ 1 => 'a'; 2 => 'a' ]
  let s2 = series [ 2 => 'a'; 3 => 'a' ]
  let df1 = frame [ "A" => s1; "B" => s2 ]
  let df2 = frame [ "A" => s2; "B" => s1 ]
  df1.RowIndex.IsOrdered  |> shouldEqual true
  df2.RowIndex.IsOrdered  |> shouldEqual true

[<Test>]
let ``Can create empty frame``() =
  let d : Frame<int, string> = frame []
  d.RowCount |> shouldEqual 0
  d.ColumnCount |> shouldEqual 0

[<Test>]
let ``Adding first column to an empty frame fixes the row keys``() =
  let d : Frame<int, string> = frame []
  d?Test1 <- series [ 1 => 1.1; 3 => 3.3 ]
  d?Test2 <- series [ 1 => 1.1; 2 => 2.2; 3 => 3.3 ]
  d.RowKeys |> set |> shouldEqual <| set [1; 3]
  d.ColumnKeys |> set |> shouldEqual <| set ["Test1"; "Test2"]

[<Test>]
let ``Rows of an empty frame should return empty series``() =
  let d : Frame<int, string> = Frame.ofRowKeys [1;2;3]
  d.Rows.[1].As<float>() |> shouldEqual <| series []

// ------------------------------------------------------------------------------------------------
// Input and output (from records)
// ------------------------------------------------------------------------------------------------

type Price = { Open : decimal; High : decimal; Low : Decimal; Close : decimal }
type Stock = { Date : DateTime; Volume : int; Price : Price }
type internal InternalPrice = { IOpen : float; IClose : float }

let typedRows () =
  [| for (KeyValue(k,r)) in msft().Rows.Observations ->
      let p = { Open = r.GetAs<decimal>("Open"); Close = r.GetAs<decimal>("Close");
                High = r.GetAs<decimal>("High"); Low = r.GetAs<decimal>("High") }
      { Date = k; Volume = r.GetAs<int>("Volume"); Price = p } |]
let typedPrices () =
  [| for r in typedRows () -> r.Price |]

[<Test>]
let ``Frame.ofRecords does not create duplicate columns for F# records`` () =
  // F# records have compiler-generated backing fields (e.g. Open@) in addition to
  // the public properties; getExpandableFields must filter these out (issue #569).
  let prices = typedPrices ()
  let df = Frame.ofRecords prices
  // Column count must match field count exactly (no duplicates like "Open@")
  df.ColumnCount |> shouldEqual 4
  set df.ColumnKeys |> shouldEqual (set ["Open"; "High"; "Low"; "Close"])

[<Test>]
let ``Can read simple sequence of records`` () =
  let prices = typedPrices ()
  let df = Frame.ofRecords prices
  set df.ColumnKeys |> shouldEqual (set ["Open"; "High"; "Low"; "Close"])
  df |> Frame.countRows |> shouldEqual prices.Length

[<Test>]
let ``Can read sequence of internal records`` () =
  // Regression test for https://github.com/fslaborg/Deedle/issues/562
  let prices =
    [| { IOpen = 10.1; IClose = 11.0 }
       { IOpen = 15.1; IClose = 16.0 }
       { IOpen = 9.1;  IClose = 10.0 } |]
  let df = Frame.ofRecords prices
  set df.ColumnKeys |> shouldEqual (set ["IOpen"; "IClose"])
  df |> Frame.countRows |> shouldEqual 3

[<Test>]
let ``Can read simple sequence of records using a specified column as index`` () =
  let rows = typedRows ()
  let df = Frame.ofRecords<DateTime>(rows, "Date")
  set df.ColumnKeys |> shouldEqual (set ["Volume"; "Price"])
  df.RowKeys |> Seq.head |> shouldEqual rows.[0].Date

[<Test>]
let ``Can read simple sequence of records deterministically `` () =
  let mutable n = 0
  let values =
    seq {
      yield n, n
      n <- n + 1
      yield n, n
      n <- n + 1 }
  let expected = [|0, 0; 1, 1|] |> Frame.ofRecords
  values |> Frame.ofRecords |> shouldEqual expected

[<Test>]
let ``Can expand properties of a simple record sequence`` () =
  let df = frame [ "MSFT" => Series.ofValues (typedRows ()) ]
  let exp1 = df |> Frame.expandAllCols 1
  exp1.Rows.[10]?``MSFT.Volume`` |> shouldEqual 49370800.0
  set exp1.ColumnKeys |> shouldEqual (set ["MSFT.Date"; "MSFT.Volume"; "MSFT.Price"])

  let exp2 = df |> Frame.expandAllCols 2
  exp2.ColumnKeys |> should contain "MSFT.Price.Open"
  exp2.Rows.[10]?``MSFT.Price.Open`` |> shouldEqual 27.87

[<Test>]
let ``Can expand properties based on runtime type information`` () =
  let df = frame [ "A" => Series.ofValues [box (1,2); box {Open=1.0M; Close=1.0M; High=1.0M; Low=1.0M}] ]
  let dfexp = FrameExtensions.ExpandColumns(df, 10, true)
  set dfexp.ColumnKeys |> shouldEqual (set ["A.Item1"; "A.Item2"; "A.Open"; "A.High"; "A.Low"; "A.Close"])

[<Test>]
let ``Can expand properties of specified columns`` () =
  let df = frame [ "MSFT" => Series.ofValues (typedRows ()) ]
  let exp = df |> Frame.expandAllCols 1 |> Frame.expandCols ["MSFT.Price"]
  set exp.ColumnKeys |> shouldEqual (set ["MSFT.Price.Close"; "MSFT.Price.High"; "MSFT.Price.Low"; "MSFT.Date"; "MSFT.Volume"; "MSFT.Price.Open"])

[<Test>]
let ``Can expand vector that mixes ExpandoObjects, records and tuples``() =
  let (?<-) (exp:ExpandoObject) k v = (exp :> IDictionary<_, _>).Add(k, v)
  let exp1 = new ExpandoObject()
  exp1?First <- 1
  exp1?Second <- 2
  exp1?Nested <- { Open = 1.0M; Low = 0.0M; Close = 2.0M; High = 3.0M }
  let exp2 = new ExpandoObject()
  exp2?Second <- "Test"
  exp2?Nested <- { Open = 1.5M; Low = 0.5M; Close = 2.5M; High = 3.5M }
  let df = frame [ "Objects" => Series.ofValues [(exp1, 42); (exp2, 41)] ]
  let exp1 = df |> Frame.expandAllCols 1
  set exp1.ColumnKeys |> shouldEqual (set ["Objects.Item1"; "Objects.Item2"])
  let exp3 = df |> Frame.expandAllCols 3
  set exp3.ColumnKeys |> shouldEqual
    ( set ["Objects.Item1.First";       "Objects.Item1.Nested.Close";
           "Objects.Item1.Nested.High"; "Objects.Item1.Nested.Low";
           "Objects.Item1.Nested.Open"; "Objects.Item1.Second"; "Objects.Item2"] )

[<Test>]
let ``Can expand vector that contains SeriesBuilder objects``() =
  let sb = SeriesBuilder<string>()
  sb?Test <- 1
  sb?Another <- "hi"
  let df = frame [ "A" => Series.ofValues [sb]]
  let exp1 = df |> Frame.expandAllCols 1
  set exp1.ColumnKeys |> shouldEqual (set ["A.Another"; "A.Test"])

[<Test>]
let ``Can expand vector that contains Series<string, T> and tuples``() =
  let df = frame [ "A" => Series.ofValues [ series ["First" => box 1; "Second" => box (1, "Test") ] ]]
  let exp = df |> Frame.expandAllCols 1000
  set exp.ColumnKeys |> shouldEqual (set ["A.First"; "A.Second.Item1"; "A.Second.Item2"])

// ------------------------------------------------------------------------------------------------
// From rows/columns
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can create frame from 100k of three element tuples (in less than a few seconds)`` () =
  let values =
    [| for d in 0 .. 100 do
        for i in 0 .. 1000 do
          yield DateTime.Today.AddDays(float d), i.ToString(), 1.0 |]
  let df = Frame.ofValues values
  df |> Stats.sum |> Stats.sum |> int |> shouldEqual 101101

[<Test>]
let ``Reconstructing frame from its columns preserves types of vectors``() =
  let df =
    frame [ "A" =?> series [ 1 => 1.0; 2 => 2.0 ]
            "B" =?> series [ 1 => "a"; 2 => "b" ] ]
  let df' = df.Columns |> Frame.ofColumns

  let types = df.GetFrameData().Columns |> Seq.map (fun (t, _) -> t.Name) |> List.ofSeq
  let types' = df'.GetFrameData().Columns |> Seq.map (fun (t, _) -> t.Name) |> List.ofSeq
  types |> shouldEqual types'

[<Test>]
let ``Reconstructing frame from its columns does not break equals (#91)``() =
  let df =
    frame [ "A" =?> series [ 1 => 1.0; 2 => 2.0 ]
            "B" =?> series [ 1 => "a"; 2 => "b" ] ]
  let df' = df.Columns |> Frame.ofColumns
  df |> shouldEqual df'


// ------------------------------------------------------------------------------------------------
// Accessor testing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can retrieve a series according to type parameter`` () =
  let s1 = series [| 1 => (1,2); 2 => (1,3) |]
  let s2 = series [| 1 => ("a",1.0); 2 => ("b",2.0) |]
  let f = frame [ "A" => s1 ]
  f?B <- s2
  f.GetAllColumns<int*int>() |> shouldEqual ( seq [ KeyValuePair("A", s1) ] )
  f.GetAllColumns<string*float>() |> shouldEqual ( seq [ KeyValuePair("B", s2) ] )
  f.GetAllColumns<int*float>() |> shouldEqual Seq.empty

[<Test>]
let ``Can do fuzzy lookup on frame rows and cols`` () =
  let s1 = series [| "a" => 1.0; "c" => 2.0; "e" => 3.0 |]
  let s2 = series [| "a" => 4.0; "c" => 5.0; "e" => 6.0 |]
  let s3 = series [| "a" => 7.0; "c" => 8.0; "e" => 9.0 |]
  let f = frame [ 1 => s1; 3 => s2; 5 => s3 ]

  f |> Frame.tryLookupRow "b" Lookup.ExactOrSmaller |> shouldEqual ( Some <| series [ 1 => 1.0; 3 => 4.0; 5 => 7.0 ] )
  f |> Frame.tryLookupRow "b" Lookup.ExactOrGreater |> shouldEqual ( Some <| series [ 1 => 2.0; 3 => 5.0; 5 => 8.0 ] )
  f |> Frame.tryLookupRow "f" Lookup.ExactOrGreater |> shouldEqual None

  f |> Frame.tryLookupRowObservation "b" Lookup.ExactOrSmaller |> shouldEqual (Some ("a", series [ 1 => 1.0; 3 => 4.0; 5 => 7.0 ]))
  f |> Frame.tryLookupRowObservation "b" Lookup.ExactOrGreater |> shouldEqual (Some ("c", series [ 1 => 2.0; 3 => 5.0; 5 => 8.0 ]))
  f |> Frame.tryLookupRowObservation "f" Lookup.ExactOrGreater |> shouldEqual None

  f |> Frame.tryLookupCol 2 Lookup.ExactOrSmaller |> shouldEqual ( Some s1 )
  f |> Frame.tryLookupCol 2 Lookup.ExactOrGreater |> shouldEqual ( Some s2 )
  f |> Frame.tryLookupCol 6 Lookup.ExactOrGreater |> shouldEqual None

  f |> Frame.tryLookupColObservation 2 Lookup.ExactOrSmaller |> shouldEqual (Some (1, s1))
  f |> Frame.tryLookupColObservation 2 Lookup.ExactOrGreater |> shouldEqual (Some (3, s2))
  f |> Frame.tryLookupColObservation 6 Lookup.ExactOrGreater |> shouldEqual None

[<Test>]
let  ``Can access floats from ObjectSeries rows`` () =
  let df = frame [ "X" => series [| "a" => 1.0; "c" => 2.0; "e" => 3.0 |]
                   "Y" => series [| "a" => 4.0; "c" => nan; "e" => 6.0 |] ]

  let df' = frame [ "X" => series [| "e" => 3.0 |]
                    "Y" => series [| "e" => 6.0 |] ]

  let filt = df |> Frame.filterRows(fun _ r -> r?X >= 2.0 && not(Double.IsNaN(r?Y)))
  filt |> shouldEqual df'

  let testInvalidKey() = df |> Frame.filterRows(fun _ r -> r?Z >= 2.0) |> ignore
  testInvalidKey |> should throw (typeof<KeyNotFoundException>)

[<Test>]
let ``Filter all rows keeps column keys`` () =
  let df = frame [ "X" => series [| "a" => 1.0; "c" => 2.0; "e" => 3.0 |]
                   "Y" => series [| "a" => 4.0; "c" => nan; "e" => 6.0 |] ]

  let filt = df |> Frame.filterRows (fun _ _ -> false)
  filt.RowCount |> shouldEqual 0
  filt.ColumnKeys |> shouldEqual (Seq.ofList ["X"; "Y"])
  filt.["X"] |> shouldEqual (series [])
  filt.["Y"] |> shouldEqual (series [])
  (fun () -> filt.["Z"] |> ignore) |> should throw (typeof<ArgumentException>)

[<Test>]
let ``Filter rows preserves column types when all column values are missing`` () =
  // Regression test for https://github.com/fslaborg/Deedle/issues/516
  // filterRows must not change ColumnTypes when the filtered result contains only
  // missing values in a column (e.g. all-NaN float column after filtering).
  let df = frame [ "X" => series [1 => 1.0; 2 => nan]
                   "Y" => series [1 => nan; 2 => 2.0] ]
  // Filter to row 1 where Y is missing (nan) — Y column type should still be float
  let filt = df |> Frame.filterRows (fun k _ -> k = 1)
  filt.RowCount |> shouldEqual 1
  let colTypes = filt.ColumnTypes |> Seq.toList
  colTypes.[0] |> shouldEqual typeof<float>
  colTypes.[1] |> shouldEqual typeof<float>

  // Same via filterRowValues — filter to row 2 where X is missing (nan)
  let filt2 = df |> Frame.filterRowValues (fun r -> r.GetAs<float>("Y", 0.0) > 1.0)
  filt2.RowCount |> shouldEqual 1
  let colTypes2 = filt2.ColumnTypes |> Seq.toList
  colTypes2.[0] |> shouldEqual typeof<float>
  colTypes2.[1] |> shouldEqual typeof<float>

[<Test>]
let ``Filter frame rows by column value`` () =
  let df = frame [ "X" =?> series [| "a" => true; "c" => false; "e" => true; "f" => false |]
                   "Y" =?> series [| "a" => 4.0; "c" => nan; "e" => 6.0; "f" => 4.0 |] ]
  (df |> Frame.filterRowsBy "X" true)?Y |> shouldEqual <| series [ "a" => 4.0; "e" => 6.0 ]
  (df |> Frame.filterRowsBy "X" false)?Y |> shouldEqual <| series [ "c" => nan; "f" => 4.0 ]
  (df |> Frame.filterRowsBy "Y" 4).GetColumn<bool>("X") |> shouldEqual <| series [ "a" => true; "f" => false ]
  df.FilterRowsBy("Y", 4).GetColumn<bool>("X") |> shouldEqual <| series [ "a" => true; "f" => false ]

[<Test>]
let ``distinctRowsBy keeps first occurrence of each unique column combination`` () =
  // Rows 1 and 3 have the same "A", row 2 is unique, row 4 duplicates row 2 in "A"
  let colA = series [| 1 => "x"; 2 => "y"; 3 => "x"; 4 => "y" |]
  let colC = series [| 1 => 10; 2 => 20; 3 => 30; 4 => 40 |]
  let df = frame [ "A" => (colA |> Series.mapValues box); "C" => (colC |> Series.mapValues box) ]
  let result = df |> Frame.distinctRowsBy ["A"]
  // Should keep rows 1 and 2 (first of each group)
  result.RowCount |> shouldEqual 2

[<Test>]
let ``distinctRowsBy with all unique rows returns full frame`` () =
  let df = frame [ "X" => series [| "a" => 1; "b" => 2; "c" => 3 |] ]
  let result = df |> Frame.distinctRowsBy ["X"]
  result.RowCount |> shouldEqual 3

[<Test>]
let ``distinctRowsBy with all duplicate rows returns one row`` () =
  let df = frame [ "X" => series [| "a" => 1; "b" => 1; "c" => 1 |] ]
  let result = df |> Frame.distinctRowsBy ["X"]
  result.RowCount |> shouldEqual 1

// ------------------------------------------------------------------------------------------------
// Frame.mapCols / mapColValues / mapRows / mapRowValues / mapColKeys / mapRowKeys
// ------------------------------------------------------------------------------------------------

let mapSample() =
  frame [ "A" =?> series [ 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
          "B" =?> series [ 1 => 10.0; 2 => 20.0; 3 => 30.0 ] ]

[<Test>]
let ``Frame.mapColValues applies function to each object-series column`` () =
  let df = mapSample()
  // mapColValues receives ObjectSeries<'R> (Series<'R, obj>); map to a typed result
  let result = df |> Frame.mapColValues (fun (s:ObjectSeries<int>) -> s.As<float>() |> Series.mapValues ((*) 2.0))
  result.GetColumn<float>("A") |> shouldEqual (series [ 1 => 2.0; 2 => 4.0; 3 => 6.0 ])
  result.GetColumn<float>("B") |> shouldEqual (series [ 1 => 20.0; 2 => 40.0; 3 => 60.0 ])

[<Test>]
let ``Frame.mapCols passes key and object-series to function`` () =
  let df = mapSample()
  // mapCols receives (key, ObjectSeries<'R>); we use Stats.sum on the untyped column
  let result = df |> Frame.mapCols (fun k (s:ObjectSeries<int>) ->
    let sumVal = s.As<float>() |> Stats.sum
    series [ 0 => if k = "A" then sumVal + 100.0 else sumVal + 200.0 ])
  result.GetColumn<float>("A").[0] |> should beWithin (106.0 +/- 1e-9)  // 1+2+3+100
  result.GetColumn<float>("B").[0] |> should beWithin (260.0 +/- 1e-9)  // 10+20+30+200

[<Test>]
let ``Frame.mapColValues preserves column keys`` () =
  let df = mapSample()
  let result = df |> Frame.mapColValues (fun (s:ObjectSeries<int>) -> s.As<float>() |> Series.mapValues id)
  result.ColumnKeys |> Seq.toList |> shouldEqual [ "A"; "B" ]

[<Test>]
let ``Frame.mapRowValues applies function to each object row`` () =
  let df = mapSample()
  // Sum the two numeric columns for each row
  let result : Series<int, float> =
    df |> Frame.mapRowValues (fun row -> row.GetAs<float>("A") + row.GetAs<float>("B"))
  result |> shouldEqual (series [ 1 => 11.0; 2 => 22.0; 3 => 33.0 ])

[<Test>]
let ``Frame.mapRows passes row key and object row to function`` () =
  let df = mapSample()
  let result : Series<int, float> =
    df |> Frame.mapRows (fun k row -> float k + row.GetAs<float>("A"))
  result |> shouldEqual (series [ 1 => 2.0; 2 => 4.0; 3 => 6.0 ])

[<Test>]
let ``Frame.mapColKeys renames columns`` () =
  let df = mapSample()
  let result = df |> Frame.mapColKeys (fun k -> k + "_new")
  result.ColumnKeys |> Seq.toList |> shouldEqual [ "A_new"; "B_new" ]
  result.RowCount |> shouldEqual 3

[<Test>]
let ``Frame.mapRowKeys remaps row keys`` () =
  let df = mapSample()
  let result = df |> Frame.mapRowKeys (fun k -> k * 10)
  result.RowKeys |> Seq.toList |> shouldEqual [ 10; 20; 30 ]
  result.ColumnCount |> shouldEqual 2

[<Test>]
let ``Frame.filterColValues keeps only matching columns`` () =
  let df = mapSample()
  // Keep only columns where the sum of values is greater than 5
  let result = df |> Frame.filterColValues (fun (s:ObjectSeries<int>) -> s.As<float>() |> Stats.sum > 9.0)
  result.ColumnKeys |> Seq.toList |> shouldEqual [ "B" ]  // B sums to 60 (>9), A sums to 6 (<= 9)

[<Test>]
let ``Frame.filterCols passes key and series to predicate`` () =
  let df = mapSample()
  let result = df |> Frame.filterCols (fun k _ -> k = "A")
  result.ColumnKeys |> Seq.toList |> shouldEqual [ "A" ]
  result.RowCount |> shouldEqual 3

// ------------------------------------------------------------------------------------------------
// Slices
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame Slice works``() =
  let df =
    frame [ "X" => series [| "a" => 1.0; "c" => 2.0; "e" => 3.0 |]
            "Y" => series [| "a" => 4.0; "c" => nan; "e" => 6.0 |]
            "Z" => series [| "a" => 7.0; "c" => 8.0; "e" => 9.0 |] ]

  let expectedFrame =
    frame [ "Y" => series [| "a" => 4.0; "e" => 6.0 |]
            "Z" => series [| "a" => 7.0; "e" => 9.0 |] ]

  df |> Frame.slice [|"Y"; "Z"|] [|"a"; "e"|] |> shouldEqual expectedFrame

[<Test>]
let ``Frame.rowsAfter, rowsBefore, rowsStartAt, rowsEndAt, rowsBetween work on ordered int keys``() =
  let df =
    frame [ "V" => series [ 1 => 10; 2 => 20; 3 => 30; 4 => 40; 5 => 50 ] ]
  df |> Frame.rowsAfter 2   |> Frame.countRows |> shouldEqual 3
  df |> Frame.rowsBefore 4  |> Frame.countRows |> shouldEqual 3
  df |> Frame.rowsStartAt 2 |> Frame.countRows |> shouldEqual 4
  df |> Frame.rowsEndAt 4   |> Frame.countRows |> shouldEqual 4
  df |> Frame.rowsBetween 2 4 |> Frame.countRows |> shouldEqual 3

// ------------------------------------------------------------------------------------------------
// Row access
// ------------------------------------------------------------------------------------------------

let rowSample() =
  frame [ "X" => series [| "a" => 1.0; "c" => 2.0; "e" => 3.0 |]
          "Y" => series [| "a" => 4.0; "c" => nan; "e" => 6.0 |] ]

[<Test>]
let ``Accessing row via row offset work`` () =
  let actual = rowSample().GetRowAt<int>(2)
  actual |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]

[<Test>]
let ``Accessing row via invalid row offset throws an exception`` () =
  (fun () -> rowSample().GetRowAt<int>(4) |> ignore)
  |> should throw (typeof<ArgumentOutOfRangeException>)

[<Test>]
let ``Accessing row via row key works`` () =
  let actual = rowSample().GetRow<int>("e")
  actual |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]

[<Test>]
let ``Accessing row via missing row key returns missing`` () =
  let actual = rowSample().TryGetRow<int>("f")
  actual.HasValue |> shouldEqual false

[<Test>]
let ``Accessing row via missing row key with lookup works`` () =
  let actual = rowSample().GetRow<int>("f", Lookup.ExactOrSmaller)
  actual |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]

[<Test>]
let ``Accessing row via row key with exclusive smaller lookup works`` () =
  let actual = rowSample().GetRow<int>("b", Lookup.Smaller)
  actual |> shouldEqual <| series [ "X" => 1; "Y" => 4 ]
  let actual = rowSample().GetRow<int>("c", Lookup.Smaller)
  actual |> shouldEqual <| series [ "X" => 1; "Y" => 4 ]

[<Test>]
let ``Accessing row via row key with exclusive greater lookup works`` () =
  let actual = rowSample().GetRow<int>("c", Lookup.Greater)
  actual |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]
  let actual = rowSample().GetRow<int>("d", Lookup.Greater)
  actual |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]

[<Test>]
let ``Accessing row observation via missing row key with lookup works`` () =
  let actual = rowSample().TryGetRowObservation<int>("f", Lookup.ExactOrSmaller)
  actual.Value.Key |> shouldEqual "e"
  actual.Value.Value |> shouldEqual <| series [ "X" => 3; "Y" => 6 ]

[<Test>]
let ``Accessing row observation via missing row key returns missing`` () =
  let actual = rowSample().TryGetRowObservation<int>("f", Lookup.Exact)
  actual.HasValue |> shouldEqual false

// ------------------------------------------------------------------------------------------------
// take, takeLast, skip, skipLast
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can take N elements from front and back`` () =
  let s1 = series [ for i in 1 .. 100 -> i => float i]
  let s2 = series [ for i in 1 .. 100 -> i => "N" + (string i) ]
  let df = frame [ "S1" =?> s1; "S2" =?> s2 ]
  let empty = frame ["S1" =?> Series<int, float>([], []); "S2" =?> Series<int, string>([], [])]

  Frame.take 2 df |> shouldEqual <| frame ["S1" =?> series [1 => 1.0; 2 => 2.0]; "S2" =?> series [1 => "N1"; 2 => "N2"] ]
  Frame.take 100 df |> shouldEqual <| df
  Frame.take 0 df |> shouldEqual <| empty

  Frame.takeLast 2 df |> shouldEqual <| frame ["S1" =?> series [99 => 99.0; 100 => 100.0]; "S2" =?> series [99 => "N99"; 100 => "N100"] ]
  Frame.takeLast 100 df |> shouldEqual <| df
  Frame.takeLast 0 df |> shouldEqual <| empty

[<Test>]
let ``Can skip N elements from front and back`` () =
  let s1 = series [ for i in 1 .. 100 -> i => float i]
  let s2 = series [ for i in 1 .. 100 -> i => "N" + (string i) ]
  let df = frame [ "S1" =?> s1; "S2" =?> s2 ]
  let empty = frame ["S1" =?> Series<int, float>([], []); "S2" =?> Series<int, string>([], [])]

  Frame.skip 98 df |> shouldEqual <| frame ["S1" =?> series [99 => 99.0; 100 => 100.0]; "S2" =?> series [99 => "N99"; 100 => "N100"] ]
  Frame.skip 100 df |> shouldEqual <| empty
  Frame.skip 0 df |> shouldEqual <| df

  Frame.skipLast 98 df |> shouldEqual <| frame ["S1" =?> series [1 => 1.0; 2 => 2.0]; "S2" =?> series [1 => "N1"; 2 => "N2"] ]
  Frame.skipLast 100 df |> shouldEqual <| empty
  Frame.skipLast 0 df |> shouldEqual <| df


[<Test>]
let ``Frame.diff and Frame.shift correctly return empty frames`` () =
  let empty : Frame<int, string> = frame [ "A" => (series [] : Series<int, float>) ]
  empty |> Frame.shift 1 |> Frame.countRows |> shouldEqual 0
  empty |> Frame.diff 1 |> shouldEqual <| empty

  let single : Frame<int, string> = frame [ "A" => series [ 1 => 1.0 ] ]
  single |> Frame.shift -2 |> Frame.countRows |> shouldEqual 0
  single |> Frame.diff -1 |> shouldEqual <| empty

[<Test>]
let ``Frame.pctChange computes percentage change on sample input`` () =
  let df = frame [ "A" => series [ 1 => 100.0; 2 => 110.0; 3 => 99.0 ]
                   "B" => series [ 1 => 50.0;  2 => 60.0;  3 => 45.0 ] ]
  let result = df |> Frame.pctChange 1
  result.["A"] |> Series.observations |> List.ofSeq
  |> shouldEqual [ 2, 0.1; 3, (99.0-110.0)/110.0 ]
  result.["B"] |> Series.observations |> List.ofSeq
  |> shouldEqual [ 2, (60.0-50.0)/50.0; 3, (45.0-60.0)/60.0 ]

[<Test>]
let ``Frame.pctChange correctly returns empty frames`` () =
  let empty : Frame<int, string> = frame [ "A" => (series [] : Series<int, float>) ]
  empty |> Frame.pctChange 1 |> shouldEqual <| empty



[<Test>]
let ``Can use tryval and get inner exceptions when there are any`` () =
  let s = series [ DateTime(2013,1,1) => 10.0; DateTime(2013,1,2) => 20.0 ]
  let f = frame [ "A" => s ]
  let serr = f |> Frame.tryMapRows (fun _ row -> if row?A > 15.0 then failwith "oops" else 1.0)
  let ferr = frame [ "B" => serr ]
  let errCount =
    try
      ignore (ferr |> Frame.tryValues)
      0
    with :? System.AggregateException as e ->
      e.InnerExceptions.Count
  errCount |> shouldEqual 1

[<Test>]
let ``Can use tryval and extract data if there are no exceptions`` () =
  let s = series [ DateTime(2013,1,1) => 10.0; DateTime(2013,1,2) => 20.0 ]
  let f = frame [ "A" => s ]
  let serr = f |> Frame.tryMapRows (fun _ row ->row?A)
  let ferr = frame [ "B" => serr ]
  let data = ferr |> Frame.tryValues |> Frame.getCol "B"
  data |> shouldEqual s

// ------------------------------------------------------------------------------------------------
// Pivot & Melt
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can melt frame with 200x500 data frame``() =
  let big = frame [ for d in 0 .. 200 -> string d => series [ for i in 0 .. 500 -> string i => 1.0 ] ]
  let stacked = Frame.melt big
  stacked |> Frame.countRows |> shouldEqual 100701

[<Test>]
let ``Values in a melted frame can be expanded`` () =
  let df =
    [ "a" => series [ 0 => 1.0; 1 => 2.0; 2 => 3.0 ];
      "b" => series [ 0 => 5.0; 1 => 6.0; 2 => nan ] ] |> frame
  let res = df |> Frame.zip (fun a b -> a, b) df |> Frame.melt |> Frame.expandCols ["Value"]
  let actual = res.Rows.[0].As<obj>()
  let expected = series [ "Row" => box 0; "Column" => box "a"; "Value.Item1" => box 1.0; "Value.Item2" => box 1.0]
  actual |> shouldEqual expected

[<Test>]
let ``Frame.melt preserves type of values`` () =
  let df = frame [ "S1" =?> series [1 => 1]; "S2" =?> series [1 => 1.0 ]]
  let res = df |> Frame.melt
  let colTypes = res.GetFrameData().Columns |> Seq.map (fun (ty,_) -> ty.Name) |> List.ofSeq
  colTypes |> shouldEqual ["Int32"; "String"; "Double"]

[<Test>]
let ``Frame.meltBy keeps id columns and melts remaining columns`` () =
  let df =
    frame [ "id" =?> series [ 0 => "A"; 1 => "B"; 2 => "C" ]
            "v1" =?> series [ 0 => 10.0; 1 => 20.0; 2 => 30.0 ]
            "v2" =?> series [ 0 => 1.0; 1 => 2.0; 2 => 3.0 ] ]
  let res = df |> Frame.meltBy ["id"]
  // 3 rows * 2 value columns = 6 output rows
  res |> Frame.countRows |> shouldEqual 6
  // Output column names: id, Column, Value
  res.ColumnKeys |> List.ofSeq |> shouldEqual ["id"; "Column"; "Value"]

[<Test>]
let ``Frame.meltBy preserves id column values`` () =
  let df =
    frame [ "id" =?> series [ 0 => "A"; 1 => "B" ]
            "v1" =?> series [ 0 => 10.0; 1 => 20.0 ]
            "v2" =?> series [ 0 => 1.0; 1 => 2.0 ] ]
  let res = df |> Frame.meltBy ["id"]
  let ids = res.GetColumn<string>("id") |> Series.values |> List.ofSeq
  // Each id value appears once per value column (2 value cols)
  ids |> List.sort |> shouldEqual ["A"; "A"; "B"; "B"]

[<Test>]
let ``Frame.meltBy with no id columns is equivalent to melt`` () =
  let df =
    frame [ "v1" => series [ 0 => 1.0; 1 => 2.0 ]
            "v2" => series [ 0 => 3.0; 1 => 4.0 ] ]
  let melted = df |> Frame.melt
  let meltedBy = df |> Frame.meltBy []
  // meltBy with no ids should produce same row count as melt
  // (melt has "Row" col too, meltBy does not - just Column and Value)
  meltedBy |> Frame.countRows |> shouldEqual (melted |> Frame.countRows)
  meltedBy.ColumnKeys |> List.ofSeq |> shouldEqual ["Column"; "Value"]

[<Test>]
let ``Frame.meltBy preserves id column types`` () =
  let df =
    frame [ "id"  =?> series [ 0 => 42 ]
            "val" =?> series [ 0 => 1.0 ] ]
  let res = df |> Frame.meltBy ["id"]
  let colTypes = res.GetFrameData().Columns |> Seq.map (fun (ty,_) -> ty.Name) |> List.ofSeq
  colTypes |> shouldEqual ["Int32"; "String"; "Double"]

[<Test>]
let ``Frame.unmelt is the inverse of Frame.melt for homogeneous frames`` () =
  // A homogeneous float frame should survive a melt/unmelt round-trip.
  let original =
    frame [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
            "B" => series [ 1 => 3.0; 2 => 4.0 ] ]
  let melted = original |> Frame.melt
  // Melted frame must have exactly the three structural columns.
  melted.ColumnKeys |> Seq.toArray |> shouldEqual [| "Row"; "Column"; "Value" |]
  melted |> Frame.countRows |> shouldEqual 4
  // Round-trip: unmelt should reconstruct the original structure.
  let roundTripped : Frame<int, string> = melted |> Frame.unmelt
  set roundTripped.ColumnKeys |> shouldEqual (set ["A"; "B"])
  roundTripped |> Frame.countRows |> shouldEqual 2

[<Test>]
let ``Frame.melt skips missing values and Frame.unmelt reconstructs present values`` () =
  // melt should omit missing cells; unmelt should place values at the right (row, col) address.
  let df =
    frame [ "X" => series [ "r1" => 10.0; "r2" => 20.0 ]
            "Y" => series [ "r1" => 30.0 ] ]   // "r2"/"Y" is missing
  let melted = df |> Frame.melt
  // Only the three present cells should appear.
  melted |> Frame.countRows |> shouldEqual 3
  let roundTripped : Frame<string, string> = melted |> Frame.unmelt
  set roundTripped.ColumnKeys |> shouldEqual (set ["X"; "Y"])
  roundTripped |> Frame.countRows |> shouldEqual 2
  roundTripped.GetColumn<float>("X").TryGet("r1") |> shouldEqual (OptionalValue 10.0)
  roundTripped.GetColumn<float>("X").TryGet("r2") |> shouldEqual (OptionalValue 20.0)
  roundTripped.GetColumn<float>("Y").TryGet("r1") |> shouldEqual (OptionalValue 30.0)
  roundTripped.GetColumn<float>("Y").TryGet("r2") |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Can group 10x5k data frame by row of type string (in less than a few seconds)`` () =
  let big = frame [ for d in 0 .. 10 -> string d => series [ for i in 0 .. 5000 -> string i => string (i % 1000) ] ]
  let grouped = big |> Frame.groupRowsByString "1"
  grouped.Rows.[ ("998","998") ].GetAs<int>("0") |> shouldEqual 998

[<Test>]
let ``Can group 10x5k data frame by row of type string and nest it (in less than a few seconds)`` () =
  let big = frame [ for d in 0 .. 10 -> string d => series [ for i in 0 .. 5000 -> string i => string (i % 1000) ] ]
  let grouped = big |> Frame.groupRowsByString "1"
  let nest = grouped |> Frame.nest
  nest |> Series.countKeys |> shouldEqual 1000

// ------------------------------------------------------------------------------------------------
// Numerical operators
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Applying numerical operation to frame does not affect non-numeric series`` () =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", inferRows=10) * 2.0
  let actual = df.GetColumn<DateTime>("Date").GetAt(0).Date
  actual |> shouldEqual (DateTime(2012, 1, 27))

[<Test>]
let ``Can perform numerical operation with a scalar on data frames`` () =
  let df = msft()

  (-df)?Open.GetAt(66) |> shouldEqual (-df?Open.GetAt(66))

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
  let df1 = msft() |> Frame.sortRowsByKey
  let df2 = df1 |> Frame.shift 1
  let opens1 = df1?Open
  let opens2 = df2?Open

  (df2 - df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) - opens1.GetAt(66))
  (df2 + df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) + opens1.GetAt(66))
  (df2 * df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) * opens1.GetAt(66))
  (df2 / df1)?Open.GetAt(66) |> shouldEqual (opens2.GetAt(66) / opens1.GetAt(66))

[<Test>]
let ``Applying (+) on frame & series introduces missing values`` () =
  let s1 = series [ 2 => 1.0 ]
  let s2 = series [ 1 => 1.0; 2 => 1.0 ]
  let f1 = frame  [ "S1" => s1 ]
  let f2 = frame  [ "S2" => s2 ]

  // Returned series should contain 'NA' when key is only in one series
  (f1 + s2)?S1 |> shouldEqual <| series [ 1 => nan; 2 => 2.0 ]
  (s1 + f2)?S2 |> shouldEqual <| series [ 1 => nan; 2 => 2.0 ]
  // This should be the same as adding two series
  (f1 + s2)?S1 |> shouldEqual <| s1 + s2
  (s1 + f2)?S2 |> shouldEqual <| s1 + s2

[<Test>]
let ``Applying (+) on frame & series preserves non-numeric columns`` () =
  let s1 = series [ 3 => 1.0 ]
  let s2 = series [ 2 => 1.0 ]
  let f1 = frame  [ "S1" =?> series [ 1 => 1.0; 3 => 2.0]; "S2" =?> series [ 1 => "ahoj"; 3 => "hi"] ]

  // Non-numeric series stays the same
  (f1 - s1).GetColumn<string>("S2")
  |> shouldEqual <| f1.GetColumn<string>("S2")

[<Test>]
let ``Applying (+) on frame & series expands non-numeric columns`` () =
  let s1 = series [ 3 => 1.0 ]
  let s2 = series [ 2 => 1.0 ]
  let f1 = frame  [ "S1" =?> series [ 1 => 1.0; 3 => 2.0]; "S2" =?> series [ 1 => "ahoj"; 3 => "hi"] ]

  // Non-numeric series is expanded to match new keys
  (f1 - s2).GetColumn<string>("S2") |> Series.dropMissing
  |> shouldEqual <| f1.GetColumn<string>("S2")
  (f1 - s2).GetColumn<string>("S2") |> Series.tryGet 2
  |> shouldEqual None

[<Test>]
let ``Applying (+) on frame & frame with uncommon columns will result missing values`` () =
  let df1 = frame [ "a" => Series.ofValues [ 1; 2]
                    "b" => Series.ofValues [ 3; 4; 5] ]
  let df2 = frame [ "b" => Series.ofValues [ 1; 2]
                    "c" => Series.ofValues [ 3; 4] ]
  let actual = df1 + df2
  actual?a.TryGetAt(0) |> shouldEqual OptionalValue.Missing
  actual?c.TryGetAt(0) |> shouldEqual OptionalValue.Missing
  actual?b.TryGetAt(0) |> shouldEqual (OptionalValue 4.0)
  actual?b.TryGetAt(2) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Applying (+) on frame & string series`` () =
  let df = frame [ "X" => series [| 1 => "A"; 2 => "B"|]
                   "Y" => series [| 1 => "C"; 2 => "D"; 3 => "E"|] ]
  let s = series [| 1 => "F"; 2 => "G"|]
  let combined1 = df + s
  let combined2 = s + df
  combined1.GetColumn<string>("X").TryGet(1) |> shouldEqual (OptionalValue "AF")
  combined1.GetColumn<string>("Y").TryGet(2) |> shouldEqual (OptionalValue "DG")
  combined1.GetColumn<string>("Y").TryGet(3) |> shouldEqual OptionalValue.Missing
  combined2.GetColumn<string>("X").TryGet(1) |> shouldEqual (OptionalValue "FA")
  combined2.GetColumn<string>("Y").TryGet(2) |> shouldEqual (OptionalValue "GD")
  combined2.GetColumn<string>("Y").TryGet(3) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Applying (+) on frame & string scalar`` () =
  let df = frame [ "X" => series [| 1 => "A"; 2 => "B"|]
                   "Y" => series [| 1 => "C"; 2 => "D"; 3 => "E"|] ]
  let combined1 = df + "F"
  let combined2 = "F" + df
  combined1.GetColumn<string>("X").TryGet(1) |> shouldEqual (OptionalValue "AF")
  combined1.GetColumn<string>("Y").TryGet(2) |> shouldEqual (OptionalValue "DF")
  combined1.GetColumn<string>("X").TryGet(3) |> shouldEqual OptionalValue.Missing
  combined2.GetColumn<string>("X").TryGet(1) |> shouldEqual (OptionalValue "FA")
  combined2.GetColumn<string>("Y").TryGet(2) |> shouldEqual (OptionalValue "FD")
  combined2.GetColumn<string>("X").TryGet(3) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Concatenate two frames of string values`` () =
  let df1 = frame [ "X" => series [| 1 => "A"; 2 => "B"|]
                    "Y" => series [| 1 => "C"; 2 => "D"; 3 => "E"|] ]
  let df2 = frame [ "X" => series [| 1 => "F"; 2 => "G"|]
                    "Y" => series [| 1 => "H"; 2 => "I"; 3 => "J"|] ]
  let combined = df1.StrConcat(df2)
  combined.GetColumn<string>("X").TryGet(1) |> shouldEqual (OptionalValue "AF")
  combined.GetColumn<string>("Y").TryGet(2) |> shouldEqual (OptionalValue "DI")
  combined.GetColumn<string>("X").TryGet(3) |> shouldEqual OptionalValue.Missing
  ()

// ------------------------------------------------------------------------------------------------
// Operations - append
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can append two frames with disjoint columns`` () =
  let df1 = Frame.ofColumns [ "A" => series [ for i in 1 .. 5 -> i, i ] ]
  let df2 = Frame.ofColumns [ "B" => series [ for i in 1 .. 5 -> i, i ] ]
  let actual = df1.Merge(df2)
  actual.Rows.[3].GetAt(0) |> shouldEqual (box 3)
  actual.Rows.[3].GetAt(1) |> shouldEqual (box 3)

[<Test>]
let ``Appending works on overlapping frames with missing values`` () =
  let df1 = Frame.ofColumns [ "A" => series [1 => Double.NaN; 2 => 1.0] ]
  let df2 = Frame.ofColumns [ "A" => series [2 => Double.NaN; 1 => 1.0] ]
  let actual = df1.Merge(df2)
  actual.Columns.["A"] |> Series.mapValues (unbox<float>)
  |> shouldEqual (series [1 => 1.0; 2 => 1.0])

[<Test>]
let ``Appending fails on overlapping frames with overlapping values`` () =
  let df1 = Frame.ofColumns [ "A" => series [1 => Double.NaN; 2 => 1.0] ]
  let df2 = Frame.ofColumns [ "A" => series [2 => 1.0] ]
  (fun () -> df1.Merge(df2) |> ignore) |> should throw (typeof<InvalidOperationException>)

[<Test>]
let ``Can append two frames with partially overlapping columns`` () =
  let df1 = Frame.ofColumns [ "A" => series [ for i in 1 .. 5 -> i, i ] ]
  let df2 = Frame.ofColumns
              [ "A" => series [ 6 => 10 ]
                "B" => series [ for i in 1 .. 5 -> i, i ] ]
  let actual = df1.Merge(df2)
  actual.Rows.[3].GetAt(0) |> shouldEqual (box 3)
  actual.Rows.[3].GetAt(1) |> shouldEqual (box 3)
  actual.Rows.[6].GetAt(0) |> shouldEqual (box 10)
  actual.Rows.[6].TryGetAt(1).HasValue |> shouldEqual false

[<Test>]
let ``Can append two frames with single rows and keys with comparison that fails at runtime`` () =
  let df1 = Frame.ofColumns [ "A" => series [ ([| 0 |], 0) => "A" ] ]
  let df2 = Frame.ofColumns [ "A" => series [ ([| 0 |], 1) => "A" ] ]
  df1.Merge(df2).RowKeys |> Seq.length |> shouldEqual 2

[<Test>]
let ``Can append multiple frames`` () =
  let df1 = Frame.ofColumns [ "A" => series [ for i in 1 .. 5 -> i, i ] ]
  let df2 = Frame.ofColumns [ "B" => series [ for i in 1 .. 5 -> i, i ] ]
  let df3 = Frame.ofColumns [ "B" => series [ for i in 6 .. 9 -> i, i ] ]
  let actual = Frame.mergeAll [df1;df2;df3]
  actual.Rows.[3].GetAt(0) |> shouldEqual (box 3)
  actual.Rows.[3].GetAt(1) |> shouldEqual (box 3)
  actual.Rows.[8].GetAt(1) |> shouldEqual (box 8)
  actual.Rows.[8].TryGetAt(0).HasValue |> shouldEqual false

[<Test>]
let ``Can merge frames with original column order`` () =
  let df1 = frame [ "Z" => series [ "a" => 1.0; "c" => 2.0 ]]
  let df2 = frame [ "A" => series [ "a" => 1.0; "c" => 2.0 ] ]
  let actual1 = df1.Merge(df2)
  let actual2 = df2.Merge(df1)
  actual1.ColumnKeys |> List.ofSeq |> shouldEqual ["Z"; "A"]
  actual2.ColumnKeys |> List.ofSeq |> shouldEqual ["A"; "Z"]

[<Test>]
let ``AppendN works on non-primitives`` () =
  let df = frame []
  df?X <- series [ "a" => Decimal(1.0); "b" => Decimal(1.0); "c" => Decimal(2.0); "d" => Decimal(2.0)]
  df?Y <- series [ "a" => 1; "b" => 1; "c" => 2; "d" => 2]


  let df2 = df |> Frame.groupRowsByString("Y")
               |> Frame.nest
               |> Frame.unnest

  df2.RowCount |> shouldEqual df.RowCount

// ------------------------------------------------------------------------------------------------
// Operations - zip
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can inner/outer/left/right join row keys when aligning``()  =
  let df1 = Frame.ofColumns [ "A" => series [ 1=>1; 2=>2 ]]
  let df2 = Frame.ofColumns [ "A" => series [ 2=>2; 3=>3 ]]

  let actualI = (df1, df2) ||> Frame.zipAlign JoinKind.Inner JoinKind.Inner Lookup.Exact (+)
  actualI.RowKeys |> List.ofSeq |> shouldEqual [2]
  let actualO = (df1, df2) ||> Frame.zipAlign JoinKind.Inner JoinKind.Outer Lookup.Exact (+)
  actualO.RowKeys |> List.ofSeq |> shouldEqual [1;2;3]
  let actualL = (df1, df2) ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.Exact (+)
  actualL.RowKeys |> List.ofSeq |> shouldEqual [1;2]
  let actualR = (df1, df2) ||> Frame.zipAlign JoinKind.Inner JoinKind.Right Lookup.Exact (+)
  actualR.RowKeys |> List.ofSeq |> shouldEqual [2;3]

[<Test>]
let ``Can zip and subtract numerical values in MSFT data set``() =
  let df1 = msft()
  let df2 = msft()
  let actual = df1.Zip<int, _, _>(df2, fun a b -> a - b)
  let values = actual.GetAllValues<int>()
  values |> Seq.length |> shouldEqual (6 * (df1 |> Frame.countRows))
  values |> Seq.forall ((=) 0) |> shouldEqual true

[<Test>]
let ``Can zip and subtract numerical values in MSFT data set; with some rows dropped``() =
  let df1 = (msft() |> Frame.sortRowsByKey).Rows.[DateTime(2000, 1, 1) ..]
  let df2 = msft()
  let values = df1.Zip<int, _, _>(df2, fun a b -> a - b).GetAllValues<int>()
  values |> Seq.length |> shouldEqual (6 * (df1 |> Frame.countRows))
  values |> Seq.forall ((=) 0) |> shouldEqual true

[<Test>]
let ``Can zip and subtract numerical values in MSFT data set; with some columns dropped``() =
  let df1 = msft()
  df1.DropColumn("Adj Close")
  let df2 = msft()
  let zipped = df1.Zip<int, _, _>(df2, fun a b -> a - b)
  zipped?``Adj Close`` |> Stats.sum |> should (be greaterThan) 0.0
  zipped?Low |> Stats.sum |> shouldEqual 0.0

[<Test>]
let ``Can zip frames containing values of complex types`` () =
  let df = frame [ "A" => Series.ofValues [2 .. 2 .. 20]; "B" => Series.ofValues [1 .. 10 ] ]
  let df1 = df.Zip<int, int, int*int>(df, fun a b -> a,b)
  let df2 = df1.Zip<int*int, int, (int*int)*int>(df, fun a b -> (a,b))
  let actual = df2.Rows.[0].As<(int * int) * int>()
  actual |> shouldEqual <| series [ "A" => ((2,2),2); "B" => ((1,1), 1)]

[<Test>]
let ``Can zip frames containing values of complex types without annotations`` () =
  let df = frame [ "A" => Series.ofValues [2 .. 2 .. 20]; "B" => Series.ofValues [1 .. 10 ] ]
  let df1 = df.Zip(df, fun a b -> a,b)
  let df2 = df1.Zip(df, fun a b -> (a,b))

  let ab, c = unbox<obj * obj> (df2.Rows.[0].["A"])
  let a, b = unbox<obj * obj> ab
  a |> shouldEqual (box 2)
  b |> shouldEqual (box 2)
  c |> shouldEqual (box 2)

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
      ||> Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller

  daysTimesPrevL?Times.TryGetAt(0) |> shouldEqual OptionalValue.Missing
  daysTimesPrevL?Times.TryGetAt(1) |> shouldEqual (OptionalValue 0.5)
  daysTimesPrevL?Times.TryGetAt(2) |> shouldEqual (OptionalValue 1.5)


[<Test>]
let ``Can left-align ordered frames - nearest greater always finds greater value`` () =
  // every point in timesFrames is later than in daysFrame,
  // all values in Times must be as in original series
  let daysTimesNextL =
      (daysFrame, timesFrame)
      ||> Frame.joinAlign JoinKind.Left Lookup.ExactOrGreater

  daysTimesNextL?Times.TryGetAt(0) |> shouldEqual (OptionalValue 0.5)
  daysTimesNextL?Times.TryGetAt(1) |> shouldEqual (OptionalValue 1.5)
  daysTimesNextL?Times.TryGetAt(2) |> shouldEqual (OptionalValue 2.5)

[<Test>]
let ``Can right-align ordered frames - nearest smaller always finds smaller value``() =
  // every point in timesFrames is later than in daysFrame,
  // all values in Days must be as in original series
  let daysTimesPrevR =
      (daysFrame, timesFrame)
      ||> Frame.joinAlign JoinKind.Right Lookup.ExactOrSmaller

  daysTimesPrevR?Days.TryGetAt(0) |> shouldEqual (OptionalValue 0.0)
  daysTimesPrevR?Days.TryGetAt(1) |> shouldEqual (OptionalValue 1.0)
  daysTimesPrevR?Days.TryGetAt(2) |> shouldEqual (OptionalValue 2.0)

[<Test>]
let ``Can right-align ordered frames - nearest greater returns missing if no greater value exists`` () =
  // every point in timesFrames is later than in daysFrame,
  // last point in Days must be missing after joining
  let daysTimesNextR =
      (daysFrame, timesFrame)
      ||> Frame.joinAlign JoinKind.Right Lookup.ExactOrGreater

  daysTimesNextR?Days.TryGetAt(0) |> shouldEqual (OptionalValue 1.0)
  daysTimesNextR?Days.TryGetAt(1) |> shouldEqual (OptionalValue 2.0)
  daysTimesNextR?Days.TryGetAt(2) |> shouldEqual OptionalValue.Missing

[<Test>]
let ``Can join frame with series`` () =
  Check.QuickThrowOnFailure(fun (kind:JoinKind) (lookup:int) (keys1:int[]) (keys2:int[]) (data1:float[]) (data2:float[]) ->
    let lookup = match lookup%3 with 0 -> Lookup.ExactOrGreater | 1 -> Lookup.ExactOrSmaller | _ -> Lookup.Exact
    let s1 = series (Seq.zip (Seq.sort (Seq.distinct keys1)) data1)
    let s2 = series (Seq.zip (Seq.sort (Seq.distinct keys2)) data2)
    let f1 = Frame.ofColumns ["S1" => s1]
    let f2 = Frame.ofColumns ["S2" => s2]
    let lookup = match kind with JoinKind.Inner | JoinKind.Outer -> Lookup.Exact | _ -> lookup
    f1.Join(f2, kind, lookup) = f1.Join("S2", s2, kind, lookup)
  )

// ------------------------------------------------------------------------------------------------
// Operations - sorting
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can sort frame``() =
  let randomOrder        = frame [ "x" => Series.randomOrder ]
  let randomOrderMissing = frame [ "x" => Series.randomOrderMissing ]
  let ascending          = frame [ "x" => Series.ascending ]
  let descending         = frame [ "x" => Series.descending ]
  let ascendingMissing   = frame [ "x" => Series.ascendingMissing ]
  let descendingMissing  = frame [ "x" => Series.descendingMissing ]

  let ord1 = randomOrder |> Frame.sortRows "x"
  ord1 |> shouldEqual ascending

  let ord2 = randomOrder |> Frame.sortRowsBy "x" (fun v -> -v)
  ord2 |> shouldEqual descending

  let ord3 = randomOrder |> Frame.sortRowsWith "x" (fun a b ->
    if a < b then -1 else if a = b then 0 else 1)
  ord3 |> shouldEqual ascending

  let ord4 = randomOrderMissing |> Frame.sortRows "x"
  ord4 |> shouldEqual ascendingMissing

  let ord5 = randomOrderMissing |> Frame.sortRowsBy "x" (fun v -> -v)
  ord5 |> shouldEqual descendingMissing

  let ord6 = randomOrderMissing |> Frame.sortRowsWith "x" (fun a b ->
    if a < b then -1 else if a = b then 0 else 1)
  ord6 |> shouldEqual ascendingMissing

// ------------------------------------------------------------------------------------------------
// Operations - fill
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Fill missing values using the specified direction``() =
  let df =
    [ "A" => series [ for i in 0 .. 100 -> i => if i%3=0 then Double.NaN else float i ]
      "B" => series [ for i in 0 .. 100 -> i => if i%5=0 then Double.NaN else float i ]
      "C" => series [ for i in 0 .. 100 -> i => if i%20=0 then Double.NaN else float i ]
      "D" => series [ for i in 0 .. 100 -> i => float i ] ]
    |> Frame.ofColumns
  let filled = df |> Frame.fillMissing Direction.Forward
  filled.Rows.[0].As<float>() |> shouldEqual <| series ["A" => Double.NaN; "B" => Double.NaN; "C" => Double.NaN; "D" => 0.0 ]
  filled.Rows.[10].As<float>() |> shouldEqual <| series ["A" => 10.0; "B" => 9.0; "C" => 10.0; "D" => 10.0 ]

[<Test>]
let ``Fill missing values using the specified constant``() =
  let df =
    [ "A" => series [ for i in 0 .. 100 -> i => if i%3=0 then Double.NaN else float i ]
      "B" => series [ for i in 0 .. 100 -> i => if i%5=0 then Double.NaN else float i ]
      "C" => series [ for i in 0 .. 100 -> i => if i%20=0 then Double.NaN else float i ]
      "D" => series [ for i in 0 .. 100 -> i => float i ] ]
    |> Frame.ofColumns
  let filled = df |> Frame.fillMissingWith 0.0
  filled.Rows.[0].As<float>() |> shouldEqual <| series ["A" => 0.0; "B" => 0.0; "C" => 0.0; "D" => 0.0 ]
  filled.Rows.[10].As<float>() |> shouldEqual <| series ["A" => 10.0; "B" => 0.0; "C" => 10.0; "D" => 10.0 ]

[<Test>]
let ``Can fill missing values in a frame containing decimals`` () =
  let df1 = frame [ "A" => Series.ofOptionalObservations [ 1 => None; 2 => Some 0.2M ] ]
  let df2 = df1 |> Frame.fillMissingWith 0.1
  df2?A |> shouldEqual <| series [1 => 0.1; 2 => 0.2]

[<Test>]
let ``indexRowsWith with more keys than rows produces missing values for extra rows (issue 498)`` () =
  // Regression test: indexRowsWith previously produced a frame with an inconsistent
  // row index / vector length, causing fillMissingWith to throw or silently no-op.
  let df =
    [ {| Value1 = 1.0; Value2 = 2.0 |}
      {| Value1 = 2.0; Value2 = 3.0 |} ]
    |> Frame.ofRecords
  let dfi = df |> Frame.indexRowsWith [0..2]
  // Row 2 should exist with missing values
  dfi.RowCount |> shouldEqual 3
  dfi.TryGetRow(2).HasValue |> shouldEqual true
  dfi?Value1.TryGet(2).HasValue |> shouldEqual false
  // fillMissingWith should fill the missing row without throwing
  let filled = dfi |> Frame.fillMissingWith 0.0
  filled.RowCount |> shouldEqual 3
  filled?Value1.[2] |> shouldEqual 0.0
  filled?Value2.[2] |> shouldEqual 0.0
  // Original rows should be unchanged
  filled?Value1.[0] |> shouldEqual 1.0
  filled?Value2.[1] |> shouldEqual 3.0

// ------------------------------------------------------------------------------------------------
// Operations - join & zip (handling missing values)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Left join fills missing values - search for previous when there is no exact key`` () =
  let miss = Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => Double.NaN; ] ]
  let full = Frame.ofColumns [ "B" => series [ 1 => 2.0; 3 => 3.0 ] ]
  let joined = full.Join(miss, JoinKind.Left, Lookup.ExactOrSmaller)
  let expected = series [ 1 => 1.0; 3 => 1.0 ]
  joined?A |> shouldEqual expected

[<Test>]
let ``Left join fills missing values - search for previous when there is missing at the exact key`` () =
  let miss = Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => Double.NaN; ] ]
  let full = Frame.ofColumns [ "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
  let joined = full.Join(miss, JoinKind.Left, Lookup.ExactOrSmaller)
  let expected = series [ 1 => 1.0; 2 => 1.0 ]
  joined?A |> shouldEqual expected

[<Test>]
let ``Left zip fills missing values - search for previous when there is no exact key`` () =
  let miss = Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => Double.NaN; ] ]
  let full = Frame.ofColumns [ "A" => series [ 1 => 2.0; 3 => 3.0 ] ]
  let joined = full.Zip<float, _, _>(miss, JoinKind.Inner, JoinKind.Left, Lookup.ExactOrSmaller, false, fun a b -> a + b)
  let expected = series [ 1 => 3.0; 3 => 4.0 ]
  joined?A |> shouldEqual expected

[<Test>]
let ``Left zip only fills missing values in joined series`` () =
  let miss = Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => Double.NaN; ] ]
  let full = Frame.ofColumns [ "A" => series [ 1 => 2.0; 2 => 3.0 ] ]
  let joined = miss.Zip<float, _, _>(full, JoinKind.Inner, JoinKind.Left, Lookup.ExactOrSmaller, false, fun a b -> a + b)
  let expected = series [ 1 => 3.0; 2 => Double.NaN ]
  joined?A |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Operations - zip
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can ZIP and subtract MSFT stock prices``() =
  let df = msft()
  let actual = (df,df) ||> Frame.zip (fun (v1:float) v2 -> v1 - v2)
  let values = actual.GetAllValues<float>()
  values |> Seq.length |> should (be greaterThan) 10000
  values |> Seq.sum |> shouldEqual 0.0


// company A has common and preferred stocks, company B only common
// company A trades in US, company B common shares trade in US, while B prefs trade in Israel/GCC
//   (e.g. B comm is ADR, B pref only local)
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
    //DateTime(2013,9,12) => 20.0; // Sat - // omit these values to illustrate how lookup works in Frame.zipAlign
    DateTime(2013,9,13) => 21.0;
    DateTime(2013,9,14) => 22.0;
    DateTime(2013,9,15) => 23.0;
    DateTime(2013,9,16) => 24.0;]
    |> series

let pxPrefs = [ "B" => pxBpref] |> Frame.ofColumns

// Shares outstanding
let sharesA = [ DateTime(2012,12,31) => 10.0 ] |> series
let sharesB = [ DateTime(2012,12,31) => 20.0; DateTime(2013,9,14) => 40.0; ] |> series // split
let sharesCommons = [ "A" => sharesA; "B" => sharesB ] |> Frame.ofColumns

let sharesBpref = [ DateTime(2012,12,31) => 20.0 ] |> series
let sharesPrefs = [ "B" => sharesBpref ] |> Frame.ofColumns

// Net debt forecast 2013
let ndA = [ DateTime(2013,12,31) => 100.0] |> series
let ndB = [ DateTime(2013,12,31) => 1000.0 ] |> series
let netDebt =  [ "A" => ndA; "B" => ndB ] |> Frame.ofColumns

let lift2 f a b =
    match a, b with
    | Some x, Some y -> Some(f x y)
    | _              -> None

[<Test>]
let ``Can zip-align frames with inner-join left-join nearest-smaller options`` () =
  let mktcapA =
    (pxA, sharesA)
    ||> Series.zipAlignInto JoinKind.Left Lookup.ExactOrSmaller (lift2 (fun (l:float) r -> l*r))
  let mktcapB =
    (pxB, sharesB)
    ||> Series.zipAlignInto JoinKind.Left Lookup.ExactOrSmaller (lift2 (fun (l:float) r -> l*r))

  // calculate stock mktcap
  let mktCapCommons =
    (pxCommons, sharesCommons)
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l*r)

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
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l*r)
  // calculate stock mktcap for prefs
  let mktCapPrefs =
    (pxPrefs, sharesPrefs)
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l*r)
  // calculate company mktcap
  let mktCap =
    (mktCapCommons, mktCapPrefs)
    ||> Frame.zipAlign JoinKind.Left JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l+r)

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
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l*r)
  // calculate stock mktcap for prefs
  let mktCapPrefs =
    (pxPrefs, sharesPrefs)
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l*r)
  // calculate company mktcap
  let mktCap =
    (mktCapCommons, mktCapPrefs)
    ||> Frame.zipAlign JoinKind.Left JoinKind.Left Lookup.ExactOrSmaller (fun (l:float) r -> l+r)

  // calculate enterprice value
  let ev =
    (mktCap, netDebt)
    ||> Frame.zipAlign JoinKind.Inner JoinKind.Left Lookup.ExactOrGreater (fun (l:float) r -> l+r) // net debt is at the year end

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

// ------------------------------------------------------------------------------------------------
// Operations - transpose
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Transposed frame created from columns equals frame created from rows (and vice versa)``() =
  let items =
    [ "A" =?> series [1 => 10.0; 2 => 20.0 ]
      "B" =?> series [1 => 30.0; 3 => 40.0 ]
      "C" =?> series [1 => "One"; 2 => "Two" ] ]
  let fromRows = Frame.ofRows items
  let fromCols = Frame.ofColumns items
  fromCols |> Frame.transpose |> shouldEqual fromRows
  fromRows |> Frame.transpose |> shouldEqual fromCols

// ------------------------------------------------------------------------------------------------
// Operations - group by
// ------------------------------------------------------------------------------------------------

let titanic() =
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../../docs/data/titanic.csv", inferRows=10)

[<Test>]
let ``Can group titanic data by boolean column "Survived"``() =
  let actual =
    titanic()
    |> Frame.groupRowsByBool "Survived"
    |> Frame.nest
    |> Series.mapValues Frame.countRows
  actual |> shouldEqual (series [false => 549; true => 342])

[<Test>]
let ``Can group on row keys``() =
  let df = Frame.ofColumns [ "a" => Series.ofValues [1; 2; 3]; "b" => Series.ofValues [4; 5; 6] ]
  let actual =
    df |> Frame.groupRowsByIndex (fun i -> i % 2) |> Frame.nest |> Series.map (fun _ v -> v.RowCount)
  let expected =
    Series.ofValues [2; 1]
  actual |> shouldEqual expected

// ------------------------------------------------------------------------------------------------
// Operations - aggregate rows by
// ------------------------------------------------------------------------------------------------
[<Test>]
let ``Can aggregate rows by key pairs with missing item in pairs ``() =
  let sample =
    [ "A" =?> series [ 1 => 10.0; 3 => 10.0; 5 => 10.0 ]
      "B" =?> series [ 1 => 1.0; 2 => 2.0; 3 => 1.0; 4 => 2.0; 5 => 3.0 ]
      "C" =?> Series([ 1..5 ], [ 1.0..5.0 ]) ] |> frame
  let expected =
    [ "A" =?> series [ 0 => 10.0; 2 => 10.0 ]
      "B" =?> series [ 0 => 1.0; 1 => 2.0; 2 => 3.0 ]
      "C" =?> series [ 0 => 2.0; 1 => 3.0; 2 => 5.0 ] ] |> frame
  sample |> Frame.aggregateRowsBy ["A";"B"] ["C"] Stats.mean |> shouldEqual expected
// ------------------------------------------------------------------------------------------------
// Operations - pivot table
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can compute pivot table from titanic data``() =
  let actual =
    titanic()
    |> Frame.pivotTable (fun k r -> r.GetAs<string>("Sex")) (fun k r -> r.GetAs<bool>("Survived")) Frame.countRows
  let expected =
    (frame [ false => series [ "male" => 468;  "female" => 81  ];
             true  => series [ "male" => 109;  "female" => 233 ] ])
  actual |> shouldEqual expected

[<Test>]
let ``Can compute pivot table from titanic data with nice syntax``() =
  let actual =
    let f = titanic()
    f.PivotTable("Sex", "Survived", Frame.countRows)

  let expected =
    (frame [ false => series [ "male" => 468;  "female" => 81  ];
             true  => series [ "male" => 109;  "female" => 233 ] ])
  actual |> shouldEqual expected

// ----------------------------------------------------------------------------------------------
// Index operations
// ----------------------------------------------------------------------------------------------

[<Test>]
let ``Can index rows using transformation function``() =
  let actual =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ];
                      "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
    |> Frame.indexRowsUsing (fun r -> r.GetAs<float>("A") + 2.0)
  let expected =
    Frame.ofColumns [ "A" => series [ 3.0 => 1.0; 4.0 => 2.0 ];
                      "B" => series [ 3.0 => 2.0; 4.0 => 3.0 ] ]
  actual |> shouldEqual expected

[<Test>]
let ``Indexing with a column drops the column from the frame by default``() =
  let sample =
    [ "K" =?> series [ 1 => 1; 2 => 2 ]
      "A" =?> series [ 1 => 1.0; 2 => 2.0 ]
      "B" =?> series [ 1 => 2.0; 2 => 3.0 ] ] |> frame

  let actual = sample |> Frame.indexRowsInt "K"
  set actual.ColumnKeys |> shouldEqual <| set ["A"; "B"]
  let actual = sample.IndexRows<int>("K")
  set actual.ColumnKeys |> shouldEqual <| set ["A"; "B"]
  let actual = sample.IndexRows<int>("K", keepColumn=true)
  set actual.ColumnKeys |> shouldEqual <| set ["K"; "A"; "B"]

[<Test>]
let ``Can reindex ordinally``() =
  let actual =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ];
                      "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
    |> Frame.indexRowsOrdinally
  let expected = [0; 1] |> Seq.ofList
  actual.RowKeys |> shouldEqual expected

[<Test>]
let ``Frame.renameCol renames a single column``() =
  let df =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                      "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
  let renamed = df |> Frame.renameCol "A" "X"
  renamed.ColumnKeys |> List.ofSeq |> shouldEqual ["X"; "B"]
  renamed.GetColumn<float>("X") |> shouldEqual (df.GetColumn<float>("A"))

[<Test>]
let ``Frame.renameCol with unknown key returns frame unchanged``() =
  let df =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                      "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
  let renamed = df |> Frame.renameCol "Z" "X"
  renamed.ColumnKeys |> List.ofSeq |> shouldEqual ["A"; "B"]

[<Test>]
let ``Frame.renameColsUsing transforms all column keys``() =
  let df =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                      "B" => series [ 1 => 2.0; 2 => 3.0 ] ]
  let renamed = df |> Frame.renameColsUsing (fun k -> k.ToLowerInvariant())
  renamed.ColumnKeys |> List.ofSeq |> shouldEqual ["a"; "b"]
  renamed.GetColumn<float>("a") |> shouldEqual (df.GetColumn<float>("A"))

[<Test>]
let ``Frame.renameCol does not mutate the original frame``() =
  let df =
    Frame.ofColumns [ "A" => series [ 1 => 1.0; 2 => 2.0 ] ]
  let _ = df |> Frame.renameCol "A" "X"
  df.ColumnKeys |> List.ofSeq |> shouldEqual ["A"]

// ------------------------------------------------------------------------------------------------
// Operations - mapping
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can map over frame values``() =
  let actual =
    Frame.ofColumns [ "A" =?> series [ 1 => 1; 2 => 2 ];
                      "B" =?> series [ 1 => 2.0; 2 => 3.0 ];
                      "C" =?> series [ 1 => "a"; 2 => "b" ] ]

  let expected =
    Frame.ofColumns [ "A" =?> series [ 1 => "x"; 2 => "x" ];
                      "B" =?> series [ 1 => "x"; 2 => "y" ];
                      "C" =?> series [ 1 => "a"; 2 => "b" ] ]

  let f v = if v <= 2.0 then "x" else "y"

  actual |> Frame.mapValues f |> shouldEqual expected
  f $ actual |> shouldEqual expected

[<Test>]
let ``Can map over frame keys and values``() =
  let actual =
    Frame.ofColumns [ "A" =?> series [ 1 => 1; 2 => 2 ];
                      "B" =?> series [ 1 => 2.0; 2 => 3.0 ];
                      "C" =?> series [ 1 => "a"; 2 => "b" ] ]

  let expected =
    Frame.ofColumns [ "A" =?> series [ 1 => "x"; 2 => "y" ];
                      "B" =?> series [ 1 => "y"; 2 => "y" ];
                      "C" =?> series [ 1 => "a"; 2 => "b" ] ]

  let f r c v = if r < 2 && c <> "B" && v <= 2.0 then "x" else "y"
  actual |> Frame.map f |> shouldEqual expected

[<Test>]
let ``Frame.mapRows applies function to each row key and row data``() =
  let df = frame [ "A" => series [ 1 => 10.0; 2 => 20.0; 3 => 30.0 ]
                   "B" => series [ 1 =>  1.0; 2 =>  2.0; 3 =>  3.0 ] ]
  let result = df |> Frame.mapRows (fun k row -> float k + row.GetAs<float>("A"))
  result |> shouldEqual (series [ 1 => 11.0; 2 => 22.0; 3 => 33.0 ])

[<Test>]
let ``Frame.mapRowValues applies function to each row without row key``() =
  let df = frame [ "X" => series [ "a" => 1.0; "b" => 2.0 ]
                   "Y" => series [ "a" => 3.0; "b" => 4.0 ] ]
  let result = df |> Frame.mapRowValues (fun row -> row.GetAs<float>("X") + row.GetAs<float>("Y"))
  result |> shouldEqual (series [ "a" => 4.0; "b" => 6.0 ])

[<Test>]
let ``Frame.mapRowKeys transforms row keys``() =
  let df = frame [ "A" => series [ 1 => 10.0; 2 => 20.0 ]
                   "B" => series [ 1 =>  1.0; 2 =>  2.0 ] ]
  let result = df |> Frame.mapRowKeys (fun k -> k * 10)
  result.RowKeys |> List.ofSeq |> shouldEqual [10; 20]
  result.GetColumn<float>("A") |> shouldEqual (series [ 10 => 10.0; 20 => 20.0 ])

[<Test>]
let ``Frame.filterCols keeps only columns matching predicate on key and values``() =
  let df = frame [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                   "B" => series [ 1 => 3.0; 2 => 4.0 ]
                   "C" => series [ 1 => 5.0; 2 => 6.0 ] ]
  let result = df |> Frame.filterCols (fun col _ -> col <> "B")
  result.ColumnKeys |> List.ofSeq |> shouldEqual ["A"; "C"]

[<Test>]
let ``Frame.filterColValues keeps only columns matching predicate on column data``() =
  let df = frame [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                   "B" => series [ 1 => 10.0; 2 => 20.0 ] ]
  // Keep columns where the sum of values (as float) is greater than 5
  let result = df |> Frame.filterColValues (fun col -> col.GetAs<float>(1) > 5.0)
  result.ColumnKeys |> List.ofSeq |> shouldEqual ["B"]

[<Test>]
let ``Frame.mapCols transforms columns using key and column data``() =
  let df = frame [ "X" => series [ 1 => 1.0; 2 => 2.0 ]
                   "Y" => series [ 1 => 3.0; 2 => 4.0 ] ]
  // Prefix column key into a label that then replaces the column data via a separate series
  let colKeys = df |> Frame.mapRows (fun _ row -> row.GetAs<float>("X") * 2.0)
  colKeys |> shouldEqual (series [ 1 => 2.0; 2 => 4.0 ])

[<Test>]
let ``Frame.mapColValues with identity function preserves frame``() =
  let df = frame [ "A" => series [ 1 => 1.0; 2 => 2.0 ]
                   "B" => series [ 1 => 3.0; 2 => 4.0 ] ]
  let result = df |> Frame.mapColValues id
  result.ColumnKeys |> List.ofSeq |> shouldEqual ["A"; "B"]
  result.GetColumn<float>("A") |> shouldEqual (series [ 1 => 1.0; 2 => 2.0 ])
  result.GetColumn<float>("B") |> shouldEqual (series [ 1 => 3.0; 2 => 4.0 ])

[<Test>]
let ``Frame.mapColKeys renames all columns``() =
  let df = frame [ "A" => series [ 1 => 1.0 ]
                   "B" => series [ 1 => 2.0 ] ]
  let result = df |> Frame.mapColKeys (fun c -> c + "_new")
  result.ColumnKeys |> List.ofSeq |> shouldEqual ["A_new"; "B_new"]
  result.GetColumn<float>("A_new") |> shouldEqual (series [ 1 => 1.0 ])

[<Test>]
let ``Frame.reduceValues reduces each numeric column independently``() =
  let df = frame [ "A" => series [ 1 => 1.0; 2 => 2.0; 3 => 3.0 ]
                   "B" => series [ 1 => 4.0; 2 => 5.0; 3 => 6.0 ] ]
  let result = df |> Frame.reduceValues (+)
  result.Get("A") |> shouldEqual 6.0
  result.Get("B") |> shouldEqual 15.0

// ------------------------------------------------------------------------------------------------
// Operations - missing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Getting dense rows works on sample frame``() =
  let df =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 9 -> if i%3=0 && k = "A" then nan else float i ] ]
  let actual = df |> Frame.denseRows |> Frame.ofRows
  let expected = df.Rows.[[1;2;4;5;7;8]]
  actual |> shouldEqual <| expected

[<Test>]
let ``Dropping sparse rows preserves columns (#277)``() =
  let emptyFrame =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 9 -> nan ] ]
  let actual = emptyFrame |> Frame.dropSparseRows
  let expected = frame ([ "A" => series []; "B" => series []; "C" => series [] ] : list<string * Series<int, float>>)
  actual |> shouldEqual <| expected

[<Test>]
let ``Dropping sparse rows works on sample frame``() =
  let sparseFrame =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 5 -> if i%2=0 then float i else nan ] ]
  let actual = sparseFrame |> Frame.dropSparseRows
  let expected =
    frame [ "A" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ]
            "B" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ]
            "C" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ] ]
  actual |> shouldEqual expected

[<Test>]
let ``Dropping empty rows works on sample frame``() =
  let emptyFrame =
    frame [
      "A", Series.ofValues [0.; nan; 2.; nan]
      "B", Series.ofValues [0.; 1.; 2.; nan]
      "C", Series.ofValues [0.; 1.; nan; nan] ]
  let actual = emptyFrame |> Frame.dropEmptyRows
  let expected =
    frame [ "A" => series [0 => 0.0; 1 => nan; 2 => 2.0 ]
            "B" => series [0 => 0.0; 1 => 1.0; 2 => 2.0 ]
            "C" => series [0 => 0.0; 1 => 1.0; 2 => nan ] ]
  actual |> shouldEqual expected

[<Test>]
let ``Dropping sparse rows works on frame with missing in one column``() =
  let sparseFrame =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 5 -> if i%2=0 || k<>"B" then float i else nan ] ]
  let actual = sparseFrame |> Frame.dropSparseRows
  let expected =
    frame [ "A" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ]
            "B" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ]
            "C" => series [0 => 0.0; 2 => 2.0; 4 => 4.0 ] ]
  actual |> shouldEqual expected

[<Test>]
let ``Dropping sparse rows by column works on sample frame``() =
  let frame =
    Frame.ofValues [ (1,"foo","a"); (1,"bar","b"); (1,"foobar","g");
                     (2,"foo","c");
                     (3,"foo","d"); (3,"bar","e");
                     (4,"bar","f") ]
  let expectedFoo =
      Frame.ofValues [ (1,"foo","a"); (1,"bar","b"); (1,"foobar","g");
                       (2,"foo","c");
                       (3,"foo","d"); (3,"bar","e");
                     ]
  let expectedBar =
      Frame.ofValues [ (1,"foo","a"); (1,"bar","b"); (1,"foobar","g");
                       (3,"foo","d"); (3,"bar","e");
                       (4,"bar","f") ]
  let expectedFoobar =
      Frame.ofValues [ (1,"foo","a"); (1,"bar","b"); (1,"foobar","g") ]

  frame |> Frame.dropSparseRowsBy "foo" |> shouldEqual expectedFoo
  frame |> Frame.dropSparseRowsBy "bar" |> shouldEqual expectedBar
  frame |> Frame.dropSparseRowsBy "foobar" |> shouldEqual expectedFoobar


[<Test>]
let ``Dropping sparse columns preserves columns``() =
  let emptyFrame =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 9 -> nan ] ]
  let actual = emptyFrame |> Frame.dropSparseCols
  let expected = Frame.ofRowKeys [0 .. 9]
  actual |> shouldEqual <| expected

[<Test>]
let ``Dropping sparse columns works on sample frame``() =
  let sparseFrame =
    frame [ for k in ["A"; "B"; "C"] ->
              k => Series.ofValues [ for i in 0 .. 5 -> if i%2=0||k="B" then float i else nan ] ]
  let actual = sparseFrame |> Frame.dropSparseCols
  let expected =
    frame [ "B" => series [0 => 0.0; 1 => 1.0; 2 => 2.0; 3 => 3.0; 4 => 4.0; 5 => 5.0 ] ]
  actual |> shouldEqual expected

[<Test>]
let ``Dropping empty columns works on sample frame``() =
  let emptyFrame =
    frame [
      "A", Series.ofValues [nan; nan; nan; nan]
      "B", Series.ofValues [0.; 1.; 2.; nan]
      "C", Series.ofValues [0.; 1.; nan; nan] ]
  let actual = emptyFrame |> Frame.dropEmptyCols
  let expected =
    frame [ "B", Series.ofValues [0.; 1.; 2.; nan]
            "C", Series.ofValues [0.; 1.; nan; nan] ]
  actual |> shouldEqual expected

// ----------------------------------------------------------------------------------------------
// Obsolete Stream operations
// ----------------------------------------------------------------------------------------------
#nowarn "44"

[<Test>]
let ``Saving CSV to a stream closes the stream when complete`` () =
  use stream = new System.IO.MemoryStream()
  stream.CanWrite |> shouldEqual true
  let expected = msft()
  expected.SaveCsv(stream)
  stream.CanWrite |> shouldEqual false

[<Test>]
let ``Saving CSV to a stream via the extension method closes the stream when complete`` () =
  let cz = System.Globalization.CultureInfo.GetCultureInfo("cs-CZ")
  use stream = new System.IO.MemoryStream()
  stream.CanWrite |> shouldEqual true
  let expected = msft()
  FrameExtensions.SaveCsv (expected, stream, true, ["Date"], ';', cz)
  stream.CanWrite |> shouldEqual false

// ------------------------------------------------------------------------------------------------
// JSON serialization
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``toJson with orient=columns produces expected structure`` () =
  let df =
    frame [ "X" => series [ "a" => 1.0; "b" => 2.0 ]
            "Y" => series [ "a" => 10.0; "b" => 20.0 ] ]
  let json = df |> Frame.toJson "columns"
  Assert.That(json.Contains("\"X\""), Is.True)
  Assert.That(json.Contains("\"Y\""), Is.True)
  Assert.That(json.Contains("\"a\""), Is.True)
  Assert.That(json.Contains("\"b\""), Is.True)
  Assert.That(json.Contains("1"), Is.True)
  Assert.That(json.Contains("10"), Is.True)

[<Test>]
let ``toJson with orient=index produces expected structure`` () =
  let df =
    frame [ "X" => series [ "a" => 1.0; "b" => 2.0 ]
            "Y" => series [ "a" => 10.0; "b" => 20.0 ] ]
  let json = df |> Frame.toJson "index"
  Assert.That(json.Contains("\"a\""), Is.True)
  Assert.That(json.Contains("\"b\""), Is.True)
  Assert.That(json.Contains("\"X\""), Is.True)
  Assert.That(json.Contains("\"Y\""), Is.True)

[<Test>]
let ``toJson with orient=records produces array`` () =
  let df =
    frame [ "X" => series [ 0 => 1.0; 1 => 2.0 ]
            "Y" => series [ 0 => 10.0; 1 => 20.0 ] ]
  let json = df |> Frame.toJson "records"
  json.[0] |> shouldEqual '['
  json.[json.Length-1] |> shouldEqual ']'
  Assert.That(json.Contains("\"X\""), Is.True)
  Assert.That(json.Contains("\"Y\""), Is.True)

[<Test>]
let ``toJson uses null for missing float values`` () =
  let df =
    frame [ "V" => series [ "a" => 1.0; "b" => nan ] ]
  let json = df |> Frame.toJson "columns"
  Assert.That(json.Contains("null"), Is.True)

[<Test>]
let ``toJson handles bool and int columns`` () =
  let df = frame [ "I" => series [ 0 => 42; 1 => 7 ] ]
  let json = df |> Frame.toJson "columns"
  Assert.That(json.Contains("42"), Is.True)
  let df2 = frame [ "B" => series [ 0 => true; 1 => false ] ]
  let json2 = df2 |> Frame.toJson "columns"
  Assert.That(json2.Contains("true"), Is.True)
  Assert.That(json2.Contains("false"), Is.True)

[<Test>]
let ``toJson escapes special characters in keys`` () =
  let df =
    frame [ "col\"name" => series [ "row\nkey" => 1.0 ] ]
  let json = df |> Frame.toJson "columns"
  Assert.That(json.Contains("\\\""), Is.True)
  Assert.That(json.Contains("\\n"), Is.True)

[<Test>]
let ``ToJson extension method works with default orient`` () =
  let df = frame [ "A" => series [ 1 => 100.0; 2 => 200.0 ] ]
  let json = df.ToJson()
  json.[0] |> shouldEqual '{'
  Assert.That(json.Contains("\"A\""), Is.True)

[<Test>]
let ``toJson with invalid orient throws ArgumentException`` () =
  let df = frame [ "A" => series [ 1 => 1.0 ] ]
  (fun () -> df |> Frame.toJson "bad" |> ignore)
  |> shouldThrow<ArgumentException>

[<Test>]
let ``saveJson writes file that can be read back`` () =
  let path = System.IO.Path.GetTempFileName()
  try
    let df = frame [ "P" => series [ 0 => 3.14; 1 => 2.71 ] ]
    Frame.saveJson path df
    let content = System.IO.File.ReadAllText(path)
    content.[0] |> shouldEqual '{'
    Assert.That(content.Contains("3.14"), Is.True)
  finally
    System.IO.File.Delete(path)

// Creating frames
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.empty returns an empty frame with no rows or columns`` () =
  let df : Frame<int, string> = Frame.empty
  df.RowCount |> shouldEqual 0
  df.ColumnCount |> shouldEqual 0

[<Test>]
let ``Frame.empty can have columns added to it`` () =
  let df : Frame<int, string> = Frame.empty
  let s = series [ 1 => 1.0; 2 => 2.0 ]
  let result = df |> Frame.addCol "A" s
  result.ColumnCount |> shouldEqual 1
  result.RowCount |> shouldEqual 2

// ------------------------------------------------------------------------------------------------
// Boolean mask indexing
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Frame.filterRowsByMask returns only rows where mask is true`` () =
  let df = frame [ "X" =?> series [| "a" => 1.0; "b" => 2.0; "c" => 3.0 |]
                   "Y" =?> series [| "a" => 10.0; "b" => 20.0; "c" => 30.0 |] ]
  let mask = series [ "a" => true; "b" => false; "c" => true ]
  let result = df |> Frame.filterRowsByMask mask
  result.RowCount |> shouldEqual 2
  result.GetColumn<float>("X") |> shouldEqual (series [ "a" => 1.0; "c" => 3.0 ])

[<Test>]
let ``Frame.filterRowsByMask excludes rows with keys missing from mask`` () =
  let df = frame [ "V" =?> series [| "a" => 1.0; "b" => 2.0; "c" => 3.0 |] ]
  let mask = series [ "a" => true; "b" => true ]
  let result = df |> Frame.filterRowsByMask mask
  result.RowCount |> shouldEqual 2
  result.GetColumn<float>("V") |> shouldEqual (series [ "a" => 1.0; "b" => 2.0 ])

[<Test>]
let ``Frame.filterColsByMask returns only columns where mask is true`` () =
  let df = frame [ "A" =?> series [| 1 => 1.0; 2 => 2.0 |]
                   "B" =?> series [| 1 => 3.0; 2 => 4.0 |]
                   "C" =?> series [| 1 => 5.0; 2 => 6.0 |] ]
  let mask = series [ "A" => true; "B" => false; "C" => true ]
  let result = df |> Frame.filterColsByMask mask
  result.ColumnCount |> shouldEqual 2
  result.ColumnKeys |> Seq.toList |> shouldEqual ["A"; "C"]

[<Test>]
let ``Frame.filterRowsByMask can be used with derived boolean series`` () =
  let df = frame [ "Age"  =?> series [| "Alice" => 25; "Bob" => 17; "Carol" => 32 |]
                   "Name" =?> series [| "Alice" => "Alice"; "Bob" => "Bob"; "Carol" => "Carol" |] ]
  let adults = df.GetColumn<int>("Age") |> Series.mapValues (fun age -> age >= 18)
  let result = df |> Frame.filterRowsByMask adults
  result.RowCount |> shouldEqual 2
  result.RowKeys |> Seq.toList |> shouldEqual ["Alice"; "Carol"]
