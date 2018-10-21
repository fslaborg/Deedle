#if INTERACTIVE
#I "../../bin/net45"
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

[<Test>]
let ``Can create empty data frame and empty series`` () =
  let f : Frame<int, int> = frame []  
  let s : Series<int, int> = series []
  s.KeyCount |> shouldEqual 0
  f.ColumnCount |> shouldEqual 0
  f.RowCount |> shouldEqual 0

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
let ``Can read MSFT data from CSV and rename``() =
  let df = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/data/MSFT.csv", schema="Day,Adj Close->Adjusted") 
  let actual = List.ofSeq df.ColumnKeys
  actual |> shouldEqual ["Day"; "Open"; "High"; "Low"; "Close"; "Volume"; "Adjusted"]

[<Test>]
let ``Can save MSFT data as CSV file and read it afterwards (with default args)`` () =
  let file = System.IO.Path.GetTempFileName()
  let expected = msft()
  expected.SaveCsv(file)
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
let ``Can save MSFT data as CSV to a TextWriter and read it afterwards (with default args)`` () =
  let builder = new System.Text.StringBuilder()
  use writer = new System.IO.StringWriter(builder)
  let expected = msft()
  expected.SaveCsv(writer)
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

let typedRows () = 
  [| for (KeyValue(k,r)) in msft().Rows.Observations -> 
      let p = { Open = r.GetAs<decimal>("Open"); Close = r.GetAs<decimal>("Close"); 
                High = r.GetAs<decimal>("High"); Low = r.GetAs<decimal>("High") }
      { Date = k; Volume = r.GetAs<int>("Volume"); Price = p } |]
let typedPrices () = 
  [| for r in typedRows () -> r.Price |]

[<Test>]  
let ``Can read simple sequence of records`` () =
  let prices = typedPrices ()
  let df = Frame.ofRecords prices
  set df.ColumnKeys |> shouldEqual (set ["Open"; "High"; "Low"; "Close"])
  df |> Frame.countRows |> shouldEqual prices.Length

[<Test>]  
let ``Can read simple sequence of records using a specified column as index`` () =
  let rows = typedRows ()
  let df = Frame.ofRecords<DateTime>(rows, "Date") 
  set df.ColumnKeys |> shouldEqual (set ["Volume"; "Price"])
  df.RowKeys |> Seq.head |> shouldEqual rows.[0].Date

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
let ``Filter frame rows by column value`` () =
  let df = frame [ "X" =?> series [| "a" => true; "c" => false; "e" => true; "f" => false |]
                   "Y" =?> series [| "a" => 4.0; "c" => nan; "e" => 6.0; "f" => 4.0 |] ]    
  (df |> Frame.filterRowsBy "X" true)?Y |> shouldEqual <| series [ "a" => 4.0; "e" => 6.0 ]
  (df |> Frame.filterRowsBy "X" false)?Y |> shouldEqual <| series [ "c" => nan; "f" => 4.0 ]
  (df |> Frame.filterRowsBy "Y" 4).GetColumn<bool>("X") |> shouldEqual <| series [ "a" => true; "f" => false ]

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

// ------------------------------------------------------------------------------------------------
// tryVal related
// ------------------------------------------------------------------------------------------------

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
  Frame.ReadCsv(__SOURCE_DIRECTORY__ + "/../../docs/content/data/titanic.csv", inferRows=10) 

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
