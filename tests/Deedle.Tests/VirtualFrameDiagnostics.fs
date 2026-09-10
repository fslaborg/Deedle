#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"
#load "VirtualInstrumentation.fs"
#else
module Deedle.Tests.VirtualFrameDiagnostics
#endif

open System
open System.IO
open FsUnit
open NUnit.Framework
open Deedle
open Deedle.Virtual
open Deedle.Tests.VirtualInstrumentation

let fixturesPath = Path.Combine(__SOURCE_DIRECTORY__, "data", "virtual-fixtures.csv")

// ------------------------------------------------------------------------------------------------
// Virtual diagnostics (src/Deedle/VirtualFrame.fs — members on Virtual)
// ------------------------------------------------------------------------------------------------

[<Test>]
let ``Can classify ordered and ordinal virtual row indexes`` () =
  let _, ordered, _ = InstrumentedOrdinalSource.createOrderedSearchFrame 10L
  let _, ordinal, _ = InstrumentedOrdinalSource.createOrdinalSearchFrame 10L
  Virtual.GetRowIndexKind ordered |> shouldEqual VirtualRowIndexKind.OrderedVirtual
  Virtual.GetRowIndexKind ordinal |> shouldEqual VirtualRowIndexKind.OrdinalVirtual
  Virtual.IsVirtualRowIndex ordered |> shouldEqual true
  Virtual.IsVirtualRowIndex ordinal |> shouldEqual true

[<Test>]
let ``Can describe virtual frame row index kind`` () =
  let _, frame, _ = InstrumentedOrdinalSource.createOrderedSearchFrame 10L
  Virtual.Describe frame |> should haveSubstring "ordered virtual"
  Virtual.Describe frame |> should haveSubstring "columns=2"

[<Test>]
let ``Can detect virtual column and row index scheme id`` () =
  let frame = Virtual.ReadCsv<DateTimeOffset>(fixturesPath, indexColumn = "Timestamp", columnKeys = [ "Id"; "Category"; "Label" ])
  Virtual.IsVirtualColumn(frame, "Category") |> shouldEqual true
  Virtual.TryGetRowIndexSchemeId frame |> shouldEqual (Some "csv-file")

[<Test>]
let ``TryGetLookupRange returns None for non-search columns`` () =
  let frame = Virtual.ReadCsv<DateTimeOffset>(fixturesPath, indexColumn = "Timestamp", columnKeys = [ "Id"; "Category"; "Label" ])
  Virtual.TryGetLookupRange(frame, "Label") |> shouldEqual None

[<Test>]
let ``TryGetLookupRange reports inferred search column mode`` () =
  let frame =
    Virtual.ReadCsv<DateTimeOffset>(
      fixturesPath,
      indexColumn = "Timestamp",
      searchColumns = [ VirtualSearchColumn.infer "Category" ],
      columnKeys = [ "Id"; "Category"; "Label" ])
  match Virtual.TryGetLookupRange(frame, "Category") with
  | Some (VirtualColumnLookupRange.Step _) | Some VirtualColumnLookupRange.IndexList -> ()
  | actual -> Assert.Fail(sprintf "expected Category Step or IndexList, got %A" actual)

[<Test>]
let ``TryGetLookupRange reports explicit Step mode with period`` () =
  let frame =
    Virtual.ReadCsv<DateTimeOffset>(
      fixturesPath,
      indexColumn = "Timestamp",
      searchColumns =
        [ VirtualSearchColumn.withString "Category" (VirtualLookupRange.forRepeatingCycle [| "a"; "b" |]) ],
      columnKeys = [ "Id"; "Category"; "Label" ])
  Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step 2))

[<Test>]
let ``TryGetLookupRange resolves multiple inferred search columns`` () =
  let path = Path.GetTempFileName() + ".csv"
  try
    File.WriteAllLines(
      path,
      [| "Category,Cycle,Label"
         "a,1,x"
         "b,2,y"
         "a,1,z"
         "b,2,w" |])
    let frame =
      Virtual.ReadCsv(
        path,
        searchColumns =
          [ VirtualSearchColumn.infer "Category"
            VirtualSearchColumn.infer "Cycle" ],
        columnKeys = [ "Category"; "Cycle"; "Label" ])
    Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step 2))
    Virtual.TryGetLookupRange(frame, "Cycle") |> shouldEqual (Some (VirtualColumnLookupRange.Step 2))
    Virtual.TryGetLookupRange(frame, "Label") |> shouldEqual None
  finally
    if File.Exists path then File.Delete path

[<Test>]
let ``TryGetLookupRange resolves multiple explicit search columns`` () =
  let path = Path.GetTempFileName() + ".csv"
  try
    File.WriteAllLines(
      path,
      [| "Category,Cycle"
         "tech,1"
         "energy,2"
         "retail,3"
         "tech,1" |])
    let frame =
      Virtual.ReadCsv(
        path,
        searchColumns =
          [ VirtualSearchColumn.withString "Category" (VirtualLookupRange.forRepeatingCycle [| "tech"; "energy"; "retail" |])
            VirtualSearchColumn.withInt64 "Cycle" (VirtualLookupRange.forRepeatingCycle [| 1L; 2L; 3L |]) ],
        columnKeys = [ "Category"; "Cycle" ])
    Virtual.TryGetLookupRange(frame, "Category") |> shouldEqual (Some (VirtualColumnLookupRange.Step 3))
    Virtual.TryGetLookupRange(frame, "Cycle") |> shouldEqual (Some (VirtualColumnLookupRange.Step 3))
  finally
    if File.Exists path then File.Delete path

[<Test>]
let ``TryGetLookupRange infers Step for Cycle on bigdeedle sample csv`` () =
  let path = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "docs", "data", "bigdeedle-prices.csv")
  if not (File.Exists path) then Assert.Ignore("docs sample csv not present")
  else
    let frame =
      Virtual.ReadCsv(
        path,
        searchColumns =
          [ VirtualSearchColumn.infer "Category"
            VirtualSearchColumn.infer "Cycle" ],
        columnKeys = [ "Category"; "Open"; "Close"; "Volume"; "Cycle" ])
    Virtual.TryGetLookupRange(frame, "Cycle") |> shouldEqual (Some (VirtualColumnLookupRange.Step 3))

[<Test>]
let ``TryGetLookupRange returns None when search column omitted from columnKeys`` () =
  let path = Path.GetTempFileName() + ".csv"
  try
    File.WriteAllLines(path, [| "Category,Cycle"; "a,1"; "b,2" |])
    let frame =
      Virtual.ReadCsv(
        path,
        searchColumns = [ VirtualSearchColumn.infer "Category"; VirtualSearchColumn.infer "Cycle" ],
        columnKeys = [ "Category" ])
    Virtual.TryGetLookupRange(frame, "Category") |> Option.isSome |> shouldEqual true
    Virtual.TryGetLookupRange(frame, "Cycle") |> shouldEqual None
  finally
    if File.Exists path then File.Delete path

[<Test>]
let ``Can report linear row index for materialized frame`` () =
  let frame = Frame.ofColumns [ "A" => series [ 0 => 1; 1 => 2 ] ]
  Virtual.GetRowIndexKind frame |> shouldEqual VirtualRowIndexKind.LinearOrOther
  Virtual.IsVirtualRowIndex frame |> shouldEqual false
  Virtual.Describe frame |> should haveSubstring "linear / materialized"

[<Test>]
let ``IsVirtualColumn returns false for materialized frame columns`` () =
  let frame = Frame.ofColumns [ "A" => series [ 0 => 1.0; 1 => 2.0 ] ]
  Virtual.IsVirtualColumn(frame, "A") |> shouldEqual false
