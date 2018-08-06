(*
This script will create a very simple Deedle frame and push it to
Excel. It will also update that frame by adding a new column, which
will be automatically reflected to Excel.
*)
#nowarn "211"
#I @"../../bin/net45"
#I "../../packages/Deedle.2.0.0-beta01/"
#I "../../packages/Deedle"
#load "Deedle.fsx"

#I "../../packages/Deedle.Excel.2.0.0-beta01/lib/net45"
#I "../../packages/NetOffice.Core.1.7.4.4/lib/net45"
#I "../../packages/NetOffice.Excel.1.7.4.4/lib/net45"
#I "../../packages/Deedle.Excel/lib/net45"
#I "../../packages/NetOffice.Core/lib/net45"
#I "../../packages/NetOffice.Excel/lib/net45"
#r "ExcelApi.dll"
#r "NetOffice.dll"
#r "OfficeApi.dll"
#r "VBIDEApi.dll"
#r "Deedle.Excel.dll"

open System
open System.Collections.Specialized
open Deedle
open Deedle.Excel

let dates = [
    DateTime(2014,1,1)
    DateTime(2014,1,4)
    DateTime(2014,1,8)
    ]

let values = [
    10.0
    20.0
    30.0
    ]

let first = Series(dates, values)

let dateRange (first : System.DateTime) count =
    seq { for i in 0 .. (count - 1) -> first.AddDays(float i) }

let rand count =
    let rnd = System.Random()
    seq { for i in 0 .. (count - 1) -> rnd.NextDouble() }

let second = Series(dateRange (DateTime(2014,1,1)) 10, rand 10)

let df1 = Frame(["first"; "second"], [first; second])

// Setting the KeepInSync flag will result in the excel sheet
// being kept in sync with the Deedle data frame for all frames
// mapped to an excel sheet through the xl session instance.
xl.KeepInSync <- true
xl?A1 <- df1

let third = Series(dateRange (DateTime(2014,1,1)) 10, rand 10)
df1?third <- third

// It's handy to remember to do this, especially in fsi before restarting your session
// otherwise, you'll get zombie excel instances that you'll need to kill
let closeExcel () = excelApp.Dispose()

[|
for values in GetFsiSeriesAndFrames(Array.empty).Values do
    for v in values do
        yield v.name
|]

let fourth = Series(dateRange (DateTime(2014,1,1)) 10, rand 10)
// Synchronize all Deedle Series and Frames to Excel
DeedleToExcel(fsi.AddPrintTransformer)

// Close excel instance
closeExcel ()

