﻿(*
This script will create a very simple Deedle frame and push it to
Excel. It will also update that frame by adding a new column, which
will be automatically reflected to Excel.
*)
#I @"..\..\bin"
#r "ExcelApi.dll"
#r "NetOffice.dll"
#r "OfficeApi.dll"
#r "VBIDEApi.dll"
#r "Deedle.dll"
//#r "Deedle.Excel.dll"
#load "Excel.fs"

// Define your library scripting code here

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

// Wrapping the Frame in KeepInSync will ensure that row or column changes
// in the Frame will be automatically reflected to the Excel sheet.
xl?A1 <- (KeepInSync(df1))

let third = Series(dateRange (DateTime(2014,1,1)) 10, rand 10)
df1?third <- third

// It's handy to remember to do this, especially in fsi before restarting your session
// otherwise, you'll get zombie excel instances that you'll need to kill
let closeExcel () = excelApp.Dispose()
