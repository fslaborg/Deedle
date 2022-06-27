#if INTERACTIVE
#I "../../bin/netstandard2.0"
#load "Deedle.fsx"
#r "../../packages/NUnit/lib/net45/nunit.framework.dll"
#r "../../packages/FsCheck/lib/net452/FsCheck.dll"
#r "../../packages/FsUnit/lib/net45/FsUnit.NUnit.dll"
#load "../Common/FsUnit.fs"

#else
module Deedle.Tests.Formatting
#endif

open System
open FsUnit
open NUnit.Framework
open Deedle

let simpleSeries = series ["R1" => 1; "R2" => 2; "R3" => 3; "R4" => 4]

let complexSeries =
  series [for ii in 1 .. 200 -> ((ii % 2),(string (ii % 10), float ii )) => (if (ii % 20 = 0) then nan else float ii)] |> Series.sortByKey

[<Test>]
let ``series FormatStrings full display`` () =
  let expected = [|[|"R1"; "->"; "1"|]; [|"R2"; "->"; "2"|]; [|"R3"; "->"; "3"|];[|"R4"; "->"; "4"|]|]
  shouldEqual expected (simpleSeries.FormatStrings(2,2))

[<Test>]
let ``series FormatStrings with ommitted values`` () =
  let expected = [|[|"R1"; "->"; "1"|]; [|"..."; "->"; "..."|]; [|"R4"; "->"; "4"|]|]
  shouldEqual expected (simpleSeries.FormatStrings(1,1))

[<Test>]
let ``series Format full display`` () =
  let expected = """R1 -> 1 
R2 -> 2 
R3 -> 3 
R4 -> 4 
"""
  shouldEqual expected (simpleSeries.Format())

[<Test>]
let ``series Format with ommitted values`` () =
  let expected = """R1  -> 1   
... -> ... 
R4  -> 4   
"""
  shouldEqual expected (simpleSeries.Format(2))

[<Test>]
let ``complexSeries FormatStrings 5 + 5`` () =
  let expected = [|
    [|"0"; "0"; "10"; "->"; "10"|]
    [|""; ""; "20"; "->"; "<missing>"|]
    [|""; ""; "30"; "->"; "30"|]
    [|""; ""; "40"; "->"; "<missing>"|]
    [|""; ""; "50"; "->"; "50"|]
    [|"..."; ""; ""; "->"; "..."|]
    [|"1"; "9"; "159"; "->"; "159"|]
    [|""; ""; "169"; "->"; "169"|]
    [|""; ""; "179"; "->"; "179"|]
    [|""; ""; "189"; "->"; "189"|]
    [|""; ""; "199"; "->"; "199"|]
  |]
  shouldEqual expected (complexSeries.FormatStrings(5,5))

[<Test>]
let ``complexSeries Format 10 rows`` () =
  let expected = """0   0 10  -> 10        
      20  -> <missing> 
      30  -> 30        
      40  -> <missing> 
      50  -> 50        
...       -> ...       
1   9 159 -> 159       
      169 -> 169       
      179 -> 179       
      189 -> 189       
      199 -> 199       
"""
  shouldEqual expected (complexSeries.Format(10))


let simpleFrame = frame [
  "C1" => series ["R1" => 11; "R2" => 21 ; "R3" => 31 ]
  "C2" => series ["R1" => 12; "R2" => 22 ; "R3" => 32 ]
  "C3" => series ["R1" => 13; "R2" => 23 ; "R3" => 33 ]
  "C4" => series ["R1" => 14; "R2" => 24 ; "R3" => 34 ]
  "C5" => series ["R1" => 15; "R2" => 25 ; "R3" => 35 ] 
]

let complexFrame =
  frame [
    for i in 1 .. 50 ->
        ((i % 2),(string (i % 10), float i)) => series [for ii in 1 .. 200 -> ((ii % 2),(string (ii % 10), float ii )) => (if (ii % 20 = 0) then nan else float ii)]
  ]
  |> Frame.sortRowsByKey
  |> Frame.sortColsByKey


[<Test>]
let ``frame FormatStrings full display no col types`` () =
  let expected = [|
    [|""; ""; "C1"; "C2"; "C3"; "C4"; "C5"|]
    [|"R1"; "->"; "11"; "12"; "13"; "14"; "15"|]
    [|"R2"; "->"; "21"; "22"; "23"; "24"; "25"|]
    [|"R3"; "->"; "31"; "32"; "33"; "34"; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(5,5,5,5,false))

[<Test>]
let ``frame FormatStrings full display with col types`` () =
  let expected = [|
    [|""; ""; "C1"; "C2"; "C3"; "C4"; "C5"|]
    [|""; ""; "(int)"; "(int)"; "(int)"; "(int)"; "(int)"|]
    [|"R1"; "->"; "11"; "12"; "13"; "14"; "15"|]
    [|"R2"; "->"; "21"; "22"; "23"; "24"; "25"|]
    [|"R3"; "->"; "31"; "32"; "33"; "34"; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(5,5,5,5,true))

[<Test>]
let ``frame FormatStrings with ommitted row values no col types`` () =
  let expected = [|
    [|""; ""; "C1"; "C2"; "C3"; "C4"; "C5"|]
    [|"R1"; "->"; "11"; "12"; "13"; "14"; "15"|]
    [|":"; ""; "..."; "..."; "..."; "..."; "..."|]
    [|"R3"; "->"; "31"; "32"; "33"; "34"; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(1,1,5,5,false))

[<Test>]
let ``frame FormatStrings with ommitted row values with col types`` () =
  let expected = [|
    [|""; ""; "C1"; "C2"; "C3"; "C4"; "C5"|]
    [|""; ""; "(int)"; "(int)"; "(int)"; "(int)"; "(int)"|]
    [|"R1"; "->"; "11"; "12"; "13"; "14"; "15"|]
    [|":"; ""; "..."; "..."; "..."; "..."; "..."|]
    [|"R3"; "->"; "31"; "32"; "33"; "34"; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(1,1,5,5,true))

[<Test>]
let ``frame FormatStrings with ommitted col values no col types`` () =
  let expected = [|
    [|""; ""; "C1"; "..."; "C5"|]
    [|"R1"; "->"; "11"; "..."; "15"|]
    [|"R2"; "->"; "21"; "..."; "25"|]
    [|"R3"; "->"; "31"; "..."; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(5,5,1,1,false))

[<Test>]
let ``frame FormatStrings with ommitted col values with col types`` () =
  let expected = [|
    [|""; ""; "C1"; "..."; "C5"|]
    [|""; ""; "(int)"; "..."; "(int)"|]
    [|"R1"; "->"; "11"; "..."; "15"|]
    [|"R2"; "->"; "21"; "..."; "25"|]
    [|"R3"; "->"; "31"; "..."; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(5,5,1,1,true))

[<Test>]
let ``frame FormatStrings with ommitted row and col values no col types`` () =
  let expected = [|
    [|""; ""; "C1"; "..."; "C5"|]
    [|"R1"; "->"; "11"; "..."; "15"|]
    [|":"; ""; "..."; "..."; "..."|]
    [|"R3"; "->"; "31"; "..."; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(1,1,1,1,false))

[<Test>]
let ``frame FormatStrings with ommitted row and col values with col types`` () =
  let expected = [|
    [|""; ""; "C1"; "..."; "C5"|]
    [|""; ""; "(int)"; "..."; "(int)"|]
    [|"R1"; "->"; "11"; "..."; "15"|]
    [|":"; ""; "..."; "..."; "..."|]
    [|"R3"; "->"; "31"; "..."; "35"|]
  |]
  shouldEqual expected (simpleFrame.FormatStrings(1,1,1,1,true))

[<Test>]
let ``frame Format full display no col types`` () =
  let expected = """      C1 C2 C3 C4 C5 
R1 -> 11 12 13 14 15 
R2 -> 21 22 23 24 25 
R3 -> 31 32 33 34 35 
"""
  shouldEqual expected (simpleFrame.Format())

[<Test>]
let ``frame Format full display with col types`` () =
  let expected = """      C1    C2    C3    C4    C5    
      (int) (int) (int) (int) (int) 
R1 -> 11    12    13    14    15    
R2 -> 21    22    23    24    25    
R3 -> 31    32    33    34    35    
"""
  shouldEqual expected (simpleFrame.Format(true))

[<Test>]
let ``frame Format with ommitted row values no col types`` () =
  let expected = """      C1  C2  C3  C4  C5  
R1 -> 11  12  13  14  15  
:     ... ... ... ... ... 
R3 -> 31  32  33  34  35  
"""
  shouldEqual expected (simpleFrame.Format(2,5))

[<Test>]
let ``frame Format with ommitted col values`` () =
  let expected = """      C1 ... C5 
R1 -> 11 ... 15 
R2 -> 21 ... 25 
R3 -> 31 ... 35 
"""
  shouldEqual expected (simpleFrame.Format(5,2))

[<Test>]
let ``frame Format with ommitted row and col values no col types`` () =
  let expected = """      C1  ... C5  
R1 -> 11  ... 15  
:     ... ... ... 
R3 -> 31  ... 35  
"""
  shouldEqual expected (simpleFrame.Format(2,2))

[<Test>]
let ``complex frame FormatStrings 5 + 5 + 5 + 5`` () =
  let expected = [|
    [|""; ""; ""; ""; "0"; ""; ""; ""; ""; "..."; ""; ""; ""; ""; ""|]
    [|""; ""; ""; ""; "0"; ""; ""; ""; ""; "..."; "9"; ""; ""; ""; ""|]
    [|""; ""; ""; ""; "10"; "20"; "30"; "40"; "50"; "..."; "9"; "19"; "29"; "39"; "49"|]
    [|"0"; "0"; "10"; "->"; "10"; "10"; "10"; "10"; "10"; "..."; "10"; "10"; "10"; "10"; "10"|]
    [|""; ""; "20"; "->"; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "..."; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "<missing>"|]
    [|""; ""; "30"; "->"; "30"; "30"; "30"; "30"; "30"; "..."; "30"; "30"; "30"; "30"; "30"|]
    [|""; ""; "40"; "->"; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "..."; "<missing>"; "<missing>"; "<missing>"; "<missing>"; "<missing>"|]
    [|""; ""; "50"; "->"; "50"; "50"; "50"; "50"; "50"; "..."; "50"; "50"; "50"; "50"; "50"|]
    [|":"; ":"; ":"; ""; "..."; "..."; "..."; "..."; "..."; "..."; "..."; "..."; "..."; "..."; "..."|]
    [|"1"; "9"; "159"; "->"; "159"; "159"; "159"; "159"; "159"; "..."; "159"; "159"; "159"; "159"; "159"|]
    [|""; ""; "169"; "->"; "169"; "169"; "169"; "169"; "169"; "..."; "169"; "169"; "169"; "169"; "169"|]
    [|""; ""; "179"; "->"; "179"; "179"; "179"; "179"; "179"; "..."; "179"; "179"; "179"; "179"; "179"|]
    [|""; ""; "189"; "->"; "189"; "189"; "189"; "189"; "189"; "..."; "189"; "189"; "189"; "189"; "189"|]
    [|""; ""; "199"; "->"; "199"; "199"; "199"; "199"; "199"; "..."; "199"; "199"; "199"; "199"; "199"|]
  |]
  shouldEqual expected (complexFrame.FormatStrings(5,5,5,5,false))

[<Test>]
let ``complex frame Format 10 rows, 10 cols`` () =
  let expected = """           0                                                 ...                                                   
           0                                                 ... 9                                                 
           10        20        30        40        50        ... 9         19        29        39        49        
0 0 10  -> 10        10        10        10        10        ... 10        10        10        10        10        
    20  -> <missing> <missing> <missing> <missing> <missing> ... <missing> <missing> <missing> <missing> <missing> 
    30  -> 30        30        30        30        30        ... 30        30        30        30        30        
    40  -> <missing> <missing> <missing> <missing> <missing> ... <missing> <missing> <missing> <missing> <missing> 
    50  -> 50        50        50        50        50        ... 50        50        50        50        50        
: : :      ...       ...       ...       ...       ...       ... ...       ...       ...       ...       ...       
1 9 159 -> 159       159       159       159       159       ... 159       159       159       159       159       
    169 -> 169       169       169       169       169       ... 169       169       169       169       169       
    179 -> 179       179       179       179       179       ... 179       179       179       179       179       
    189 -> 189       189       189       189       189       ... 189       189       189       189       189       
    199 -> 199       199       199       199       199       ... 199       199       199       199       199       
"""
  shouldEqual expected (complexFrame.Format(10,10))
