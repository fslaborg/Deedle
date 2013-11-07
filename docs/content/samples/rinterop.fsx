(*** hide ***)
#I "../../../packages/FSharp.Charting.0.87"
#r "../../../bin/MathNet.Numerics.dll"
#I @"../../../bin"
let airQuality = __SOURCE_DIRECTORY__ + "/../data/AirQuality.csv"

(**
R Provider interoperability
==========================
*)

#load "Deedle.fsx"
#load "FSharp.Charting.fsx"
#r "RProvider.dll"
#r "RDotNet.dll"

open Deedle
open RProvider
open RDotNet
(**

Data frames
-----------

*)
open FSharp.Charting
open RProvider.datasets

// Get 'mtcars' data set as string-indexed data frame
let mtcars : Frame<string, string> = R.mtcars.GetValue()
// [fsi:val mtcars : Frame<string,string> =]
// [fsi:                      mpg  cyl disp  hp  drat wt    qsec  vs  am  gear carb ]
// [fsi:Mazda RX4          -> 21   6   160   110 3.9  2.62  16.46 0   1   4    4    ]
// [fsi:Mazda RX4 Wag      -> 21   6   160   110 3.9  2.875 17.02 0   1   4    4    ]
// [fsi:Datsun 710         -> 22.8 4   108   93  3.85 2.32  18.61 1   1   4    1    ]
// [fsi::                     ...  ... ...   ... ...  ...   ...   ... ... ...  ...  ]
// [fsi:Maserati Bora      -> 15   8   301   335 3.54 3.57  14.6  0   1   5    8    ]
// [fsi:Volvo 142E         -> 21.4 4   121   109 4.11 2.78  18.6  1   1   4    2    ]

(**
Write some computation
*) 

mtcars
|> Frame.groupRowsByInt "gear"
|> Frame.meanLevel fst
|> Frame.getSeries "mpg"
|> Series.observations |> Chart.Column

(**

Pass data frame back to R:

*)
open RProvider.``base``

let air = Frame.ReadCsv(airQuality, separators=";")
// [fsi:val air : Frame<int,string> =]
// [fsi:       Ozone     Solar.R   Wind Temp Month Day ]
// [fsi:0   -> <missing> 190       7.4  67   5     1   ]
// [fsi:1   -> 36        118       8    72   5     2   ]
// [fsi:2   -> 12        149       12.6 74   5     3   ]
// [fsi::      ...       ...       ...  ...  ...   ... ]
// [fsi:151 -> 18        131       8    76   9     29  ]
// [fsi:152 -> 20        223       11.5 68   9     30  ]


// Using R functions
R.colMeans(air).Print()
// [fsi:val it : string =]
// [fsi:  Ozone  Solar.R  Wind  Temp  Month   Day ]
// [fsi:    NaN      NaN  9.96 77.88   6.99  15.8]

(** 
more

*)

// Create sample data frame with missing values
let df = 
  [ "Floats" =?> series [ 1 => 10.0; 2 => nan; 4 => 15.0]
    "Names"  =?> series [ 1 => "one"; 3 => "three"; 4 => "four" ] ] 
  |> frame

R.as_data_frame(df).Print()

// 
R.assign("x",  df).Print()
// [fsi:val it : string = ]
// [fsi:   Floats Names ]
// [fsi: 1     10   one ] 
// [fsi: 2    NaN  <NA> ]
// [fsi: 4     15  four ]
// [fsi: 3    NaN three ]

// Check that it worked
R.eval(R.parse(text="x")).Value

// Pass 'mtcars' back to R
R.assign("cars", mtcars).Print()

(**
Time series
-----------
*)

open System

// Date times
R.assign("x", DateTime.Now).Print()
// [fsi:val it : string = "[1] "2013-11-07 19:08:11 EST"]

(**
Getting data from R
*)


open RProvider.zoo
open RProvider.``base``

R.austres.Value

let austres : Series<float, float> = R.austres.GetValue()

let ftseStr = R.parse(text="EuStockMarkets[,\"FTSE\"]")
let ftse : Series<_, _> = R.eval(ftseStr).Value
R.EuStockMarkets.Value



R.strftime(namedParams [ "x", box (R.index(R.EuStockMarkets)); "format", box "%Y-%m-%d %H:%M:%S"; "origin", box "1960-10-01" ]).AsCharacter()

//.GetValue<Series<DateTime,float>>()

(**
more
*)

open RProvider.zoo
open RProvider.``base``

let rnd = Random()
let ts = series [ for i in 0.0 .. 100.0 -> DateTime.Today.AddHours(i), rnd.NextDouble() ]

R.assign("x", ts).Print()
R.rollmean(ts, 20).Print()

let tf = frame [ "Input" => ts ]
tf?Means <- R.rollmean(ts, 20).GetValue<Series<DateTime, float>>()


R.get("x").GetValue<Series<DateTime, float>>() = ts




open RProvider.datasets

open RDotNet
fsi.AddPrinter(fun (se:SymbolicExpression) -> se.Print())

R.mtcars
R.BJsales
R.AirPassengers.Class
R.DNase.Value

R.ChickWeight


R.eval(R.parse(text="EuStockMarkets")).Class
R.eval(R.parse(text="Nile")).Class
R.eval(R.parse(text="mtcars")).Class

//.Value :?> Frame<string, string>

let rdf = R.eval(R.parse(text="""data.frame(c(2, 3, 5), c("aa", "bb", "cc"), c(TRUE, FALSE, TRUE)) """))
rdf.Value


open RProvider.zoo

R.zoo([1;2;3], [System.DateTime.Now])

R.assign("x", System.DateTime.Now).Class

R.Sys_time().Class