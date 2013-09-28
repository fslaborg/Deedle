(*** hide ***)
#I "../bin"
#load "FSharp.DataFrame.fsx"
#load "../packages/FSharp.Charting.0.84/FSharp.Charting.fsx"
#r "../packages/FSharp.Data.1.1.9/lib/net40/FSharp.Data.dll"
open System
open FSharp.Data
open FSharp.DataFrame
open FSharp.Charting

let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Data frame features
===================

In this section, we look at various features of the F# data frame library (using both
`Series` and `Frame` types and modules). Feel free to jump to the section you are interested
in, but note that some sections refer back to values built in "Createing & loading".

You can also get this page as an [F# script file](https://github.com/BlueMountainCapital/FSharp.DataFrame/blob/master/samples/features.fsx)
from GitHub and run the samples interactively.

<a name="creating"></a>
Creating frames & loading data
------------------------------

<a name="creating-csv"></a>
### Loading CSV files

The easiest way to get data into data frame is to use a CSV file. The `Frame.readCsv`
function exposes this functionality:
*)

// Assuming 'root' is a directory containing the file
let titanic = Frame.readCsv(root + "Titanic.csv")

// Specify column separator
let air = Frame.readCsv(root + "AirQuality.csv", separators=";")

(**
The `readCsv` method has a number of optional arguments that you can use to control 
the loading. It supports both CSV files, TSV files and other formats. If the file name
ends with `tsv`, the Tab is used automatically, but you can set `separator` explicitly.
The following parameters can be used:

 * `location` - Specifies a file name or an web location of the resource.
 * `inferTypes` - Specifies whether the method should attempt to infer types
   of columns automatically (set this to `false` if you want to specify schema)
 * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
   rows to use for type inference. The default value is 0, meaninig all rows.
 * `schema` - A string that specifies CSV schema. See the documentation for 
   information about the schema format.
 * `separators` - A string that specifies one or more (single character) separators
   that are used to separate columns in the CSV file. Use for example `";"` to 
   parse semicolon separated files.
 * `culture` - Specifies the name of the culture that is used when parsing 
   values in the CSV file (such as `"en-US"`). The default is invariant culture. 

The parameters are the same as those used by the [CSV type provider in F# Data](http://fsharp.github.io/FSharp.Data/library/CsvProvider.html),
so you can find additional documentation there.

<a name="creating-recd"></a>
### Loading F# records or .NET objects

If you have another .NET or F# components that returns data as a sequence of F# records,
C# anonymous types or other .NET objects, you can use `Frame.ofRecords` to turn them
into a data frame. Assume we have:
*)
type Person = 
  { Name:string; Age:int; Countries:string list; }

let peopleRecds = 
  [ { Name = "Joe"; Age = 51; Countries = [ "UK"; "US"; "UK"] }
    { Name = "Tomas"; Age = 28; Countries = [ "CZ"; "UK"; "US"; "CZ" ] }
    { Name = "Eve"; Age = 2; Countries = [ "FR" ] }
    { Name = "Suzanne"; Age = 15; Countries = [ "US" ] } ]
(**
Now we can easily create a data frame that contains three columns 
(`Name`, `Age` and `Countries`) containing data of the same type as 
the properties of `Person`:
*)
// Turn the list of records into data frame 
let peopleList = Frame.ofRecords peopleRecds
// Use the 'Name' column as a key (of type string)
let people = peopleList |> Frame.indexRowsString "Name"

(**
Note that this does not perform any conversion on the column data. Numerical series
can be accessed using the `?` operator. For other types, we need to explicitly call
`GetSeries` with the right type arguments:
*)
people?Age
people.GetSeries<string list>("Countries")

(**
<a name="creating-wb"></a>
### F# Data providers

In general, you can use any data source that exposes data as series of tuples. This
means that we can easily load data using, for example, the World Bank type provider 
from [F# Data library](https://github.com/fsharp/FSharp.Data).
*)
// Connect to the World Bank
let wb = WorldBankData.GetDataContext()

/// Given a region, load GDP in current US$ and return data as 
/// a frame with two-level column key (region and country name)
let loadRegion (region:WorldBankData.ServiceTypes.Region) =
  [ for country in region.Countries -> 
      // Create two-level column key using tuple
      (region.Name, country.Name) => 
        // Create series from tuples returned by WorldBank
        Series.ofObservations country.Indicators.``GDP (current US$)`` ]
  |> Frame.ofColumns

(**
To make data manipulation more convenient, we read country information per region
and create data frame with a hierarchical index (for more information, see the
[advanced indexing section](#indexing)). Now we can easily read data for OECD and
Euro area:
*)
// Load Euro and OECD regions
let eu = loadRegion wb.Regions.``Euro area``
let oecd = loadRegion wb.Regions.``OECD members``

// Join and convert to in billions of USD
let world = eu.Join(oecd) / 1e9

// [fsi:val world : Frame<int,(string * string)> =]
// [fsi:        Euro area                OECD members  ]
// [fsi:        Austria  Estonia   (...) Canada  Chile   (...)]
// [fsi:1960 -> 6.592    <missing>       41.093  4.2117]
// [fsi:1961 -> 7.311    <missing>       40.767  4.7053]
// [fsi::       ...]
// [fsi:2011 -> 417.6    22.15           1777.7  250.99]
// [fsi:2012 -> 399.6    21.85           1821.4  268.18]

(**
The loaded data look something like the sample above. As you can see, the columns
are grouped by the region and some data are not available.

<a name="slicing"></a>
Advanced slicing
----------------
*)
"A"
(**
<a name="joining"></a>
Advanced joining 
----------------
*)
"A"
(**
<a name="indexing"></a>
Hierarchical indexing
---------------------
*)
let byClassAndPort1 = titanic.GroupRowsBy<int>("Pclass").GroupRowsBy<string>("Embarked") |> Frame.mapRowKeys Tuple.flatten3
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3
(**
<a name="missing"></a>
Missing values
--------------
*)

let nulls =
  [ Nullable(1); Nullable(); Nullable(3); Nullable(4) ]
  |> Series.ofValues
  |> Series.map (fun _ v -> v.Value)

let test = Frame.ofColumns [ "Test" => nulls ]
test?Test

[ Nullable(1); Nullable(); Nullable(3); Nullable(4) ]
|> Series.ofNullables


let ozone = air?Ozone

air?OzoneFilled <-
  ozone |> Series.mapAll (fun k -> function 
    | None -> ozone.TryGet(k, Lookup.NearestSmaller)
    | value -> value)

// Might not be super usefu but does the trick
ozone |> Series.fillMissingWith 0.0
// This works
ozone |> Series.fillMissing Direction.Backward
// but here, the first value is still missing
ozone |> Series.fillMissing Direction.Forward
// we can be more explicit and write
ozone |> Series.fillMissingUsing (fun k -> 
  defaultArg (ozone.TryGet(k, Lookup.NearestSmaller)) 0.0)
// Or we can drop the first value
ozone |> Series.fillMissing Direction.Forward
      |> Series.dropMissing



// air.WithRowIndex<int>("Month")

// System.Linq.Enumerable.group
// air.Rows.Aggregate(Aggregation.GroupBy(


(**
<a name="grouping"></a>
Grouping data
-------------
*)

