(**
---
title: Working with data frames in F#
category: Documentation
categoryindex: 1
index: 3
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net9.0/Deedle.dll"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#endif // FSX

open System
open System.IO
open Deedle

let root = __SOURCE_DIRECTORY__ + "/data/"

(**

# Working with data frames

In this section, we look at various features of the F# data frame library (using both
`Series` and `Frame` types and modules). Feel free to jump to the section you are interested
in, but note that some sections refer back to values built in "Creating & loading".

<a name="creating"></a>

## Creating frames & loading data

<a name="creating-csv"></a>

### Loading and saving CSV files

The easiest way to get data into data frame is to use a CSV file. The `Frame.ReadCsv`
function exposes this functionality:
*)

// Assuming 'root' is a directory containing the file
let titanic = Frame.ReadCsv(root + "titanic.csv")

// Read data and set the index column & order rows
let msft = 
  Frame.ReadCsv(root + "stocks/msft.csv") 
  |> Frame.indexRowsDate "Date"
  |> Frame.sortRowsByKey

// Specify column separator
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")

(**
In the second example, we call `indexRowsDate` to use the "Date" column as a row index
of the resulting data frame. This is a very common scenario and so Deedle provides an
easier option using a generic overload of the `ReadCsv` method:
*)
let msftSimpler = 
  Frame.ReadCsv<DateTime>(root + "stocks/msft.csv", indexCol="Date") 
  |> Frame.sortRowsByKey

(**
The `ReadCsv` method has a number of optional arguments that you can use to control 
the loading. It supports both CSV files, TSV files and other formats. If the file name
ends with `tsv`, the Tab is used automatically, but you can set `separator` explicitly.
The following parameters can be used:

 * `path` - Specifies a file name or an web location of the resource.
 * `indexCol` - Specifies the column that should be used as an index in the 
   resulting frame. The type is specified via a type parameter.
 * `inferTypes` - Specifies whether the method should attempt to infer types
   of columns automatically (set this to `false` if you want to specify schema)
 * `inferRows` - If `inferTypes=true`, this parameter specifies the number of
   rows to use for type inference. The default value is 100. Value 0 means all rows.
 * `schema` - A string that specifies CSV schema. See the documentation for 
   information about the schema format.
 * `separators` - A string that specifies one or more (single character) separators
   that are used to separate columns in the CSV file. Use for example `";"` to 
   parse semicolon separated files.
 * `culture` - Specifies the name of the culture that is used when parsing 
   values in the CSV file (such as `"en-US"`). The default is invariant culture. 

Once you have a data frame, you can also save it to a CSV file using the 
`SaveCsv` method. For example:
*)
// Save CSV with semicolon separator
air.SaveCsv(Path.GetTempFileName(), separator=';')
// Save as CSV and include row key as "Date" column
msft.SaveCsv(Path.GetTempFileName(), keyNames=["Date"], separator='\t')

(**
By default, the `SaveCsv` method does not include the key from the data frame. This can be
overridden by calling `SaveCsv` with the optional argument `includeRowKeys=true`, or with an
additional argument `keyNames` (demonstrated above) which sets the headers for the key column(s)
in the CSV file.

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

people?Age
people.GetColumn<string list>("Countries")

(**
### Expanding objects in columns

For frames that contain complex .NET objects as column values, you can use `Frame.expandCols`
to create a new frame that contains properties of the object as new columns. For example: 
*)

(*** define-output:ppl ***)
// Create frame with single column 'People'
let peopleNested = 
  [ "People" => Series.ofValues peopleRecds ] |> frame

// Expand the 'People' column
peopleNested |> Frame.expandCols ["People"]
(*** include-it:ppl ***)

(**
<a name="dataframe"></a>

## Manipulating data frames

### Getting data from a frame
*)
(*** include-value:people ***)
(**
To get a column (series) from a frame `df`, you can use operations that are exposed directly
by the data frame, or you can use `df.Columns` which returns all columns of the frame as a
series of series.
*)

// Get the 'Age' column as a series of 'float' values
people?Age
// Get the 'Countries' column as a series of 'string list' values
people.GetColumn<string list>("Countries")
// Get all frame columns as a series of series
people.Columns

(**
### Adding rows and columns

The series type is _immutable_ and so it is not possible to add new values to a series or 
change the values stored in an existing series. However, you can use operations that return
a new series as the result such as `Merge`.
*)

// Create series with more value
let more = series [ "John" => 48.0 ]
// Create a new, concatenated series
people?Age.Merge(more)

(**
Data frame allows a very limited form of mutation. It is possible to add new series (as a column)
to an existing data frame, drop a series or replace a series.
*)
// Calculate age + 1 for all people
let add1 = people?Age |> Series.mapValues ((+) 1.0)

// Add as a new series to the frame
people?AgePlusOne <- add1

// Add new series from a list of values
people?Siblings <- [0; 2; 1; 3]

// Replace existing series with new values
people.ReplaceColumn("Siblings", [3; 2; 1; 0])

// Create new object series with values for required columns
let newRow = 
  [ "Name" => box "Jim"; "Age" => box 51;
    "Countries" => box ["US"]; "Siblings" => box 5 ]
  |> series
// Create a new data frame, containing the new series
people.Merge("Jim", newRow)

(**
<a name="slicing"></a>

## Advanced slicing and lookup

Given a series, we have a number of options for getting one or more values or 
observations from the series.
*)

// Get an unordered sample series 
let ages = people?Age

// Returns value for a given key
ages.["Tomas"]
// Returns series with two keys from the source
ages.[ ["Tomas"; "Joe"] ]

// Returns 'None' when key is not present
ages |> Series.tryGet "John"
// Returns series with missing value for 'John'
ages |> Series.getAll [ "Tomas"; "John" ]

(**
We can also obtain all data from the series. The data frame library uses the
term _observations_ for all key-value pairs:
*)

// Get all observations as a sequence of tuples
ages |> Series.observations
// Get all observations, with 'None' for missing values
ages |> Series.observationsAll

(**
With ordered series, we can use slicing to get a sub-range:
*)

(*** define-output:opens ***)
let opens = msft?Open
opens.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)]
|> Series.mapKeys (fun k -> k.ToShortDateString())

(*** include-it:opens ***)

(**
<a name="grouping"></a>

## Grouping data

### Grouping series
*)
let travels = people.GetColumn<string list>("Countries")

// Group by name length (ignoring visited countries)
travels |> Series.groupBy (fun k v -> k.Length)
// Group by visited countries (people visited/not visited US)
travels |> Series.groupBy (fun k v -> List.exists ((=) "US") v)

// Group by name length and get number of values in each group
travels |> Series.groupInto 
  (fun k v -> k.Length) 
  (fun len people -> Series.countKeys people)

(*** define-output: trav ***)
travels
|> Series.mapValues (Seq.countBy id >> series)
|> Frame.ofRows
|> Frame.fillMissingWith 0

(*** include-it: trav ***)

(**
### Grouping data frames
*)

// Group using column 'Sex' of type 'string'
titanic |> Frame.groupRowsByString "Sex"

// Group using calculated value - length of name
titanic |> Frame.groupRowsUsing (fun k row -> 
  row.GetAs<string>("Name").Length)

let bySex = titanic |> Frame.groupRowsByString "Sex" 
// Returns series with two frames as values
let bySex1 = bySex |> Frame.nest
// Converts unstacked data back to a single frame
let bySex2 = bySex |> Frame.nest |> Frame.unnest

// Group by passanger class and port
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3

// Get average ages in each group
byClassAndPort?Age
|> Stats.levelMean Pair.get1And2Of3

// Averages for all numeric columns
byClassAndPort
|> Frame.getNumericCols
|> Series.dropMissing
|> Series.mapValues (Stats.levelMean Pair.get1And2Of3)
|> Frame.ofColumns

// Count number of survivors in each group
byClassAndPort.GetColumn<bool>("Survived")
|> Series.applyLevel Pair.get1And2Of3 (Series.values >> Seq.countBy id >> series)
|> Frame.ofRows

(**

### Aggregating by columns with `Frame.aggregateRowsBy`

`Frame.aggregateRowsBy` provides a convenient way to group by one or more columns and
apply an aggregation function to other columns — similar to SQL `GROUP BY … SELECT aggregate`.

The first argument specifies the grouping columns; the second specifies the columns to
aggregate; the third is the aggregation function to apply to each group series:
*)

(*** define-output:aggby1 ***)
// Average fare by Pclass and Sex
titanic
|> Frame.aggregateRowsBy ["Pclass"; "Sex"] ["Fare"] Stats.mean
(*** include-it:aggby1 ***)

(*** define-output:aggby2 ***)
// Count survivors (sum of bool-as-int) and average age per Pclass
titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Age"] Stats.mean
(*** include-it:aggby2 ***)

(**
<a name="pivot"></a>

## Summarizing data with pivot table

A pivot table is a useful tool if you want to summarize data in the frame based
on two keys that are available in the rows of the data frame. 
*)

(*** define-output:pivot1 ***)
titanic 
|> Frame.pivotTable 
    // Returns a new row key
    (fun k r -> r.GetAs<string>("Sex")) 
    // Returns a new column key
    (fun k r -> r.GetAs<bool>("Survived")) 
    // Specifies aggregation for sub-frames
    Frame.countRows 

(*** include-it:pivot1 ***)

(*** define-output:pivot2 ***)
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    (fun frame -> frame?Age |> Stats.mean)
|> round

(*** include-it:pivot2 ***)

(**
<a name="indexing"></a>

## Hierarchical indexing

### Grouping and aggregating

Hierarchical keys are often created as a result of grouping. For example, we can group
the rows (representing individual years) by decades:
*)
let decades = msft |> Frame.groupRowsUsing (fun k _ -> 
  sprintf "%d0s" (k.Year / 10))

// Calculate means per decade for the Close column
decades?Close |> Stats.levelMean fst

// Calculate means per decade for all numeric columns
decades
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofColumns

(**
<a name="missing"></a>

## Handling missing values

The support for missing values is built-in, which means that any series or frame can
contain missing values. When constructing series or frames from data, certain values
are automatically treated as "missing values". This includes `Double.NaN`, `null` values
for reference types and for nullable types:
*)
(*** define-output:misv1 ***)
Series.ofValues [ Double.NaN; 1.0; 3.14 ]

(*** include-it:misv1 ***)

(*** define-output:misv2 ***)
[ Nullable(1); Nullable(); Nullable(3) ]
|> Series.ofValues

(*** include-it:misv2 ***)

(**
Missing values are automatically skipped when performing statistical computations such
as `Series.mean`. They are also ignored by projections and filtering, including
`Series.mapValues`. When you want to handle missing values, you can use `Series.mapAll` 
that gets the value as `option<T>`:
*)

// Get column with missing values
let ozone = air?Ozone 

// Replace missing values with zeros
ozone |> Series.mapAll (fun k v -> 
  match v with None -> Some 0.0 | v -> v)

// Fill missing values with constant
ozone |> Series.fillMissingWith 0.0

// Available values are copied in backward 
// direction to fill missing values
ozone |> Series.fillMissing Direction.Backward

// Available values are propagated forward
ozone |> Series.fillMissing Direction.Forward

// Fill values and drop those that could not be filled
ozone |> Series.fillMissing Direction.Forward
      |> Series.dropMissing

(**
Various other strategies for handling missing values are not currently directly 
supported by the library, but can be easily added using `Series.fillMissingUsing`.
It takes a function and calls it on all missing values:
*)

// Fill missing values using interpolation function
ozone |> Series.fillMissingUsing (fun k -> 
  // Get previous and next values
  let prev = ozone.TryGet(k, Lookup.ExactOrSmaller)
  let next = ozone.TryGet(k, Lookup.ExactOrGreater)
  // Pattern match to check which values were available
  match prev, next with 
  | OptionalValue.Present(p), OptionalValue.Present(n) -> 
      (p + n) / 2.0
  | OptionalValue.Present(v), _ 
  | _, OptionalValue.Present(v) -> v
  | _ -> 0.0)

(**
For a comprehensive reference covering `OptionalValue`, sentinel types, all fill strategies,
and how missing values interact with joins and statistics, see the dedicated
[Handling missing values](missing.html) page.
*)
