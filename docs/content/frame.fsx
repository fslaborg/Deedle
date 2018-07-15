(*** hide ***)
#I "../../packages/FSharp.Data/lib/net45"
#r "FSharp.Data.dll"
ignore <| FSharp.Data.WorldBankData.GetDataContext() // Force fsi to load F# Data (required on Mono)

#load "../../bin/net45/Deedle.fsx"
open System
open System.IO
open FSharp.Data
open Deedle

let root = __SOURCE_DIRECTORY__ + "/data/"

(**
Working with data frames in F#
==============================

In this section, we look at various features of the F# data frame library (using both
`Series` and `Frame` types and modules). Feel free to jump to the section you are interested
in, but note that some sections refer back to values built in "Creating & loading".

You can also get this page as an [F# script file](https://github.com/fslaborg/Deedle/blob/master/docs/content/frame.fsx)
from GitHub and run the samples interactively.

<a name="creating"></a>
Creating frames & loading data
------------------------------

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

Once you have a data frame, you can also save it to a CSV file using the 
`SaveCsv` method. For example:
*)
// Save CSV with semicolon separator
air.SaveCsv(Path.GetTempFileName(), separator=';')
// Save as CSV and include row key as "Date" column
msft.SaveCsv(Path.GetTempFileName(), keyNames=["Date"], separator='\t')

(**
By default, the `SaveCsv` method does not include the key from the data frame. This can be
overriden by calling `SaveCsv` with the optional argument `includeRowKeys=true`, or with an
additional argument `keyNames` (demonstrated above) which sets the headers for the key columns(s)
in the CSV file. Usually, there is just a single row key, but there may be multiple when 
[hierarchical indexing](#indexing) is used.

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
`GetColumn` with the right type arguments:
*)
people?Age
people.GetColumn<string list>("Countries")

(**
<a name="creating-wb"></a>
### F# Data providers

In general, you can use any data source that exposes data as series of tuples. This
means that we can easily load data using, for example, the World Bank type provider 
from [F# Data library](https://github.com/fsharp/FSharp.Data).
*)
// Connect to the World Bank
let wb = WorldBankData.GetDataContext()

/// Given a region, load GDP in current LCU and return data as 
/// a frame with two-level column key (region and country name)
let loadRegion (region:WorldBankData.ServiceTypes.Region) =
  [ for country in region.Countries -> 
      // Create two-level column key using tuple
      (region.Name, country.Name) => 
        // Create series from tuples returned by WorldBank
        Series.ofObservations country.Indicators.``GDP (current LCU)`` ]
  |> frame

(**
To make data manipulation more convenient, we read country information per region
and create data frame with a hierarchical index (for more information, see the
[advanced indexing section](#indexing)). Now we can easily read data for OECD and
Euro area:
*)
// Load Euro and OECD regions
let eu = loadRegion wb.Regions.``Euro area``
let oecd = loadRegion wb.Regions.``OECD members``

// Join and convert to billions 
let world = eu.Join(oecd) / 1e9

(*** include-value:(round (world*100.0))/100.0 ***)

(**
The loaded data look something like the sample above. As you can see, the columns
are grouped by the region and some data are not available.

### Expanding objects in columns

It is possible to create data frames that contain other .NET objects as members in a 
series. This might be useful, for example, when you get multiple data sources producing
objects and you want to align or join them before working with them. However, working
with frames that contain complex .NET objects is less conveninet.

For this reason, the data frame supports _expansion_. Given a data frame with some object
in a column, you can use `Frame.expandCols` to create a new frame that contains properties
of the object as new columns. For example: 
*)

(*** define-output:ppl ***)
// Create frame with single column 'People'
let peopleNested = 
  [ "People" => Series.ofValues peopleRecds ] |> frame

// Expand the 'People' column
peopleNested |> Frame.expandCols ["People"]
(*** include-it:ppl ***)

(**
As you can see, the operation generates columns based on the properties of the original 
column type and generates new names by prefixing the property names with the name of the
original column.

Aside from properties of .NET objects, the expansion can also handle values of type
`IDictionary<K, V>` and series that contain nested series with `string` keys 
(i.e. `Series<string, T>`). If you have more complex structure, you can use
`Frame.expandAllCols` to expand columns to a specified level recursively:

*)
// Series that contains dictionaries, containing tuples
let tuples = 
  [ dict ["A", box 1; "C", box (2, 3)]
    dict ["B", box 1; "C", box (3, 4)] ] 
  |> Series.ofValues

// Expand dictionary keys (level 1) and tuple items (level 2)
frame ["Tuples" => tuples]
|> Frame.expandAllCols 2
(**
Here, the resulting data frame will have 4 columns including
`Tuples.A` and `Tuples.B` (for the first keys) and `Tuples.C.Item1`
together with `Tuples.C.Item2` representing the two items of the tuple
nested in a dictionary.

<a name="dataframe"></a>
Manipulating data frames
------------------------

The series type `Series<K, V>` represents a series with keys of type `K` and values
of type `V`. This means that when working with series, the type of values is known 
statically. When working with data frames, this is not the case - a frame is represented
as `Frame<R, C>` where `R` and `C` are the types of row and column indices, respectively
(typically, `R` will be an `int` or `DateTime` and `C` will be `string` representing 
different column/series names. 

A frame can contain heterogeneous data. One column may contain integers, another may
contain floating point values and yet another can contain strings, dates or other objects
like lists of strings. This information is not captured statically - and so when working
with frames, you may need to specify the type explicitly, for example, when reading
a series from a frame.

### Getting data from a frame

We'll use the data frame `people` which contains three columns - `Name` of type `string`,
`Age` of type `int` and `Countries` of type `string list` (we created it from F# records
in [the previous section](#creating-recd)):

*)
(*** include-value:people ***)
(**
To get a column (series) from a frame `df`, you can use operations that are exposed directly
by the data frame, or you can use `df.Columns` which returns all columns of the frame as a
series of series.
*)

// Get the 'Age' column as a series of 'float' values
// (the '?' operator converts values automatically)
people?Age
// Get the 'Countries' column as a series of 'string list' values
people.GetColumn<string list>("Countries")
// Get all frame columns as a series of series
people.Columns

(**
A series `s` of type `Series<string, V>` supports the question mark operator `s?Foo` to get
a value of type `V` associated with the key `Foo`. For other key types, you can sue the `Get` 
method. Note that, unlike with frames, there is no implicit conversion:
*)
// Get Series<string, float> 
let numAges = people?Age

// Get value using question mark
numAges?Tomas
// Get value using 'Get' method
numAges.Get("Tomas")
// Returns missing when key is not found
numAges.TryGet("Fridrich")

(**
The question mark operator and `Get` method can be used on the `Columns` property of data frame.
The return type of `df?Columns` is `ColumnSeries<string, string>` which is just a thin wrapper
over `Series<C, ObjectSeries<R>>`. This means that you get back a series indexed by column names
where the values are `ObjectSeries<R>` representing individual columns. The type
`ObjectSeries<R>` is a thin wrapper over `Series<R, obj>` which adds several functions 
for getting the values as values of specified type.

In our case, the returned values are individual columns represented as `ObjectSeries<string>`:
*)
// Get column as an object series
people.Columns?Age
people.Columns?Countries
// [fsi:val it : ObjectSeries<string> =]
// [fsi:  Joe     -> [UK; US; UK]       ]
// [fsi:  Tomas   -> [CZ; UK; US; ... ] ]
// [fsi:  Eve     -> [FR]               ]
// [fsi:  Suzanne -> [US]]

// Get column & try get column using members
people.Columns.Get("Countries")
people.Columns.TryGet("CreditCard")
// Get column at a specified offset
people.Columns.GetAt(0)

// Get column as object series and convert it
// to a typed Series<string, string>
people.Columns?Age.As<int>()
// Try converting column to Series<string, string>
people.Columns?Age.TryAs<string>()
(**
The type `ObjectSeries<string>` has a few methods in addition to ordinary `Series<K, V>` type.
On the lines 18 and 20, we use `As<T>` and `TryAs<T>` that can be used to convert object series
to a series with statically known type of values. The expression on line 18 is equivalent to
`people.GetColumn<obj>("Age")`, but it is not specific to frame columns - you can use the
same approach to work with frame rows (using `people.Rows`) if your data set has rows of 
homogeneous types.

Another case where you'll need to work with `ObjectSeries<T>` is when mapping over rows:
*)
// Iterate over rows and get the length of country list
people.Rows |> Series.mapValues (fun row ->
  row.GetAs<string list>("Countries").Length)

(**
The rows that you get as a result of `people.Rows` are heterogeneous (they contain values
of different types), so we cannot use `row.As<T>()` to convert all values of the series
to some type. Instead, we use `GetAs<T>(...)` which is similar to `Get(...)` but converts
the value to a given type. You could also achieve the same thing by writing `row?Countries` 
and then casting the result to `string list`, but the `GetAs` method provides a more convenient
syntax.

### Typed access to rows

Accessing columns using `ObjectSeries<T>` is fine for simple tasks, but it has two problems.
First, it is not type-safe and you can easily get a runtime exception if you specify wrong
type. Second, it involves boxing and unboxing and so it may be inefficient.

To address these two issues, Deedle provides another alternative. You can specify an _interface_
that defines the types of columns once and then use this interface to get a series of rows
where every row is an instance of the interface:
*)
/// Expected columns & their types in a row
type IPerson = 
  abstract Age : int
  abstract Countries : string list

// Get rows as series of 'IPerson' values
let rows = people.GetRowsAs<IPerson>()
rows.["Tomas"].Countries 
(**
You still need to be careful and define the types in the `IPerson` interface correctly, but
once the `GetRowsAs<IPerson>` call returns a value, you will be able to access the rows in
a nice typed way. Alternatively, you can also specify the type with `OptionalValue<T>`, in 
case you want to explicitly handle missing values.
*)
/// Alternative that lets us handle missing 'Age' values
type IPersonOpt = 
  abstract Age : OptionalValue<int>
  abstract Countries : string list
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
to an existing data frame, drop a series or replace a series. However, individual series
are still immutable.
*)
// Calculate age + 1 for all people
let add1 = people?Age |> Series.mapValues ((+) 1.0)

// Add as a new series to the frame
people?AgePlusOne <- add1

// Add new series from a list of values
people?Siblings <- [0; 2; 1; 3]

// Replace existing series with new values
// (Equivalent to people?Siblings <- ...)
people.ReplaceColumn("Siblings", [3; 2; 1; 0])

(**
Finally, it is also possible to append one data frame or a single row to an existing data
frame. The operation is immutable, so the result is a new data frame with the added
rows. To create a new row for the data frame, we can use standard ways of constructing
series from key-value pairs, or we can use the `SeriesBuilder` type:
*)

// Create new object series with values for required columns
let newRow = 
  [ "Name" => box "Jim"; "Age" => box 51;
    "Countries" => box ["US"]; "Siblings" => box 5 ]
  |> series
// Create a new data frame, containing the new series
people.Merge("Jim", newRow)

// Another option is to use mutable SeriesBuilder
let otherRow = SeriesBuilder<string>()
otherRow?Name <- "Jim"
otherRow?Age <- 51
otherRow?Countries <- ["US"]
otherRow?Siblings <- 5
// The Series property returns the built series
people.Merge("Jim", otherRow.Series)


(**

<a name="slicing"></a>
Advanced slicing and lookup
---------------------------

Given a series, we have a number of options for getting one or more values or 
observations (keys and an associated values) from the series. First, let's look
at different lookup operations that are available on any (even unordered series).
*)

// Sample series with different keys & values
let nums = series [ 1 => 10.0; 2 => 20.0 ]
let strs = series [ "en" => "Hi"; "cz" => "Ahoj" ]

// Lookup values using keys
nums.[1]
strs.["en"]
// Supported when key is string
strs?en      

(**
For more examples, we use the `Age` column from [earlier data set](#creating-recd) as example:
*)

// Get an unordered sample series 
let ages = people?Age

// Returns value for a given key
ages.["Tomas"]
// Returns series with two keys from the source
ages.[ ["Tomas"; "Joe"] ]

(**
The `Series` module provides another set of useful functions (many of those
are also available as members, for example via `ages.TryGet`):
*)

// Fails when key is not present
try ages |> Series.get "John" with _ -> nan
// Returns 'None' when key is not present
ages |> Series.tryGet "John"
// Returns series with missing value for 'John'
// (equivalent to 'ages.[ ["Tomas"; "John"] ]')
ages |> Series.getAll [ "Tomas"; "John" ]

(**
We can also obtain all data from the series. The data frame library uses the
term _observations_ for all key-value pairs
*)

// Get all observations as a sequence of 'KeyValuePair'
ages.Observations
// Get all observations as a sequence of tuples
ages |> Series.observations
// Get all observations, with 'None' for missing values
ages |> Series.observationsAll

(**
The previous examples were always looking for an exact key. If we have an ordered
series, we can search for a nearest available key and we can also perform slicing.
We use MSFT stock prices [from earlier example](#creating-csv):
*)

// Get series with opening prices
let opens = msft?Open

// Fails. The key is not available in the series
try opens.[DateTime(2013, 1, 1)] with e -> nan
// Works. Find value for the nearest greater key
opens.Get(DateTime(2013, 1, 1), Lookup.ExactOrSmaller)
// Works. Find value for the nearest smaler key
opens.Get(DateTime(2013, 1, 1), Lookup.ExactOrSmaller)

(**
When using instance members, we can use `Get` which has an overload taking
`Lookup`. The same functionality is exposed using `Series.lookup`. We can
also obtain values for a sequence of keys:
*)
// Find value for the nearest greater key
opens |> Series.lookup (DateTime(2013, 1, 1)) Lookup.ExactOrGreater

// Get first price for each month in 2012
let dates = [ for m in 1 .. 12 -> DateTime(2012, m, 1) ]
opens |> Series.lookupAll dates Lookup.ExactOrGreater

(**
With ordered series, we can use slicing to get a sub-range of a series:
*)

(*** define-output:opens ***)
opens.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)]
|> Series.mapKeys (fun k -> k.ToShortDateString())

(*** include-it:opens ***)

(** 
The slicing works even if the keys are not available in the series. The lookup
automatically uses nearest greater lower bound and nearest smaller upper bound
(here, we have no value for January 1).

Several other options - discussed in [a later section](#indexing) - are available when using
hierarchical (or multi-level) indices. But first, we need to look at grouping.

<a name="grouping"></a>
Grouping data
-------------

Grouping of data can be performed on both unordered and ordered series and frames.
For ordered series, more options (such as floating window or grouping of consecutive
elements) are available - these can be found in the [time series tutorial](series.html).
There are essentially two options: 

 - You can group series of any values and get a series of series (representing individual 
   groups). The result can easily be turned into a data frame using `Frame.ofColumns` or
   `Frame.ofRows`, but this is not done automatically.

 - You can group a frame rows using values in a specified column, or using a function.
   The result is a frame with multi-level (hierarchical) index. Hierarchical indexing
   [is discussed later](#indexing).

Keep in mind that you can easily get a series of rows or a series of columns from a frame
using `df.Rows` and `df.Columns`, so the first option is also useful on data frames.

### Grouping series

In the following sample, we use the data frame `people` loaded from F# records in 
[an earlier section](#creating-recd). Let's first get the data:
*)
let travels = people.GetColumn<string list>("Countries")
// [fsi:val travels : Series<string,string list> =]
// [fsi:  Joe     -> [UK; US; UK]       ]
// [fsi:  Tomas   -> [CZ; UK; US; ... ] ]
// [fsi:  Eve     -> [FR] ]              
// [fsi:  Suzanne -> [US]]
(**
Now we can group the elements using both key (e.g. length of a name) and using the
value (e.g. the number of visited countries):
*)
// Group by name length (ignoring visited countries)
travels |> Series.groupBy (fun k v -> k.Length)
// Group by visited countries (people visited/not visited US)
travels |> Series.groupBy (fun k v -> List.exists ((=) "US") v)

// Group by name length and get number of values in each group
travels |> Series.groupInto 
  (fun k v -> k.Length) 
  (fun len people -> Series.countKeys people)
(**
The `groupBy` function returns a series of series (series with new keys, containing
series with all values for a given new key). You can than transform the values using
`Series.mapValues`. However, if you want to avoid allocating all intermediate series,
you can also use `Series.groupInto` which takes projection function as a second argument.
In the above examples, we count the number of keys in each group.

As a final example, let's say that we want to build a data frame that contains individual
people (as rows), all countries that appear in someone's travel list (as columns). 
The frame contains the number of visits to each country by each person:
*)
(*** define-output: trav ***)
travels
|> Series.mapValues (Seq.countBy id >> series)
|> Frame.ofRows
|> Frame.fillMissingWith 0

(*** include-it: trav ***)

(**
The problem can be solved just using `Series.mapValues`, together with standard F#
`Seq` functions. We iterate over all rows (people and their countries). For each
country list, we generate a series that contains individual countries and the count
of visits (this is done by composing `Seq.countBy` and a function `series` to build
a series of observations). Then we turn the result to a data frame and fill missing
values with the constant zero (see a section about [handling missing values](#missing)).

### Grouping data frames

So far, we worked with series and series of series (which can be turned into data frames
using `Frame.ofRows` and `Frame.ofColumns`). Next, we look at working with data frames.

Assume we loaded [Titanic data set](http://www.kaggle.com/c/titanic-gettingStarted) 
that is also used on the [project home page](index.html). First, let's look at basic
grouping (also used in the home page demo):
*)

// Group using column 'Sex' of type 'string'
titanic |> Frame.groupRowsByString "Sex"

// Grouping using column converted to 'decimal'
let byDecimal : Frame<decimal * _, _> = 
  titanic |> Frame.groupRowsBy "Fare"

// This is easier using member syntax
titanic.GroupRowsBy<decimal>("Fare")

// Group using calculated value - length of name
titanic |> Frame.groupRowsUsing (fun k row -> 
  row.GetAs<string>("Name").Length)

(**
When working with frames, you can group data using both rows and columns. For most
functions there is `groupRows` and `groupCols` equivalent.
The easiest functions to use are `Frame.groupRowsByXyz` where `Xyz` specifies the 
type of the column that we're using for grouping. For example, we can easily group
rows using the "Sex" column.

When using less common type, you need to specify the type of the column. You can 
see this on lines 5 and 9 where we use `decimal` as the key. Finally, you can also
specify key selector as a function. The function gets the original key and the row
as a value of `ObjectSeries<K>`. The type has various members for getting individual
values (columns) such as `GetAs` which allows us to get a column of a specified type.

### Grouping by single key

A grouped data frame uses multi-level index. This means that the index is a tuple
of keys that represent multiple levels. For example:
*)
titanic |> Frame.groupRowsByString "Sex"
// [fsi:val it : Frame<(string * int),string> =]
// [fsi:                Survive   Name                    ]
// [fsi:  female 2   -> True      Heikkinen, Miss. Laina  ]
// [fsi:         11  -> True      Bonnell, Miss. Elizabeth]
// [fsi:         19  -> True      Masselmani, Mrs. Fatima ]
// [fsi:                ...       ...                     ]
// [fsi:  male   870 -> False     Balkic, Mr. Cerin       ]
// [fsi:         878 -> False     Laleff, Mr. Kristo      ]

(**
As you can see, the pretty printer understands multi-level indices and 
outputs the first level (sex) followed by the second level (passanger id).
You can turn frame with two-level index into a series of data frames
(and vice versa) using `Frame.unnest` and `Frame.nest`:
*)
let bySex = titanic |> Frame.groupRowsByString "Sex" 
// Returns series with two frames as values
let bySex1 = bySex |> Frame.nest
// Converts unstacked data back to a single frame
let bySex2 = bySex |> Frame.nest |> Frame.unnest
(**

### Grouping by multiple keys
Finally, we can also apply grouping operation repeatedly to group data using
multiple keys (and get a frame indexed by more than 2 levels). For example,
we can group passangers by their class and port where they embarked:
*)
// Group by passanger class and port
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3

// Get just the Age series with the same row index
let ageByClassAndPort = byClassAndPort?Age
(**
If you look at the type of `byClassAndPort`, you can see that it is
`Frame<(string * int * int),string>`. The row key is a tripple consisting
of port identifier (string), passanger class (int between 1 and 3) and the
passanger id. The multi-level indexing is preserved when we get a single
series from the frame.

As our last example, we look at various ways of aggregating the groups:
*)
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
The second snippet combines a number of useful functions. It uses `Frame.getNumericColumns`
to obtain just numerical columns from a data frame. Then it drops the non-numerical columns
using `Series.dropMissing`. Then we use `Series.mapValues` to apply the averaging operation
to all columns.

The last snippet is alo interesting. We get the "Survived" column (which 
contains Boolean values) and we aggregate each group using a specified function.
The function is composed from three components - it first gets the values in the
group, counts them (to get a number of `true` and `false` values) and then creates
a series with the results. The result looks as the following table (some values
were omitted):

    [lang=text]
             True  False     
    C 1  ->  59    26        
      2  ->  9     8         
      3  ->  25    41        
    S 1  ->  74    53        
      2  ->  76    88        
      3  ->  67    286      
                  

<a name="pivot"></a>
Summarizing data with pivot table
---------------------------------

In the previous section, we looked at grouping, which is a very general 
data manipulation operation. However, very often we want to perform two operations
at the same time - group the data by certain keys and produce an aggregate. This
combination is captured by the concept of a _pivot table_. 

A pivot table is a useful tool if you want to summarize data in the frame based
on two keys that are available in the rows of the data frame. 

For example, given the titanic data set that [we loaded earlier](#creating-csv) and
explored in the previous section, we might want to compare the survival rate for males 
and females. The pivot table makes this possible using just a single call:
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
(**
The `pivotTable` function (and the corresponding `PivotTable` method) take three arguments.
The first two specify functions that, given a row in the original frame, return a new
row key and column key, respectively. In the above example, the new row key is
the `Sex` value and the new column key is whether a person survived or not. As a result
we get the following two by two table:
*)

(*** include-it:pivot1 ***)

(**
Note, we could also use the `PivotTable` member method along with a type annotation on the
result for readability:
*)
let table : Frame<string,bool> = 
  titanic.PivotTable("Sex", "Survived", Frame.countRows)

(**
The pivot table operation takes the source frame, partitions the data (rows) based on the 
new row and column keys and then aggregates each frame using the specified aggregation. In the
above example, we used `Frame.countRows` to simply return number of people in each sub-group.
However, we could easily calculate other statistic - such as average age:
*) 

(*** define-output:pivot2 ***)
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    (fun frame -> frame?Age |> Stats.mean)
|> round
(**
The results suggest that older males were less likely survive than younger males, but 
older females were more likely to survive then younger females:
*)

(*** include-it:pivot2 ***)

(**
<a name="indexing"></a>
Hierarchical indexing
---------------------

For some data sets, the index is not a simple sequence of keys, but instead a more
complex hierarchy. This can be captured using hierarchical indices. They also provide
a convenient way of dealing with multi-dimensional data. The most common source
of multi-level indices is grouping (the previous section has a number of examples).

### Lookup in the World Bank data set

In this section, we start by looking at the [World Bank data set from earlier](#creating-wb).
It is a data frame with two-level hierarchy of columns, where the first level is the name
of region and the second level is the name of country.

Basic lookup can be performed using slicing operators. The following are only available 
in F# 3.1:
*)

// Get all countries in Euro area
world.Columns.["Euro area", *]
// Get Belgium data from Euro area group
world.Columns.[("Euro area", "Belgium")]
// Belgium is returned twice - from both Euro and OECD
world.Columns.[*, "Belgium"]

(**
In F# 3.0, you can use a family of helper functions `LookupXOfY` as follows:
*)

// Get all countries in Euro area
world.Columns.[Lookup1Of2 "Euro area"]
// Belgium is returned twice - from both Euro and OECD
world.Columns.[Lookup2Of2 "Belgium"]

(**
The lookup operations always return data frame of the same type as the original frame.
This means that even if you select one sub-group, you get back a frame with the same
multi-level hierarchy of keys. This can be easily changed using projection on keys:
*)
// Drop the first level of keys (and get just countries)
let euro = 
  world.Columns.["Euro area", *]
  |> Frame.mapColKeys snd

(**
### Grouping and aggregating World Bank data

Hierarchical keys are often created as a result of grouping. For example, we can group
the rows (representing individual years) in the Euro zone data set by decades
(for more information about grouping see also [grouping section](#grouping) in this
document).

*)
let decades = euro |> Frame.groupRowsUsing (fun k _ -> 
  sprintf "%d0s" (k / 10))
// [fsi: ]
// [fsi:val decades : Frame<(string * int),string> =]
// [fsi:                Austria  Estonia   ...      ]
// [fsi:  1960s 1960 -> 6.592    <missing> ]     
// [fsi:        1961 -> 7.311    <missing> ]
// [fsi:        ...  ]
// [fsi:  2010s 2010 -> 376.8    18.84 ]
// [fsi:        2011 -> 417.6    22.15 ]
// [fsi:        2012 -> 399.6    21.85 ]
(**
Now that we have a data frame with hierarchical index, we can select data
in a single group, such as 1990s. The result is a data frame of the same type.
We can also multiply the values, to get original GDP in USD (rather than billions):
*)
decades.Rows.["1990s", *] * 1e9

(**
The `Frame` and `Series` modules provide a number of functions for aggregating the
groups. We can access a specific country and aggregate GDP for a country, or we can
apply aggregation to the entire data set:
*)

// Calculate means per decades for Slovakia
decades?``Slovak Republic`` |> Stats.levelMean fst

// Calculate means per decateds for all countries
decades
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofColumns

// Calculate standard deviation per decades in USD
decades?Belgium * 1.0e9 
|> Stats.levelStdDev fst

(**
So far, we were working with data frames that only had one hierarchical index. However,
it is perfectly possible to have hierarchical index for both rows and columns. The following
snippet groups countries by their average GDP (in addition to grouping rows by decades):
*)

// Group countries by comparing average GDP with $500bn
let byGDP = 
  decades |> Frame.transpose |> Frame.groupRowsUsing (fun k v -> 
    v.As<float>() |> Stats.mean > 500.0)
(**
You can see (by hovering over `byGDP`) that the two hierarchies are captured in the type.
The column key is `bool * string` (rich? and name) and the row key is `string * int` 
(decade, year). This creates two groups of columns. One containing France, Germany and
Italy and the other containing remaining countries.

The aggregations are only (directly) supported on rows, but we can use `Frame.transpose`
to switch between rows and columns. 

<a name="missing"></a>
Handling missing values
-----------------------

THe support for missing values is built-in, which means that any series or frame can
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
that gets the value as `option<T>` (we use sample data set from [earlier section](#creating-csv)):
*)

// Get column with missing values
let ozone = air?Ozone 

// Replace missing values with zeros
ozone |> Series.mapAll (fun k v -> 
  match v with None -> Some 0.0 | v -> v)

(**
In practice, you will not need to use `Series.mapAll` very often, because the
series module provides functions that fill missing values more easily:
*)

// Fill missing values with constant
ozone |> Series.fillMissingWith 0.0

// Available values are copied in backward 
// direction to fill missing values
ozone |> Series.fillMissing Direction.Backward

// Available values are propagated forward
// (if the first value is missing, it is not filled!)
ozone |> Series.fillMissing Direction.Forward

// Fill values and drop those that could not be filled
ozone |> Series.fillMissing Direction.Forward
      |> Series.dropMissing

(**
Various other strategies for handling missing values are not currently directly 
supported by the library, but can be easily added using `Series.fillMissingUsing`.
It takes a function and calls it on all missing values. If we have an interpolation
function, then we can pass it to `fillMissingUsing` and perform any interpolation 
needed.

For example, the following snippet gets the previous and next values and averages
them (if they are available) or returns one of them (or zero if there are no values
at all):
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
