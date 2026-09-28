# Data frame features

This page is a comprehensive reference for `Frame<'R,'C>` operations.
If you are new to Deedle, start with the [quick start tutorial](tutorial.html)
which introduces loading, filtering, grouping, and missing values
using the Titanic data set.

## Loading data

### CSV files

`Frame.ReadCsv` loads CSV (and TSV) files. The most common usage is a
single path, but it accepts many optional parameters:

* `path` — file path or URL

* `indexCol` — column to use as row index (the type is inferred from the type parameter)

* `inferTypes` — whether to auto-detect column types (default `true`)

* `inferRows` — number of rows used for type inference (default 100; 0 = all)

* `schema` — explicit CSV schema string

* `separators` — column separator characters (e.g. `";"`)

* `culture` — culture name for parsing (default invariant)

```fsharp
let titanic = Frame.ReadCsv(root + "titanic.csv")

// Use a typed index column and sort
let msft = 
  Frame.ReadCsv(root + "stocks/msft.csv") 
  |> Frame.indexRowsDateTime "Date"
  |> Frame.sortRowsByKey

// Semicolon-separated file
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")

// Shorthand: specify index column via type parameter
let msftSimpler = 
  Frame.ReadCsv<DateTime>(root + "stocks/msft.csv", indexCol="Date") 
  |> Frame.sortRowsByKey
```

### Saving CSV files

```fsharp
// Save with semicolon separator
air.SaveCsv(Path.GetTempFileName(), separator=';')

// Include the row key as a column named "Date"
msft.SaveCsv(Path.GetTempFileName(), keyNames=["Date"], separator='\t')
```

By default `SaveCsv` omits the row key. Pass `includeRowKeys=true` or
provide `keyNames` to include it.

### From F# records or .NET objects

`Frame.ofRecords` turns any sequence of records or objects into a frame,
using public properties as columns:

```fsharp
type Person = 
  { Name: string; Age: int; Countries: string list }

let peopleRecds = 
  [ { Name = "Joe"; Age = 51; Countries = ["UK"; "US"; "UK"] }
    { Name = "Tomas"; Age = 28; Countries = ["CZ"; "UK"; "US"; "CZ"] }
    { Name = "Eve"; Age = 2; Countries = ["FR"] }
    { Name = "Suzanne"; Age = 15; Countries = ["US"] } ]

let people = 
  Frame.ofRecords peopleRecds 
  |> Frame.indexRowsString "Name"
```

```
type Person =
  {
    Name: string
    Age: int
    Countries: string list
  }
val peopleRecds: Person list =
  [{ Name = "Joe"
     Age = 51
     Countries = ["UK"; "US"; "UK"] };
   { Name = "Tomas"
     Age = 28
     Countries = ["CZ"; "UK"; "US"; "CZ"] }; { Name = "Eve"
                                               Age = 2
                                               Countries = ["FR"] };
   { Name = "Suzanne"
     Age = 15
     Countries = ["US"] }]
val people: Frame<string,string> =
  
           Age Countries          
Joe     -> 51  [UK; US; UK]       
Tomas   -> 28  [CZ; UK; US; ... ] 
Eve     -> 2   [FR]               
Suzanne -> 15  [US]
```

### Expanding nested objects

If a column contains complex .NET objects, `Frame.expandCols` flattens
their properties into new columns:

```fsharp
let peopleNested = 
  [ "People" => Series.ofValues peopleRecds ] |> frame

peopleNested |> Frame.expandCols ["People"]
```

```
val peopleNested: Frame<int,string> =
  
     People                                             
0 -> { Name = "Joe"
  Age = 51
  Countries = ["UK"; ... 
1 -> { Name = "Tomas"
  Age = 28
  Countries = ["CZ"... 
2 -> { Name = "Eve"
  Age = 2
  Countries = ["FR"] }    
3 -> { Name = "Suzanne"
  Age = 15
  Countries = ["U... 

val it: Frame<int,string> =
  
     People.Name People.Age People.Countries   
0 -> Joe         51         [UK; US; UK]       
1 -> Tomas       28         [CZ; UK; US; ... ] 
2 -> Eve         2          [FR]               
3 -> Suzanne     15         [US]
```

## Getting and setting data

### Columns and rows

```fsharp
// Get column as float series (using ?)
people?Age

// Get column with explicit type
people.GetColumn<string list>("Countries")

// All columns as a series of series
people.Columns
```

### Adding and replacing columns

```fsharp
// Add a computed column
people?AgePlusOne <- people?Age |> Series.mapValues ((+) 1.0)

// Add from a list (must match row count)
people?Siblings <- [0; 2; 1; 3]

// Replace an existing column
people.ReplaceColumn("Siblings", [3; 2; 1; 0])
```

### Adding rows

```fsharp
let newRow = 
  [ "Name" => box "Jim"; "Age" => box 51;
    "Countries" => box ["US"]; "Siblings" => box 5 ]
  |> series

people.Merge("Jim", newRow)
```

## Slicing and lookup

### Indexing into series

```fsharp
let ages = people?Age

// Single key
ages.["Tomas"]

// Multiple keys
ages.[ ["Tomas"; "Joe"] ]

// Safe lookup (returns None for missing keys)
ages |> Series.tryGet "John"

// Series that may contain missing values for unknown keys
ages |> Series.getAll [ "Tomas"; "John" ]
```

### Iterating observations

```fsharp
// All key-value pairs
ages |> Series.observations

// Including missing values as None
ages |> Series.observationsAll
```

### Slicing ordered series by range

```fsharp
let opens = msft?Open
opens.[DateTime(2013, 1, 1) .. DateTime(2013, 1, 31)]
|> Series.mapKeys (fun k -> k.ToShortDateString())
```

```
val opens: Series<DateTime,float> =
  
03/13/1986 -> 25.5  
03/14/1986 -> 28    
03/17/1986 -> 29    
03/18/1986 -> 29.5  
03/19/1986 -> 28.75 
03/20/1986 -> 28.25 
03/21/1986 -> 27.5  
03/24/1986 -> 26.75 
03/25/1986 -> 26    
03/26/1986 -> 26.5  
03/27/1986 -> 27.25 
03/31/1986 -> 27.75 
04/01/1986 -> 27.5  
04/02/1986 -> 27.25 
04/03/1986 -> 27.75 
...        -> ...   
10/18/2013 -> 34.82 
10/21/2013 -> 34.98 
10/22/2013 -> 35.02 
10/23/2013 -> 34.35 
10/24/2013 -> 33.82 
10/25/2013 -> 35.88 
10/28/2013 -> 35.61 
10/29/2013 -> 35.63 
10/30/2013 -> 35.53 
10/31/2013 -> 35.66 
11/01/2013 -> 35.67 
11/04/2013 -> 35.59 
11/05/2013 -> 35.79 
11/06/2013 -> 37.24 
11/07/2013 -> 37.96 

val it: Series<string,float> =
  
01/02/2013 -> 27.25 
01/03/2013 -> 27.63 
01/04/2013 -> 27.27 
01/07/2013 -> 26.77 
01/08/2013 -> 26.75 
01/09/2013 -> 26.72 
01/10/2013 -> 26.65 
01/11/2013 -> 26.49 
01/14/2013 -> 26.9  
01/15/2013 -> 26.83 
01/16/2013 -> 27.15 
01/17/2013 -> 27.19 
01/18/2013 -> 27.1  
01/22/2013 -> 27.3  
01/23/2013 -> 27.2  
01/24/2013 -> 27.7  
01/25/2013 -> 27.58 
01/28/2013 -> 28.01 
01/29/2013 -> 27.82 
01/30/2013 -> 28.01 
01/31/2013 -> 27.79
```

## Grouping and aggregation

The [quick start tutorial](tutorial.html) shows basic `Frame.groupRowsByString`,
`Frame.aggregateRowsBy`, and `Frame.pivotTable`. This section covers deeper
features: hierarchical keys, `Frame.nest`/`Frame.unnest`, and multi-level
aggregation.

### Hierarchical (multi-level) keys

Grouping produces tuple keys treated as a hierarchical index:

```fsharp
// Group MSFT stock prices by decade
let decades = msft |> Frame.groupRowsUsing (fun k _ -> 
  sprintf "%d0s" (k.Year / 10))

// Mean Close price per decade
decades?Close |> Stats.levelMean fst
```

```
val decades: Frame<(string * DateTime),string> =
  
                    Open  High  Low   Close Volume     Adj Close 
1980s 03/13/1986 -> 25.50 29.25 25.50 28.00 1031788800 0.07      
      03/14/1986 -> 28.00 29.50 28.00 29.00 308160000  0.07      
      03/17/1986 -> 29.00 29.75 29.00 29.50 133171200  0.08      
      03/18/1986 -> 29.50 29.75 28.50 28.75 67766400   0.07      
      03/19/1986 -> 28.75 29.00 28.00 28.25 47894400   0.07      
      03/20/1986 -> 28.25 28.25 27.25 27.50 58435200   0.07      
      03/21/1986 -> 27.50 28.00 26.25 26.75 59990400   0.07      
      03/24/1986 -> 26.75 26.75 25.75 26.00 65289600   0.07      
      03/25/1986 -> 26.00 26.50 25.75 26.50 32083200   0.07      
      03/26/1986 -> 26.50 27.50 26.25 27.25 22752000   0.07      
      03/27/1986 -> 27.25 27.75 27.25 27.75 16848000   0.07      
      03/31/1986 -> 27.75 27.75 27.00 27.50 12873600   0.07      
      04/01/1986 -> 27.50 27.50 27.25 27.25 11088000   0.07      
      04/02/1986 -> 27.25 28.00 27.25 27.50 27014400   0.07      
      04/03/1986 -> 27.75 28.50 27.75 27.75 23040000   0.07      
:     :             ...   ...   ...   ...   ...        ...       
2010s 10/18/2013 -> 34.82 34.99 34.33 34.96 41811700   34.96     
      10/21/2013 -> 34.98 35.20 34.91 34.99 27433500   34.99     
      10/22/2013 -> 35.02 35.10 34.52 34.58 40438500   34.58     
      10/23/2013 -> 34.35 34.49 33.67 33.76 58600500   33.76     
      10/24/2013 -> 33.82 34.10 33.57 33.72 53209700   33.72     
      10/25/2013 -> 35.88 36.29 35.47 35.73 113494000  35.73     
      10/28/2013 -> 35.61 35.73 35.27 35.57 38383600   35.57     
      10/29/2013 -> 35.63 35.72 35.26 35.52 31702200   35.52     
      10/30/2013 -> 35.53 35.79 35.43 35.54 36997700   35.54     
      10/31/2013 -> 35.66 35.69 35.34 35.41 41682300   35.41     
      11/01/2013 -> 35.67 35.69 35.39 35.53 40264600   35.53     
      11/04/2013 -> 35.59 35.98 35.55 35.94 28060700   35.94     
      11/05/2013 -> 35.79 36.71 35.77 36.64 51646300   36.64     
      11/06/2013 -> 37.24 38.22 37.06 38.18 88615100   38.18     
      11/07/2013 -> 37.96 38.01 37.43 37.50 60437400   37.50     

val it: Series<string,float> =
  
1980s -> 60.043264033263945 
1990s -> 95.34143196202483  
2000s -> 38.155157057654094 
2010s -> 28.533113402061844
```

```fsharp
// Means for all numeric columns
decades
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofColumns
```

```
val it: Frame<string,string> =
  
         Open               High               Low                Close              Volume             Adj Close           
1980s -> 59.946569646569586 61.082629937629896 58.91826403326398  60.043264033263945 70968296.04989605  0.24493762993763055 
1990s -> 95.24579905063263  96.62888449367058  93.89387262658194  95.34143196202483  66713447.7056962   7.927614715189868   
2000s -> 38.16654870775352  38.72730019880724  37.625677932405495 38.155157057654094 70747895.8250497   22.633316103379645  
2010s -> 28.524896907216455 28.779319587628898 28.26515463917527  28.533113402061844 55734837.422680415 27.18783505154637
```

### Multi-level grouping

```fsharp
// Group Titanic by class then port of embarkation
let byClassAndPort = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Embarked"
  |> Frame.mapRowKeys Pair.flatten3

// Average age per (Embarked, Pclass) group
byClassAndPort?Age
|> Stats.levelMean Pair.get1And2Of3
```

```
val byClassAndPort: Frame<(string * int * int),string> =
  
           PassengerId Survived Pclass Name                                               Sex    Age       SibSp Parch Ticket           Fare    Cabin Embarked 
S 3 0   -> 1           False    3      Braund, Mr. Owen Harris                            male   22        1     0     A/5 21171        7.25          S        
S 3 2   -> 3           True     3      Heikkinen, Miss. Laina                             female 26        0     0     STON/O2. 3101282 7.925         S        
S 3 4   -> 5           False    3      Allen, Mr. William Henry                           male   35        0     0     373450           8.05          S        
S 3 7   -> 8           False    3      Palsson, Master. Gosta Leonard                     male   2         3     1     349909           21.075        S        
S 3 8   -> 9           True     3      Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)  female 27        0     2     347742           11.1333       S        
S 3 10  -> 11          True     3      Sandstrom, Miss. Marguerite Rut                    female 4         1     1     PP 9549          16.7    G6    S        
S 3 12  -> 13          False    3      Saundercock, Mr. William Henry                     male   20        0     0     A/5. 2151        8.05          S        
S 3 13  -> 14          False    3      Andersson, Mr. Anders Johan                        male   39        1     5     347082           31.275        S        
S 3 14  -> 15          False    3      Vestrom, Miss. Hulda Amanda Adolfina               female 14        0     0     350406           7.8542        S        
S 3 18  -> 19          False    3      Vander Planke, Mrs. Julius (Emelia Maria Vandem... female 31        1     0     345763           18            S        
S 3 24  -> 25          False    3      Palsson, Miss. Torborg Danira                      female 8         3     1     349909           21.075        S        
S 3 25  -> 26          True     3      Asplund, Mrs. Carl Oscar (Selma Augusta Emilia ... female 38        1     5     347077           31.3875       S        
S 3 29  -> 30          False    3      Todoroff, Mr. Lalio                                male   <missing> 0     0     349216           7.8958        S        
S 3 37  -> 38          False    3      Cann, Mr. Ernest Charles                           male   21        0     0     A./5. 2152       8.05          S        
S 3 38  -> 39          False    3      Vander Planke, Miss. Augusta Maria                 female 18        2     0     345764           18            S        
: : :      ...         ...      ...    ...                                                ...    ...       ...   ...   ...              ...     ...   ...      
C 2 181 -> 182         False    2      Pernot, Mr. Rene                                   male   <missing> 0     0     SC/PARIS 2131    15.05         C        
C 2 292 -> 293         False    2      Levy, Mr. Rene Jacques                             male   36        0     0     SC/Paris 2163    12.875  D     C        
C 2 308 -> 309         False    2      Abelson, Mr. Samuel                                male   30        1     0     P/PP 3381        24            C        
C 2 361 -> 362         False    2      del Carlo, Mr. Sebastiano                          male   29        1     0     SC/PARIS 2167    27.7208       C        
C 2 389 -> 390         True     2      Lehmann, Miss. Bertha                              female 17        0     0     SC 1748          12            C        
C 2 473 -> 474         True     2      Jerwan, Mrs. Amin S (Marie Marthe Thuillard)       female 23        0     0     SC/AH Basle 541  13.7917 D     C        
C 2 547 -> 548         True     2      Padro y Manent, Mr. Julian                         male   <missing> 0     0     SC/PARIS 2146    13.8625       C        
C 2 608 -> 609         True     2      Laroche, Mrs. Joseph (Juliette Marie Louise Laf... female 22        1     2     SC/Paris 2123    41.5792       C        
C 2 685 -> 686         False    2      Laroche, Mr. Joseph Philippe Lemercier             male   25        1     2     SC/Paris 2123    41.5792       C        
C 2 817 -> 818         False    2      Mallet, Mr. Albert                                 male   31        1     1     S.C./PARIS 2079  37.0042       C        
C 2 827 -> 828         True     2      Mallet, Master. Andre                              male   1         0     2     S.C./PARIS 2079  37.0042       C        
C 2 866 -> 867         True     2      Duran y More, Miss. Asuncion                       female 27        1     0     SC/PARIS 2149    13.8583       C        
C 2 874 -> 875         True     2      Abelson, Mrs. Samuel (Hannah Wizosky)              female 28        1     0     P/PP 3381        24            C        
  1 61  -> 62          True     1      Icard, Miss. Amelie                                female 38        0     0     113572           80      B28            
  1 829 -> 830         True     1      Stone, Mrs. George Nelson (Martha Evelyn)          female 62        0     0     113572           80      B28            

val it: Series<(string * int),float> =
  
S 3 -> 25.69655172413793  
S 1 -> 38.15203703703704  
S 2 -> 30.38673076923077  
Q 3 -> 25.9375            
Q 1 -> 38.5               
Q 2 -> 43.5               
C 3 -> 20.741951219512195 
C 1 -> 38.027027027027025 
C 2 -> 22.766666666666666 
  1 -> 50
```

```fsharp
// Survival counts per group
byClassAndPort.GetColumn<bool>("Survived")
|> Series.applyLevel Pair.get1And2Of3 (Series.values >> Seq.countBy id >> series)
|> Frame.ofRows
```

```
val it: Frame<(string * int),bool> =
  
       False     True 
S 3 -> 286       67   
S 1 -> 53        74   
S 2 -> 88        76   
Q 3 -> 45        27   
Q 1 -> 1         1    
Q 2 -> 1         2    
C 3 -> 41        25   
C 1 -> 26        59   
C 2 -> 8         9    
  1 -> <missing> 2
```

### Nesting and unnesting

`Frame.nest` splits a grouped frame into a series of sub-frames;
`Frame.unnest` reverses this:

```fsharp
let bySex = titanic |> Frame.groupRowsByString "Sex"
let nested = bySex |> Frame.nest      // Series of frames
let flat = nested |> Frame.unnest     // Back to single frame
```

### Grouping series

```fsharp
let travels = people.GetColumn<string list>("Countries")

// Country visit frequency per person, as a frame
travels
|> Series.mapValues (Seq.countBy id >> series)
|> Frame.ofRows
|> Frame.fillMissingWith 0
```

```
val travels: Series<string,string list> =
  
Joe     -> [UK; US; UK]       
Tomas   -> [CZ; UK; US; ... ] 
Eve     -> [FR]               
Suzanne -> [US]               

val it: Frame<string,string> =
  
           UK US CZ FR 
Joe     -> 2  1  0  0  
Tomas   -> 1  1  2  0  
Eve     -> 0  0  0  1  
Suzanne -> 0  1  0  0
```

### aggregateRowsBy

`Frame.aggregateRowsBy` is Deedle's equivalent of SQL
`GROUP BY … SELECT aggregate(col)`:

```fsharp
titanic
|> Frame.aggregateRowsBy ["Pclass"; "Sex"] ["Fare"] Stats.mean
```

```
val it: Frame<int,string> =
  
     Pclass Sex    Fare               
0 -> 3      male   12.661632564841513 
1 -> 1      female 106.12579787234041 
2 -> 3      female 16.118809722222224 
3 -> 1      male   67.22612704918033  
4 -> 2      female 21.97012105263158  
5 -> 2      male   19.74178240740741
```

```fsharp
titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Age"] Stats.mean
```

```
val it: Frame<int,string> =
  
     Pclass Age                
0 -> 3      25.14061971830986  
1 -> 1      38.233440860215055 
2 -> 2      29.87763005780347
```

### Pivot tables

Cross-tabulate two categorical variables with an aggregation function:

```fsharp
// Passenger counts by Sex × Survived
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    Frame.countRows 
```

```
val it: Frame<string,bool> =
  
          False True 
male   -> 468   109  
female -> 81    233
```

```fsharp
// Mean age by Sex × Survived
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex")) 
    (fun k r -> r.GetAs<bool>("Survived")) 
    (fun frame -> frame?Age |> Stats.mean)
|> round
```

```
val it: Frame<string,bool> =
  
          False True 
male   -> 32    27   
female -> 25    29
```

## Handling missing values

The [quick start](tutorial.html) covers `fillMissingWith`, `fillMissing`, and `dropMissing`.
This section shows how missing values arise and the more advanced `fillMissingUsing` strategy.

### How missing values arise

`Double.NaN`, `null`, and empty `Nullable<T>` are all treated as missing:

```fsharp
Series.ofValues [ Double.NaN; 1.0; 3.14 ]
```

```
val it: Series<int,float> = 
0 -> <missing> 
1 -> 1         
2 -> 3.14
```

```fsharp
[ Nullable(1); Nullable(); Nullable(3) ] |> Series.ofValues
```

```
val it: Series<int,Nullable<int>> =
  
0 -> 1         
1 -> <missing> 
2 -> 3
```

### Custom fill with interpolation

`Series.fillMissingUsing` calls a function for each missing key, enabling
strategies like linear interpolation:

```fsharp
let ozone = air?Ozone

ozone |> Series.fillMissingUsing (fun k -> 
  let prev = ozone.TryGet(k, Lookup.ExactOrSmaller)
  let next = ozone.TryGet(k, Lookup.ExactOrGreater)
  match prev, next with 
  | OptionalValue.Present(p), OptionalValue.Present(n) -> (p + n) / 2.0
  | OptionalValue.Present(v), _ 
  | _, OptionalValue.Present(v) -> v
  | _ -> 0.0)
```

For the full missing-value reference (sentinel types, all fill strategies,
interaction with joins), see [Handling missing values](missing.html).
