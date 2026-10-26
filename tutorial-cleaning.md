# Tutorial: Cleaning messy data

*This is the Deedle equivalent of the classic "pandas data cleaning" tutorial.*
Real data sets have missing values, odd separators and columns that need transforming.
We work through the New York air quality measurements (May–September 1973), a well-known
data set that has gaps in the `Ozone` and `Solar.R` columns.

## Load the data

The file is semicolon-separated and marks missing values as `#N/A`.
Deedle recognises `#N/A`, `NA` and empty fields as missing values automatically:

```fsharp
let air = Frame.ReadCsv(root + "airquality.csv", separators = ";")

air |> Frame.take 3
```

```
val air: Frame<int,string> =
  
       Ozone     Solar.R   Wind Temp Month Day 
0   -> <missing> 190       7.4  67   5     1   
1   -> 36        118       8    72   5     2   
2   -> 12        149       12.6 74   5     3   
3   -> 18        313       11.5 62   5     4   
4   -> <missing> <missing> 14.3 56   5     5   
5   -> 28        <missing> 14.9 66   5     6   
6   -> 23        299       8.6  65   5     7   
7   -> 19        99        13.8 59   5     8   
8   -> 8         19        20.1 61   5     9   
9   -> <missing> 194       8.6  69   5     10  
10  -> 7         <missing> 6.9  74   5     11  
11  -> 16        256       9.7  69   5     12  
12  -> 11        290       9.2  66   5     13  
13  -> 14        274       10.9 68   5     14  
14  -> 18        65        13.2 58   5     15  
:      ...       ...       ...  ...  ...   ... 
138 -> 46        237       6.9  78   9     16  
139 -> 18        224       13.8 67   9     17  
140 -> 13        27        10.3 76   9     18  
141 -> 24        238       10.3 68   9     19  
142 -> 16        201       8    82   9     20  
143 -> 13        238       12.6 64   9     21  
144 -> 23        14        9.2  71   9     22  
145 -> 36        139       10.3 81   9     23  
146 -> 7         49        10.3 69   9     24  
147 -> 14        20        16.6 63   9     25  
148 -> 30        193       6.9  70   9     26  
149 -> <missing> 145       13.2 77   9     27  
150 -> 14        191       14.3 75   9     28  
151 -> 18        131       8    76   9     29  
152 -> 20        223       11.5 68   9     30  

val it: Frame<int,string> =
  
     Ozone     Solar.R Wind Temp Month Day 
0 -> <missing> 190     7.4  67   5     1   
1 -> 36        118     8    72   5     2   
2 -> 12        149     12.6 74   5     3
```

How big is it?

```fsharp
air.RowCount, air.ColumnCount
```

```
val it: int * int = (153, 6)
```

## Find the gaps

`ValueCount` on a series counts only the values that are present. Compare it with
the 153 rows to see which columns are incomplete:

```fsharp
air.Columns |> Series.mapValues (fun col -> col.ValueCount)
```

```
val it: Series<string,int> =
  
Ozone   -> 115 
Solar.R -> 146 
Wind    -> 153 
Temp    -> 153 
Month   -> 153 
Day     -> 153
```

`Stats.describe` also skips missing values, so its statistics describe the data that is there:

```fsharp
Stats.describe air
```

```
val it: Frame<string,string> =
  
          Ozone             Solar.R            Wind               Temp              Month              Day                
unique -> 66                117                31                 40                5                  31                 
mean   -> 42.13913043478261 185.93150684931507 9.95751633986928   77.88235294117646 6.993464052287582  15.803921568627452 
std    -> 33.13208201858655 90.05842222838167  3.5230013522126056 9.465269740971456 1.4165224840123147 8.864520368425419  
min    -> 1                 7                  1.7                56                5                  1                  
0.25   -> 18                115.75             7.4                72                6                  8                  
0.5    -> 31                205                9.7                79                7                  16                 
0.75   -> 63.5              258.75             11.5               85                8                  23                 
max    -> 168               334                20.7               97                9                  31
```

## Option 1: drop incomplete rows

`Frame.dropSparseRows` removes every row that has a missing value:

```fsharp
let complete = air |> Frame.dropSparseRows
complete.RowCount
```

```
val complete: Frame<int,string> =
  
       Ozone Solar.R Wind Temp Month Day 
1   -> 36    118     8    72   5     2   
2   -> 12    149     12.6 74   5     3   
3   -> 18    313     11.5 62   5     4   
6   -> 23    299     8.6  65   5     7   
7   -> 19    99      13.8 59   5     8   
8   -> 8     19      20.1 61   5     9   
11  -> 16    256     9.7  69   5     12  
12  -> 11    290     9.2  66   5     13  
13  -> 14    274     10.9 68   5     14  
14  -> 18    65      13.2 58   5     15  
15  -> 14    334     11.5 64   5     16  
16  -> 34    307     12   66   5     17  
17  -> 6     78      18.4 57   5     18  
18  -> 30    322     11.5 68   5     19  
19  -> 11    44      9.7  62   5     20  
:      ...   ...     ...  ...  ...   ... 
137 -> 13    112     11.5 71   9     15  
138 -> 46    237     6.9  78   9     16  
139 -> 18    224     13.8 67   9     17  
140 -> 13    27      10.3 76   9     18  
141 -> 24    238     10.3 68   9     19  
142 -> 16    201     8    82   9     20  
143 -> 13    238     12.6 64   9     21  
144 -> 23    14      9.2  71   9     22  
145 -> 36    139     10.3 81   9     23  
146 -> 7     49      10.3 69   9     24  
147 -> 14    20      16.6 63   9     25  
148 -> 30    193     6.9  70   9     26  
150 -> 14    191     14.3 75   9     28  
151 -> 18    131     8    76   9     29  
152 -> 20    223     11.5 68   9     30  

val it: int = 110
```

This is simple, but we lost 43 of 153 rows. Often it is better to repair the gaps.

## Option 2: fill the gaps

Replace missing ozone readings with a constant:

```fsharp
air?Ozone |> Series.fillMissingWith 0.0 |> Series.take 6
```

```
val it: Series<int,float> =
  
0 -> 0  
1 -> 36 
2 -> 12 
3 -> 18 
4 -> 0  
5 -> 28
```

Or carry the previous measurement forward, which suits sensor readings:

```fsharp
air?Ozone |> Series.fillMissing Direction.Forward |> Series.take 6
```

```
val it: Series<int,float> =
  
0 -> <missing> 
1 -> 36        
2 -> 12        
3 -> 18        
4 -> 18        
5 -> 28
```

See [Handling missing values](missing.html) for interpolation and other strategies.

## Add a derived column

Series support arithmetic, so converting Fahrenheit to Celsius is a one-liner.
Assign to a new column name with `<-`:

```fsharp
air?TempC <- (air?Temp - 32.0) * 5.0 / 9.0
air |> Frame.take 2
```

```
val it: Frame<int,string> =
  
     Ozone     Solar.R Wind Temp Month Day TempC              
0 -> <missing> 190     7.4  67   5     1   19.444444444444443 
1 -> 36        118     8    72   5     2   22.22222222222222
```

## Filter rows

Keep only the scorching days, i.e. those above 90°F:

```fsharp
let hot = air |> Frame.filterRowValues (fun row -> row.GetAs<float>("Temp") > 90.0)
hot.RowCount
```

```
val hot: Frame<int,string> =
  
       Ozone     Solar.R Wind Temp Month Day TempC              
41  -> <missing> 259     10.9 93   6     11  33.888888888888886 
42  -> <missing> 250     9.2  92   6     12  33.333333333333336 
68  -> 97        267     6.3  92   7     8   33.333333333333336 
69  -> 97        272     5.7  92   7     9   33.333333333333336 
74  -> <missing> 291     14.9 91   7     14  32.77777777777778  
101 -> <missing> 222     8.6  92   8     10  33.333333333333336 
119 -> 76        203     9.7  97   8     28  36.111111111111114 
120 -> 118       225     2.3  94   8     29  34.44444444444444  
121 -> 84        237     6.3  96   8     30  35.55555555555556  
122 -> 85        188     6.3  94   8     31  34.44444444444444  
123 -> 96        167     6.9  91   9     1   32.77777777777778  
124 -> 78        197     5.1  92   9     2   33.333333333333336 
125 -> 73        183     2.8  93   9     3   33.888888888888886 
126 -> 91        189     4.6  93   9     4   33.888888888888886 

val it: int = 14
```

Sort to see the three hottest days:

```fsharp
air |> Frame.sortRowsBy "Temp" (fun t -> -t) |> Frame.take 3
```

```
val it: Frame<int,string> =
  
       Ozone Solar.R Wind Temp Month Day TempC              
119 -> 76    203     9.7  97   8     28  36.111111111111114 
121 -> 84    237     6.3  96   8     30  35.55555555555556  
122 -> 85    188     6.3  94   8     31  34.44444444444444
```

## Group and aggregate

Which month has the most ozone? `Frame.aggregateRowsBy` groups by the given columns and
applies a function to the listed columns (missing values are skipped by `Stats.mean`):

```fsharp
air |> Frame.aggregateRowsBy ["Month"] ["Ozone"; "Temp"] Stats.mean
```

```
val it: Frame<int,string> =
  
     Month Ozone              Temp              
0 -> 5     22.92              65.54838709677419 
1 -> 6     29.444444444444443 79.1              
2 -> 7     59.11538461538461  83.90322580645162 
3 -> 8     59.96153846153846  83.96774193548387 
4 -> 9     31.448275862068964 76.9
```

## Relationships between columns

Ozone rises with temperature:

```fsharp
Stats.corr air?Ozone air?Temp
```

```
val it: float = 0.7020755178
```

## Save the cleaned result

```fsharp
air.SaveCsv("airquality-clean.csv", separator = ',')
```

## Where next?

* [Handling missing values](missing.html) — all the ways of detecting and filling gaps

* [Data frame features](frame.html) — grouping, pivoting and reshaping

* [Joining and merging frames](joining.html) — combine several data sources
