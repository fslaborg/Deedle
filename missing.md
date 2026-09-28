# Handling missing values

Missing-value support is a first-class feature of Deedle. Every series and frame
can contain missing values; the library tracks them explicitly and handles them
consistently across all operations.

<a name="representation"></a>
## How missing values are represented

Deedle uses `OptionalValue<'T>` to represent a value that may or may not be
present. You rarely construct `OptionalValue` directly; instead, certain input
values are automatically treated as missing:

Input type | Treated as missing when
--- | ---
`float` / `double` | Value is `Double.NaN`
Reference type (string, obj …) | Value is `null`
`Nullable<T>` | `.HasValue` is `false`


The following examples show series created from inputs that include missing values:

```fsharp
// float NaN becomes a missing value
Series.ofValues [ 1.0; Double.NaN; 3.0 ]
```

```
val it: Series<int,float> = 
0 -> 1         
1 -> <missing> 
2 -> 3
```

```fsharp
// null in a reference-type series
Series.ofValues [ "a"; null; "c" ]
```

```
val it: Series<int,string> = 
0 -> a         
1 -> <missing> 
2 -> c
```

```fsharp
// Nullable<int> without a value
[ Nullable(1); Nullable(); Nullable(3) ] |> Series.ofValues
```

```
val it: Series<int,Nullable<int>> =
  
0 -> 1         
1 -> <missing> 
2 -> 3
```

You can also construct a series with explicit missing values using `None`:

```fsharp
Series.ofOptionalObservations
  [ 1 => Some(10.0)
    2 => None
    3 => Some(30.0) ]
```

```
val it: Series<int,float> = 
1 -> 10        
2 -> <missing> 
3 -> 30
```

<a name="counting"></a>
## Counting present and missing values

`Stats.count` counts the number of **present** values; `Series.countValues`
is an alias. The total number of keys (including missing) is `KeyCount`:

```fsharp
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone

// total keys (rows) in the series
ozone.KeyCount
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

val ozone: Series<int,float> =
  
0   -> <missing> 
1   -> 36        
2   -> 12        
3   -> 18        
4   -> <missing> 
5   -> 28        
6   -> 23        
7   -> 19        
8   -> 8         
9   -> <missing> 
10  -> 7         
11  -> 16        
12  -> 11        
13  -> 14        
14  -> 18        
... -> ...       
138 -> 46        
139 -> 18        
140 -> 13        
141 -> 24        
142 -> 16        
143 -> 13        
144 -> 23        
145 -> 36        
146 -> 7         
147 -> 14        
148 -> 30        
149 -> <missing> 
150 -> 14        
151 -> 18        
152 -> 20        

val it: int = 153
```

```fsharp
// present (non-missing) values
Stats.count ozone
```

```
val it: int = 115
```

```fsharp
// number of missing values
ozone.KeyCount - int (Stats.count ozone)
```

```
val it: int = 38
```

<a name="statistics"></a>
## Statistics skip missing values

All functions in the `Stats` module, as well as common projections such as
`Series.mapValues` and `Series.filter`, automatically skip missing values.
The operation is applied only to present observations:

```fsharp
Stats.mean ozone      // mean of the 116 present values
```

```
val it: float = 42.13913043
```

```fsharp
Stats.max ozone       // maximum of the present values
```

```
val it: float = 168.0
```

<a name="accessing"></a>
## Accessing values with `TryGet`

The safe way to look up a single value by key is `TryGet`, which returns an
`OptionalValue<'T>`:

```fsharp
let v = ozone.TryGet(1)
match v with
| OptionalValue.Present x -> sprintf "present: %g" x
| OptionalValue.Missing   -> "missing"
```

```
val v: OptionalValue<float> = 36
val it: string = "present: 36"
```

You can also use `Series.observationsAll` to iterate over all key-value pairs
including missing ones (as `option`), or `Series.observations` to skip
missing values:

```fsharp
ozone
|> Series.observationsAll
|> Seq.truncate 6
|> Seq.map (fun (k, v) ->
    match v with
    | Some x -> sprintf "%d => %g" k x
    | None   -> sprintf "%d => <missing>" k)
|> Seq.toList
```

```
val it: string list =
  ["0 => <missing>"; "1 => 36"; "2 => 12"; "3 => 18"; "4 => <missing>";
   "5 => 28"]
```

<a name="mapall"></a>
## Custom handling with `Series.mapAll`

`Series.mapValues` skips missing values. When you need to transform the value
**or** the missing-ness of each element, use `Series.mapAll`, which receives an
`option<'T>` for each key:

```fsharp
ozone
|> Series.mapAll (fun k v ->
    match v with
    | None   -> Some 0.0     // replace missing with zero
    | Some x -> Some (x * 2.0)) // double present values
|> Series.take 5
```

```
val it: Series<int,float> = 
0 -> 0  
1 -> 72 
2 -> 24 
3 -> 36 
4 -> 0
```

<a name="filling"></a>
## Filling missing values

### Fill with a constant

The simplest strategy replaces every missing value with a fixed constant:

```fsharp
ozone |> Series.fillMissingWith 0.0 |> Series.take 6
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

### Forward and backward fill

`Series.fillMissing` propagates the most recent available value in the
specified direction:

```fsharp
// Carry the last known value forward
ozone |> Series.fillMissing Direction.Forward |> Series.take 6
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

```fsharp
// Fill from the next available value backward
ozone |> Series.fillMissing Direction.Backward |> Series.take 6
```

```
val it: Series<int,float> =
  
0 -> 36 
1 -> 36 
2 -> 12 
3 -> 18 
4 -> 28 
5 -> 28
```

### Custom fill strategy with `fillMissingUsing`

For interpolation or other context-sensitive strategies, use
`Series.fillMissingUsing`. The function receives the missing key and should
return a replacement value:

```fsharp
ozone
|> Series.fillMissingUsing (fun k ->
    // Linear interpolation from neighbours
    let prev = ozone.TryGet(k, Lookup.ExactOrSmaller)
    let next = ozone.TryGet(k, Lookup.ExactOrGreater)
    match prev, next with
    | OptionalValue.Present p, OptionalValue.Present n -> (p + n) / 2.0
    | OptionalValue.Present v, _
    | _, OptionalValue.Present v -> v
    | _ -> 0.0)
|> Series.take 6
```

```
val it: Series<int,float> =
  
0 -> 36 
1 -> 36 
2 -> 12 
3 -> 18 
4 -> 23 
5 -> 28
```

### Combining fill and drop

Often the cleanest approach is to fill as much as possible in one direction
and then discard the remaining missing values:

```fsharp
ozone
|> Series.fillMissing Direction.Forward
|> Series.dropMissing
|> Series.countValues
```

```
val it: int = 152
```

<a name="dropping"></a>
## Dropping missing values

### Drop from a series

`Series.dropMissing` removes all missing observations from a series:

```fsharp
ozone |> Series.dropMissing |> Series.countValues
```

```
val it: int = 115
```

### Drop sparse rows and columns from a frame

`Frame.dropSparseRows` removes any row that contains at least one missing
value; `Frame.dropSparseCols` removes columns with any missing value.

After reading the air quality CSV the frame has missing values in several
columns:

```fsharp
air.RowCount
```

```
val it: int = 153
```

```fsharp
// Keep only rows that are fully observed
let airComplete = air |> Frame.dropSparseRows
airComplete.RowCount
```

```
val airComplete: Frame<int,string> =
  
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

<a name="frames"></a>
## Missing values in frames

The same filling functions are available at the frame level and operate
column-by-column:

```fsharp
// Fill every missing cell with 0.0
air
|> Frame.fillMissingWith 0.0
|> Frame.dropSparseRows   // now no rows should be dropped
|> fun f -> f.RowCount
```

```
val it: int = 153
```

```fsharp
// Forward-fill each column independently
air
|> Frame.fillMissing Direction.Forward
|> Frame.dropSparseRows
|> fun f -> f.RowCount
```

```
val it: int = 152
```

`Frame.fillMissingUsing` accepts a function `Series<'R,'T> -> 'R -> 'T` so it
can base the fill value on the whole column series:

```fsharp
air
|> Frame.fillMissingUsing (fun col key ->
    // Fill with that column's mean
    Stats.mean col)
|> Frame.dropSparseRows
|> fun f -> f.RowCount
```

```
val it: int = 153
```

<a name="joins"></a>
## Missing values in joins

When two frames or series are joined, rows that exist in one source but not
the other receive missing values for the absent columns. The join kind controls
which rows are retained:

Join kind | Rows kept | Missing values introduced
--- | --- | ---
`JoinKind.Inner` | Only rows present in **both** | None
`JoinKind.Left` | All rows from the **left** | Right columns for unmatched rows
`JoinKind.Right` | All rows from the **right** | Left columns for unmatched rows
`JoinKind.Outer` | All rows from **either** | Both sides for unmatched rows


```fsharp
let s1 = series [ 1 => 10.0; 2 => 20.0; 3 => 30.0 ]
let s2 = series [ 2 => 200.0; 3 => 300.0; 4 => 400.0 ]

// Outer join introduces missing values for key 1 (not in s2) and key 4 (not in s1)
Frame.ofColumns ["A" => s1; "B" => s2]
```

```
val s1: Series<int,float> = 
1 -> 10 
2 -> 20 
3 -> 30 

val s2: Series<int,float> = 
2 -> 200 
3 -> 300 
4 -> 400 

val it: Frame<int,string> =
  
     A         B         
1 -> 10        <missing> 
2 -> 20        200       
3 -> 30        300       
4 -> <missing> 400
```

```fsharp
// Inner join keeps only keys present in both
let f1 = frame ["A" => s1]
let f2 = frame ["B" => s2]
f1.Join(f2, JoinKind.Inner)
```

```
val f1: Frame<int,string> = 
     A  
1 -> 10 
2 -> 20 
3 -> 30 

val f2: Frame<int,string> = 
     B   
2 -> 200 
3 -> 300 
4 -> 400 

val it: Frame<int,string> = 
     A  B   
2 -> 20 200 
3 -> 30 300
```

After an outer join, `Frame.dropSparseRows` or `Frame.fillMissing` can be
used to bring the frame back to a fully populated state.

## Summary

Goal | Function
--- | ---
Count present values | `Stats.count series`
Count keys (incl. missing) | `series.KeyCount`
Safe lookup | `series.TryGet(key)` → `OptionalValue<T>`
Iterate with missing | `Series.observationsAll`
Handle in a transform | `Series.mapAll`
Fill with constant | `Series.fillMissingWith value` / `Frame.fillMissingWith value`
Carry forward/back | `Series.fillMissing Direction.*` / `Frame.fillMissing Direction.*`
Custom fill | `Series.fillMissingUsing f` / `Frame.fillMissingUsing f`
Remove missing rows | `Series.dropMissing` / `Frame.dropSparseRows`
Remove missing cols | `Frame.dropSparseCols`


## See also

* [Joining and merging frames](joining.html) — how joins interact with missing values.

* [Data frame features](frame.html) — the frame overview including a shorter introduction
to missing values in context.

* [Series features](series.html) — windowing and resampling functions that produce missing
values at boundaries.

* [Statistics](stats.html) — how `Stats.*` functions skip over missing values.
