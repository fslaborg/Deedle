# Joining, Merging and Appending Frames

Deedle provides a rich set of operations for combining data frames and series. This page covers:

* [Frame joins](#joins) — align two frames on their row/column keys

* [Series zipping](#zipping) — combine two series element-by-element

* [Append / concatenate](#append) — stack frames vertically or horizontally

* [Merge series](#merge) — merge sparse series into one

<a name="joins"></a>
## Frame joins

`Frame.join` combines two frames that share the same row-key type. The `JoinKind`
controls which rows appear in the result.

```fsharp
// Stock prices for two securities
let aapl =
    Frame.ofColumns [
        "AAPL Open",  Series.ofValues [ 150.0; 152.0; 151.0; 153.0 ]
        "AAPL Close", Series.ofValues [ 151.5; 151.0; 153.5; 154.0 ]
    ]
    |> Frame.indexRowsWith [| "2024-01-15"; "2024-01-16"; "2024-01-17"; "2024-01-18" |]
```

```
val aapl: Frame<string,string> =
  
              AAPL Open AAPL Close 
2024-01-15 -> 150       151.5      
2024-01-16 -> 152       151        
2024-01-17 -> 151       153.5      
2024-01-18 -> 153       154
```

```fsharp
let msft =
    Frame.ofColumns [
        "MSFT Open",  Series.ofValues [ 370.0; 375.0; 373.0 ]
        "MSFT Close", Series.ofValues [ 374.0; 373.0; 376.0 ]
    ]
    |> Frame.indexRowsWith [| "2024-01-15"; "2024-01-16"; "2024-01-18" |]
```

```
val msft: Frame<string,string> =
  
              MSFT Open MSFT Close 
2024-01-15 -> 370       374        
2024-01-16 -> 375       373        
2024-01-18 -> 373       376
```

### Outer join — keep all rows from both frames

```fsharp
let outerJoin = Frame.join JoinKind.Outer aapl msft
```

```
val outerJoin: Frame<string,string> =
  
              AAPL Open AAPL Close MSFT Open MSFT Close 
2024-01-15 -> 150       151.5      370       374        
2024-01-16 -> 152       151        375       373        
2024-01-17 -> 151       153.5      <missing> <missing>  
2024-01-18 -> 153       154        373       376
```

An outer join includes all row keys from either frame. Columns from each frame
are combined. When column names clash, they are suffixed with `.1` and `.2`.

Missing values appear where one frame had no data for a given key.

### Inner join — keep only rows present in both frames

```fsharp
let innerJoin = Frame.join JoinKind.Inner aapl msft
```

```
val innerJoin: Frame<string,string> =
  
              AAPL Open AAPL Close MSFT Open MSFT Close 
2024-01-15 -> 150       151.5      370       374        
2024-01-16 -> 152       151        375       373        
2024-01-18 -> 153       154        373       376
```

### Left join — keep all rows from the left frame

```fsharp
let leftJoin = Frame.join JoinKind.Left aapl msft
```

```
val leftJoin: Frame<string,string> =
  
              AAPL Open AAPL Close MSFT Open MSFT Close 
2024-01-15 -> 150       151.5      370       374        
2024-01-16 -> 152       151        375       373        
2024-01-17 -> 151       153.5      <missing> <missing>  
2024-01-18 -> 153       154        373       376
```

### Lookup join — align on nearest key

When frames have different but ordered key sets, you can fill missing values by
looking up the nearest available value using `Frame.joinAlign`.

```fsharp
let daily =
    frame [ "Price" => Series.ofValues [ 100.0; 101.0; 102.0; 103.0; 104.0 ] ]
    |> Frame.indexRowsWith [| 1; 2; 3; 4; 5 |]
```

```
val daily: Frame<int,string> =
  
     Price 
1 -> 100   
2 -> 101   
3 -> 102   
4 -> 103   
5 -> 104
```

```fsharp
let sparse =
    frame [ "Signal" => Series.ofValues [ 0.0; 1.0 ] ]
    |> Frame.indexRowsWith [| 1; 3 |]
```

```
val sparse: Frame<int,string> = 
     Signal 
1 -> 0      
3 -> 1
```

```fsharp
// Fill missing signal values by carrying forward the last known value
let aligned = Frame.joinAlign JoinKind.Left Lookup.ExactOrSmaller daily sparse
```

```
val aligned: Frame<int,string> =
  
     Price Signal 
1 -> 100   0      
2 -> 101   0      
3 -> 102   1      
4 -> 103   1      
5 -> 104   1
```

`Lookup.ExactOrSmaller` picks the nearest key that is ≤ the row key (forward fill).
Use `Lookup.ExactOrGreater` for backward fill.

<a name="zipping"></a>
## Series zipping

`Series.zip` and `Series.zipInto` combine two series into a series of tuples or
aggregated values, aligning on matching keys.

```fsharp
let s1 = Series.ofValues [ 1.0; 2.0; 3.0 ] |> Series.indexWith [| "a"; "b"; "c" |]
let s2 = Series.ofValues [ 10.0; 30.0 ]    |> Series.indexWith [| "a"; "c" |]
```

```
val s1: Series<string,float> = 
a -> 1 
b -> 2 
c -> 3 

val s2: Series<string,float> = 
a -> 10 
c -> 30
```

```fsharp
// Pair up values — missing in s2 where key is absent
let zipped = Series.zip s1 s2
```

```
val zipped: Series<string,(float opt * float opt)> =
  
a -> (1, 10)        
b -> (2, <missing>) 
c -> (3, 30)
```

```fsharp
// Sum corresponding values (inner join semantics — only where both have values)
let summed = Series.zipInto (fun a b -> a + b) s1 s2
```

```
val summed: Series<string,float> = 
a -> 11 
c -> 33
```

<a name="append"></a>
## Appending and concatenating frames

### `Frame.appendRowsBy` / `Frame.merge`

Use `Frame.appendRowsBy` to stack two frames with compatible columns vertically.

```fsharp
let q1 =
    frame [ "Sales" => Series.ofValues [ 100.0; 120.0; 130.0 ] ]
    |> Frame.indexRowsWith [| 1; 2; 3 |]
```

```
val q1: Frame<int,string> = 
     Sales 
1 -> 100   
2 -> 120   
3 -> 130
```

```fsharp
let q2 =
    frame [ "Sales" => Series.ofValues [ 140.0; 110.0; 150.0 ] ]
    |> Frame.indexRowsWith [| 4; 5; 6 |]
```

```
val q2: Frame<int,string> = 
     Sales 
4 -> 140   
5 -> 110   
6 -> 150
```

```fsharp
// Concatenate the two frames vertically (row-wise)
let fullYear = Frame.merge q1 q2
```

```
val fullYear: Frame<int,string> =
  
     Sales 
1 -> 100   
2 -> 120   
3 -> 130   
4 -> 140   
5 -> 110   
6 -> 150
```

### Adding columns from another frame

Use `Frame.addCol` or `Frame.join` to add columns to an existing frame.

```fsharp
let withExtra =
    fullYear
    |> Frame.addCol "Month" (Series.ofValues [ "Jan"; "Feb"; "Mar"; "Apr"; "May"; "Jun" ]
                              |> Series.indexWith [| 1; 2; 3; 4; 5; 6 |])
```

```
val withExtra: Frame<int,string> =
  
     Sales Month 
1 -> 100   Jan   
2 -> 120   Feb   
3 -> 130   Mar   
4 -> 140   Apr   
5 -> 110   May   
6 -> 150   Jun
```

<a name="merge"></a>
## Merging sparse series

`Series.merge` fills gaps in one series with values from another. The *first* series
takes priority; values from the *second* are used only where the first is missing.

```fsharp
let primary   = Series.ofOptionalObservations [ (1, Some 10.0); (2, None); (3, Some 30.0); (4, None) ]
let secondary = Series.ofOptionalObservations [ (2, Some 20.0); (4, Some 40.0) ]
```

```
val primary: Series<int,float> =
  
1 -> 10        
2 -> <missing> 
3 -> 30        
4 -> <missing> 

val secondary: Series<int,float> = 
2 -> 20 
4 -> 40
```

```fsharp
let merged = Series.merge primary secondary
```

```
val merged: Series<int,float> = 
1 -> 10 
2 -> 20 
3 -> 30 
4 -> 40
```

The result contains values from `primary` where present, and falls back to `secondary`
for missing positions.

You can also use `Frame.mergeAll` to merge a list of frames, or combine `Series.merge`
with `Series.map` and `Series.zip` for more complex merge strategies.

## See also

* [Handling missing values](missing.html) — how to fill or drop the missing values
introduced by outer joins.

* [Data frame features](frame.html) — frame construction, slicing, grouping, and aggregation.

* [Series features](series.html) — alignment, resampling, and windowing operations.
