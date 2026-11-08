# Frame and series statistics

The `Stats` type contains functions for fast calculation of statistics over
series and frames as well as over a moving and an expanding window in a series.
The standard statistical functions that are available in the `Stats` type
are overloaded and can be applied to both data frames and series. More advanced
functionality is available only for series (but can be applied to frame columns
easily using the `Frame.getNumericCols` function).

<a name="stats"></a>
## Series and frame statistics

In this section, we look at calculating simple statistics over data frame and
series. An important aspect is handling of missing values, so we demonstrate that
using a data set about air quality that contains missing values:

```fsharp
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone
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
```

### Series statistics

Given a series `ozone`, we can use a number of `Stats` functions to calculate
statistics. The following example creates a series (indexed by strings) that
stores mean, extremes and median of the input series:

```fsharp
series [
  "Mean" => round (Stats.mean ozone)
  "Max" => Stats.max ozone
  "Min" => Stats.min ozone
  "Median" => Stats.median ozone ]
```

```
val it: Series<string,float> =
  
Mean   -> 42  
Max    -> 168 
Min    -> 1   
Median -> 31
```

To make the output simpler, we round the value of the mean (although the result is
a floating point number). Note that the value is calculated from the *available* values
in the series. All of the statistical functions skip over missing values in the
input series.

`Stats.max` and `Stats.min` return `option<float>` rather than just `float`. The result
value is `None` when the series contains no values.

### Frame statistics

Functions such as `Stats.mean` can be called on series, but also on entire data frames.
In that case, they calculate the statistics for each column of a data frame and return
`Series<'C, float>` where `'C` is the column key of the original frame.

```fsharp
let info = 
  [ "Min" => Stats.min air
    "Max" => Stats.max air
    "Mean" => Stats.mean air
    "+/-" => Stats.stdDev air ] |> frame
```

```
val info: Frame<string,string> =
  
           Min Max  Mean               +/-                
Ozone   -> 1   168  42.13913043478261  33.13208201858655  
Solar.R -> 7   334  185.93150684931507 90.05842222838167  
Wind    -> 1.7 20.7 9.95751633986928   3.5230013522126056 
Temp    -> 56  97   77.88235294117646  9.465269740971456  
Month   -> 5   9    6.993464052287582  1.4165224840123147 
Day     -> 1   31   15.803921568627452 8.864520368425419
```

<a name="moving"></a>
## Moving window statistics

The `Stats` type provides an efficient implementation of moving window statistics using
an online algorithm. The moving window function names are prefixed with the word `moving`:

```fsharp
ozone |> Stats.movingMean 3
```

```
val it: Series<int,float> =
  
0   -> <missing>          
1   -> <missing>          
2   -> 24                 
3   -> 22                 
4   -> 15                 
5   -> 23                 
6   -> 25.5               
7   -> 23.333333333333332 
8   -> 16.666666666666668 
9   -> 13.5               
10  -> 7.5                
11  -> 11.5               
12  -> 11.333333333333334 
13  -> 13.666666666666666 
14  -> 14.333333333333334 
... -> ...                
138 -> 22.666666666666668 
139 -> 25.666666666666668 
140 -> 25.666666666666668 
141 -> 18.333333333333332 
142 -> 17.666666666666668 
143 -> 17.666666666666668 
144 -> 17.333333333333332 
145 -> 24                 
146 -> 22                 
147 -> 19                 
148 -> 17                 
149 -> 22                 
150 -> 22                 
151 -> 16                 
152 -> 17.333333333333332
```

Statistical moving functions (count, sum, mean, variance, standard deviation, skewness
and kurtosis) over a window of size *n* always mark the first *n-1* values with missing
(i.e. they only perform the calculation over complete windows).

The boundary behavior of the functions that calculate minimum and maximum over a moving window
differs. Rather than returning *N/A* for the first *n-1* values, they return the extreme
value over a smaller window:

```fsharp
ozone |> Stats.movingMin 3
```

```
val it: Series<int,float> =
  
0   -> <missing> 
1   -> 36        
2   -> 12        
3   -> 12        
4   -> 12        
5   -> 18        
6   -> 23        
7   -> 19        
8   -> 8         
9   -> 8         
10  -> 7         
11  -> 7         
12  -> 7         
13  -> 11        
14  -> 11        
... -> ...       
138 -> 9         
139 -> 13        
140 -> 13        
141 -> 13        
142 -> 13        
143 -> 13        
144 -> 13        
145 -> 13        
146 -> 7         
147 -> 7         
148 -> 7         
149 -> 14        
150 -> 14        
151 -> 14        
152 -> 14
```

<a name="exp"></a>
## Expanding windows

Expanding window means that the window starts as a single-element sized window at the beginning
of a series and expands as it moves over the series. The expanding window functions are
prefixed with `expanding`.

```fsharp
let exp =
  [ "Ozone" => ozone 
    "Mean" => Stats.expandingMean(ozone)
    "+/-" => Stats.expandingStdDev(ozone) ] |> frame
```

```
val exp: Frame<int,string> =
  
       Ozone     Mean               +/-                
0   -> <missing> <missing>          <missing>          
1   -> 36        36                 <missing>          
2   -> 12        24                 16.97056274847714  
3   -> 18        22                 12.489995996796797 
4   -> <missing> 22                 12.489995996796797 
5   -> 28        23.5               10.63014581273465  
6   -> 23        23.4               9.20869154657707   
7   -> 19        22.666666666666664 8.430104783848577  
8   -> 8         20.57142857142857  9.484322904265804  
9   -> <missing> 20.57142857142857  9.484322904265804  
10  -> 7         18.875             10.006248048094749 
11  -> 16        18.555555555555557 9.408920117514961  
12  -> 11        17.8               9.186947262284681  
13  -> 14        17.454545454545457 8.790490729915325  
14  -> 18        17.500000000000004 8.382882992904486  
:      ...       ...                ...                
138 -> 46        45.09803921568629  33.96460294988006  
139 -> 18        44.8349514563107   33.90300382532458  
140 -> 13        44.528846153846175 33.88213629363948  
141 -> 24        44.33333333333336  33.778311961588    
142 -> 16        44.06603773584908  33.7295317454896   
143 -> 13        43.77570093457946  33.70412561142138  
144 -> 23        43.58333333333336  33.60577526023052  
145 -> 36        43.51376146788993  33.45771645450393  
146 -> 7         43.1818181818182   33.4853608485753   
147 -> 14        42.91891891891894  33.44768918098695  
148 -> 30        42.80357142857145  33.31905260371709  
149 -> <missing> 42.80357142857145  33.31905260371709  
150 -> 14        42.54867256637171  33.28046165074146  
151 -> 18        42.33333333333336  33.21255404681616  
152 -> 20        42.13913043478263  33.13208201858655
```

<a name="multi"></a>
## Multi-level indexed statistics

For a series with multi-level (hierarchical) index, the functions prefixed with `level` provide
a way to apply statistical operation on a single level of the index.

The following example demonstrates the idea - the `air` data set contains data for each
day between May and September. We create a frame with two-level row key using
`Frame.indexRowsUsing` and returning a tuple as the index:

```fsharp
let dateFormat = CultureInfo.CurrentCulture.DateTimeFormat
let byMonth = air |> Frame.indexRowsUsing (fun r ->
    dateFormat.GetMonthName(r.GetAs<int>("Month")), r.GetAs<int>("Day"))
```

We can now access individual columns and calculate statistics over the
first level (individual months) using functions prefixed with `level`:

```fsharp
byMonth?Ozone |> Stats.levelMean fst
```

```
val it: Series<string,float> =
  
May       -> 22.92              
June      -> 29.444444444444443 
July      -> 59.11538461538461  
August    -> 59.96153846153846  
September -> 31.448275862068964
```

```fsharp
byMonth
|> Frame.sliceCols ["Ozone";"Solar.R";"Wind";"Temp"]
|> Frame.getNumericCols
|> Series.mapValues (Stats.levelMean fst)
|> Frame.ofRows
```

```
val it: Frame<string,string> =
  
           May                June               July               August             September          
Ozone   -> 22.92              29.444444444444443 59.11538461538461  59.96153846153846  31.448275862068964 
Solar.R -> 181.2962962962963  190.16666666666666 216.48387096774192 171.85714285714286 167.43333333333334 
Wind    -> 11.622580645161287 10.266666666666667 8.941935483870967  8.793548387096777  10.180000000000001 
Temp    -> 65.54838709677419  79.1               83.90322580645162  83.96774193548387  76.9
```

<a name="advanced"></a>
## Advanced statistics with Deedle.MathNetNumerics

The `Deedle.MathNetNumerics` package extends Deedle's statistical capabilities using
[MathNet.Numerics](https://numerics.mathnetchr.net). It adds:

* **Correlation and covariance matrices** — `Stats.corr`, `Stats.cov` for full-frame
Pearson or Spearman correlation and covariance.

* **Exponentially weighted moving statistics** — `Stats.ewmMean`, `Finance.ewmVolStdDev`,
`Finance.ewmCov` etc. for time-decayed calculations common in quantitative finance.

* **Quantiles and ranks** — `Stats.quantile`, `Stats.ranks` backed by MathNet's exact
algorithms.

* **Linear algebra** — transpose, inverse, decompositions (LU, QR, SVD, Cholesky, Eigen)
directly on `Frame<'R,'C>` values.

* **PCA** — principal component analysis via `PCA.pca`.

* **Linear regression** — ordinary least squares via `LinearRegression.ols`.

Install with:

```fsharp
dotnet add package Deedle.MathNetNumerics

```

See the [Deedle.MathNetNumerics documentation](math.html) for detailed examples.
