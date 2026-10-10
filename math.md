# Deedle.MathNetNumerics — MathNet.Numerics integration

`Deedle.MathNetNumerics` is a separate NuGet package that extends Deedle with linear algebra,
advanced statistics, PCA, and financial time-series functions via the
[MathNet.Numerics](https://numerics.mathdotnet.com/) library.

## Installation

```fsharp
dotnet add package Deedle.MathNetNumerics

```

Then reference in F# script or notebook:

#r "nuget: Deedle"
#r "nuget: Deedle.MathNetNumerics"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"

open Deedle
open Deedle.MathNetNumerics

-----------------------

<a name="matrix"></a>
## Frame and Series ↔ Matrix conversions

`Deedle.MathNetNumerics` adds a `Frame` type alias and a `Series` type alias in the `Deedle.MathNetNumerics`
namespace that provide `toMatrix` / `ofMatrix` and `toVector` / `ofVector` helpers.
Opening `Deedle.MathNetNumerics` after `Deedle` makes these available without qualifying.

```fsharp
// Build a simple 3×3 frame
let df =
  frame [ "A" => series [ 1 => 1.0; 2 => 4.0; 3 => 7.0 ]
          "B" => series [ 1 => 2.0; 2 => 5.0; 3 => 8.0 ]
          "C" => series [ 1 => 3.0; 2 => 6.0; 3 => 9.0 ] ]

// Convert frame to a MathNet DenseMatrix
let m : Matrix<float> = Frame.toMatrix df
m
```

```
val df: Frame<int,string> = 
     A B C 
1 -> 1 2 3 
2 -> 4 5 6 
3 -> 7 8 9 

val m: Matrix<float> = DenseMatrix 3x3-Double
1  2  3
4  5  6
7  8  9

val it: Matrix<float> = DenseMatrix 3x3-Double
1  2  3
4  5  6
7  8  9
```

```fsharp
// Convert matrix back to a frame with named rows and columns
Frame.ofMatrix [1;2;3] ["A";"B";"C"] m
```

```
val it: Frame<int,string> = 
     A B C 
1 -> 1 2 3 
2 -> 4 5 6 
3 -> 7 8 9
```

Series ↔ Vector works the same way:

```fsharp
let s = series [ "x" => 1.0; "y" => 2.0; "z" => 3.0 ]
let v : Vector<float> = Series.toVector s
v
```

```
val s: Series<string,float> = 
x -> 1 
y -> 2 
z -> 3 

val v: Vector<float>
val it: Vector<float>
```

```fsharp
Series.ofVector ["x";"y";"z"] v
```

```
val it: Series<string,float> = 
x -> 1 
y -> 2 
z -> 3
```

There is also a `Matrix` type with explicit `ofFrame` / `toFrame` helpers and dot-product
overloads that let you multiply frames, series, and vectors directly:

```fsharp
// Frame × Frame matrix multiply (column keys of left must equal row keys of right)
Matrix.dot df df

// Frame × Vector
Matrix.dot df v

// Series (as row vector) × Frame
Matrix.dot s df

```

-----------------------

<a name="linalg"></a>
## Linear algebra on frames

`LinearAlgebra` provides matrix operations that accept and return `Frame<'R,'C>` values
directly. All operations convert to/from `Matrix<float>` internally.

```fsharp
// Transpose (faster than generic Frame.transpose for numeric frames)
LinearAlgebra.transpose df
```

```
val it: Frame<string,int> = 
     1 2 3 
A -> 1 4 7 
B -> 2 5 8 
C -> 3 6 9
```

```fsharp
// Matrix inverse
let sq = frame [ "A" => series [1=>4.0;2=>7.0]; "B" => series [1=>3.0;2=>6.0] ]
LinearAlgebra.inverse sq
```

```
val sq: Frame<int,string> = 
     A B 
1 -> 4 3 
2 -> 7 6 

val it: Matrix<float> =
  DenseMatrix 2x2-Double
       2       -1
-2.33333  1.33333
```

Other available operations:

Function | Description
--- | ---
`LinearAlgebra.pseudoInverse df` | Moore–Penrose pseudo-inverse
`LinearAlgebra.determinant df` | Scalar determinant
`LinearAlgebra.trace df` | Scalar trace
`LinearAlgebra.rank df` | Integer rank
`LinearAlgebra.norm df` | Frobenius norm (float)
`LinearAlgebra.normRows df` | Vector of row norms
`LinearAlgebra.normCols df` | Vector of column norms
`LinearAlgebra.condition df` | Condition number
`LinearAlgebra.nullity df` | Nullity
`LinearAlgebra.kernel df` | Kernel (null space)
`LinearAlgebra.isSymmetric df` | Boolean symmetry test
`LinearAlgebra.cholesky df` | Cholesky decomposition
`LinearAlgebra.lu df` | LU decomposition
`LinearAlgebra.qr df` | QR decomposition
`LinearAlgebra.svd df` | SVD decomposition
`LinearAlgebra.eigen df` | Eigenvalues and eigenvectors


-----------------------

<a name="stats"></a>
## Descriptive statistics

`Deedle.MathNetNumerics.Stats` extends the base `Deedle.Stats` with richer descriptive statistics
from MathNet.Numerics.

```fsharp
let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone |> Series.dropMissing

// Median (uses MathNet's exact median algorithm)
Stats.median ozone
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
  
1   -> 36  
2   -> 12  
3   -> 18  
5   -> 28  
6   -> 23  
7   -> 19  
8   -> 8   
10  -> 7   
11  -> 16  
12  -> 11  
13  -> 14  
14  -> 18  
15  -> 14  
16  -> 34  
17  -> 6   
... -> ... 
137 -> 13  
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
150 -> 14  
151 -> 18  
152 -> 20  

val it: float = 31.0
```

```fsharp
// 25th and 75th percentile
Stats.quantile(ozone, 0.25), Stats.quantile(ozone, 0.75)
```

```
val it: float * float = (18.0, 63.5)
```

```fsharp
// Ranks (average rank for ties by default)
ozone |> Stats.ranks |> Series.take 6
```

```
val it: Series<int,float> =
  
1 -> 65.5 
2 -> 15.5 
3 -> 30.5 
5 -> 53   
6 -> 45.5 
7 -> 33
```

All three functions also work on entire frames:

```fsharp
// Median of each numeric column
Stats.median air

// 90th percentile of each numeric column
Stats.quantile(air, 0.90)

```

-----------------------

<a name="corr"></a>
## Correlation and covariance

```fsharp
// Use a small subset of air quality numeric columns
let numAir = air |> Frame.sliceCols ["Ozone";"Solar.R";"Wind";"Temp"] |> Frame.dropSparseRows

// Pearson correlation matrix (default)
Stats.corr numAir
```

```
val numAir: Frame<int,string> =
  
       Ozone Solar.R Wind Temp 
1   -> 36    118     8    72   
2   -> 12    149     12.6 74   
3   -> 18    313     11.5 62   
6   -> 23    299     8.6  65   
7   -> 19    99      13.8 59   
8   -> 8     19      20.1 61   
11  -> 16    256     9.7  69   
12  -> 11    290     9.2  66   
13  -> 14    274     10.9 68   
14  -> 18    65      13.2 58   
15  -> 14    334     11.5 64   
16  -> 34    307     12   66   
17  -> 6     78      18.4 57   
18  -> 30    322     11.5 68   
19  -> 11    44      9.7  62   
:      ...   ...     ...  ...  
137 -> 13    112     11.5 71   
138 -> 46    237     6.9  78   
139 -> 18    224     13.8 67   
140 -> 13    27      10.3 76   
141 -> 24    238     10.3 68   
142 -> 16    201     8    82   
143 -> 13    238     12.6 64   
144 -> 23    14      9.2  71   
145 -> 36    139     10.3 81   
146 -> 7     49      10.3 69   
147 -> 14    20      16.6 63   
148 -> 30    193     6.9  70   
150 -> 14    191     14.3 75   
151 -> 18    131     8    76   
152 -> 20    223     11.5 68   

val it: Frame<string,string> =
  
           Ozone               Solar.R              Wind                 Temp                
Ozone   -> 1                   0.34836591218820606  -0.6141530630103393  0.7023458156557731  
Solar.R -> 0.34836591218820606 1                    -0.12710934002770172 0.2964335602155499  
Wind    -> -0.6141530630103393 -0.12710934002770172 1                    -0.5087914965402979 
Temp    -> 0.7023458156557731  0.2964335602155499   -0.5087914965402979  1
```

```fsharp
// Spearman rank correlation
Stats.corr(numAir, CorrelationMethod.Spearman)
```

```
val it: Frame<string,string> =
  
           Ozone               Solar.R              Wind                 Temp                
Ozone   -> 1                   0.35053386000742803  -0.6027870894327034  0.7827104662215925  
Solar.R -> 0.35053386000742803 1                    -0.06377856427131096 0.20782359283634763 
Wind    -> -0.6027870894327034 -0.06377856427131096 1                    -0.5129716950336768 
Temp    -> 0.7827104662215925  0.20782359283634763  -0.5129716950336768  1
```

```fsharp
// Covariance frame
Stats.cov numAir
```

```
val it: Frame<string,string> =
  
           Ozone              Solar.R            Wind               Temp               
Ozone   -> 1117.4375312760633 1066.3297748123437 -73.20231859883299 223.44320266889065 
Solar.R -> 1066.3297748123437 8384.719015846538  -41.50097581317802 258.3307756463719  
Wind    -> -73.20231859883299 -41.50097581317802 12.71373561301105  -17.26557130942466 
Temp    -> 223.44320266889068 258.3307756463719  -17.26557130942466 90.57514595496245
```

To correlate two individual series:

```fsharp
Stats.corr(air?Ozone, air?Temp)

```

### Converting between correlation and covariance

`Stats.cov2Corr` decomposes a covariance matrix into a standard-deviation series and a
correlation frame; `Stats.corr2Cov` inverts that operation:

```fsharp
let stdDevs, corrFrame = Stats.cov2Corr (Stats.cov numAir)
let recoveredCov       = Stats.corr2Cov(stdDevs, corrFrame)

```

-----------------------

<a name="ewm"></a>
## Exponentially weighted moving statistics

The `Stats` and `Finance` types provide a full suite of exponentially weighted moving
(EWM) statistics. The decay rate can be specified via one of four mutually exclusive
parameters:

Parameter | Meaning
--- | ---
`com` | Center of mass — α = 1 / (1 + com), com ≥ 0
`span` | Span — α = 2 / (span + 1), span ≥ 1
`halfLife` | Half-life — α = 1 − exp(ln(0.5)/halfLife), halfLife &gt; 0
`alpha` | Direct smoothing factor — 0 &lt; α ≤ 1


```fsharp
// Sample daily returns series
let returns =
  series [ for i in 1..20 -> i => Math.Sin(float i * 0.3) * 0.02 ]

// EWM mean with span=5
Stats.ewmMean(returns, span=5.0)
```

```
val returns: Series<int,float> =
  
1  -> 0.005910404133226791   
2  -> 0.011292849467900708   
3  -> 0.015666538192549668   
4  -> 0.018640781719344527   
5  -> 0.01994989973208109    
6  -> 0.019476952617563905   
7  -> 0.017264187332977476   
8  -> 0.01350926361102302    
9  -> 0.008547597604676603   
10 -> 0.002822400161197344   
11 -> -0.0031549138828649644 
12 -> -0.008850408865897042  
13 -> -0.013755323183679476  
14 -> -0.017431515448271765  
15 -> -0.01955060235330194   
16 -> -0.019923292176716813  
17 -> -0.01851629364655465   
18 -> -0.015455289751119754  
19 -> -0.011013710851952753  
20 -> -0.005588309963978517  

val it: Series<int,float> =
  
1  -> 0.005910404133226791  
2  -> 0.007704552578118098  
3  -> 0.01035854778292862   
4  -> 0.01311929242840059   
5  -> 0.015396161529627424  
6  -> 0.01675642522560625   
7  -> 0.01692567926139666   
8  -> 0.015786874044605447  
9  -> 0.013373781897962499  
10 -> 0.009856654652374114  
11 -> 0.005519465140627755  
12 -> 0.0007295071384528238 
13 -> -0.004098769635591276 
14 -> -0.008543018239818106 
15 -> -0.012212212944312718 
16 -> -0.014782572688447418 
17 -> -0.01602714634114983  
18 -> -0.01583652747780647  
19 -> -0.014228921935855232 
20 -> -0.011348717945229661
```

```fsharp
// EWM mean on a whole frame (applied column by column)
Stats.ewmMean(numAir, span=10.0)
```

```
val it: Frame<int,string> =
  
       Ozone              Solar.R            Wind               Temp               
1   -> 36                 118                8                  72                 
2   -> 31.636363636363633 123.63636363636363 8.836363636363636  72.36363636363636  
3   -> 29.157024793388427 158.06611570247932 9.320661157024793  70.47933884297521  
6   -> 28.037565740045075 183.69045830202853 9.189631855747557  69.48309541697971  
7   -> 26.394371969127786 168.29219315620514 10.027880609248001 67.57707806843794  
8   -> 23.04994070201364  141.14815803689513 11.859175043930183 66.3812456923583   
11  -> 21.768133301647524 162.03031112109602 11.466597763215603 66.85738283920224  
12  -> 19.810290883166154 185.29752728089673 11.054489078994584 66.70149505025637  
13  -> 18.753874358954125 201.42524959346093 11.026400155541022 66.93758685930067  
14  -> 18.616806293689738 176.6206587582862  11.421600127260836 65.31257106670054  
15  -> 17.77738696756433  205.23508443859777 11.435854649577045 65.0739217818459   
16  -> 20.72695297346172  223.7377963588527  11.538426531472128 65.24229963969209  
17  -> 18.04932516010504  197.24001520269766 12.78598534393174  63.743699705202616 
18  -> 20.222175130995033 219.92364880220714 12.552169826853241 64.51757248607487  
19  -> 18.545416016268664 187.93753083816947 12.033593494698106 64.05983203406126  
:      ...                ...                ...                ...                
137 -> 28.35087017123703  172.93674754861382 10.62939356863658  76.97098060271549  
138 -> 31.55980286737575  184.58461163068404 9.951322010702656  77.1580750385854   
139 -> 29.09438416421652  191.75104587965055 10.651081645120355 75.31115230429714  
140 -> 26.168132497995334 161.79631026516861 10.587248618734835 75.43639733987948  
141 -> 25.773926589268907 175.6515265805925  10.535021597146683 74.08432509626502  
142 -> 23.99684902758365  180.26033992957565 10.07410857948365  75.52353871512592  
143 -> 21.99742193165935  190.75845994238009 10.533361565032076 73.4283498578303   
144 -> 22.179708853175832 158.62055813467458 10.290932189571697 72.98683170186115  
145 -> 24.69248906168932  155.05318392837012 10.292580882376843 74.44377139243184  
146 -> 21.475672868654897 135.77078685048463 10.29392981285378  73.45399477562604  
147 -> 20.116459619808552 114.72155287766924 11.440488028698546 71.55326845278495  
148 -> 21.91346696166154  128.9539978090021  10.614944750753354 71.27085600682405  
150 -> 20.47465478681399  140.23508911645624 11.284954796070926 71.9488821874015   
151 -> 20.024717552847807 138.55598200437328 10.687690287694393 72.6854490624194   
152 -> 20.020223452330022 153.90943982175995 10.835382962659047 71.83354923288859
```

### Moving statistics on frames

`Stats.movingStdDevParallel`, `Stats.movingVarianceParallel`, and
`Stats.movingCovarianceParallel` compute rolling window standard deviation, variance,
and covariance matrices over a frame using parallel evaluation:

```fsharp
// Rolling 10-day standard deviation of each column
let rollingStd = Stats.movingStdDevParallel 10 numAir

// Rolling 10-day covariance matrix (returns Series<rowKey, Matrix<float>>)
let rollingCov = Stats.movingCovarianceParallel 10 numAir

```

-----------------------

<a name="finance"></a>
## Financial time-series: EWM volatility and covariance

`Finance` (in `Deedle.MathNetNumerics`) provides exponentially weighted volatility and covariance
functions that are common in quantitative finance.

```fsharp
let prices =
  series [ for i in 1..30 -> i => 100.0 * Math.Exp(Math.Sin(float i * 0.2) * 0.1) ]

let dailyReturns = prices.Diff(1) / prices.Shift(1)

// Mean-corrected EWM volatility (standard deviation form) with half-life of 10 days
Finance.ewmVolStdDev(dailyReturns, halfLife=10.0)
```

```
val prices: Series<int,float> =
  
1  -> 102.00655940080074 
2  -> 103.97100063591236 
3  -> 105.80887846348735 
4  -> 107.43712525166751 
5  -> 108.77888941343824 
6  -> 109.76855399675529 
7  -> 110.35640338624115 
8  -> 110.51237949285047 
9  -> 110.22844103923283 
10 -> 109.51920572994001 
11 -> 108.420786323006   
12 -> 106.98798132024343 
13 -> 105.29019745068742 
14 -> 103.40662183732053 
15 -> 101.4212045143998  
16 -> 99.41795902566265  
17 -> 97.47696325563075  
18 -> 95.67127930478267  
19 -> 94.06484613148905  
20 -> 92.71126038603612  
21 -> 91.65326595654231  
22 -> 90.92272578743619  
23 -> 90.54084597502928  
24 -> 90.51845251400941  
25 -> 90.85617471011706  
26 -> 91.54445675083369  
27 -> 92.56339278854479  
28 -> 93.88245510845117  
29 -> 95.46025358686424  
30 -> 97.24452047374984  

val dailyReturns: Series<int,float> =
  
2  -> 0.019257989355302194   
3  -> 0.017676831196526625   
4  -> 0.01538856485225898    
5  -> 0.012488831571281276   
6  -> 0.009097947116885996   
7  -> 0.005355353314604513   
8  -> 0.0014133851940010078  
9  -> -0.0025692909239729655 
10 -> -0.0064342315159876835 
11 -> -0.010029468344050737  
12 -> -0.01321522423286958   
13 -> -0.01586892143028759   
14 -> -0.017889372980320013  
15 -> -0.019200098481547793  
16 -> -0.019751742235054295  
17 -> -0.019523593011307714  
18 -> -0.01852421218860419   
19 -> -0.016791174791088187  
20 -> -0.01438992143314427   
21 -> -0.01141171444642721   
22 -> -0.007970694349861014  
23 -> -0.004200047997897531  
24 -> -0.0002473299291465638 
25 -> 0.003730976245482984   
26 -> 0.0075755119881795695  
27 -> 0.011130505045045458   
28 -> 0.014250367020574686   
29 -> 0.016806105854288007   
30 -> 0.018691202043183416   

val it: Series<int,float> =
  
2  -> 0.013664333631420325 
3  -> 0.013205217009834477 
4  -> 0.012792526547233599 
5  -> 0.012467649357350036 
6  -> 0.012284777733702706 
7  -> 0.012298472040201569 
8  -> 0.012546187598639522 
9  -> 0.013033195668680301 
10 -> 0.013728112366462469 
11 -> 0.014570624960344116 
12 -> 0.015485400930932995 
13 -> 0.016395099994245498 
14 -> 0.01722929430909024  
15 -> 0.01792954535585723  
16 -> 0.018452016095533576 
17 -> 0.018768775418971387 
18 -> 0.01886844414807086  
19 -> 0.018756439444258043 
20 -> 0.01845481680718146  
21 -> 0.018001503423977678 
22 -> 0.017448491542290185 
23 -> 0.016858308794847145 
24 -> 0.01629794423197617  
25 -> 0.015829762026497888 
26 -> 0.015500257687858511 
27 -> 0.015329668408967484 
28 -> 0.015306847494267814 
29 -> 0.015392334479554334 
30 -> 0.015528537839504815
```

`Finance.ewmVolRMS` computes the same quantity using root-mean-square (no mean correction),
which is appropriate for already-centred return series:

```fsharp
Finance.ewmVolRMS(dailyReturns, span=20.0)

```

**Note**: the older `Finance.ewmVol` is deprecated. Use `ewmVolStdDev` or `ewmVolRMS`
depending on whether you want mean correction.

### EWM variance

```fsharp
// Scalar EWM variance per time step
Finance.ewmVar(dailyReturns, com=5.0)

```

### EWM covariance and correlation on frames

```fsharp
// Returns Series<rowKey, Frame<colKey,colKey>> — one covariance frame per row
let ewmCovFrames = Finance.ewmCov(numAir, span=20.0)

// Returns Series<rowKey, Frame<colKey,colKey>> — one correlation frame per row
let ewmCorrFrames = Finance.ewmCorr(numAir, span=20.0)

```

-----------------------

<a name="pca"></a>
## Principal Component Analysis (PCA)

The `PCA` module provides a simple API for principal component analysis. It normalises
the columns by z-score internally and returns a record containing the eigen values and
eigen vectors in descending order of explained variance.

```fsharp
// Use the numeric air quality columns
let normed = PCA.normalizeColumns numAir

let result = PCA.pca numAir
// Eigen values (proportion of variance explained by each PC)
result.EigenValues
```

```
val normed: Frame<int,string> =
  
       Ozone                Solar.R              Wind                 Temp                 
1   -> -0.18275318770542529 -0.7290152048765213  -0.5504569624217873  -0.6189816857657777  
2   -> -0.9007121394053104  -0.39046939953416293 0.73963670587569     -0.4088335825736927  
3   -> -0.7212224014803391  1.400547119051217    0.4311360460654237   -1.669722201726203   
6   -> -0.5716476198761964  1.247655465025636    -0.3821838752525512  -1.3545000469380752  
7   -> -0.6913074451595106  -0.9365110210540958  1.0761828802141626   -1.9849443565143305  
8   -> -1.0203719646886245  -1.8101776154859885  2.8430502954911425   -1.7747962533222454  
11  -> -0.7810523141219962  0.7780596705184936   -0.073683215442285   -0.9342038405539053  
12  -> -0.9306270957261389  1.1493679731520479   -0.21391078808331512 -1.2494259953420328  
13  -> -0.8408822267636533  0.9746346542656694   0.26286295889618766  -1.0392778921499477  
14  -> -0.7212224014803391  -1.3078193236876503  0.907909793044926    -2.0900184081103728  
15  -> -0.8408822267636533  1.629884600089589    0.4311360460654237   -1.459574098534118   
16  -> -0.2425831003470824  1.3350221244688252   0.5713636187064538   -1.2494259953420328  
17  -> -1.0802018773302817  -1.1658485020924676  2.3662765485116393   -2.1950924597064154  
18  -> -0.36224292563039656 1.498834610924805    0.4311360460654237   -1.0392778921499477  
19  -> -0.9306270957261389  -1.537156804726022   -0.073683215442285   -1.669722201726203   
:      ...                  ...                  ...                  ...                  
137 -> -0.8707971830844818  -0.7945401994589133  0.4311360460654237   -0.7240557373618203  
138 -> 0.11639637550286015  0.570563854340919    -0.8589576222320535  0.011462623810477393 
139 -> -0.7212224014803391  0.4285930327457365   1.0761828802141626   -1.1443519437459904  
140 -> -0.8707971830844818  -1.7228109560427993  0.09458987172695157  -0.19868547938160763 
141 -> -0.5417326635553679  0.5814846867713177   0.09458987172695157  -1.0392778921499477  
142 -> -0.7810523141219962  0.17741388684656734  -0.5504569624217873  0.43175883019464745  
143 -> -0.8707971830844818  0.5814846867713177   0.73963670587569     -1.459574098534118   
144 -> -0.5716476198761964  -1.8647817776379818  -0.21391078808331512 -0.7240557373618203  
145 -> -0.18275318770542529 -0.4996777238381495  0.09458987172695157  0.32668477859860495  
146 -> -1.050286921009453   -1.4825526425740287  0.09458987172695157  -0.9342038405539053  
147 -> -0.8408822267636533  -1.79925678305559    1.8614572870039316   -1.5646481501301603  
148 -> -0.36224292563039656 0.09004722740337807  -0.8589576222320535  -0.8291297889578627  
150 -> -0.8408822267636533  0.06820556254258076  1.2164104528551927   -0.3037595309776502  
151 -> -0.7212224014803391  -0.5870443832813388  -0.5504569624217873  -0.19868547938160763 
152 -> -0.661392488838682   0.41767220031533786  0.4311360460654237   -1.0392778921499477  

val result: PCA.t<string> =
  { EigenVectors =
     
           PC1                   PC2                 PC3                 PC4                  
Ozone   -> -0.14302573845191544  -0.96711437980886   0.20286001687822686 0.05550881064636915  
Solar.R -> -0.9891158377073584   0.14700234921163152 0.00463025073308681 -0.00432778244380515 
Wind    -> 0.0061047618879213165 0.06855028063392826 0.05859960622971001 0.9959064599936202   
Temp    -> -0.03400337658302009  -0.1959105528505185 -0.9774417939089967 0.07120649677325888  

    EigenValues =
     
PC1 -> 8548.046777408817  
PC2 -> 1005.8064115893684 
PC3 -> 44.012708445536944 
PC4 -> 7.5795312468514755 
 }
val it: Series<string,float> =
  
PC1 -> 8548.046777408817  
PC2 -> 1005.8064115893684 
PC3 -> 44.012708445536944 
PC4 -> 7.5795312468514755
```

```fsharp
// Eigen vectors (loadings): rows = original variables, columns = PC1, PC2, …
result.EigenVectors
```

```
val it: Frame<string,string> =
  
           PC1                   PC2                 PC3                 PC4                  
Ozone   -> -0.14302573845191544  -0.96711437980886   0.20286001687822686 0.05550881064636915  
Solar.R -> -0.9891158377073584   0.14700234921163152 0.00463025073308681 -0.00432778244380515 
Wind    -> 0.0061047618879213165 0.06855028063392826 0.05859960622971001 0.9959064599936202   
Temp    -> -0.03400337658302009  -0.1959105528505185 -0.9774417939089967 0.07120649677325888
```

Access the fields via the helper functions `PCA.eigenValues` and `PCA.eigenVectors`:

```fsharp
let ev = PCA.eigenValues  result   // Series<string, float>
let vecs = PCA.eigenVectors result  // Frame<colKey, string>

```

-----------------------

<a name="regression"></a>
## Linear regression

`LinearRegression.ols` fits an ordinary-least-squares model from columns in a frame:

```fsharp
open Deedle.MathNetNumerics

// Fit: Ozone ~ Solar.R + Wind + Temp (with intercept)
let fit = LinearRegression.ols ["Solar.R"; "Wind"; "Temp"] "Ozone" true numAir

```

The returned `Fit.t` record provides:

```fsharp
let fit = LinearRegression.ols ["Solar.R"; "Wind"; "Temp"] "Ozone" true numAir

// Regression coefficients (Intercept, Solar.R, Wind, Temp)
LinearRegression.Fit.coefficients fit
```

```
val fit: LinearRegression.Fit.t<int> =
  { InputFrame =
     
       Ozone Solar.R Wind Temp 
1   -> 36    118     8    72   
2   -> 12    149     12.6 74   
3   -> 18    313     11.5 62   
6   -> 23    299     8.6  65   
7   -> 19    99      13.8 59   
8   -> 8     19      20.1 61   
11  -> 16    256     9.7  69   
12  -> 11    290     9.2  66   
13  -> 14    274     10.9 68   
14  -> 18    65      13.2 58   
15  -> 14    334     11.5 64   
16  -> 34    307     12   66   
17  -> 6     78      18.4 57   
18  -> 30    322     11.5 68   
19  -> 11    44      9.7  62   
:      ...   ...     ...  ...  
137 -> 13    112     11.5 71   
138 -> 46    237     6.9  78   
139 -> 18    224     13.8 67   
140 -> 13    27      10.3 76   
141 -> 24    238     10.3 68   
142 -> 16    201     8    82   
143 -> 13    238     12.6 64   
144 -> 23    14      9.2  71   
145 -> 36    139     10.3 81   
146 -> 7     49      10.3 69   
147 -> 14    20      16.6 63   
148 -> 30    193     6.9  70   
150 -> 14    191     14.3 75   
151 -> 18    131     8    76   
152 -> 20    223     11.5 68   

    Coefficients =
     
Intercept -> -66.01138344691108  
Solar.R   -> 0.05943745007818751 
Wind      -> -3.2973494133331394 
Temp      -> 1.6688690596747198  

    FitIntercept = true
    yKey = "Ozone" }
val it: Series<string,float> =
  
Intercept -> -66.01138344691108  
Solar.R   -> 0.05943745007818751 
Wind      -> -3.2973494133331394 
Temp      -> 1.6688690596747198
```

```fsharp
// Fitted values (ŷ)
LinearRegression.Fit.fittedValues fit |> Series.take 6
```

```
val it: Series<int,float> =
  
1 -> 34.78201265222975   
2 -> 24.794504422670556  
3 -> 18.142901874063128  
6 -> 31.879698050658774  
7 -> -7.167223272359379  
8 -> -29.357782463263717
```

```fsharp
// Residuals (y − ŷ)
LinearRegression.Fit.residuals fit |> Series.take 6
```

```
val it: Series<int,float> =
  
1 -> 1.2179873477702472   
2 -> -12.794504422670556  
3 -> -0.14290187406312782 
6 -> -8.879698050658774   
7 -> 26.16722327235938    
8 -> 37.35778246326372
```

For a full summary including the t-table and R²:

```fsharp
let summary = LinearRegression.Fit.summary fit
printfn "%O" summary
// Formula: Ozone ~ Solar.R + Wind + Temp
//              Min:   1Q:   Median:   3Q   Max:
// ...
// R^2: 0.606, Adj. R^2: 0.596

```

To fit without an intercept pass `false` as the third argument to `ols`.

-----------------------

<a name="tips"></a>
## Tips and common patterns

### Working with the full pipeline

A typical quantitative pipeline combines Deedle frame operations with `Deedle.MathNetNumerics`:

```fsharp
open Deedle
open Deedle.MathNetNumerics

// 1. Load data
let prices = Frame.ReadCsv("prices.csv") |> Frame.indexRowsDate "Date"

// 2. Compute daily log-returns
let logReturns = log prices - log (Frame.shift 1 prices) |> Frame.dropSparseRows

// 3. Rolling 60-day correlation matrix (one frame per row)
let rollingCorr = Finance.ewmCorr(logReturns, span=60.0)

// 4. Latest correlation frame
let latestCorr = rollingCorr |> Series.lastValue

```

### Missing values

`LinearRegression.ols` will raise an error if any input column has missing values.
Use `Frame.dropSparseRows` or `Frame.fillMissingWith` to clean data first:

```fsharp
let cleanDf = numAir |> Frame.dropSparseRows
let fit = LinearRegression.ols ["Solar.R";"Wind";"Temp"] "Ozone" true cleanDf

```

Similarly, `Stats.corrMatrix` / `Stats.corr` treat `NaN` as 0 in the covariance step
(matching MATLAB semantics). Use `Frame.dropSparseRows` if you want listwise deletion.

### Performance note

`LinearAlgebra.transpose` on a purely numeric frame is significantly faster than
`Frame.transpose` because it bypasses the generic object boxing layer and works
directly in `float[]` space.
