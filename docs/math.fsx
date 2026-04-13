(**
---
title: Deedle.Math — MathNet.Numerics integration
category: Integrations
categoryindex: 2
index: 4
description: Linear algebra, matrix conversions, and numerical methods via MathNet.Numerics integration
keywords: MathNet, linear algebra, matrix, numerics, regression, integration
---
*)
(*** condition: prepare ***)
#nowarn "211"
#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Math.dll"
#r "nuget: MathNet.Numerics, 5.0.0"
#r "nuget: MathNet.Numerics.FSharp, 5.0.0"
(*** condition: fsx ***)
#if FSX
#r "nuget: Deedle,{{fsdocs-package-version}}"
#r "nuget: Deedle.Math,{{fsdocs-package-version}}"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"
#endif // FSX
(*** condition: prepare ***)

open System
open Deedle
open Deedle.Math
open MathNet.Numerics.LinearAlgebra

fsi.AddPrinter(fun (o: obj) ->
  let iface = o.GetType().GetInterface("IFsiFormattable")
  if iface <> null then
    let fmt = iface.GetMethod("Format")
    fmt.Invoke(o, [||]) :?> string
  else null)

let root = __SOURCE_DIRECTORY__ + "/data/"

(**

# Deedle.Math — MathNet.Numerics integration

`Deedle.Math` is a separate NuGet package that extends Deedle with linear algebra,
advanced statistics, PCA, and financial time-series functions via the
[MathNet.Numerics](https://numerics.mathnetchr.net) library.

## Installation

```
dotnet add package Deedle.Math
```

Then reference in F# script or notebook:

```fsx
#r "nuget: Deedle"
#r "nuget: Deedle.Math"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"

open Deedle
open Deedle.Math
```

---

<a name="matrix"></a>

## Frame and Series ↔ Matrix conversions

`Deedle.Math` adds a `Frame` type alias and a `Series` type alias in the `Deedle.Math`
namespace that provide `toMatrix` / `ofMatrix` and `toVector` / `ofVector` helpers.
Opening `Deedle.Math` after `Deedle` makes these available without qualifying.

*)

// Build a simple 3×3 frame
let df =
  frame [ "A" => series [ 1 => 1.0; 2 => 4.0; 3 => 7.0 ]
          "B" => series [ 1 => 2.0; 2 => 5.0; 3 => 8.0 ]
          "C" => series [ 1 => 3.0; 2 => 6.0; 3 => 9.0 ] ]

// Convert frame to a MathNet DenseMatrix
let m : Matrix<float> = Frame.toMatrix df
m
(*** include-it ***)

// Convert matrix back to a frame with named rows and columns
Frame.ofMatrix [1;2;3] ["A";"B";"C"] m
(*** include-it ***)

(**
Series ↔ Vector works the same way:
*)

let s = series [ "x" => 1.0; "y" => 2.0; "z" => 3.0 ]
let v : Vector<float> = Series.toVector s
v
(*** include-it ***)

Series.ofVector ["x";"y";"z"] v
(*** include-it ***)

(**

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

---

<a name="linalg"></a>

## Linear algebra on frames

`LinearAlgebra` provides matrix operations that accept and return `Frame<'R,'C>` values
directly. All operations convert to/from `Matrix<float>` internally.

*)

// Transpose (faster than generic Frame.transpose for numeric frames)
LinearAlgebra.transpose df
(*** include-it ***)

// Matrix inverse
let sq = frame [ "A" => series [1=>4.0;2=>7.0]; "B" => series [1=>3.0;2=>6.0] ]
LinearAlgebra.inverse sq
(*** include-it ***)

(**
Other available operations:

| Function | Description |
|---|---|
| `LinearAlgebra.pseudoInverse df` | Moore–Penrose pseudo-inverse |
| `LinearAlgebra.determinant df` | Scalar determinant |
| `LinearAlgebra.trace df` | Scalar trace |
| `LinearAlgebra.rank df` | Integer rank |
| `LinearAlgebra.norm df` | Frobenius norm (float) |
| `LinearAlgebra.normRows df` | Vector of row norms |
| `LinearAlgebra.normCols df` | Vector of column norms |
| `LinearAlgebra.condition df` | Condition number |
| `LinearAlgebra.nullity df` | Nullity |
| `LinearAlgebra.kernel df` | Kernel (null space) |
| `LinearAlgebra.isSymmetric df` | Boolean symmetry test |
| `LinearAlgebra.cholesky df` | Cholesky decomposition |
| `LinearAlgebra.lu df` | LU decomposition |
| `LinearAlgebra.qr df` | QR decomposition |
| `LinearAlgebra.svd df` | SVD decomposition |
| `LinearAlgebra.eigen df` | Eigenvalues and eigenvectors |

---

<a name="stats"></a>

## Descriptive statistics

`Deedle.Math.Stats` extends the base `Deedle.Stats` with richer descriptive statistics
from MathNet.Numerics.

*)

let air = Frame.ReadCsv(root + "airquality.csv", separators=";")
let ozone = air?Ozone |> Series.dropMissing

// Median (uses MathNet's exact median algorithm)
Stats.median ozone
(*** include-it ***)

// 25th and 75th percentile
Stats.quantile(ozone, 0.25), Stats.quantile(ozone, 0.75)
(*** include-it ***)

// Ranks (average rank for ties by default)
ozone |> Stats.ranks |> Series.take 6
(*** include-it ***)

(**
All three functions also work on entire frames:

```fsharp
// Median of each numeric column
Stats.median air

// 90th percentile of each numeric column
Stats.quantile(air, 0.90)
```

---

<a name="corr"></a>

## Correlation and covariance

*)

// Use a small subset of air quality numeric columns
let numAir = air |> Frame.sliceCols ["Ozone";"Solar.R";"Wind";"Temp"] |> Frame.dropSparseRows

// Pearson correlation matrix (default)
Stats.corr numAir
(*** include-it ***)

// Spearman rank correlation
Stats.corr(numAir, CorrelationMethod.Spearman)
(*** include-it ***)

// Covariance frame
Stats.cov numAir
(*** include-it ***)

(**
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

---

<a name="ewm"></a>

## Exponentially weighted moving statistics

The `Stats` and `Finance` types provide a full suite of exponentially weighted moving
(EWM) statistics. The decay rate can be specified via one of four mutually exclusive
parameters:

| Parameter | Meaning |
|---|---|
| `com` | Center of mass — α = 1 / (1 + com), com ≥ 0 |
| `span` | Span — α = 2 / (span + 1), span ≥ 1 |
| `halfLife` | Half-life — α = 1 − exp(ln(0.5)/halfLife), halfLife > 0 |
| `alpha` | Direct smoothing factor — 0 < α ≤ 1 |

*)

// Sample daily returns series
let returns =
  series [ for i in 1..20 -> i => Math.Sin(float i * 0.3) * 0.02 ]

// EWM mean with span=5
Stats.ewmMean(returns, span=5.0)
(*** include-it ***)

// EWM mean on a whole frame (applied column by column)
Stats.ewmMean(numAir, span=10.0)
(*** include-it ***)

(**

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

---

<a name="finance"></a>

## Financial time-series: EWM volatility and covariance

`Finance` (in `Deedle.Math`) provides exponentially weighted volatility and covariance
functions that are common in quantitative finance.

*)

let prices =
  series [ for i in 1..30 -> i => 100.0 * Math.Exp(Math.Sin(float i * 0.2) * 0.1) ]

let dailyReturns = prices.Diff(1) / prices.Shift(1)

// Mean-corrected EWM volatility (standard deviation form) with half-life of 10 days
Finance.ewmVolStdDev(dailyReturns, halfLife=10.0)
(*** include-it ***)

(**

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

---

<a name="pca"></a>

## Principal Component Analysis (PCA)

The `PCA` module provides a simple API for principal component analysis. It normalises
the columns by z-score internally and returns a record containing the eigen values and
eigen vectors in descending order of explained variance.

*)

// Use the numeric air quality columns
let normed = PCA.normalizeColumns numAir

let result = PCA.pca numAir
// Eigen values (proportion of variance explained by each PC)
result.EigenValues
(*** include-it ***)

// Eigen vectors (loadings): rows = original variables, columns = PC1, PC2, …
result.EigenVectors
(*** include-it ***)

(**

Access the fields via the helper functions `PCA.eigenValues` and `PCA.eigenVectors`:

```fsharp
let ev = PCA.eigenValues  result   // Series<string, float>
let vecs = PCA.eigenVectors result  // Frame<colKey, string>
```

---

<a name="regression"></a>

## Linear regression

`LinearRegression.ols` fits an ordinary-least-squares model from columns in a frame:

```fsharp
open Deedle.Math

// Fit: Ozone ~ Solar.R + Wind + Temp (with intercept)
let fit = LinearRegression.ols ["Solar.R"; "Wind"; "Temp"] "Ozone" true numAir
```

The returned `Fit.t` record provides:

*)

let fit = LinearRegression.ols ["Solar.R"; "Wind"; "Temp"] "Ozone" true numAir

// Regression coefficients (Intercept, Solar.R, Wind, Temp)
LinearRegression.Fit.coefficients fit
(*** include-it ***)

// Fitted values (ŷ)
LinearRegression.Fit.fittedValues fit |> Series.take 6
(*** include-it ***)

// Residuals (y − ŷ)
LinearRegression.Fit.residuals fit |> Series.take 6
(*** include-it ***)

(**

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

---

<a name="tips"></a>

## Tips and common patterns

### Working with the full pipeline

A typical quantitative pipeline combines Deedle frame operations with `Deedle.Math`:

```fsharp
open Deedle
open Deedle.Math

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

*)
