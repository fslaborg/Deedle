Deedle 
======
<img align="right" src="https://github.com/fslaborg/Deedle/raw/master/docs/files/images/logo.png" alt="Deedle" />

[![Discord](https://img.shields.io/discord/836161044501889064?color=purple&label=Join%20our%20Discord%21&logo=discord&logoColor=white)](https://discord.gg/tNxJkz9KaA)

Deedle is an easy to use library for data and time series manipulation and for scientific programming. It supports working with structured data frames, ordered and unordered data, as well as time series. Deedle is designed to work well for exploratory programming using F# and C# interactive console, but can be also used in efficient compiled .NET code.

The library implements a wide range of operations for data manipulation including advanced indexing and slicing, joining and aligning data, handling of missing values, grouping and aggregation, statistics and more.

Build
-------------
Install .Net SDK 3.0.100 or higher

Windows: Run *fake build*

Linux/Mac: Run *./fake.sh build*

[![Build Status](https://github.com/fslaborg/deedle/actions/workflows/push-master.yml/badge.svg)](https://github.com/fslaborg/Deedle/actions) 

[![Deedle Nuget](https://buildstats.info/nuget/Deedle)](https://www.nuget.org/packages/Deedle/) Deedle  
[![Deedle.Math Nuget](https://buildstats.info/nuget/Deedle.Math)](https://www.nuget.org/packages/Deedle.Math/) Deedle.Math

Documentation
-------------

More information can be found in the [documentation](http://fslab.org/Deedle/).

 * [Quick start tutorial](http://fslab.org/Deedle/tutorial.html) shows how to use the most important 
   features of Deedle. Start here for a 10 minute intro!
 * [Data frame features](http://fslab.org/Deedle/frame.html) provides more examples of using data frames including slicing, joining, grouping and aggregation.
 * [Time series features](http://fslab.org/Deedle/series.html) discusses data and time-series manipulation, such as sliding windows, sampling and statistics.
 * [Using Deedle from C#](http://fslab.org/Deedle/csharpintro.html) shows the idiomatic C# API for working with Deedle.

Automatically generated documentation for all types, modules and functions in the library 
is available in the [API Reference](http://fslab.org/Deedle/reference/index.html):

 * [`Series` module](http://fslab.org/Deedle/reference/deedle-seriesmodule.html) for working with data and time-series values
 * [`Frame` module](http://fslab.org/Deedle/reference/deedle-framemodule.html) for data frame manipulation
 * [`Stats` module](http://fslab.org/Deedle/reference/deedle-stats.html) for statistical functions, moving windows and a lot more.

More functions related to linear algebra, statistical analysis and financial analysis can be found in **Deedle.Math** extension. Deedle.Math has dependency on MathNet.Numerics.
 * [`LinearAlgebra` module](http://fslab.org/Deedle/reference/deedle-math-linearalgebra.html) provides linear algebra functions on frame.
 * [`Matrix` module](http://fslab.org/Deedle/reference/deedle-math-matrix.html) provides matrix multiplication between frame, series, matrix and vector. They are also available via type extensions.
 * [`Stats` module](http://fslab.org/Deedle/reference/deedle-math-stats.html) provides extra statistical functions on frame and series by applying existing functions in MathNet.Numerics.
 * [`Finance` module](http://fslab.org/Deedle/reference/deedle-math-finance.html) provides statistical functions specific to finance domain.  

Maintainers
-----------

* @kMutagene
* @zyzhu 

All fsprojects and fslaborg projects have @fsprojectsgit as a backup maintainer who can help recruit new maintainers should things go cold.


