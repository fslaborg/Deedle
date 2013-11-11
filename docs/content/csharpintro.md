Using Deedle from C#
====================

Deedle is a .NET library for data manipulation and it can be used from both F# and C# as well as from
other .NET languages. Deedle is a single managed library `Deedle.dll` that contains the core Deedle
types, together with extension methods for convenient use from C#. To install Deedle, just use the
Deedle NuGet package.

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The F# DataFrame library can be <a href="https://nuget.org/packages/Deedle">installed from NuGet</a>:
      <pre>PM> Install-Package Deedle</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Overview
--------

The Deedle library provides types for working with _series_ and _data frames_. A series is a collection 
mapping a set of keys to values. The keys are used for lookup in the series, but also for automatic 
alignment when working with multiple series. For example, the keys can be ordinal numbers or strings 
(when you use it to store different properties) or ordered dates or times (when you use series to 
represent time series such as stock prices). When performing operations on multiple series (e.g. adding
or zipping), the keys are used to automatically match corresponding values. Furthermore, series automatically
handles missing data.

A data frame is a structure containing multiple series (multiple columns) that share the same row keys.
A typical example includes data frame that stores different properties about objects (each row represents
a single object), or data frame that stores aligned time series data such as prices for multiple stocks.

The best way to learn about using Deedle from C# is to go through the two tutorials below that disucss 
working with series and frames, respectively.

Documentation
-------------

 * [**Working with data series**](csharpseries.html) explains the `Series<K, V>` type, discusses how to 
   build series, retrieve data from a series, perform series computations including aggregation
   and handle missing data and how to work with series values and indices.

 * [**Working with data frames**](csharpframe.html) discusses how to combine multiple series in a data
   frame, how to align data and time series using dates, how to work with rows and columns of a
   data frame and how to perform calculations over entire data frames.

 * [**Design notes document**](http://bluemountaincapital.github.io/Deedle/design.html) is worth checking
   out if you want to get better understanding of the library principles - how it has been designed and
   how the implementation works.
