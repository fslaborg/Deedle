# Release Notes

## 2.3.0 - 2020-09-16
* Add dropEmptyRows and dropEmptyCols [510](https://github.com/fslaborg/Deedle/pull/510)
* Fix ambiguous sum function when calling it from Frame [515](https://github.com/fslaborg/Deedle/pull/515)
* Update to latest NetOffice Excel package [505](https://github.com/fslaborg/Deedle/pull/505)

## 2.2.0 - 2020-06-15
* Add linear regression and PCA functions in Deedle.Math thanks to @Ildhesten [496](https://github.com/fslaborg/Deedle/pull/496)
* Add descriptions to Stats functions to clarify valid input types by @Arlofin [501](https://github.com/fslaborg/Deedle/pull/501)

## 2.1.2 - 2020-02-25
* Fix parsing csv of multi-line column headers [#479](https://github.com/fslaborg/Deedle/issues/479)
* Fix type inference of empty cell [#441](https://github.com/fslaborg/Deedle/issues/441)

## 2.1.1 - 2019-11-4
* Fix FilterRowsBy [#491](https://github.com/fslaborg/Deedle/issues/491)
* Add basic stats functions of Frame in extension [#490](https://github.com/fslaborg/Deedle/pull/490)

## 2.1.0 - 2019-11-4
* Release Deedle.Math to extend linear algebra, statisitical analysis and financial analysis on Frame and Series by leveraging MathNet.Numerics [#475](https://github.com/fslaborg/Deedle/pull/475)
* Override + operator and add Frame.strConcat to concatenate Frame and Series of string values [#482](https://github.com/fslaborg/Deedle/issues/482)
* Fix ResampleUniform with missing values [#470](https://github.com/fslaborg/Deedle/issues/480)

## 2.0.4 - 2019-04-29
* Fix assembly version [#472](https://github.com/fslaborg/Deedle/issues/472)

## 2.0.3 - 2019-04-24
* Fix missing preferoptions in C# extension Frame.Readcsv from stream [#471](https://github.com/fslaborg/Deedle/issues/471)
* Optimize Frame.AggregateRowsBy [#469](https://github.com/fslaborg/Deedle/issues/469)

## 2.0.2 - 2019-04-08
* Fix missing signatures to control access of FSharp.Data implementations [#465](https://github.com/fslaborg/Deedle/issues/465)

## 2.0.1 - 2019-03-25
* Fix linear interpolation [#458](https://github.com/fslaborg/Deedle/pull/458)
* Fix FillMissingWith with nan [#461](https://github.com/fslaborg/Deedle/pull/461)
* Fix InvalidOperationException from ValuesAll [#462](https://github.com/fslaborg/Deedle/pull/462)
* Fix iterating seq multiple times in Frame.ofRecords [#406](https://github.com/fslaborg/Deedle/pull/406)

## 2.0.0 - 2019-03-13
* Breaking changes of Stats.min and Stats.max [#422](https://github.com/fslaborg/Deedle/pull/422)
* Inline stats functions [#418](https://github.com/fslaborg/Deedle/pull/418)
* Fix error handling of group by column when dealing with missing value [253](https://github.com/fslaborg/Deedle/pull/405) and [380](https://github.com/fslaborg/Deedle/pull/380)
* Fix aggregateRowsBy with missing value [375](https://github.com/fslaborg/Deedle/pull/408)
* Fix format function [#416](https://github.com/fslaborg/Deedle/pull/416)
* Fix arithmetic operator on frames [#432](https://github.com/fslaborg/Deedle/pull/434)
* Fix wrong exception for empty Series [#365](https://github.com/fslaborg/Deedle/pull/437)
* Fix tryLastValue using tryGetAt [#339](https://github.com/fslaborg/Deedle/pull/438)
* Fix bug in ReadCsv missingValues parameters [#439](https://github.com/fslaborg/Deedle/pull/440)
* Fix handling of missing column keys in case when inferTypes is false [#63](https://github.com/fslaborg/Deedle/pull/450)
* Add Frame.dropSparseRowsBy [#404](https://github.com/fslaborg/Deedle/pull/404)
* Add Series.intersect [#407](https://github.com/fslaborg/Deedle/pull/407)
* Add Series.compare [#411](https://github.com/fslaborg/Deedle/pull/411)
* Add Series.uniqueCount [#413](https://github.com/fslaborg/Deedle/pull/413)
* Add Series.describe [#414](https://github.com/fslaborg/Deedle/pull/414) and [$422](https://github.com/fslaborg/Deedle/pull/442)
* Add Series.replace [#427](https://github.com/fslaborg/Deedle/pull/427)
* Add Stats.quantile for series [#428](https://github.com/fslaborg/Deedle/pull/428)
* Add Frame.slice [#445](https://github.com/fslaborg/Deedle/pull/445)
* Removed compiler warning from ReadCsv [#426](https://github.com/fslaborg/Deedle/pull/426)
* Rename stack/unstack to melt/unmelt [#436](https://github.com/fslaborg/Deedle/pull/436)

## 2.0.0-beta01 - 2018-08-04
* Support for netstandard2.0 [#382](https://github.com/fslaborg/Deedle/pull/382), [#393](https://github.com/fslaborg/Deedle/pull/393) and [#391](https://github.com/fslaborg/Deedle/pull/391)
* Excel support [#255](https://github.com/fslaborg/Deedle/pull/255) and [#399](https://github.com/fslaborg/Deedle/pull/399)
* Iterate once in Frame.ofRowsOrdinal [#396](https://github.com/fslaborg/Deedle/pull/396)
* Fix for some concurrency errors [#394](https://github.com/fslaborg/Deedle/pull/394)
* Fix bug in Series.hasNot [#361](https://github.com/fslaborg/Deedle/pull/361)
* Fix bug in Frame.tryValues [#359](https://github.com/fslaborg/Deedle/pull/359)
* Arithmetic operators for decimal series [#351](https://github.com/fslaborg/Deedle/pull/351)

## 1.2.5
 * Reading CSV (#332) and DropSparseRows (#333)
 * Fix where filter in C# (#338)

## 1.2.4
 * Fix RowsDense broken by BigDeedle changes (#319)
 * Make ChunkSizeInto behave according to documentation (#314)
 * Expand public fields (#313)
 * Keep order of columns/rows in FrameBuilder (#322)

## 1.2.3
 * Finish cleanup of BigDeedle code with partitioning support
 * Add BigDeedle partitioning comment to design notes
 * Update documentation tools dependencies

## 1.2.2
 * BigDeedle: Materialize series on grouping and other operations
 * BigDeedle: Support resampling without materializing series
 * Better handling of materialization via addressing schemes
 * Refactoring and cleanup of BigDeedle code
 * Fix bugs in ordinal virtual index
 * SelectOptional and SelectValues can be performed lazilly

## 1.2.1
 * Support public fields in Frame.ofRecords

## 1.2.0
 * Update version number for a BigDeedle release

## 1.1.5
 * Aggregate bug fixes from previous beta releases
 * Provide virtual index and virtual vector (aka BigDeedle)
 * Compare indices using lazy sequences (to support BigDeedle)

## 1.1.4-beta
 * Allow creation of empty ranges
 * Support more operations on virtualized sources
 * Fix handling of missing values in virtual Series.map

## 1.1.3-beta
 * Introduce generic `Ranges<T>` type to simplify working with ranges
   (mainly useful for custom BigDeedle implementations)

## 1.1.2-beta
 * Abstract handling of addresses (mainly for BigDeedle)
 * Avoid accessing series Length in series and frame printing

## 1.1.1-beta
 * Allow specifying custom NA values (#231)
 * Documentation improvements and add F# Frame extension docs (#254)
 * Use 100 rows for inference by default in C# and fix docs (#271)
 * Fix R interop documentation issue (#287)
 * More flexible conversion from R frames (#212)
 * Dropping sparse rows/columns should preserve frame structure (#277)
 * Change Stats.sum to return NaN for empty series (#259)
 * Change C#-version of ReadCsv to accept inferTypes param (#270)

## 1.1.0-beta
 * Enable materializing delayed series into a virtual series

## 1.0.7
 * Add typed frame access (frame.GetRowsAs<T>) (#281)
 * BigDeedle improvements (#284, #285)
 * Expose type information via frame.ColumnTypes (#286)
 * Simplify load script (#292)
 * Remove F# Data dependency & use Paket (#288, #293)
 * Update depndencies (F# Formatting 2.6.2 and RProvider 1.1.8)

## 1.0.6
 * Fix bugs related to frame with no columns (#272)
 * Remove FSharp.Core dependency from BigDeedle public API

## 1.0.5
 * Update R provider reference to 1.0.17

## 1.0.4
 * Merge BigDeedle pull request (#247), add merging on big frames
 * Fix PivotTable (#248) and CSV writing (#242)
 * Update R provider reference to 1.0.16 (support shadow copy in F# 3.2.1)

## 1.0.3
 * Added Stats.min and Stats.max for frame

## 1.0.2
 * Operations GetAs, TryAs (ObjectSeries), GetColumns, GetRows, GetAllValues, ColumnApply (Frame)
   and filling of missing values uses "safe" conversion (allows conversion to bigger numeric type)
 * Avoid boxing when filling missing values (#222)
 * Fix documentation bugs (#221, #226) and update formatters from FsLab

## 1.0.1
 * Update RProvider references

## 1.0.0
 * Performance and API design improvements

## 1.0.0-alpha1
 * API redesign, performance improvements and new features

## 1.0.0-alpha2
 * Update to a new pre-release of RProvider

## 0.9.12
 * Improved C# compatibility, added C# documentation

## 0.9.11-beta
 * Fix bug when creating empty data frame

## 0.9.10-beta
 * Support time series in the R plugin

## 0.9.9-beta
 * Performance improvements, API additions, experimental R plugin

## 0.9.8-beta
 * Add reflection-based frame expansion

## 0.9.7-beta
 * Fix series formatting

## 0.9.6-beta
 * Load script automatically references F# data (for CSV reading)

## 0.9.5-beta
 * Update documentation and tools, adding functionality

## 0.9.4-beta
 * Rename and various fixes and additions

## 0.9.3-beta
 * Saving CSV, fix series alignment

## 0.9.2-beta
 * Update paths in NuGet package

## 0.9.1-beta
 * First beta version on NuGet

## 0.9.0-beta
 * Initial release
