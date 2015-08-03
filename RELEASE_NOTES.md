### 0.9.0-beta
 * Initial release

### 0.9.1-beta
 * First beta version on NuGet

### 0.9.2-beta
 * Update paths in NuGet package

### 0.9.3-beta
 * Saving CSV, fix series alignment

### 0.9.4-beta
 * Rename and various fixes and additions

### 0.9.5-beta
 * Update documentation and tools, adding functionality

### 0.9.6-beta
 * Load script automatically references F# data (for CSV reading)

### 0.9.7-beta
 * Fix series formatting

### 0.9.8-beta
 * Add reflection-based frame expansion

### 0.9.9-beta
 * Performance improvements, API additions, experimental R plugin

### 0.9.10-beta
 * Support time series in the R plugin

### 0.9.11-beta
 * Fix bug when creating empty data frame

### 0.9.12
 * Improved C# compatibility, added C# documentation

### 1.0.0-alpha1
 * API redesign, performance improvements and new features

### 1.0.0-alpha2
 * Update to a new pre-release of RProvider

### 1.0.0
 * Performance and API design improvements

### 1.0.1
 * Update RProvider references

### 1.0.2
 * Operations GetAs, TryAs (ObjectSeries), GetColumns, GetRows, GetAllValues, ColumnApply (Frame)
   and filling of missing values uses "safe" conversion (allows conversion to bigger numeric type)
 * Avoid boxing when filling missing values (#222)
 * Fix documentation bugs (#221, #226) and update formatters from FsLab

### 1.0.3
 * Added Stats.min and Stats.max for frame

### 1.0.4
 * Merge BigDeedle pull request (#247), add merging on big frames
 * Fix PivotTable (#248) and CSV writing (#242)
 * Update R provider reference to 1.0.16 (support shadow copy in F# 3.2.1)

### 1.0.5
 * Update R provider reference to 1.0.17

### 1.0.6
 * Fix bugs related to frame with no columns (#272)
 * Remove FSharp.Core dependency from BigDeedle public API

### 1.0.7
 * Add typed frame access (frame.GetRowsAs<T>) (#281)
 * BigDeedle improvements (#284, #285)
 * Expose type information via frame.ColumnTypes (#286)
 * Simplify load script (#292)
 * Remove F# Data dependency & use Paket (#288, #293)
 * Update depndencies (F# Formatting 2.6.2 and RProvider 1.1.8)

### 1.1.0-beta
 * Enable materializing delayed series into a virtual series

### 1.1.1-beta
 * Allow specifying custom NA values (#231)
 * Documentation improvements and add F# Frame extension docs (#254)
 * Use 100 rows for inference by default in C# and fix docs (#271)
 * Fix R interop documentation issue (#287)
 * More flexible conversion from R frames (#212)
 * Dropping sparse rows/columns should preserve frame structure (#277)
 * Change Stats.sum to return NaN for empty series (#259)
 * Change C#-version of ReadCsv to accept inferTypes param (#270)

### 1.1.2-beta
 * Abstract handling of addresses (mainly for BigDeedle)
 * Avoid accessing series Length in series and frame printing

### 1.1.3-beta
 * Introduce generic `Ranges<T>` type to simplify working with ranges
   (mainly useful for custom BigDeedle implementations)

### 1.1.4-beta
 * Allow creation of empty ranges
 * Support more operations on virtualized sources
 * Fix handling of missing values in virtual Series.map

### 1.1.5
 * Aggregate bug fixes from previous beta releases
 * Provide virtual index and virtual vector (aka BigDeedle)
 * Compare indices using lazy sequences (to support BigDeedle)

### 1.2.0
 * Update version number for a BigDeedle release

### 1.2.2
 * BigDeedle: Materialize series on grouping and other operations
 * BigDeedle: Support resampling without materializing series
 * Better handling of materialization via addressing schemes
 * Refactoring and cleanup of BigDeedle code
 * Fix bugs in ordinal virtual index
 * SelectOptional and SelectValues can be performed lazilly

### 1.2.3
 * Finish cleanup of BigDeedle code with partitioning support
 * Add BigDeedle partitioning comment to design notes
 * Update documentation tools dependencies
