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