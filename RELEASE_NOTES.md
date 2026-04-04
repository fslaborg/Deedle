# Release Notes

## 5.0.0 - 2026-04-02

### New operations — Deedle.Math.Finance

- Add `Finance.ewmCrossCov` — EWMA pairwise covariance between two return series (mean-corrected, consistent with `ewmCovMatrix`) ([#81](https://github.com/fslaborg/Deedle/issues/81))
- Add `Finance.ewmCrossVol` — signed square root of `ewmCrossCov`, giving cross-volatility in vol units with sign indicating direction of co-movement ([#81](https://github.com/fslaborg/Deedle/issues/81))

### Infrastructure

- Migrate to .NET 10 and FSharp.Core 10.0
- Update fsdocs-tool to 22.0.0-alpha.3
- Remove unnecessary System.Reflection.Emit package references (now in-box on net10.0)
- Suppress NU1510 warnings for in-box packages pulled transitively via Paket
- Suppress NU1701 warnings for NetOfficeFw packages (no net10.0 assets)
- Fix self-referential `open FSharp.Data.Runtime` in vendored TextRuntime.fs for newer F# compilers
- Replace deprecated `Frame.indexRowsDate`/`Frame.indexRowsDateOffs` usage in tests with `Frame.indexRowsDateTime`/`Frame.indexRowsDateTimeOffset`

### Bug fixes

- Fix doc generation "No value returned by any evaluator" errors ([#692](https://github.com/fslaborg/Deedle/issues/692))

## 4.0.1 - 2026-03-22

### New packages

- **Deedle.Arrow**: Apache Arrow / Feather interop — `Frame`/`Series` ↔ `RecordBatch`, IPC I/O, Feather aliases, UInt/Date types, C# extensions, row-key preservation ([#675](https://github.com/fslaborg/Deedle/pull/675), [#681](https://github.com/fslaborg/Deedle/pull/681), [#685](https://github.com/fslaborg/Deedle/pull/685))
- **Deedle.MicrosoftML**: ML.NET `IDataView` integration with `VBuffer<float32>`/`VBuffer<float>` vector column support ([#677](https://github.com/fslaborg/Deedle/pull/677), [#680](https://github.com/fslaborg/Deedle/pull/680))
- **Deedle.Excel.Reader**: cross-platform xlsx/xls reading via ExcelDataReader ([#679](https://github.com/fslaborg/Deedle/pull/679))

### New Frame operations

- `Frame.interleave` — side-by-side frame combining with tuple column keys ([#691](https://github.com/fslaborg/Deedle/pull/691))
- `Frame.joinOn`, `Frame.joinOnString`, `Frame.joinOnInt` — join frames on a column value ([#630](https://github.com/fslaborg/Deedle/pull/630), [#663](https://github.com/fslaborg/Deedle/pull/663))
- `Frame.nestRowsBy` — hierarchical row nesting ([#652](https://github.com/fslaborg/Deedle/pull/652))
- `Frame.compare` — column-by-column frame diffing ([#637](https://github.com/fslaborg/Deedle/pull/637))
- `Frame.rankRowsBy` — rank rows within groups ([#636](https://github.com/fslaborg/Deedle/pull/636))
- `Frame.indexRowsApply` — build row index from a projection ([#650](https://github.com/fslaborg/Deedle/pull/650))
- `Frame.indexRowsDateTime` / `Frame.indexRowsDateTimeOffset` aliases ([#653](https://github.com/fslaborg/Deedle/pull/653))
- `Frame.mapColValuesAs` — typed, non-boxing column mapping ([#629](https://github.com/fslaborg/Deedle/pull/629))
- `Frame.renameCol`, `Frame.renameColsUsing` ([#603](https://github.com/fslaborg/Deedle/pull/603))
- `Frame.meltBy` — pandas-style melt with identity columns ([#598](https://github.com/fslaborg/Deedle/pull/598))
- `Frame.distinctRowsBy` — remove duplicate rows by column values ([#596](https://github.com/fslaborg/Deedle/pull/596))
- `Frame.toJson` / `Frame.saveJson` — JSON serialisation ([#609](https://github.com/fslaborg/Deedle/pull/609))
- `Frame.pctChange` — percentage change ([#610](https://github.com/fslaborg/Deedle/pull/610))
- `Frame.filterRowsByMask`, `Frame.filterColsByMask` — boolean mask filtering ([#619](https://github.com/fslaborg/Deedle/pull/619))
- `Frame.empty` / `Series.empty` module functions ([#607](https://github.com/fslaborg/Deedle/pull/607))
- `iloc` integer-position indexing for `Frame` and `Series` ([#631](https://github.com/fslaborg/Deedle/pull/631))

### New Series operations

- `Series.rank`, `Series.rankWith`, `Series.ntile` — ranking and quantile binning ([#636](https://github.com/fslaborg/Deedle/pull/636))
- `Series.sweepLevel` — hierarchical group normalisation ([#638](https://github.com/fslaborg/Deedle/pull/638))
- `Series.replaceValue` — pandas-style value replacement ([#628](https://github.com/fslaborg/Deedle/pull/628))
- `Series.maskValues`, `Series.maskAll` — replace values with missing ([#605](https://github.com/fslaborg/Deedle/pull/605))
- `Series.diffDate`, `Series.diffDateOffset` — `DateTime`/`DateTimeOffset` differences ([#592](https://github.com/fslaborg/Deedle/pull/592))
- `Series.pctChange` — percentage change ([#610](https://github.com/fslaborg/Deedle/pull/610))
- `Series.filterByMask` — boolean mask filtering ([#619](https://github.com/fslaborg/Deedle/pull/619))
- `Series.before`, `Series.after`, `Series.startAt`, `Series.endAt`, `Series.between` ([#620](https://github.com/fslaborg/Deedle/pull/620))
- `Series.windowWhileFromEnd`, `Series.chunkWhileFromEnd` ([#648](https://github.com/fslaborg/Deedle/pull/648))

### New Stats operations

- `Stats.movingMedian`, `Stats.expandingMedian` ([#661](https://github.com/fslaborg/Deedle/pull/661))
- `Stats.levelMin`, `Stats.levelMax` ([#647](https://github.com/fslaborg/Deedle/pull/647))
- `Stats.interpolateLinearWith` — linear interpolation with configurable extrapolation ([#627](https://github.com/fslaborg/Deedle/pull/627))
- `Stats.cov`, `Stats.corr` — pairwise series covariance and correlation ([#614](https://github.com/fslaborg/Deedle/pull/614))
- `Stats.corrMatrix`, `Stats.covMatrix` — full correlation/covariance matrix for a `Frame` ([#682](https://github.com/fslaborg/Deedle/pull/682))
- `Stats.describe` for `Frame` — pandas-style per-column summary statistics ([#612](https://github.com/fslaborg/Deedle/pull/612))

### New C# extension methods

- `WindowWhile`, `ChunkWhile`, `PairwiseWith`, `MapRows`, `MapCols` ([#666](https://github.com/fslaborg/Deedle/pull/666))
- `ZipInto`, `ZipAlignInto`, `ZipInner` for `Series` ([#657](https://github.com/fslaborg/Deedle/pull/657))

### I/O enhancements

- `Frame.ReadCsv` accepts URLs ([#658](https://github.com/fslaborg/Deedle/pull/658))
- `Frame.ReadCsv` `typeResolver` parameter for custom type inference ([#649](https://github.com/fslaborg/Deedle/pull/649))
- `Frame.ReadCsv` / `Frame.SaveCsv` `encoding` parameter ([#617](https://github.com/fslaborg/Deedle/pull/617))

### Performance

- Eliminate `seq`/`ref` overhead in `Stats` moving and expanding window helpers ([#683](https://github.com/fslaborg/Deedle/pull/683))
- Optimise `Frame.AggregateRowsBy` to avoid per-group sub-frame allocation ([#669](https://github.com/fslaborg/Deedle/pull/669))
- Cache `ToString()` in CSV writer for repeated values ([#621](https://github.com/fslaborg/Deedle/pull/621))

### Bug fixes

- Fix delayed series display showing `(Suppressed)` — show key range instead ([#662](https://github.com/fslaborg/Deedle/pull/662))
- Fix `DateTime` key formatting and locale-dependent formatting in FSI output ([#633](https://github.com/fslaborg/Deedle/pull/633), [#643](https://github.com/fslaborg/Deedle/pull/643))
- Fix `Frame.fillMissingWith` to handle numeric type widening ([#632](https://github.com/fslaborg/Deedle/pull/632))
- Fix: throw `InvalidOperationException` for inexact `Lookup` on unordered series ([#644](https://github.com/fslaborg/Deedle/pull/644))
- Fix `DelayedSeries.Between` to correctly update `RangeMin`/`RangeMax` ([#624](https://github.com/fslaborg/Deedle/pull/624))
- Fix `AsDecimal` to support scientific notation ([#623](https://github.com/fslaborg/Deedle/pull/623))
- Fix `Frame.ReadCsv` schema being ignored when `inferTypes=false` ([#615](https://github.com/fslaborg/Deedle/pull/615))
- Fix `Finance.ewmVol` computing RMS instead of standard deviation ([#593](https://github.com/fslaborg/Deedle/pull/593))
- Fix `Frame.GetRowKeyAt` to accept `int` instead of `int64` ([#588](https://github.com/fslaborg/Deedle/pull/588))
- Fix `Frame.ofRecords` for internal/private record types ([#580](https://github.com/fslaborg/Deedle/pull/580))
- Fix `IndexOutOfRangeException` in `Series.windowSize` for series shorter than window ([#573](https://github.com/fslaborg/Deedle/pull/573))
- Truncate long cell values in FSI/`ToString` output ([#646](https://github.com/fslaborg/Deedle/pull/646))
- Fix `filterRows`/`filterRowValues`/`Where` losing `ColumnTypes` on all-missing columns ([#601](https://github.com/fslaborg/Deedle/pull/601))
- Fix CSV schema parsing for column names containing parentheses ([#604](https://github.com/fslaborg/Deedle/pull/604))
- Fix `Frame.indexRowsWith` to produce missing values for extra row keys ([#595](https://github.com/fslaborg/Deedle/pull/595))

### Documentation

- Add C# cookbook (`docs/csharp.md`) — covers `SeriesBuilder`, `Frame.ReadCsv`, `Frame.FromRecords`, column/row slicing via `Columns[...]`/`Rows[...]`, `GetColumn<T>`, dynamic access, missing values, statistics, windowing, joining, and `GetRowsAs<T>` ([#308](https://github.com/fslaborg/Deedle/issues/308))

### Infrastructure

- Add SourceLink and deterministic build support ([#660](https://github.com/fslaborg/Deedle/pull/660))
- Add BenchmarkDotNet benchmark suite under `benchmarks/` ([#687](https://github.com/fslaborg/Deedle/pull/687))
- Update test infrastructure: NUnit 4, FsUnit 7, FsCheck 3 ([#616](https://github.com/fslaborg/Deedle/pull/616))
- Add `[RequiresExplicitTypeArguments]` to `GetRow`/`TryGetRow` and `ObjectSeries` typed accessors ([#654](https://github.com/fslaborg/Deedle/pull/654))

## 4.0.0 - 2026-03-10

- **Breaking change**: `Frame.stack` and `Frame.unstack` now implement pandas-style
  reshape operations. `stack` converts `Frame<'R,'C>` to a long-format
  `Frame<'R*'C, string>` (tuple row keys, single `"Value"` column). `unstack` converts
  `Frame<'R1*'R2,'C>` to wide-format `Frame<'R1, 'C*'R2>` by promoting the inner row-key
  level to column keys.
- The old denormalised-table operations previously called `stack`/`unstack` were already
  renamed to `melt`/`unmelt` in an earlier release; the obsolete aliases are now removed.

## 4.0.0-alpha-001 - 2026-03-09

- Migrate to .NET 9
- Migrate documentation to fsdocs
- Remove FAKE build system, replace with simple build scripts
- Remove R Provider plugin

## 3.0.0 - 2023-01-17

- Fix missing value in Stats.cov[552](https://github.com/fslaborg/Deedle/pull/552)

## 3.0.0-beta.1 - 2022-06-22

- Add Deedle.Interactive package for formatting in dotnet notebooks
- Adjust and extend formatting interfaces accordingly.

## 2.5.0 - 2021-11-10

- Restore Deedle.RPlugin targeting net5.0

## 2.4.3 - 2021-09-11

- Add TryMin and TryMax in extension to be utilized in C#

## 2.4.2 - 2021-08-31

- Relax types for Matrix.dot of two frames [536](https://github.com/fslaborg/Deedle/issues/536)

## 2.4.1 - 2021-07-25

- Add conversions between jagged array and frame [532](https://github.com/fslaborg/Deedle/pull/532)

## 2.4.0 - 2021-07-06

- Add Frame.ReadCsvString [530](https://github.com/fslaborg/Deedle/pull/530)
- .NET Standard 2.0 only

## 2.3.0 - 2020-09-16

- Add dropEmptyRows and dropEmptyCols [510](https://github.com/fslaborg/Deedle/pull/510)
- Fix ambiguous sum function when calling it from Frame [515](https://github.com/fslaborg/Deedle/pull/515)
- Update to latest NetOffice Excel package [505](https://github.com/fslaborg/Deedle/pull/505)

## 2.2.0 - 2020-06-15

- Add linear regression and PCA functions in Deedle.Math thanks to @Ildhesten [496](https://github.com/fslaborg/Deedle/pull/496)
- Add descriptions to Stats functions to clarify valid input types by @Arlofin [501](https://github.com/fslaborg/Deedle/pull/501)

## 2.1.2 - 2020-02-25

- Fix parsing csv of multi-line column headers [#479](https://github.com/fslaborg/Deedle/issues/479)
- Fix type inference of empty cell [#441](https://github.com/fslaborg/Deedle/issues/441)

## 2.1.1 - 2019-11-4

- Fix FilterRowsBy [#491](https://github.com/fslaborg/Deedle/issues/491)
- Add basic stats functions of Frame in extension [#490](https://github.com/fslaborg/Deedle/pull/490)

## 2.1.0 - 2019-11-4

- Release Deedle.Math to extend linear algebra, statisitical analysis and financial analysis on Frame and Series by leveraging MathNet.Numerics [#475](https://github.com/fslaborg/Deedle/pull/475)
- Override + operator and add Frame.strConcat to concatenate Frame and Series of string values [#482](https://github.com/fslaborg/Deedle/issues/482)
- Fix ResampleUniform with missing values [#470](https://github.com/fslaborg/Deedle/issues/480)

## 2.0.4 - 2019-04-29

- Fix assembly version [#472](https://github.com/fslaborg/Deedle/issues/472)

## 2.0.3 - 2019-04-24

- Fix missing preferoptions in C# extension Frame.Readcsv from stream [#471](https://github.com/fslaborg/Deedle/issues/471)
- Optimize Frame.AggregateRowsBy [#469](https://github.com/fslaborg/Deedle/issues/469)

## 2.0.2 - 2019-04-08

- Fix missing signatures to control access of FSharp.Data implementations [#465](https://github.com/fslaborg/Deedle/issues/465)

## 2.0.1 - 2019-03-25

- Fix linear interpolation [#458](https://github.com/fslaborg/Deedle/pull/458)
- Fix FillMissingWith with nan [#461](https://github.com/fslaborg/Deedle/pull/461)
- Fix InvalidOperationException from ValuesAll [#462](https://github.com/fslaborg/Deedle/pull/462)
- Fix iterating seq multiple times in Frame.ofRecords [#406](https://github.com/fslaborg/Deedle/pull/406)

## 2.0.0 - 2019-03-13

- Breaking changes of Stats.min and Stats.max [#422](https://github.com/fslaborg/Deedle/pull/422)
- Inline stats functions [#418](https://github.com/fslaborg/Deedle/pull/418)
- Fix error handling of group by column when dealing with missing value [253](https://github.com/fslaborg/Deedle/pull/405) and [380](https://github.com/fslaborg/Deedle/pull/380)
- Fix aggregateRowsBy with missing value [375](https://github.com/fslaborg/Deedle/pull/408)
- Fix format function [#416](https://github.com/fslaborg/Deedle/pull/416)
- Fix arithmetic operator on frames [#432](https://github.com/fslaborg/Deedle/pull/434)
- Fix wrong exception for empty Series [#365](https://github.com/fslaborg/Deedle/pull/437)
- Fix tryLastValue using tryGetAt [#339](https://github.com/fslaborg/Deedle/pull/438)
- Fix bug in ReadCsv missingValues parameters [#439](https://github.com/fslaborg/Deedle/pull/440)
- Fix handling of missing column keys in case when inferTypes is false [#63](https://github.com/fslaborg/Deedle/pull/450)
- Add Frame.dropSparseRowsBy [#404](https://github.com/fslaborg/Deedle/pull/404)
- Add Series.intersect [#407](https://github.com/fslaborg/Deedle/pull/407)
- Add Series.compare [#411](https://github.com/fslaborg/Deedle/pull/411)
- Add Series.uniqueCount [#413](https://github.com/fslaborg/Deedle/pull/413)
- Add Series.describe [#414](https://github.com/fslaborg/Deedle/pull/414) and [$422](https://github.com/fslaborg/Deedle/pull/442)
- Add Series.replace [#427](https://github.com/fslaborg/Deedle/pull/427)
- Add Stats.quantile for series [#428](https://github.com/fslaborg/Deedle/pull/428)
- Add Frame.slice [#445](https://github.com/fslaborg/Deedle/pull/445)
- Removed compiler warning from ReadCsv [#426](https://github.com/fslaborg/Deedle/pull/426)
- Rename stack/unstack to melt/unmelt [#436](https://github.com/fslaborg/Deedle/pull/436)

## 2.0.0-beta01 - 2018-08-04

- Support for netstandard2.0 [#382](https://github.com/fslaborg/Deedle/pull/382), [#393](https://github.com/fslaborg/Deedle/pull/393) and [#391](https://github.com/fslaborg/Deedle/pull/391)
- Excel support [#255](https://github.com/fslaborg/Deedle/pull/255) and [#399](https://github.com/fslaborg/Deedle/pull/399)
- Iterate once in Frame.ofRowsOrdinal [#396](https://github.com/fslaborg/Deedle/pull/396)
- Fix for some concurrency errors [#394](https://github.com/fslaborg/Deedle/pull/394)
- Fix bug in Series.hasNot [#361](https://github.com/fslaborg/Deedle/pull/361)
- Fix bug in Frame.tryValues [#359](https://github.com/fslaborg/Deedle/pull/359)
- Arithmetic operators for decimal series [#351](https://github.com/fslaborg/Deedle/pull/351)

## 1.2.5

- Reading CSV (#332) and DropSparseRows (#333)
- Fix where filter in C# (#338)

## 1.2.4

- Fix RowsDense broken by BigDeedle changes (#319)
- Make ChunkSizeInto behave according to documentation (#314)
- Expand public fields (#313)
- Keep order of columns/rows in FrameBuilder (#322)

## 1.2.3

- Finish cleanup of BigDeedle code with partitioning support
- Add BigDeedle partitioning comment to design notes
- Update documentation tools dependencies

## 1.2.2

- BigDeedle: Materialize series on grouping and other operations
- BigDeedle: Support resampling without materializing series
- Better handling of materialization via addressing schemes
- Refactoring and cleanup of BigDeedle code
- Fix bugs in ordinal virtual index
- SelectOptional and SelectValues can be performed lazilly

## 1.2.1

- Support public fields in Frame.ofRecords

## 1.2.0

- Update version number for a BigDeedle release

## 1.1.5

- Aggregate bug fixes from previous beta releases
- Provide virtual index and virtual vector (aka BigDeedle)
- Compare indices using lazy sequences (to support BigDeedle)

## 1.1.4-beta

- Allow creation of empty ranges
- Support more operations on virtualized sources
- Fix handling of missing values in virtual Series.map

## 1.1.3-beta

- Introduce generic `Ranges<T>` type to simplify working with ranges
  (mainly useful for custom BigDeedle implementations)

## 1.1.2-beta

- Abstract handling of addresses (mainly for BigDeedle)
- Avoid accessing series Length in series and frame printing

## 1.1.1-beta

- Allow specifying custom NA values (#231)
- Documentation improvements and add F# Frame extension docs (#254)
- Use 100 rows for inference by default in C# and fix docs (#271)
- Fix R interop documentation issue (#287)
- More flexible conversion from R frames (#212)
- Dropping sparse rows/columns should preserve frame structure (#277)
- Change Stats.sum to return NaN for empty series (#259)
- Change C#-version of ReadCsv to accept inferTypes param (#270)

## 1.1.0-beta

- Enable materializing delayed series into a virtual series

## 1.0.7

- Add typed frame access (frame.GetRowsAs<T>) (#281)
- BigDeedle improvements (#284, #285)
- Expose type information via frame.ColumnTypes (#286)
- Simplify load script (#292)
- Remove F# Data dependency & use Paket (#288, #293)
- Update depndencies (F# Formatting 2.6.2 and RProvider 1.1.8)

## 1.0.6

- Fix bugs related to frame with no columns (#272)
- Remove FSharp.Core dependency from BigDeedle public API

## 1.0.5

- Update R provider reference to 1.0.17

## 1.0.4

- Merge BigDeedle pull request (#247), add merging on big frames
- Fix PivotTable (#248) and CSV writing (#242)
- Update R provider reference to 1.0.16 (support shadow copy in F# 3.2.1)

## 1.0.3

- Added Stats.min and Stats.max for frame

## 1.0.2

- Operations GetAs, TryAs (ObjectSeries), GetColumns, GetRows, GetAllValues, ColumnApply (Frame)
  and filling of missing values uses "safe" conversion (allows conversion to bigger numeric type)
- Avoid boxing when filling missing values (#222)
- Fix documentation bugs (#221, #226) and update formatters from FsLab

## 1.0.1

- Update RProvider references

## 1.0.0

- Performance and API design improvements

## 1.0.0-alpha1

- API redesign, performance improvements and new features

## 1.0.0-alpha2

- Update to a new pre-release of RProvider

## 0.9.12

- Improved C# compatibility, added C# documentation

## 0.9.11-beta

- Fix bug when creating empty data frame

## 0.9.10-beta

- Support time series in the R plugin

## 0.9.9-beta

- Performance improvements, API additions, experimental R plugin

## 0.9.8-beta

- Add reflection-based frame expansion

## 0.9.7-beta

- Fix series formatting

## 0.9.6-beta

- Load script automatically references F# data (for CSV reading)

## 0.9.5-beta

- Update documentation and tools, adding functionality

## 0.9.4-beta

- Rename and various fixes and additions

## 0.9.3-beta

- Saving CSV, fix series alignment

## 0.9.2-beta

- Update paths in NuGet package

## 0.9.1-beta

- First beta version on NuGet

## 0.9.0-beta

- Initial release
