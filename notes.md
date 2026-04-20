## 2026-04-20 run (command mode: fix issue 712)

### Status: Branch committed, NOT pushed (safeoutputs MCP unavailable)
Branch: `repo-assist/fix-issue-712-parquet-rowgroup-dispose`
Local commit: 2e2dbd0

**Action needed**: Push the branch and create PR using safeoutputs tools when available.

### Fix applied
- Bug: `Frame.writeParquet` and `Frame.writeParquetStream` bound `IRowGroupWriter` with `let` instead of `use`, so `Dispose()` was never called. Parquet.Net requires disposal to commit `num_rows` to the file footer. External readers (DuckDB, pandas, viewers) saw 0 rows. Deedle's own reader was unaffected (reads raw data pages).
- Fix: `let rg = writer.CreateRowGroup()` → `use rg = writer.CreateRowGroup()` in both functions.
- 15 new regression tests added using `Parquet.ParquetReader.OpenRowGroupReader` to verify footer metadata.
- 50/50 Parquet tests pass, 54/54 C# tests pass.

## 2026-04-15 run

**Important design constraint**: Cannot add frame overloads to the `Stats` type with the same names as series functions (e.g., `Stats.movingMean`) because F# FS0816 forbids mixing curried (series) and tupled (frame) overloads of the same method name on a type. Frame stats must be implemented as extension methods in `FrameStatsExtensions.fs`.

### Work done
- Task 9: Added tests for `Series.has/hasNot/hasAll/hasSome/hasNone/lookupAll/getAll/sample/tryLookupObservation`
- Task 10: Added `MovingCount/Sum/Mean/Variance/StdDev/Min/Max` and `ExpandingCount/Sum/Mean/Variance/StdDev/Min/Max` extension methods on `Frame<'R,'C>`
- 734/734 tests pass

### PR not created (both runs)
safeoutputs MCP tools were unavailable in both runs. Local branches exist but not pushed.
