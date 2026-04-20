## 2026-04-20 run

**Command mode**: dsyme requested implementing issue #714 (ExcelWriter package).

**Work done**: Created `Deedle.ExcelWriter` package (branch `repo-assist/feature-excel-writer-714`).
- `src/Deedle.ExcelWriter/ExcelWriter.fs` — MiniExcel-based Frame.saveExcel/saveExcelSheet/saveExcelSheets/saveExcelToStream
- `tests/Deedle.ExcelWriter.Tests/Tests.fs` — 9 passing tests
- Updated paket.dependencies/lock (MiniExcel 1.43.1), Deedle.sln, RELEASE_NOTES.md
- All 9 new tests pass; existing tests unaffected

**safeoutputs MCP tools unavailable**: Branch committed locally but not pushed. No PR created, no comment on #714. Next run should push and create PR.

**Local branch**: `repo-assist/feature-excel-writer-714` on fslaborg/Deedle workspace.

## 2026-04-15 run

Tasks 9+10 completed. All changes committed to local branch `repo-assist/improve-frame-moving-stats-20260415`.

**Important design constraint**: Cannot add frame overloads to the `Stats` type with the same names as series functions (e.g., `Stats.movingMean`) because F# FS0816 forbids mixing curried (series) and tupled (frame) overloads of the same method name on a type. Frame stats must be implemented as extension methods in `FrameStatsExtensions.fs`.

### Work done
- Task 9: Added tests for `Series.has/hasNot/hasAll/hasSome/hasNone/lookupAll/getAll/sample/tryLookupObservation`
- Task 10: Added `MovingCount/Sum/Mean/Variance/StdDev/Min/Max` and `ExpandingCount/Sum/Mean/Variance/StdDev/Min/Max` extension methods on `Frame<'R,'C>`
- 734/734 tests pass

### PR not created
safeoutputs MCP tools were unavailable in this run. Local branch exists but was not pushed to GitHub.
