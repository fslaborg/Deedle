## 2026-04-15 run

Tasks 9+10 completed. All changes committed to local branch `repo-assist/improve-frame-moving-stats-20260415`.

**Important design constraint**: Cannot add frame overloads to the `Stats` type with the same names as series functions (e.g., `Stats.movingMean`) because F# FS0816 forbids mixing curried (series) and tupled (frame) overloads of the same method name on a type. Frame stats must be implemented as extension methods in `FrameStatsExtensions.fs`.

### Work done
- Task 9: Added tests for `Series.has/hasNot/hasAll/hasSome/hasNone/lookupAll/getAll/sample/tryLookupObservation`
- Task 10: Added `MovingCount/Sum/Mean/Variance/StdDev/Min/Max` and `ExpandingCount/Sum/Mean/Variance/StdDev/Min/Max` extension methods on `Frame<'R,'C>`
- 734/734 tests pass

### PR not created
safeoutputs MCP tools were unavailable in this run. Local branch exists but was not pushed to GitHub.
