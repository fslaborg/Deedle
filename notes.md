## 2026-05-03 run (Tasks 5 + 4)

### Work done
- **Task 5 (Coding Improvements)**: Added `Frame.chunk` and `Frame.chunkInto` to FrameModule.fs — closes the API symmetry gap between `Frame.window` and the Series chunking API. Six new tests added. PR #aw_chunk720 created.
- **Task 4 (Engineering)**: Upgraded `fsdocs-tool` from pre-release `22.0.0-alpha.3` to stable `22.0.0` GA release. Build confirmed working. PR #aw_fsdocs721 created.
- 709 tests pass.

## 2026-04-26 run (Tasks 8 + 4)

### Work done
- **Task 8 (Performance)**: Eliminated O(N) intermediate `Series<'K,float>` allocation in `Stats.applyMovingSumsTransform` and `Stats.applyExpandingMomentsTransform` by adding `applySeriesProjLazy` helper that streams `'V→float` conversion lazily. Also replaced `pown x 2/3/4` with direct multiplication in `updateSumsDense` / `initSumsDense`.
- **Task 4 (Engineering)**: Updated CI cache keys in both workflows to include `.config/dotnet-tools.json` hash alongside `paket.lock`, ensuring tool version changes correctly invalidate cached NuGet packages.
- 703/703 Deedle tests pass, 54/54 C# tests pass
- PR #719 created and merged.

### Design constraint (from 2026-04-15)
Cannot add frame overloads to the `Stats` type with same names as series functions (F# FS0816). Frame stats must use `FrameStatsExtensions.fs`.

### Design constraint: inline + internal
F# FS1113: `inline` functions cannot call `internal`/`private` constructors like `Series(...)`. Must wrap in a non-inline helper (`applySeriesProjLazy`) and call that.

## 2026-04-20 run (command mode: fix issue 712)
- Bug fixed and merged as PR #716: `Frame.writeParquet` using `let` instead of `use` for IRowGroupWriter

## 2026-04-15 run
- Task 9: Added tests for `Series.has/hasNot/hasAll/hasSome/hasNone/lookupAll/getAll/sample/tryLookupObservation`
- Task 10: Added `MovingCount/Sum/Mean/Variance/StdDev/Min/Max` and `ExpandingCount/Sum/Mean/Variance/StdDev/Min/Max` extension methods on `Frame<'R,'C>`
