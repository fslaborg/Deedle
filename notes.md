## 2026-09-06 run (Tasks 4, 5, 7→2 fallback, 11)

### Work done
- **Task 4 (Engineering)**: Fixed high-severity NuGet vuln GHSA-pggp-6c3x-2xmx by pinning `Snappier >= 1.3.1` in paket.dependencies (transitive via Parquet.Net). Also bumped `fsdocs-tool` 22.0.0 → 22.2.0. Build + 720 Deedle tests + 55 Parquet tests all pass. PR created on branch `repo-assist/eng-paket-update-20260906`.
- **Task 5 (Coding Improvements)**: Added `Frame.Between/After/Before/StartAt/EndAt` members (Frame.fs) + matching module functions (FrameModule.fs), mirroring existing `Series` API — closes API symmetry gap. Added 2 new test functions (member + module-function coverage). 722/722 tests pass. PR created on branch `repo-assist/improve-frame-subrange-slicing`.
- **Task 7 (Stale PR Nudges) → not applicable**: only non-repo-assist open PR is #733, updated same day (freshly reviewed by Repo Assist already) — not stale. Fell back to Task 2: reviewed issues #509 and #531, both already have recent thorough Repo Assist comments with no new human activity since — no further comment made (avoided redundant posting).
- **Task 11**: Closed previous month's issue #727 (2026-08, now stale). Created new "[Repo Assist] Monthly Activity 2026-09" issue with Suggested Actions covering PR #733, PR #734, the two new PRs from this run, issues #509/#531, and the known OpenTelemetry.Api moderate-severity advisory (not yet fixed — flagged as future work).

### Known unresolved items (carry forward)
- `OpenTelemetry.Api` 1.15.1 has moderate severity advisory GHSA-g94r-2vxg-569j (transitive, likely via BenchmarkDotNet/test SDK). Not fixed this run — needs investigation into top-level package and whether a major bump is unavoidable. Good Task 4 candidate for a future run.
- PR #734 (repo-assist testing PR) still has no CI check runs recorded as of this run — check again in a future Task 6 pass.
- Issues #509 and #531: no new human activity since last Repo Assist comment (March 2026) — do not re-comment unless new activity appears.

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

## 2026-09-13 run (Tasks 4, 2, 8, 11)

### Task selection
- Selected tasks: 4 (Engineering Investments), 2 (Issue Investigation and Comment), 8 (Performance Improvements). Task 2 substituted with more Task 4/8 work since #509 and #531 had no new activity (both already commented, verified via fresh comment fetch).

### Work done
- **Task 4 (Engineering)**: Fixed the two currently-open NuGet security advisories:
  - Pinned `Snappier >= 1.3.1` in `paket.dependencies` (transitive via Parquet.Net) — fixes GHSA-pggp-6c3x-2xmx (high, CVE-2026-44302).
  - Pinned `OpenTelemetry.Api >= 1.15.3` in `paket.dependencies` (transitive via BenchmarkDotNet/test SDK chain) — fixes GHSA-g94r-2vxg-569j (moderate, CVE-2026-40894); resolved to 1.18.
  - Verified with a clean restore (cleared `obj/` caches) that both NU1902 warnings are gone.
  - Build + 928 Deedle.Tests + 55 Parquet.Tests all pass. PR created on branch `repo-assist/eng-security-deps-20260913`.
  - Noted in PR body: this supersedes the still-open PR #735 from 2026-09-06 which could not auto-push due to protected `.config/dotnet-tools.json` — flagged for maintainer to close #735 if this PR is merged.
- **Task 2 (fallback investigation)**: Re-checked issues #509 and #531 via fresh `issue_read`/comments fetch — confirmed no new human activity since the last Repo Assist comment on either (March 2026). No comment posted (avoiding redundant noise), consistent with prior run's assessment.
- **Task 8 (Performance)**: Found 3 spots using `Seq.length` over full row/column key sequences (O(N)/O(C)) where the O(1) `Frame.RowCount`/`Frame.ColumnCount` properties (wrapping `IIndex.KeyCount`) were already available but unused:
  - `Frame.AddColumn` (seq overload) in `Frame.fs`
  - `Frame.RenameColumns` in `Frame.fs`
  - `Frame.indexColsWith` module function in `FrameModule.fs`
  - Benchmarked with ad-hoc `dotnet fsi` script: `AddColumn` x5 on 2M-row frame ~995ms before → ~854ms after (~14% faster). 928/928 tests pass unchanged. PR created on branch `repo-assist/perf-avoid-seqlength-rowcount-20260913`.
- **Task 11**: Updated issue #737 (current month, no rewrite needed) to the mandated checkbox (`* [ ]`) Suggested Actions format instead of plain bullets; added entries for the two new PRs from this run and PR #735's blocked/superseded status; prepended new Run History entry.

### Known unresolved items (carry forward)
- PR #735 (Snappier + fsdocs-tool bump) still open, blocked by protected-file push restriction — superseded by this run's `eng-security-deps-20260913` PR (source pin only, no protected files). Flag for maintainer to close #735 once new PR is reviewed.
- PR #734 (repo-assist testing PR) — still no CI check runs recorded as of this run; check again in a future Task 6 pass.
- PR #733 (large external contribution) — awaiting maintainer decision on splitting; no repo-assist action needed unless requested.
- Issues #509 and #531 — no new human activity; do not re-comment unless new activity appears.
