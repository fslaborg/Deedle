# Deedle – Agent Guide

## Repository Overview

Deedle is an F# library for data manipulation providing efficient frame and series data structures (similar to Python's pandas). It supports missing values, aggregations, grouping, joining, statistics, and time-series alignment.

Key sub-projects:

| Project | Path |
|---|---|
| Core library | `src/Deedle/` |
| .NET Interactive formatting | `src/Deedle.Interactive/` |
| MathNet.Numerics integration | `src/Deedle.Math/` |
| Excel integration | `src/Deedle.Excel/` |

Tests live under `tests/`.

---

## Prerequisites

### .NET SDK
See `global.json` 

---

## Setup (one-time after cloning)

```bash
# 1. Restore .NET local tools (includes Paket)
dotnet tool restore

# 2. Restore NuGet packages via Paket
dotnet paket restore
```

Both steps are required before any build or test command.

---

## Building and Testing

### Linux / macOS

```bash
# Full build + tests
./build.sh

# Build only (skip tests)
./build.sh --no-tests
```

### Windows

```powershell
# Full build + tests
.\build.ps1

# Build only (skip tests)
.\build.ps1 -NoTests
```

### Individual projects

```bash
dotnet build src/Deedle/Deedle.fsproj -c Release
dotnet test tests/Deedle.Tests/Deedle.Tests.fsproj -c Release
```

---

## Versioning

The library version is set in `Directory.Build.props` at the repo root. Update `<Version>` there when releasing, and add a corresponding entry at the top of `RELEASE_NOTES.md`.

---

## CI Workflow

CI is defined in `.github/workflows/pull-requests.yml` (PRs) and `.github/workflows/push-master.yml` (pushes to `master`). Both run on **`ubuntu-latest`** and **`windows-latest`**.

The exact CI steps are:

```yaml
- dotnet tool restore
- dotnet paket restore
- ./build.sh          # or .\build.ps1 on Windows
```

A PR is considered passing when both the Windows and Ubuntu jobs succeed.

---

## Common Failure Modes

| Symptom | Likely cause |
|---|---|
| Build fails with "paket not found" | `dotnet tool restore` was not run |
| Paket errors about missing packages | `dotnet paket restore` was not run |
| `Deedle.RProvider.Plugin` compile errors | R is not installed or `R_HOME` is unset |
| Test failures in `Deedle.Tests` | Logic regression in `src/Deedle/` |
| Test failures in `Deedle.Math.Tests` | Logic regression in `src/Deedle.Math/` |

---

## Code Style and Conventions

- All source is **F#** (`.fs`) except `Deedle.CSharp.Tests` which is C#.
- Library projects target `netstandard2.0` / `netstandard2.1`; test and plugin projects target `net8.0`.
- Version is set in `Directory.Build.props` and `RELEASE_NOTES.md`.
