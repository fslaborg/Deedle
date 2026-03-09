# Usage: .\build.ps1 [-NoTests]
param(
    [switch]$NoTests
)

$ErrorActionPreference = "Stop"

dotnet build Deedle.sln -c Release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if (-not $NoTests) {
    dotnet test Deedle.sln -c Release --no-build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

dotnet pack Deedle.sln -c Release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

dotnet fsdocs build --eval --parameters fsdocs-package-version 3.0.0
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "--- Build complete ---"
