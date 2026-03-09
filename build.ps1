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

$version = ([xml](Get-Content Directory.Build.props)).Project.PropertyGroup.Version
# Don't fail the build if API doc generation fails
dotnet fsdocs build --eval --parameters fsdocs-package-version $version
if ($LASTEXITCODE -ne 0) { 
    Write-Host "Warning: API doc generation failed, but continuing build" -ForegroundColor Yellow 
    $LASTEXITCODE = 0
}

Write-Host "--- Build complete ---"
