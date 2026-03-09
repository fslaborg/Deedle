#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh [--no-tests]
NO_TESTS=false
for arg in "$@"; do
  case $arg in
    --no-tests) NO_TESTS=true ;;
  esac
done

dotnet build Deedle.sln -c Release

if [ "$NO_TESTS" = false ]; then
  dotnet test Deedle.sln -c Release --no-build
fi

dotnet pack Deedle.sln -c Release

dotnet fsdocs build --eval --parameters fsdocs-package-version 3.0.0

echo "--- Build complete ---"
