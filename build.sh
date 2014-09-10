#!/bin/bash
if test "$OS" = "Windows_NT"
then
  # use .Net
  .nuget/NuGet.exe install FAKE -OutputDirectory packages -ExcludeVersion
  packages/FAKE/tools/FAKE.exe build.fsx $@
else
  # use mono
  mono .nuget/NuGet.exe install FAKE -OutputDirectory packages -ExcludeVersion
  mono packages/FAKE/tools/FAKE.exe $@ --fsiargs -d:MONO build.fsx
fi