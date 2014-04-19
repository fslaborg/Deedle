#!/bin/bash
if [ ! -f packages/FAKE/tools/Fake.exe ]; then
  mono --runtime=v4.0 .NuGet/NuGet.exe install FAKE -OutputDirectory packages -ExcludeVersion
fi
mono --runtime=v4.0 packages/FAKE/tools/FAKE.exe build.fsx -d:MONO $@
