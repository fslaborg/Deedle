# Clean up the previously-cached NuGet packages.
# Lower-case is intentional (that's how nuget stores those packages).
Remove-Item -Recurse ~\.nuget\packages\deedle.interactive* -Force
Remove-Item -Recurse ~\.nuget\packages\deedle* -Force

# build and pack Interactive 
cd ../../
dotnet restore Deedle.Core.sln
dotnet build Deedle.Core.sln
dotnet pack -c Release -p:PackageVersion=0.0.0-dev -o "./pkg" Deedle.Core.sln
cd src/Deedle.Interactive
