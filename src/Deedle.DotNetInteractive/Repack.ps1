# Clean up the previously-cached NuGet packages.
# Lower-case is intentional (that's how nuget stores those packages).
Remove-Item -Recurse ~\.nuget\packages\deedle.dotnetinteractive* -Force
Remove-Item -Recurse ~\.nuget\packages\deedle* -Force

# build and pack DotNetInteractive 
cd ../../
dotnet restore Deedle.sln
dotnet build Deedle.sln
dotnet pack -c Release -p:PackageVersion=0.0.0-dev -o "./pkg" Deedle.sln
cd src/Deedle.DotNetInteractive
