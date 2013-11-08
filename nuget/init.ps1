# Nuget gives us some information in the following variables
param($installPath, $toolsPath, $package)

# Destination "RProvider-X.Y\lib" (where RProivder.dll lives)
$destPath = $installPath + "\..\RProvider.1.0.4\lib\"

# Copy the R provider plugin to the directory with RProvider.dll
# Source path that contains R type provider plugin DLLs
$srcPath = $installPath + "\lib\net40"
$files = Get-ChildItem $srcPath | where {$_.PSIsContainer -eq $False}
Foreach ($file in $files)
{  
    # This gets executed each time project is loaded, so skip files if they exist already
    Copy-Item $file.FullName ($destPath + $file.Name) -Force -ErrorAction SilentlyContinue
}

# And we also need to copy Deedle.dll to the R provider directory
# (otherwise it won't be able to load Deedle; this is silly)
# Source path that contains Deedle DLLs
$srcPath = $installPath.Replace(".RPlugin", "") + "\lib\net40"
$files = Get-ChildItem $srcPath | where {$_.PSIsContainer -eq $False}
Foreach ($file in $files)
{  
    # This gets executed each time project is loaded, so skip files if they exist already
    Copy-Item $file.FullName ($destPath + $file.Name) -Force -ErrorAction SilentlyContinue
}
