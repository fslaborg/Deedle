@echo off
if not exist tools\FAKE\tools\Fake.exe ( 
  .nuget\nuget.exe install FAKE -OutputDirectory tools -ExcludeVersion -Prerelease
)
tools\FAKE\tools\FAKE.exe build.fsx %*
pause
