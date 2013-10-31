@echo off
if not exist packages\FAKE\tools\Fake.exe ( 
  .nuget\nuget.exe install FAKE -OutputDirectory packages -ExcludeVersion -Prerelease
  REM we want to use whatever FSI.EXE is available on the current system
  REM hoping that this will be F# 3.1 which is needed to generate documentation
  del packages\FAKE\tools\fsi.exe*
  REM but we want to build the library using VS 2012 version of MS build
  REM because the RC1 (??) version produces broken DLLs that don't work with Nunit (???)
  copy misc\fake\FAKE.exe.config packages\FAKE\tools
)
packages\FAKE\tools\FAKE.exe build.fsx %*
pause
