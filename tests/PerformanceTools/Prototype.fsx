#r "../../packages/FSharp.Compiler.Service.0.0.44/lib/net40/FSharp.Compiler.Service.dll"

open System
open System.IO
open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

let scs = SimpleSourceCodeServices()

(* 
let references = 
  [ @"C:\Tomas\Public\Deedle\packages\FsCheck.0.9.1.0\lib\net40-Client\FsCheck.dll"
    @"C:\Tomas\Public\Deedle\packages\FSharp.Data.2.0.5\lib\net40\FSharp.Data.dll"
    @"C:\Tomas\Public\Deedle\packages\MathNet.Numerics.3.0.0-beta01\lib\net40\MathNet.Numerics.dll"
    @"C:\Tomas\Public\Deedle\packages\NUnit.2.6.3\lib\nunit.framework.dll"
    @"C:\Tomas\Public\Deedle\tests\PerformanceTools\bin\Deedle.PerfTest.Core.dll" ]
*)

let sources = 
  [ @"C:\Tomas\Public\Deedle\tests\Common\FsUnit.fs" ]

let references = 
  [ yield! Directory.GetFiles(@"C:\Tomas\Public\Deedle\tests\Performance\builds\0.9.13", "*.dll")
    yield @"C:\Tomas\Public\Deedle\tests\PerformanceTools\bin\Deedle.PerfTest.Core.dll" ]

let compile file = 
  let file = @"C:\Tomas\Public\Deedle\tests\Deedle.Tests\Performance.fs"
  let tempDll = Path.GetTempFileName() + ".dll"
  let commandLine = 
    [| yield "fsc.exe"
       yield "-a"
       yield! [ "-o"; tempDll ]
       for ref in references do 
          if ref.Contains("DesignTime") |> not then
            yield! [ "-r"; ref ]
       for src in sources do yield src
       yield file |]
  let errors1, exitCode1 = scs.Compile(commandLine)
  ()

