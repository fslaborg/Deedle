namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Deedle")>]
[<assembly: AssemblyProductAttribute("Deedle")>]
[<assembly: AssemblyDescriptionAttribute("Easy to use .NET library for data manipulation and scientific programming")>]
[<assembly: AssemblyVersionAttribute("1.1.2")>]
[<assembly: AssemblyFileVersionAttribute("1.1.2")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.1.2"
