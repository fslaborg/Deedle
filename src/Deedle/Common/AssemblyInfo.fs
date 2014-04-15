namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Deedle")>]
[<assembly: AssemblyProductAttribute("Deedle")>]
[<assembly: AssemblyDescriptionAttribute("Easy to use .NET library for data manipulation and scientific programming")>]
[<assembly: AssemblyVersionAttribute("1.0.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0.0")>]
()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.0"
