namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Deedle")>]
[<assembly: AssemblyProductAttribute("Deedle")>]
[<assembly: AssemblyDescriptionAttribute("Easy to use .NET library for data manipulation and scientific programming")>]
[<assembly: AssemblyVersionAttribute("0.9.11")>]
[<assembly: AssemblyFileVersionAttribute("0.9.11")>]
()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.9.11"
