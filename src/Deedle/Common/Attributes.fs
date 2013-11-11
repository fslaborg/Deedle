namespace FSharp.Data

// Make the internals visible to the Testing library
// (This is only used in limited way - so that the tests
// can be also executed from F# interactive)
[<assembly:System.Runtime.CompilerServices.InternalsVisibleTo("Deedle.Tests")>]
// The R plugin uses internals for more efficient conversions
[<assembly:System.Runtime.CompilerServices.InternalsVisibleTo("Deedle.RProvider.Plugin")>]
// This is required so that C# extension methods work
[<assembly: System.Runtime.CompilerServices.Extension>]
do ()