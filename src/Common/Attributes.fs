namespace FSharp.Data

// Make the internals visible to the Testing library
// (This is only used in limited way - so that the tests
// can be also executed from F# interactive)
[<assembly:System.Runtime.CompilerServices.InternalsVisibleTo("FSharp.DataFrame.Tests")>]
do ()