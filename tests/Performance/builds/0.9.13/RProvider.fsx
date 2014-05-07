#nowarn "211"
// Try including various folders where RProvider might be (version updated by FAKE)
#I "../../bin"
#I "../bin"
#I "bin"
#I "lib"
#I "packages/RProvider.1.0.7-alpha/lib"
#I "../packages/RProvider.1.0.7-alpha/lib"
#I "../../packages/RProvider.1.0.7-alpha/lib"
#I "../../../packages/RProvider.1.0.7-alpha/lib"
// Reference RProvider and RDotNet (which should be copied to the same directory)
#r "RDotNet.dll"
#r "RProvider.dll"
#r "RProvider.Runtime.dll"
open RProvider

do fsi.AddPrinter(fun (synexpr:RDotNet.SymbolicExpression) -> synexpr.Print())

