#nowarn "211"
#I "packages/MathNet.Numerics/lib/netstandard2.0/"
#I "packages/MathNet.Numerics.FSharp/lib/netstandard2.0/"
#I "../packages/MathNet.Numerics/lib/netstandard2.0/"
#I "../packages/MathNet.Numerics.FSharp/lib/netstandard2.0/"
#I "../../packages/MathNet.Numerics/lib/netstandard2.0/"
#I "../../packages/MathNet.Numerics.FSharp/lib/netstandard2.0/"
#r "MathNet.Numerics.dll"
#r "MathNet.Numerics.FSharp.dll"

#nowarn "211"
// Standard NuGet or Paket location
#I "."
#I "lib/netstandard2.0"

// Try various folders that people might like
#I "bin/netstandard2.0"
#I "../bin/netstandard2.0"
#I "../../bin/netstandard2.0"
#I "lib"
// Reference Deedle
#r "Deedle.Math.dll"

open MathNet.Numerics
open MathNet.Numerics.LinearAlgebra

fsi.AddPrinter(fun (matrix:Matrix<float>) -> matrix.ToString())
fsi.AddPrinter(fun (matrix:Matrix<float32>) -> matrix.ToString())
fsi.AddPrinter(fun (matrix:Matrix<complex>) -> matrix.ToString())
fsi.AddPrinter(fun (matrix:Matrix<complex32>) -> matrix.ToString())
fsi.AddPrinter(fun (vector:Vector<float>) -> vector.ToString())
fsi.AddPrinter(fun (vector:Vector<float32>) -> vector.ToString())
fsi.AddPrinter(fun (vector:Vector<complex>) -> vector.ToString())
fsi.AddPrinter(fun (vector:Vector<complex32>) -> vector.ToString())
