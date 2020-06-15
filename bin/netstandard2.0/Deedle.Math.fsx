#nowarn "211"
#I "packages/MathNet.Numerics/lib/net40/"
#I "packages/MathNet.Numerics.4.7.0/lib/net40/"
#I "packages/MathNet.Numerics.4.8.0-beta1/lib/net40/"
#I "packages/MathNet.Numerics.4.8.0-beta2/lib/net40/"
#I "packages/MathNet.Numerics.4.8.0/lib/net40/"
#I "packages/MathNet.Numerics.4.8.1/lib/net40/"
#I "packages/MathNet.Numerics.4.9.0/lib/net40/"
#I "packages/MathNet.Numerics.FSharp.4.9.1/lib/net40/"
#I "packages/MathNet.Numerics.FSharp.4.10.0/lib/net40/"
#I "packages/MathNet.Numerics.FSharp.4.11.0/lib/net40/"

#I "packages/MathNet.Numerics.FSharp/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.7.0/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.8.0-beta1/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.8.0-beta2/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.8.0/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.8.1/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.9.0/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.9.1/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.10.0/lib/net45/"
#I "packages/MathNet.Numerics.FSharp.4.11.0/lib/net45/"

#I "../packages/MathNet.Numerics/lib/net40/"
#I "../packages/MathNet.Numerics.4.7.0/lib/net40/"
#I "../packages/MathNet.Numerics.4.8.0-beta1/lib/net40/"
#I "../packages/MathNet.Numerics.4.8.0-beta2/lib/net40/"
#I "../packages/MathNet.Numerics.4.8.0/lib/net40/"
#I "../packages/MathNet.Numerics.4.8.1/lib/net40/"
#I "../packages/MathNet.Numerics.4.9.0/lib/net40/"
#I "../packages/MathNet.Numerics.FSharp.4.9.1/lib/net40/"
#I "../packages/MathNet.Numerics.FSharp.4.10.0/lib/net40/"
#I "../packages/MathNet.Numerics.FSharp.4.11.0/lib/net40/"

#I "../packages/MathNet.Numerics.FSharp/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.7.0/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.8.0-beta1/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.8.0-beta2/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.8.0/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.8.1/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.9.0/lib/net45/"
#I "../packages/MathNet.Numerics.FSharp.4.9.1/lib/net40/"
#I "../packages/MathNet.Numerics.FSharp.4.10.0/lib/net40/"
#I "../packages/MathNet.Numerics.FSharp.4.11.0/lib/net40/"

#I "../../packages/MathNet.Numerics/lib/net40/"
#I "../../packages/MathNet.Numerics.4.7.0/lib/net40/"
#I "../../packages/MathNet.Numerics.4.8.0-beta1/lib/net40/"
#I "../../packages/MathNet.Numerics.4.8.0-beta2/lib/net40/"
#I "../../packages/MathNet.Numerics.4.8.0/lib/net40/"
#I "../../packages/MathNet.Numerics.4.8.1/lib/net40/"
#I "../../packages/MathNet.Numerics.4.9.0/lib/net40/"
#I "../../packages/MathNet.Numerics.FSharp.4.9.1/lib/net40/"
#I "../../packages/MathNet.Numerics.FSharp.4.10.0/lib/net40/"
#I "../../packages/MathNet.Numerics.FSharp.4.11.0/lib/net40/"

#I "../../packages/MathNet.Numerics.FSharp/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.7.0/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.8.0-beta1/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.8.0-beta2/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.8.0/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.8.1/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.9.0/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.9.1/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.10.0/lib/net45/"
#I "../../packages/MathNet.Numerics.FSharp.4.11.0/lib/net45/"

#r "MathNet.Numerics.dll"
#r "MathNet.Numerics.FSharp.dll"

#nowarn "211"
// Standard NuGet or Paket location
#I "."
#I "lib/net45"

// Try various folders that people might like
#I "bin/net45"
#I "../bin/net45"
#I "../../bin/net45"
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
