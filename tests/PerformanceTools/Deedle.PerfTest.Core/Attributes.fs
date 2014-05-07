namespace Deedle.PerfTest

open System

/// Marks a method (or an F# function) as a performance test.
/// The method is executed as part of performance analysis.
[<AttributeUsage(AttributeTargets.Method)>]
type PerfTestAttribute() = 
  inherit System.Attribute()
  /// Recommended number of iterations for running the test
  member val Iterations = 1 with get, set

