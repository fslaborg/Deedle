namespace Deedle 

open Deedle.Internal
open Deedle.Indices
open Deedle.Indices.Linear

// ------------------------------------------------------------------------------------------------
// F# friendly operations for creating vectors
// ------------------------------------------------------------------------------------------------

/// Set concrete IIndexBuilder implementation
///
/// [category:Vectors and indices]
[<AutoOpen>]
module ``F# IndexBuilder implementation`` =
  type IndexBuilder = 
    /// Returns concrete implementation for IVectorBuilder
    static member Instance = Deedle.Indices.Linear.LinearIndexBuilder.Instance