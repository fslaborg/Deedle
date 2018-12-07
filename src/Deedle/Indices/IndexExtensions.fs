namespace Deedle 

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