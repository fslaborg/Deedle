namespace Deedle

// ------------------------------------------------------------------------------------------------
// F# friendly operations for creating vectors
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Set concrete IIndexBuilder implementation
/// </summary>
///
/// <category>Vectors and indices</category>
[<AutoOpen>]
module ``F# IndexBuilder implementation`` =
  type IndexBuilder =
    /// Returns concrete implementation for IVectorBuilder
    static member Instance = Deedle.Indices.Linear.LinearIndexBuilder.Instance
