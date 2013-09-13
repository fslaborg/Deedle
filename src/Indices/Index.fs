namespace FSharp.DataFrame

// --------------------------------------------------------------------------------------
// Indexing - index provides access to data in vector via keys. Optionally, the keys
// can be sorted. The index provides various operations for joining indices (etc.) that
// produce a new index, together with transformations to be applied on the data vector.
// Index is an interface and so you can define your own. 
// --------------------------------------------------------------------------------------

type Lookup = 
  | Exact = 0
  | NearestGreater = 1
  | NearestSmaller = 2

type Aggregation<'K> =
  | WindowSize of int * Boundary
  | ChunkSize of int * Boundary
  | WindowWhile of ('K -> 'K -> bool)
  | ChunkWhile of ('K -> 'K -> bool)

type Aggregation =
  static member WindowSize(size, boundary) = WindowSize(size, boundary)
  static member ChunkSize(size, boundary) = ChunkSize(size, boundary)
//  static member WindowWhile<'K>(Func

namespace FSharp.DataFrame.Indices

open FSharp.DataFrame
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Vectors

/// Specifies the boundary behavior of the `IIndexBuilder.GetRange` operation
type BoundaryBehavior = Inclusive | Exclusive

/// An interface that represents index mapping keys of type 'T to locations
/// of address Address.
type IIndex<'K when 'K : equality> = 
  abstract Keys : seq<'K>
  abstract Lookup : 'K * Lookup * (Address -> bool) -> OptionalValue<Address>  
  abstract Mappings : seq<'K * Address>
  abstract Range : Address * Address
  abstract KeyRange : 'K * 'K
  abstract Ordered : bool
  abstract Comparer : System.Collections.Generic.Comparer<'K>
  abstract Builder : IIndexBuilder

/// A builder represents various ways of constructing index
and IIndexBuilder =
  abstract Create : seq<'K> * Option<bool> -> IIndex<'K>
    
  abstract GetRange : 
    IIndex<'K> * option<'K * BoundaryBehavior> * option<'K * BoundaryBehavior> * VectorConstruction ->
    IIndex<'K> * VectorConstruction 

  abstract Union : 
    IIndex<'K> * IIndex<'K> * VectorConstruction * VectorConstruction -> 
    IIndex<'K> * VectorConstruction * VectorConstruction

  abstract Intersect :
    IIndex<'K> * IIndex<'K> * VectorConstruction * VectorConstruction -> 
    IIndex<'K> * VectorConstruction * VectorConstruction

  abstract Append :
    IIndex<'K> * IIndex<'K> * VectorConstruction * VectorConstruction * IVectorValueTransform -> 
    IIndex<'K> * VectorConstruction

  abstract Reindex :
    IIndex<'K> * IIndex<'K> * Lookup * VectorConstruction -> VectorConstruction

  abstract WithIndex :
    IIndex<'K> * (Address -> OptionalValue<'TNewKey>) * VectorConstruction -> 
    IIndex<'TNewKey> * VectorConstruction

  abstract DropItem : IIndex<'K> * 'K * VectorConstruction -> 
    IIndex<'K> * VectorConstruction 

  abstract OrderIndex : IIndex<'K> * VectorConstruction ->
    IIndex<'K> * VectorConstruction

  abstract Aggregate : IIndex<'K> * Aggregation<'K> * VectorConstruction *
    (DataSegmentKind * IIndex<'K> * VectorConstruction -> OptionalValue<'R>) *
    (DataSegmentKind * IIndex<'K> * VectorConstruction -> 'TNewKey) -> IIndex<'TNewKey> * IVector<'R> // Returning vector might be too concrete?

  abstract GroupBy : IIndex<'K> * ('K -> 'TNewKey) * VectorConstruction *
    ('TNewKey * IIndex<'K> * VectorConstruction -> OptionalValue<'R>) -> IIndex<'TNewKey> * IVector<'R> // Returning vector might be too concrete?
