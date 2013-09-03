namespace FSharp.DataFrame

// --------------------------------------------------------------------------------------
// Indexing - index provides access to data in vector via keys. Optionally, the keys
// can be sorted. The index provides various operations for joining indices (etc.) that
// produce a new index, together with transformations to be applied on the data vector.
// Index is an interface and so you can define your own. 
// --------------------------------------------------------------------------------------

type LookupSemantics = 
  | Exact = 0
  | NearestGreater = 1
  | NearestSmaller = 2

namespace FSharp.DataFrame.Indices

open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Vectors

/// An interface that represents index mapping keys of type 'TKey to locations
/// of address 'TAddress.
type IIndex<'TKey, 'TAddress when 'TKey : equality and 'TAddress : equality> = 
  abstract Keys : seq<'TKey>
  abstract Lookup : 'TKey * LookupSemantics -> OptionalValue<'TAddress>  
  abstract Mappings : seq<'TKey * 'TAddress>
  abstract Range : 'TAddress * 'TAddress
  abstract Ordered : bool
  abstract Comparer : System.Collections.Generic.Comparer<'TKey>
  
/// A builder represents various ways of constructing index
type IIndexBuilder =
  abstract Create : seq<'TKey> * Option<bool> -> IIndex<'TKey, 'TAddress>
    
  abstract GetRange : 
    IIndex<'TKey, 'TAddress> * option<'TKey> * option<'TKey> * VectorConstruction<'TAddress> ->
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> 

  abstract Union : 
    IIndex<'TKey, 'TAddress> * IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress>

  abstract Intersect :
    IIndex<'TKey, 'TAddress> * IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress>

  abstract Append :
    IIndex<'TKey, 'TAddress> * IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> * VectorConstruction<'TAddress> * IVectorValueTransform -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress>

  abstract Reindex :
    IIndex<'TKey, 'TAddress> * IIndex<'TKey, 'TAddress> * LookupSemantics * VectorConstruction<'TAddress> -> VectorConstruction<'TAddress>

  abstract WithIndex :
    IIndex<'TKey, 'TAddress> * ('TAddress -> OptionalValue<'TNewKey>) * VectorConstruction<'TAddress> -> 
    IIndex<'TNewKey, 'TAddress> * VectorConstruction<'TAddress>

  abstract DropItem : IIndex<'TKey, 'TAddress> * 'TKey * VectorConstruction<'TAddress> -> 
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> 

  abstract OrderIndex : IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress> ->
    IIndex<'TKey, 'TAddress> * VectorConstruction<'TAddress>