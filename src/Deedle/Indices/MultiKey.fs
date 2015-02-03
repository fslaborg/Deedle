namespace Deedle

// ------------------------------------------------------------------------------------------------
// Support for multi-key indexing
// ------------------------------------------------------------------------------------------------

/// Represents a special lookup. This can be used to support hierarchical or duplicate keys
/// in an index. A key type `K` can come with associated `ICustomLookup<K>` to provide 
/// customized pattern matching (equality testing) 
///
/// [category:Parameters and results of various operations]
type ICustomLookup<'K> = 
  /// Tests whether a specified key matches the current key (for example, in hierarchical indexing
  /// based on tuples, if the current key represents a pair (1, _) then the value (1, 42) would match).
  abstract Matches : 'K -> bool

// ------------------------------------------------------------------------------------------------
// Multi-key indexing internals
// ------------------------------------------------------------------------------------------------

namespace Deedle.Keys

open Deedle
open Microsoft.FSharp.Reflection

/// Represents a special hierarchical key. This is mainly used in pretty printing (where we want to 
/// get parts of the keys based on levels. `CustomKey.Get` provides a way of getting `ICustomKey`.
type ICustomKey =
  /// Returns the number of levels of a hierarchical key. For example, a tuple (1, 42) has 2 levels.
  /// This is used for pretty printing only.
  abstract Levels : int
  /// Gets a value at the specified level (the levels are indexed from 1). 
  /// This is used for pretty printing only. If `Levels=1` then this method is not
  /// called and the pretty printer invokes `ToString` on the whole object instead.
  abstract GetLevel : int -> obj
  /// Gets values of the key at all levels
  abstract GetLevels : unit -> obj[]


/// Helper type that can be used to get `ICustomKey` for any object (including objects
/// that actually implement the interface and tuples)
type CustomKey private() =
  static let (|AnyTuple|_|) (v:obj) =
    if FSharpType.IsTuple(v.GetType()) then
      let fields = FSharpValue.GetTupleFields(v)
      Some(List.ofSeq (fields.[0 .. fields.Length - 2]), fields.[fields.Length - 1])
    else None

  // Implement custom key behavior
  static member GetForTuple(tuple:obj) =
    { new ICustomKey with

      /// Calculate the level (number of elements) in a tuple key. We assume that tuples
      /// are either T1*T2*T3*... or that they are T1*(T2*(T3*...)). This needs reflection
      /// and so it is probably slow.
      member x.Levels =
        let rec level (typ:System.Type) =
          if FSharpType.IsTuple(typ) then 
            let elems = FSharpType.GetTupleElements(typ)
            elems.Length - 1 + (level elems.[elems.Length - 1])
          else 1
        level (tuple.GetType())

      /// Assumes the structure discussed in the comment for `Levels`. Extracts
      /// values at all levels and returns them as object array.
      member x.GetLevels() =
        let rec levels inp = seq {
          match inp with
          | AnyTuple(vals, last) -> 
              yield! vals
              yield! levels last 
          | v -> yield v }
        levels tuple |> Array.ofSeq

      // Get specified level
      member x.GetLevel(n) = 
        let all = x.GetLevels()
        all.[n] }

  /// Returns `ICustomKey` instance for the specified key. If the specified key implements
  /// `ICustomKey`, then it is just returned; if it is a tuple, we use special key for tuples
  /// otherwise, it must be a primitive key so we just return it
  static member Get(key:obj) =
    if key :? ICustomKey then key :?> ICustomKey
    elif FSharpType.IsTuple(key.GetType()) then
      CustomKey.GetForTuple(key) 
    else 
      { new ICustomKey with 
          member x.GetLevel(i) = if i > 0 then invalidOp "level" else key
          member x.GetLevels() = [| key |]
          member x.Levels = 1 }

/// Implements a simple lookup that matches any multi-level key against a specified array of
/// optional objects (that represent missing/set parts of a key)
type SimpleLookup<'T>(patterns) = 
  interface ICustomLookup<'T> with
    /// Dynamically tests whether this pattern matches with another key. 
    /// For example, `[|None, Some 1|]` matches `(42, 1)`, but if the second 
    /// values differed, then they would not match.
    member x.Matches(value) = 
      let cust = CustomKey.Get(value)
      let keys = cust.GetLevels() 
      if keys.Length <> Array.length patterns then invalidOp "SimpleLookup.Matches: Key has invalid number of levels"
      (patterns, keys) ||> Seq.forall2 (fun pat key ->
          match pat with None -> true | Some k -> k = key)

// ------------------------------------------------------------------------------------------------
// F#-friendly functions for creating multi-level keys and lookups
// ------------------------------------------------------------------------------------------------

namespace Deedle
open Deedle.Keys

/// F#-friendly functions for creating multi-level keys and lookups
///
/// [category:Parameters and results of various operations]
[<AutoOpen>]
module MultiKeyExtensions =

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a two-level hierarchical key.
  let Lookup1Of2 k = SimpleLookup [| Some (box k); None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a two-level hierarchical key.
  let Lookup2Of2 k = SimpleLookup [| None; Some (box k) |] :> ICustomLookup<_>
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a two-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf2 k1 k2 = 
    SimpleLookup [| Option.map box k1; Option.map box k2 |] :> ICustomLookup<_>

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a three-level hierarchical key.
  let Lookup1Of3 k = SimpleLookup [| Some (box k); None; None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a three-level hierarchical key.
  let Lookup2Of3 k = SimpleLookup [| None; Some (box k); None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// third element of a three-level hierarchical key.
  let Lookup3Of3 k = SimpleLookup [| None; None; Some (box k) |] :> ICustomLookup<_>
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a three-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf3 k1 k2 k3 = 
    SimpleLookup [| Option.map box k1; Option.map box k2; Option.map box k3 |] :> ICustomLookup<_>

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a four-level hierarchical key.
  let Lookup1Of4 k = SimpleLookup [| Some (box k); None; None; None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a four-level hierarchical key.
  let Lookup2Of4 k = SimpleLookup [| None; Some (box k); None; None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// third element of a four-level hierarchical key.
  let Lookup3Of4 k = SimpleLookup [| None; None; Some (box k); None |] :> ICustomLookup<_>
  /// Creates a hierarchical key lookup that allows matching on the 
  /// fourth element of a four-level hierarchical key.
  let Lookup4Of4 k = SimpleLookup [| None; None; None; Some (box k) |] :> ICustomLookup<_>
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a four-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf4 k1 k2 k3 k4 = 
    SimpleLookup [| Option.map box k1; Option.map box k2; Option.map box k3; Option.map box k4 |] :> ICustomLookup<_>


/// Module with helper functions for extracting values from hierarchical tuples
///
/// [category:Primitive types and values]
module Pair =
  /// Returns the first value of a two-level hierarchical tuple
  let get1Of2(v, _) = v
  /// Returns the second value of a two-level hierarchical tuple
  let get2Of2(_, v) = v

  /// Returns the first value of a three-level hierarchical tuple
  let get1Of3(v, _, _) = v
  /// Returns the second value of a three-level hierarchical tuple
  let get2Of3(_, v, _) = v
  /// Returns the third value of a three-level hierarchical tuple
  let get3Of3(_, _, v) = v

  /// Returns the first and the second value of a three-level hierarchical tuple
  let get1And2Of3(v1, v2, _) = v1, v2
  /// Returns the second and the third value of a three-level hierarchical tuple
  let get2And3Of3(_, v1, v2) = v1, v2
  /// Returns the first and the third value of a three-level hierarchical tuple
  let get1And3Of3(v1, _, v2) = v1, v2

  /// Returns the first value of a four-level hierarchical tuple
  let get1Of4(v, _, _, _) = v
  /// Returns the second value of a four-level hierarchical tuple
  let get2Of4(_, v, _, _) = v
  /// Returns the third value of a four-level hierarchical tuple
  let get3Of4(_, _, v, _) = v
  /// Returns the fourth value of a four-level hierarchical tuple
  let get4Of4(_, _, _, v) = v

  /// Flatten a two-level nested tuple into a flat tuple of 3 elements
  let flatten3(v1, (v2, v3)) = (v1, v2, v3)
  /// Flatten a two-level nested tuple into a flat tuple of 4 elements
  let flatten4(v1, (v2, (v3, v4))) = (v1, v2, v3, v4)