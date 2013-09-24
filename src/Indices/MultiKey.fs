namespace FSharp.DataFrame

// ------------------------------------------------------------------------------------------------
// Support for multi-key indexing
// ------------------------------------------------------------------------------------------------

/// Represents a special key. This can be used to support hierarchical or duplicate keys
/// in an index. A type `T` should implement `ICustomKey<T>` to provide customized pattern
/// matching (equality testing) and getting information about key (values at specific levels).
type ICustomKey<'K> = 
  /// Tests whether a specified key matches the current key (for example, in hierarchical indexing
  /// based on tuples, if the current key represents a pair (1, _) then the value (1, 42) would match).
  abstract Matches : ICustomKey<'K> -> bool
  /// Returns the number of levels of a hierarchical key. For example, a tuple (1, 42) has 2 levels.
  /// This is used for pretty printing only.
  abstract Levels : int
  /// Gets a value at the specified level (the levels are indexed from 1). 
  /// This is used for pretty printing only. If `Levels=1` then this method is not
  /// called and the pretty printer invokes `ToString` on the whole object instead.
  abstract GetLevel : int -> obj


/// A wrapper which specifies that a hierarchical lookup should be used when indexing into a 
/// series or data frame. For example, `Series<K, V>` provides an indexer that takes `K` for
/// exact lookup and an indexer that takes `HierarchicalLookup<K>` and behaves differently
/// (uses the `ICustomKey<K>` interface implemented by `K`).
///
/// The type is not frequently created directly, because functions such as `Level1Of2` 
/// create it implicitly, but you can construct values of this type, using non-generic 
/// `HierarchicalLookup.Create`.
type HierarchicalLookup<'K> = 
  internal | HL of 'K
  /// Returns the key to be used for the lookup
  member x.Key = let (HL k) = x in k
  /// Returns the key as `ICustomKey<K>`
  member x.CustomKey = unbox<ICustomKey<'K>> x.Key


/// Non-generic type that simplifies the construction of `HierarchicalLookup<K>`. See the
/// documentation for `HierarchicalLookup<K>` for more details.
type HierarchicalLookup = 
  /// Create a hierarchical lookup value for a specified key
  static member Create(k) = HL k


/// Internal type that represents a combined (hierarchical) key. This 
/// is a non-generic interface used in the implementation of `MultiKey<K1, K2>`.
type IMultiKey = 
  /// Returns the two keys hold by `MultiKey<K1, K2>` as objects
  abstract Keys : option<obj> * option<obj>


/// Internal module with active patterns for working with `IMultiKey`.
[<AutoOpen>]
module internal IMultiKeyExtensions = 
  /// Succeeds when a given object is `IMultiKey`
  let (|IMultiKey|_|) (obj:obj) = 
    match obj with :? IMultiKey as mk -> Some(mk.Keys) | _ -> None


/// Represents a hierarchical (multi-level) key. To create values representing keys
/// with values, use associated functions `MultiKey`, `MultiKey3`, etc. To create
/// patterns that can be used for lookup, use `Level1Of2`, `Level2Of2`, etc.
type MultiKey<'K1, 'K2> = 
  internal | MK of option<'K1> * option<'K2>
  // Non-generic interface is used when recursively walking over the key
  interface IMultiKey with
    member x.Keys = 
      let (MK(l, r)) = x in Option.map box l, Option.map box r
  /// Print the key as a tuple
  override x.ToString() = 
    match x with
    | MK(None, None) -> "_"
    | MK(None, Some k) -> sprintf "(_, %A)" k
    | MK(Some k, None) -> sprintf "(%A, _)" k
    | MK(Some k1, Some k2) -> sprintf "%A" (k1, k2)

  // Implement custom key behavior
  interface ICustomKey<MultiKey<'K1, 'K2>> with

    /// Assumes that keys are constructed as `MultiKey<T1, MultiKey<T2, ...>>` and
    /// calculates the level of nesting (dynamically using reflection).
    member x.Levels =
      let rec level (typ:System.Type) =
        if typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<MultiKey<_, _>> then 
          level (typ.GetGenericArguments().[1])
        else 1
      1 + level (typeof<'K2>)

    /// Assumes the structure discussed in the comment for `Levels`. Extracts
    /// the value at the specified level and returns it as object.
    member x.GetLevel(n) =
      let rec level = function
        | 1, IMultiKey(Some v, _) -> v
        | 1, IMultiKey(None, _) -> failwith "Unexpected multi-key format"
        | 1, v -> v
        | n, IMultiKey(_, Some v) -> level (n-1, v)
        | _ -> failwith "Unexpected multi-key format"
      level (n, x)

    /// Dynamically tests whether this key matches with another key. For example,
    /// `MK(None, Some 1)` matches `MK(Some 42, Some 1)`, but if the second values
    /// differed, then they would not match.
    member x.Matches(another) = 
      let rec matches template value = 
        match template, value with
        | IMultiKey(Some v1, _), IMultiKey(Some v2, None) 
        | IMultiKey(_, Some v1), IMultiKey(None, Some v2)
        | IMultiKey(Some v1, None), IMultiKey(Some v2, _) 
        | IMultiKey(None, Some v1), IMultiKey(_, Some v2) -> matches v1 v2
        | IMultiKey(None, None), _ 
        | _, IMultiKey(None, None) -> true
        | IMultiKey(Some l1, Some r1), IMultiKey(Some l2, Some r2) -> matches l1 l2 && matches r1 r2
        | o1, o2 -> o1.Equals(o2)
      matches x another

type ILevelReader<'Original, 'K, 'New> = 
  abstract GetKey : 'Original -> 'K
  abstract DropKey : 'Original -> 'New

// ------------------------------------------------------------------------------------------------
// F#-friendly functions for creating multi-level keys and lookups
// ------------------------------------------------------------------------------------------------

/// F#-friendly functions for creating multi-level keys and lookups
[<AutoOpen>]
module MultiKeyExtensions =
  /// Creates a two-level key that can be used for hierarchical indexing into series and frames.
  let MultiKey(l, r) = MK(Some l, Some r)
  /// Extracts individual components from a specified two-level hierarchical key.
  let (|MultiKey|) = function
    | MK(Some l, Some r) -> l, r
    | _ -> failwith "Multi key contains a hole"

  /// Creates a three-level key that can be used for hierarchical indexing into series and frames.
  let MultiKey3(v1, v2, v3) = MK(Some v1, Some (MK(Some v2, Some v3)))
  /// Extracts individual components from a specified three-level hierarchical key.
  let (|MultiKey3|) = function
    | MK(Some v1, Some (MK(Some v2, Some v3))) -> v1,v2,v3
    | _ -> failwith "Multi key contains a hole"

  /// Creates a four-level key that can be used for hierarchical indexing into series and frames.
  let MultiKey4(v1, v2, v3, v4) = MK(Some v1, Some (MK(Some v2, Some (MK(Some v3, Some v4)))))
  /// Extracts individual components from a specified four-level hierarchical key.
  let (|MultiKey4|) = function
    | MK(Some v1, Some (MK(Some v2, Some (MK(Some v3, Some v4))))) -> v1,v2,v3,v4
    | _ -> failwith "Multi key contains a hole"

  let Level1Of2 = 
    { new ILevelReader<MultiKey<'T1, 'T2>, _, _> with
        member x.GetKey(MultiKey(l, _)) = l
        member x.DropKey(MultiKey(_, r)) = r }
  let Level2Of2 = 
    { new ILevelReader<MultiKey<'T1, 'T2>, _, _> with
        member x.GetKey(MultiKey(_, r)) = r
        member x.DropKey(MultiKey(l, _)) = l }
  let Level1Of3 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, 'T3>>, _, _> with
        member x.GetKey(MultiKey3(k, _, _)) = k
        member x.DropKey(MultiKey3(_, k1, k2)) = MultiKey(k1, k2) }
  let Level2Of3 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, 'T3>>, _, _> with
        member x.GetKey(MultiKey3(_, k, _)) = k
        member x.DropKey(MultiKey3(k1, _, k2)) = MultiKey(k1, k2) }
  let Level3Of3 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, 'T3>>, _, _> with
        member x.GetKey(MultiKey3(_, _, k)) = k
        member x.DropKey(MultiKey3(k1, k2, _)) = MultiKey(k1, k2) }
  let Level1Of4 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, MultiKey<'T3, 'T4>>>, _, _> with
        member x.GetKey(MultiKey4(k, _, _, _)) = k
        member x.DropKey(MultiKey4(_, k1, k2, k3)) = MultiKey3(k1, k2, k3) }
  let Level2Of4 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, MultiKey<'T3, 'T4>>>, _, _> with
        member x.GetKey(MultiKey4(_, k, _, _)) = k
        member x.DropKey(MultiKey4(k1, _, k2, k3)) = MultiKey3(k1, k2, k3) }
  let Level3Of4 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, MultiKey<'T3, 'T4>>>, _, _> with
        member x.GetKey(MultiKey4(_, _, k, _)) = k
        member x.DropKey(MultiKey4(k1, k2, _, k3)) = MultiKey3(k1, k2, k3) }
  let Level4Of4 = 
    { new ILevelReader<MultiKey<'T1, MultiKey<'T2, MultiKey<'T3, 'T4>>>, _, _> with
        member x.GetKey(MultiKey4(_, _, _, k)) = k
        member x.DropKey(MultiKey4(k1, k2, k3, _)) = MultiKey3(k1, k2, k3) }

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a two-level hierarchical key.
  let Lookup1Of2 k = HL(MK(Some k, None))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a two-level hierarchical key.
  let Lookup2Of2 k = HL(MK(None, Some k))
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a two-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf2 k1 k2 = HL(MK(k1, k2))

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a three-level hierarchical key.
  let Lookup1Of3 k = HL(MK(Some k, Some(MK(None, None))))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a three-level hierarchical key.
  let Lookup2Of3 k = HL(MK(None, Some(MK(Some k, None))))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// third element of a three-level hierarchical key.
  let Lookup3Of3 k = HL(MK(None, Some(MK(None, Some k))))
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a three-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf3 k1 k2 k3 = HL(MK(k1, Some (MK(k2, k3))))

  /// Creates a hierarchical key lookup that allows matching on the 
  /// first element of a four-level hierarchical key.
  let Lookup1Of4 k = HL(MK(Some k, Some(MK(None, Some(MK(None, None))))))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// second element of a four-level hierarchical key.
  let Lookup2Of4 k = HL(MK(None, Some(MK(Some k, Some(MK(None, None))))))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// third element of a four-level hierarchical key.
  let Lookup3Of4 k = HL(MK(None, Some(MK(None, Some(MK(Some k, None))))))
  /// Creates a hierarchical key lookup that allows matching on the 
  /// fourth element of a four-level hierarchical key.
  let Lookup4Of4 k = HL(MK(None, Some(MK(None, Some(MK(None, Some k))))))
  /// Creates an arbitrary lookup key that allows matching on elements
  /// of a four-level hierarchical index. Specify `None` to ignore a level
  /// or `Some k` to require match on a given level.
  let LookupAnyOf4 k1 k2 k3 k4 = HL(MK(k1, Some (MK(k2, Some (MK(k3, k4))))))

/// Module with helper functions for extracting values from hierarchical keys
module Key =
  /// Returns the first value of a two-level hierarchical key
  let key1Of2(MultiKey(v, _)) = v
  /// Returns the second value of a two-level hierarchical key
  let key2Of2(MultiKey(_, v)) = v

  /// Returns the first value of a three-level hierarchical key
  let key1Of3(MultiKey3(v, _, _)) = v
  /// Returns the second value of a three-level hierarchical key
  let key2Of3(MultiKey3(_, v, _)) = v
  /// Returns the third value of a three-level hierarchical key
  let key3Of3(MultiKey3(_, _, v)) = v

  /// Returns the first value of a four-level hierarchical key
  let key1Of4(MultiKey4(v, _, _, _)) = v
  /// Returns the second value of a four-level hierarchical key
  let key2Of4(MultiKey4(_, v, _, _)) = v
  /// Returns the third value of a four-level hierarchical key
  let key3Of4(MultiKey4(_, _, v, _)) = v
  /// Returns the fourth value of a four-level hierarchical key
  let key4Of4(MultiKey4(_, _, _, v)) = v