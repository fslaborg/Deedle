namespace Deedle

// --------------------------------------------------------------------------------------
// Operations and types related to joining - used by both Series and Frame
// --------------------------------------------------------------------------------------

open Deedle
open Deedle.Indices
open Deedle.Vectors

/// This enumeration specifies joining behavior for `Join` method provided
/// by `Series` and `Frame`. Outer join unions the keys (and may introduce
/// missing values), inner join takes the intersection of keys; left and
/// right joins take the keys of the first or the second series/frame.
type JoinKind = 
  /// Combine the keys available in both structures, align the values that
  /// are available in both of them and mark the remaining values as missing.
  | Outer = 0
  /// Take the intersection of the keys available in both structures and align the 
  /// values of the two structures. The resulting structure cannot contain missing values.
  | Inner = 1
  /// Take the keys of the left (first) structure and align values from the right (second)
  /// structure with the keys of the first one. Values for keys not available in the second
  /// structure will be missing.
  | Left = 2
  /// Take the keys of the right (second) structure and align values from the left (first)
  /// structure with the keys of the second one. Values for keys not available in the first
  /// structure will be missing.
  | Right = 3


// --------------------------------------------------------------------------------------
// Helpers related to join implementations
// --------------------------------------------------------------------------------------

/// [omit]
/// Implements various helpers that are used by Join operations
module internal JoinHelpers = 

  /// When doing exact join on ordered indices, restrict the new index
  /// so that we do not have to load all data for lazy indices
  let restrictToRowIndex lookup (restriction:IIndex<_>) (sourceIndex:IIndex<_>) vector = 
    if lookup = Lookup.Exact && 
       restriction.IsOrdered && sourceIndex.IsOrdered &&
       not restriction.IsEmpty then
      let min, max = restriction.KeyRange
      sourceIndex.Builder.GetRange(sourceIndex, Some(min, BoundaryBehavior.Inclusive), Some(max, BoundaryBehavior.Inclusive), vector)
    else sourceIndex, vector

  /// When using fancy lookup, first fill values in the vector, before doing the join
  let fillMissing vector lookup = 
    match lookup with
    | Lookup.NearestSmaller -> Vectors.FillMissing(vector, VectorFillMissing.Direction Direction.Forward)
    | Lookup.NearestGreater -> Vectors.FillMissing(vector, VectorFillMissing.Direction Direction.Backward)
    | Lookup.Exact | _ -> vector

  /// Create transformation on indices/vectors representing the join operation
  let createJoinTransformation 
        (indexBuilder:IIndexBuilder) kind lookup (thisIndex:IIndex<_>) 
        (otherIndex:IIndex<_>) vector1 vector2 =
    if thisIndex = otherIndex && lookup = Lookup.Exact then
      thisIndex, vector1, vector2
    else 
      // Inner/outer join only makes sense with exact lookup
      if lookup <> Lookup.Exact && kind = JoinKind.Inner then
        invalidOp "Join/Zip - Inner join can only be used with Lookup.Exact."
      if lookup <> Lookup.Exact && kind = JoinKind.Outer then
        invalidOp "Join/Zip - Outer join can only be used with Lookup.Exact."
      // Only Lookup.Exact makes sense for unordered series
      if not (thisIndex.IsOrdered && otherIndex.IsOrdered) && lookup <> Lookup.Exact then
        invalidOp "Join/Zip - Lookup can be only used when joining/zipping ordered series/frames."

      match kind with 
      | JoinKind.Inner ->
          indexBuilder.Intersect( (thisIndex, vector1), (otherIndex, vector2) )
      | JoinKind.Left ->
          let otherRowIndex, vector2 = restrictToRowIndex lookup thisIndex otherIndex vector2
          let vector2 = fillMissing vector2 lookup
          let otherRowCmd = indexBuilder.Reindex(otherRowIndex, thisIndex, lookup, vector2, fun _ -> true)
          thisIndex, vector1, otherRowCmd
      | JoinKind.Right ->
          let thisRowIndex, vector1 = restrictToRowIndex lookup otherIndex thisIndex vector1
          let vector1 = fillMissing vector1 lookup
          let thisRowCmd = indexBuilder.Reindex(thisRowIndex, otherIndex, lookup, vector1, fun _ -> true)
          otherIndex, thisRowCmd, vector2
      | JoinKind.Outer | _ ->
          indexBuilder.Union( (thisIndex, vector1), (otherIndex, vector2) )
