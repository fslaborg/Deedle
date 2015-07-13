namespace Deedle

// --------------------------------------------------------------------------------------
// Operations and types related to joining - used by both Series and Frame
// --------------------------------------------------------------------------------------

open System
open Deedle.Indices
open Deedle.Vectors

/// This enumeration specifies joining behavior for `Join` method provided
/// by `Series` and `Frame`. Outer join unions the keys (and may introduce
/// missing values), inner join takes the intersection of keys; left and
/// right joins take the keys of the first or the second series/frame.
///
/// [category:Parameters and results of various operations]
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
  open Deedle.Internal

  /// When doing exact join on ordered indices, restrict the new index
  /// so that we do not have to load all data for lazy indices
  let restrictToRowIndex lookup (restriction:IIndex<_>) (sourceIndex:IIndex<_>) vector = 
    if lookup = Lookup.Exact && 
       restriction.IsOrdered && sourceIndex.IsOrdered &&
       not restriction.IsEmpty then
      let min, max = restriction.KeyRange
      sourceIndex.Builder.GetRange((sourceIndex, vector), (Some(min, BoundaryBehavior.Inclusive), Some(max, BoundaryBehavior.Inclusive)))
    else sourceIndex, vector

  /// When using fancy lookup, first fill values in the vector, before doing the join
  let fillMissing vector lookup = 
    match lookup with
    | Lookup.ExactOrSmaller -> Vectors.FillMissing(vector, VectorFillMissing.Direction Direction.Forward)
    | Lookup.ExactOrGreater -> Vectors.FillMissing(vector, VectorFillMissing.Direction Direction.Backward)
    | Lookup.Exact -> vector
    | _ -> invalidOp "Lookup.Smaller and Lookup.Greater are not supported when joining"

  // Helpers for returning one or the other object
  let inline returnLeft index left right = index, left, right
  let inline returnRight index left right = index, right, left

  /// Create transformation on indices/vectors representing the join operation
  let createJoinTransformation 
        (indexBuilder:IIndexBuilder) (otherIndexBuilder:IIndexBuilder) kind lookup 
        (thisIndex:IIndex<_>) (otherIndex:IIndex<_>) thisVector otherVector =
    // Inner/outer join only makes sense with exact lookup
    if lookup <> Lookup.Exact && kind = JoinKind.Inner then
      invalidOp "Join/Zip - Inner join can only be used with Lookup.Exact."
    if lookup <> Lookup.Exact && kind = JoinKind.Outer then
      invalidOp "Join/Zip - Outer join can only be used with Lookup.Exact."
    // Only Lookup.Exact makes sense for unordered series
    if not (thisIndex.IsOrdered && otherIndex.IsOrdered) && lookup <> Lookup.Exact then
      invalidOp "Join/Zip - Lookup can be only used when joining/zipping ordered series/frames."

    match kind with
    // If this is RIGHT join, then swap 'this' and 'other' - then the rest is the same
    | Let (thisVector, otherVector, thisIndex, otherIndex, returnLeft) 
          ((thisVector, otherVector, thisIndex, otherIndex, returnOp), JoinKind.Left) 
    | Let (thisVector, otherVector, thisIndex, otherIndex, returnRight) 
          ((otherVector, thisVector, otherIndex, thisIndex, returnOp), JoinKind.Right) ->

        // If they are the same instance, they are the same (no need to realign)
        if lookup = Lookup.Exact && Object.ReferenceEquals(thisIndex, otherIndex) then
          returnOp thisIndex thisVector otherVector
        else
          // Otherwise, restrict the other index to the "this" range and check structural equality
          let otherRowIndex, otherVector = restrictToRowIndex lookup thisIndex otherIndex otherVector
          if lookup = Lookup.Exact && thisIndex = otherRowIndex then
            returnOp thisIndex thisVector otherVector
          else
             // If they are not the same, we actually need to reindex
            let otherVector = fillMissing otherVector lookup
            let otherRowCmd = otherIndexBuilder.Reindex(otherRowIndex, thisIndex, lookup, otherVector, fun _ -> true)
            returnOp thisIndex thisVector otherRowCmd


    | JoinKind.Inner | JoinKind.Outer when Object.ReferenceEquals(thisIndex, otherIndex) || thisIndex = otherIndex ->
        // If they are the same object & same structure, we are done
        thisIndex, thisVector, otherVector

 
    // If they are different, we intersect or union the keys
    | JoinKind.Inner -> indexBuilder.Intersect( (thisIndex, thisVector), (otherIndex, otherVector) )
    | JoinKind.Outer -> indexBuilder.Union( (thisIndex, thisVector), (otherIndex, otherVector) )
    | _ -> invalidOp "Join/Zip - Invalid JoinKind value!"
