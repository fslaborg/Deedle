/// Purely functional Binomial Heap implementation
/// Based on: http://cs.hubfs.net/topic/None/56608
///
/// Characteristics:
///   isEmpty = O(1)
///   insert = O(1) amortized; O(log n) worst case
///   merge = O(log n)
///   findMin, removeMin, deleteMin = O(log n)
namespace Deedle.Internal

open System.Collections.Generic

/// Tree where nodes contain values and zero or more child trees.
/// For each node, we store rank - the number of children
///
/// [omit]
type RankedTree<'T> = Node of int * 'T * RankedTree<'T> list
  
/// Binomial heap stores a list of trees together with custom comparer
///
/// [omit]
type BinomialHeap<'T> = 
  { Comparer : IComparer<'T>
    Heap : RankedTree<'T> list }
 
/// Module with functions for working with binomial heaps
///
/// [omit]
module BinomialHeap =
  /// Active pattern that makes it easier to deal with results from IComparer
  let (|LT|GT|EQ|) n =
    if n < 0 then LT elif n > 0 then GT else EQ
 
  /// Creates an empty heap that uses the default .NET comparer
  let empty<'T> : BinomialHeap<'T> = 
    { Comparer = Comparer<'T>.Default; Heap = [] }

  /// Creates an empty heap using the specified comparer
  let emptyCustom comparer = 
    { Comparer = comparer; Heap = [] }
 
  /// Returns true when the specified heap is emtpy
  let isEmpty heap = heap.Heap = []
 
  /// Returns the rank of the specified tree node
  let internal rank (Node (r,_,_)) = r
  
  /// Returns the value in the root of the specified tree 
  let internal root (Node (_,x,_)) = x
 
  /// Link two tree nodes. The new root is the smaller of the two nodes.
  let internal link (comparer:IComparer<_>) (Node (r, x1, c1) as t1) (Node (_, x2, c2) as t2) =
    match comparer.Compare(x1, x2) with
    | GT -> Node (r+1, x2, t1 :: c2)
    | _  -> Node (r+1, x1, t2 :: c1)

  /// Inser the specified tree into the list of trees forming binomial heap.
  /// If the rank of the inserted tree is higher than the rank of the head, 
  /// then join the trees and add the resulting tree. 
  let rec insertTree comparer (t:RankedTree<_>) ts =
    match ts with
    | [] -> [t]
    | t'::ts' -> 
        if (rank t) < (rank t') then t::ts
        else insertTree comparer (link comparer t t') ts'
 
  /// Insert the specified element to the heap
  let insert x heap = 
    let newHeap = insertTree heap.Comparer (Node (0, x, [])) heap.Heap
    { heap with Heap = newHeap }

  /// Merge two lists of ranked trees, producing a new list.
  let rec mergeTrees comparer = function
    // When one or the other is empty, just return it.
    | ts, [] | [], ts -> ts
    | ((t1::ts1' as ts1), (t2::ts2' as ts2)) ->
        // Merge them based on ranks. When the rank is equal,
        // we actually have to combine the trees and insert the
        // result into a recursively merged list.
        match compare (rank t1) (rank t2) with
        | LT -> t1 :: (mergeTrees comparer (ts1', ts2))
        | GT -> t2 :: (mergeTrees comparer (ts1, ts2'))
        | EQ -> insertTree comparer (link comparer t1 t2) (mergeTrees comparer (ts1', ts2'))

  /// Merge two binomial heaps (using the comparer of the first one)
  let merge heap1 heap2 = 
    let newHeap = mergeTrees heap1.Comparer (heap1.Heap, heap2.Heap)
    { heap1 with  Heap = newHeap}

  /// Remove the  smallest tree from a list of ranked trees.
  let rec internal removeMinTree (comparer:IComparer<_>) heap =
    match heap with
    | [] -> invalidOp "The heap is empty."
    | [t] -> t, []
    | (t::ts) ->
        let (t', ts') = removeMinTree comparer ts 
        match comparer.Compare(root t, root t') with
        | LT | EQ -> (t, ts)
        | _ -> (t', t::ts')

  /// Find minimal value from the specified binomial heap 
  let findMin { Comparer=cf; Heap=heap } =
    let (t, _) = removeMinTree cf heap 
    root t
 
  /// Remove minimal value from the specified binomial heap
  /// (and return the value, together with a new heap)
  let removeMin { Comparer=comparer; Heap=heap } =
    let (Node (_, x, ts1), ts2) = removeMinTree comparer heap 
    x, { Comparer = comparer; Heap = mergeTrees comparer ((List.rev ts1), ts2)}
 
  /// Remove minimal value from the specified binomial heap
  /// (and return the new heap)
  let deleteMin heap = snd (removeMin heap)