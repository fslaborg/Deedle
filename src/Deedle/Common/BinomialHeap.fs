/// Purely functional Binomial Heap implementation
///
/// http://cs.hubfs.net/topic/None/56608
///
/// Characteristics:
///   isEmpty = O(1)
///   insert = O(1) amortized; O(log n) worst case
///   merge = O(log n)
///   findMin, removeMin, deleteMin = O(log n)

module BinomialHeap
 
type ord = LT | EQ | GT
 
type 'a comparer = 'a -> 'a -> ord
 
type 'a tree = Node of int * 'a * 'a tree list
 
type 'a heap = { cf : 'a comparer ;
                 heap : 'a tree list }
 
let basic_compare x1 x2 =
  let a = compare x1 x2 in
    if a < 0 then LT
    else if a > 0 then GT
    else EQ

let custom_compare compareFn x1 x2 =
  let a = compareFn x1 x2 in
    if a < 0 then LT
    else if a > 0 then GT
    else EQ
 
let empty_custom cf = { cf = cf ; heap = [] }
let empty = {cf=basic_compare; heap = []}
 
let isEmpty {heap=heap} = heap = []
 
let rank (Node (r,_,_)) = r
 
let root (Node (_,x,_)) = x
 
let link cf (Node (r, x1, c1) as t1) (Node (_, x2, c2) as t2) =
  match cf x1 x2 with
    | GT -> Node (r+1, x2, t1 :: c2)
    | _  -> Node (r+1, x1, t2 :: c1)
 
let rec insTree cf (t: 'a tree) ts =
  match ts with
    | [] -> [t]
    | t'::ts' -> 
    match basic_compare (rank t) (rank t') with
      | LT -> t::ts
      | _ -> insTree cf (link cf t t') ts'
 
let insert x {cf=cf;heap=heap} = 
  let newheap = insTree cf (Node (0, x, [])) heap in
    {cf=cf; heap=newheap}
 
let rec merge' cf treepair =
  match treepair with
    | (ts1, []) -> ts1
    | ([], ts2) -> ts2
    | ((t1::ts1' as ts1), (t2::ts2' as ts2)) ->
    match basic_compare (rank t1) (rank t2) with
      | LT -> t1 :: (merge' cf (ts1', ts2))
      | GT -> t2 :: (merge' cf (ts1, ts2'))
      | EQ -> insTree cf (link cf t1 t2) (merge' cf (ts1', ts2'))
 
let merge {cf=cf;heap=heap1} {heap=heap2} = 
  let newheap = merge' cf (heap1, heap2) in
    {cf=cf;heap=newheap}
 
exception Empty_Heap
 
let rec removeMinTree cf heap =
  match heap with
    | [] -> raise Empty_Heap
    | [t] -> (t, [])
    | (t::ts) ->
    let (t', ts') = removeMinTree cf ts in
      match cf (root t) (root t') with
        | LT | EQ -> (t, ts)
        | _ -> (t', t::ts')
 
let findMin {cf=cf;heap=heap} =
  let (t, _) = removeMinTree cf heap in
    root t
 
let removeMin {cf=cf;heap=heap} =
  let (Node (_, x, ts1), ts2) = removeMinTree cf heap in
    (x, {cf=cf; heap=(merge' cf ((List.rev ts1), ts2))})
 
let deleteMin heap = snd (removeMin heap)