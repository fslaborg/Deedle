module FSharp.DataFrame.Addressing

(**
An "address" is used as an interface between vectors and indices. The index maps
keys of various types to address, which is then used to get a value from the vector.

We want to make the address "generic". In the basic case, the address will be 
`int` (and can represent index in an array), but it is possible to imagine other
addresses - `int64` could be used with arrays of arrays (to handle very large data).
A lazy vector/index might use something like 

...

// ∃t.F<t>
// ∀x.(∀t.F<t> → x) → x

// exists 'TAddress . 
//  ( IIndex<'TKey, 'TAddress> * IVector<'TAddress, 'TValue> *
//    IVectorBuilder<'TAddress> * IIndexBuilder<'TAddress> )

// forall 'X .
//  ( forall 'TAddress .
//      ( IIndex<'TKey, 'TAddress> * IVector<'TAddress, 'TValue> *
//        IVectorBuilder<'TAddress> * IIndexBuilder<'TAddress> ) -> 'X ) 
//  -> 'X

(*
type SeriesFields<'TKey, 'TAddress, 'TValue when 'TKey : equality and 'TAddress : equality> = 
  { Index : IIndex<'TKey, 'TAddress>
    Vector : IVector<'TAddress, 'TValue>
    VectorBuilder : IVectorBuilder<'TAddress>
    IndexBuilder : IIndexBuilder<'TAddress> }

type SeriesOperation<'TKey, 'TValue, 'R when 'TKey : equality> = 
  abstract Apply<'TAddress when 'TAddress : equality> : SeriesFields<'TKey, 'TAddress, 'TValue> -> 'R

type SeriesInternals<'TKey, 'TValue when 'TKey : equality> = 
  abstract Apply<'R> : SeriesOperation<'TKey, 'TValue, 'R> -> 'R

module A = 
  let test =
    { new SeriesOperation<_, _, int> with
        member x.Apply(ops) = 0 }
*)
// Oh oh..


*)

type CustomAddress = obj

type Address = 
  | Int of int
  | Int64 of int64
  | Custom of CustomAddress

/// ArrayVectors assume that the address is an integer
let (|IntAddress|) = function
  | Address.Int n -> n
  | _ -> failwith "Addressing: Only int addresses are supported."

type IAddressOperations = 
  abstract GenerateRange : Address * Address -> seq<Address>
  abstract Add : Address * Address -> Address
  abstract Int32Convertor : option<int -> Address>
  abstract RangeOf : seq<'T> -> Address * Address
  abstract GetRange : seq<'T> * Address * Address -> seq<'T>

module AddressHelpers =
  let getAddressOperations() = 
    { new IAddressOperations with
        member x.GenerateRange(IntAddress lo, IntAddress hi) = 
          let lo, hi = if hi < lo then hi, lo else lo, hi
          seq { lo .. hi } |> Seq.map Int
        member x.Int32Convertor = Some(fun v -> Int v)
        member x.Add(IntAddress a, IntAddress b) = Int (a + b)
        member x.RangeOf(seq) = Int 0, Int ((Seq.length seq) - 1)
        member x.GetRange(seq, IntAddress lo, IntAddress hi) = 
          if hi >= lo then seq |> Seq.skip lo |> Seq.take (hi - lo + 1) 
          else Seq.empty } 
