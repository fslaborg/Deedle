/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// 
/// ## Details
///
/// In the most common case, the address will be `int` (and can represent index in an array),
/// but it is possible to imagine other addresses - `int64` could be used with arrays of 
/// arrays (to handle very large data). A lazily loaded vector might use something completely
/// different (perhaps a date?). In principle this should be generic, but that is hard to do - 
/// we want something like:
///
///     Series.Create : \forall 'TKey, 'TValue. \exists 'TAddress. 
///       Index<'TKey, 'TAddress> * Vector<'TAddress, 'TValue> -> Series<'TKey, 'TValue>
///
/// The .NET encoding of this is a bit ugly. So instead, we just have `Address` which currently
/// supports `Int` and `Int64`, but we keep all operations in the `Address` module, so that
/// this can be easily extended.
module FSharp.DataFrame.Addressing

type Address = 
  | Int of int
  | Int64 of int64

/// ArrayVectors assume that the address is an integer
let (|IntAddress|) = function
  | Address.Int n -> n
  | _ -> failwith "ArrayVectorBuilder: Only int addresses are supported."

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let increment = function
    | Int lo -> Int (lo + 1)
    | _ -> failwith "Not supported"
  let decrement = function
    | Int lo -> Int (lo - 1)
    | _ -> failwith "Not supported"
  let generateRange = function
    | Int lo, Int hi -> 
        ( if hi < lo then seq { lo .. -1 .. hi } 
          else seq { lo .. hi } ) |> Seq.map Int
    | _ -> failwith "Not supported"

  let int32Convertor = 
    Some(fun v -> Int v)

  let add = function
    | Int a, Int b -> Int (a + b)
    | _ -> failwith "Not supported"

  let rangeOf(seq) = 
    Int 0, Int ((Seq.length seq) - 1)

  let getRange = function 
    | (seq:_[]), Int lo, Int hi ->
        if hi >= lo then seq.[lo .. hi]
        else [| |]
    | _ -> failwith "Not supported"