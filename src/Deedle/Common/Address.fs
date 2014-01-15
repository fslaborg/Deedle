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
module Deedle.Addressing

type Address = int64

/// ArrayVectors assume that the address is an integer
let (|IntAddress|) = function _ : int64 as n -> int n
let int32Convertor v = int64 v

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let increment (x:Address) = (x + 1L)
  let decrement (x:Address) = (x - 1L)
  let add (a:Address, b:Address) = a + b
