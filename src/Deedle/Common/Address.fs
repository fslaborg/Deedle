/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// In the current implementation, the address is just `int64`, but most address-related
/// functionality is in a separate module to make this easy to change.
module Deedle.Addressing

/// Represents a type used for addressing values in a vector
type Address = int64

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let zero = 0L
  let inline asInt (x:Address) = int x
  let inline ofInt (x:int) : Address = int64 x
  let inline ofInt64 (x:int64) : Address = x
  let inline increment (x:Address) = (x + 1L)
  let inline decrement (x:Address) = (x - 1L)
  let generateRange (lo:Address, hi:Address) = 
    if hi < lo then seq { lo .. -1L .. hi } else seq { lo .. hi }
