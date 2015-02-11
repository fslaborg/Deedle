/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// In the current implementation, the address is just `int64`, but most address-related
/// functionality is in a separate module to make this easy to change.
///
/// [category:Vectors and indices]
module Deedle.Addressing

open System
open System.Reflection
open System.Linq.Expressions

type [<Measure>] address
type Address = int64<address>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let invalid = -1L<address>

module LinearAddres =
  let invalid = -1L<address>
  let inline asInt64 (x:Address) : int64 = int64 x
  let inline asInt (x:Address) : int = int x
  let inline ofInt64 (x:int64) : Address = LanguagePrimitives.Int64WithMeasure x
  let inline ofInt (x:int) : Address = LanguagePrimitives.Int64WithMeasure (int64 x)
  let inline increment (x:Address) = x + 1L<address>
  let inline decrement (x:Address) = x - 1L<address>
