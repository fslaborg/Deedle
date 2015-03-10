namespace Deedle

open System
open System.Reflection
open System.Linq.Expressions

// --------------------------------------------------------------------------------------
// Address type and operations
// --------------------------------------------------------------------------------------

/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// In the current implementation, the address is just `int64`, but most address-related
/// functionality is in a separate module to make this easy to change.
///
/// [category:Vectors and indices]
module Addressing =

  type [<Measure>] address
  type Address = int64<address>

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Address =
    let invalid = -1L<address>

  module LinearAddress =
    let invalid = -1L<address>
    let inline asInt64 (x:Address) : int64 = int64 x
    let inline asInt (x:Address) : int = int x
    let inline ofInt64 (x:int64) : Address = LanguagePrimitives.Int64WithMeasure x
    let inline ofInt (x:int) : Address = LanguagePrimitives.Int64WithMeasure (int64 x)
    let inline increment (x:Address) = x + 1L<address>
    let inline decrement (x:Address) = x - 1L<address>

// --------------------------------------------------------------------------------------
// Address-related things like ranges
// --------------------------------------------------------------------------------------

open Addressing 

/// A sequence of indicies together with the total number. Use `AddressRange.ofSeq` to
/// create one from a sequence. This can be implemented by concrete vector/index 
/// builders to allow further optimizations (e.g. when the underlying source directly
/// supports range operations)
type IAddressRange<'TAddress> = 
  inherit seq<'TAddress>
  abstract Count : int64

/// Specifies a sub-range within index that can be accessed via slicing
/// (see the `GetAddressRange` method). For in-memory data structures, accessing
/// range via known addresses is typically sufficient, but for virtual Big Deedle
/// sources, `Start` and `End` let us avoid fully evaluating addresses.
/// `Custom` range can be used for optimizations.
[<RequireQualifiedAccess>]
type AddressRange<'TAddress> =
  /// Range specified as a pair of (inclusive) lower and upper addresses
  | Fixed of 'TAddress * 'TAddress
  /// Range referring to the specified number of elements from the start
  | Start of int64
  /// Range referring to the specified number of elements from the end
  | End of int64 
  /// Custom range, which is a sequence of indices, or other representation of it
  | Custom of IAddressRange<'TAddress>

module AddressRange =
  let map (f:'TOldAddress -> 'TNewAddress) = function
    | AddressRange.Fixed(lo, hi) -> AddressRange.Fixed(f lo, f hi)
    | AddressRange.Start n -> AddressRange.Start n
    | AddressRange.End n -> AddressRange.End n
    | AddressRange.Custom c ->
        { new IAddressRange<'TNewAddress> with
            member x.Count = c.Count
          interface System.Collections.IEnumerable with
            member x.GetEnumerator() = (x :?> seq<'TNewAddress>).GetEnumerator() :> _ 
          interface seq<'TNewAddress> with
            member x.GetEnumerator() = (Seq.map f c).GetEnumerator() }
        |> AddressRange.Custom

// --------------------------------------------------------------------------------------
// Internal address range helpers
// --------------------------------------------------------------------------------------
namespace Deedle.Internal

open Deedle
open Deedle.Addressing
open System.Runtime.CompilerServices

/// [omit]
[<AutoOpen>]
module AddressingExtensions = 
  [<Extension>]
  type AddressRangeExtensions =
    [<Extension>]
    static member AsAbsolute(range, total) =
      match range with
      | AddressRange.Fixed(lo, hi) -> Choice1Of2(lo, hi)
      | AddressRange.Start(count) ->
          (LinearAddress.ofInt 0, LinearAddress.ofInt64 (count-1L))
          |> Choice1Of2
      | AddressRange.End(count) -> 
          (LinearAddress.ofInt64 (total - count), LinearAddress.ofInt64 (total-1L))
          |> Choice1Of2
      | AddressRange.Custom(ar) -> Choice2Of2(ar)
