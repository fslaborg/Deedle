namespace Deedle

open System
open System.Reflection
open System.Linq.Expressions

// --------------------------------------------------------------------------------------
// Address type and operations
// --------------------------------------------------------------------------------------

/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
///
/// Here is a brief summary of what we assume (and don't assume) about addresses:
///
///  - Address is `int64` (although we might need to generalize this in the future)
///  - Different data sources can use different addressing schemes
///    (as long as both index and vector use the same scheme)
///  - Addresses don't have to be continuous (e.g. if the source is partitioned, it
///    can use 32bit partition index + 32bit offset in the partition)
///  - In the in-memory representation, address is just index into an array
///  - In the BigDeedle representation, address is abstracted and comes with
///    `AddressOperations` that specifies how to use it (tests use linear
///    offset and partitioned representation)
///
/// [category:Vectors and indices]
module Addressing =

  /// Address is `int64<address>`. We use unit of measure annotation
  /// to make sure that correct conversion functions are used.
  type Address = int64<address>
  and [<Measure>] address

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Address =
    /// Represents an invalid address (which is returned from 
    /// optimized lookup functions when they fail)
    let invalid = -1L<address>
   
  /// Address operations that are used by the standard in-memory Deedle structures
  /// (LinearIndex and ArrayVector). Here, address is a positive array offset.
  module LinearAddress =
    let invalid = Address.invalid
    let inline asInt64 (x:Address) : int64 = int64 x
    let inline asInt (x:Address) : int = int x
    let inline ofInt64 (x:int64) : Address = LanguagePrimitives.Int64WithMeasure x
    let inline ofInt (x:int) : Address = LanguagePrimitives.Int64WithMeasure (int64 x)
    let inline increment (x:Address) = x + 1L<address>
    let inline decrement (x:Address) = x - 1L<address>

  /// An empty interface that is used as an marker for "addressing schemes". As discussed
  /// above, Deedle can use different addressing schemes. We need to make sure that the index
  /// and vector share the scheme - this is done by attaching `IAddressingScheme` to each
  /// index or vector and checking that they match. Implementations must support equality!
  type IAddressingScheme = interface end

  /// Represents a linear addressing scheme where the addresses are `0 .. <size>-1`.
  type LinearAddressingScheme private() = 
    static let instance = LinearAddressingScheme() :> IAddressingScheme
    /// Returns a single instance of the object (to enable reference equality)
    static member Instance = instance 
    interface IAddressingScheme

  /// Various implementations can use different schemes for working with addresses 
  /// (for example, address can be just a global offset, or it can be pair of `int32` values
  /// that store partition and offset in a partition). This interface represents a specific
  /// address range and abstracts operations that BigDeedle needs to perform on addresses
  /// (within the specified range)
  type IAddressOperations =
    /// Returns the first address of the range
    abstract FirstElement : Address

    /// Returns the last address of the range
    abstract LastElement : Address

    /// Returns a sequence that iterates over `FirstElement .. LastElement`
    abstract Range : seq<Address>

    /// Given an address, return the absolute offset of the address in the range
    /// This might be tricky for partitioned ranges. For example if you have two 
    /// partitions with 10 values addressed by (0,0)..(0,9); (1,0)..(1,9), the the
    /// offset of address (1, 5) is 15.
    abstract OffsetOf : Address -> int64

    /// Return the address of a value at the specified absolute offset.
    /// (See the comment for `OffsetOf` for more info about partitioning)
    abstract AddressOf : int64 -> Address

    /// Increment or decrement the specified address by a given number
    abstract AdjustBy : Address * int64 -> Address

// --------------------------------------------------------------------------------------
// Address-related things like ranges
// --------------------------------------------------------------------------------------

open Addressing 

/// A sequence of indicies together with the total number. Use `RangeRestriction.ofSeq` to
/// create one from a sequence. This can be implemented by concrete vector/index 
/// builders to allow further optimizations (e.g. when the underlying source directly
/// supports range operations). 
///
/// For example, if your source has an optimised way for getting every 10th address, you 
/// can create your own `IRangeRestriction` and then check for it in `LookupRange` and 
/// use optimised implementation rather than actually iterating over the sequence of indices.
type IRangeRestriction<'TAddress> = 
  inherit seq<'TAddress>
  abstract Count : int64

/// Specifies a sub-range within index that can be accessed via slicing
/// (see the `GetAddressRange` method). For in-memory data structures, accessing
/// range via known addresses is typically sufficient, but for virtual Big Deedle
/// sources, `Start` and `End` let us avoid fully evaluating addresses.
/// `Custom` range can be used for optimizations.
[<RequireQualifiedAccess>]
type RangeRestriction<'TAddress> =
  /// Range specified as a pair of (inclusive) lower and upper addresses
  | Fixed of 'TAddress * 'TAddress
  /// Range referring to the specified number of elements from the start
  | Start of int64
  /// Range referring to the specified number of elements from the end
  | End of int64 
  /// Custom range, which is a sequence of indices, or other representation of it
  | Custom of IRangeRestriction<'TAddress>

/// Provides additional operations for working with the `RangeRestriction<'TAddress>` type
module RangeRestriction =
  /// Transforms all absolute addresses in the specified range restriction
  /// using the provided function (this is useful for mapping between different
  /// address spaces).
  let map (f:'TOldAddress -> 'TNewAddress) = function
    | RangeRestriction.Fixed(lo, hi) -> RangeRestriction.Fixed(f lo, f hi)
    | RangeRestriction.Start n -> RangeRestriction.Start n
    | RangeRestriction.End n -> RangeRestriction.End n
    | RangeRestriction.Custom c ->
        { new IRangeRestriction<'TNewAddress> with
            member x.Count = c.Count
          interface System.Collections.IEnumerable with
            member x.GetEnumerator() = (x :?> seq<'TNewAddress>).GetEnumerator() :> _ 
          interface seq<'TNewAddress> with
            member x.GetEnumerator() = (Seq.map f c).GetEnumerator() }
        |> RangeRestriction.Custom

/// Transforms all absolute addresses in the specified range restriction
/// using the provided function (this is useful for mapping between different
/// address spaces).
type RangeRestriction<'TAddress> with
  member x.Select(f:Func<_, _>) = RangeRestriction.map f.Invoke x

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
    /// When the address represents an absolute offset, this can be used to turn 'Start' 
    /// and 'End' restrictions into the usual 'Fixed' restriction. The result is a choice
    /// with either new absolute range or custom (sequence of addresses)
    [<Extension>]
    static member AsAbsolute(range, total) =
      match range with
      | RangeRestriction.Fixed(lo, hi) -> Choice1Of2(lo, hi)
      | RangeRestriction.Start(count) ->
          Choice1Of2(LinearAddress.ofInt 0, LinearAddress.ofInt64 (count-1L))
      | RangeRestriction.End(count) -> 
          Choice1Of2(LinearAddress.ofInt64 (total - count), LinearAddress.ofInt64 (total-1L))
      | RangeRestriction.Custom(ar) -> Choice2Of2(ar)
