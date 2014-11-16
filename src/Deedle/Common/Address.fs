/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// In the current implementation, the address is just `int64`, but most address-related
/// functionality is in a separate module to make this easy to change.
module Deedle.Addressing
open System
 
#if TYPED_ADDRESS
// Interface for address
type IAddress<'T when 'T :> IAddress<'T> and 'T : comparison>= 
  //inherit IAddress
  interface
    /// Position from zero
    abstract Index : int64 with get
    /// Distance between two address positions
    abstract Distance : 'T -> int64 // TODO! make in 'T: Addr
    /// Next possible address
    abstract Increment : unit -> 'T
    /// Previous possible address
    abstract Decrement : unit -> 'T
    abstract AsInt : unit -> int
    abstract AsInt64 : unit -> int64
  end


// Implement generic helper type similar to Address module.
// When IAddress is a structure there is no boxing while accessing properties (at least true for C#)
// http://stackoverflow.com/a/1289537/801189
[<AbstractClass>]
type AddressHelper<'T when 'T : comparison and 'T :> IAddress<'T> and 'T : struct and 'T : (new : unit -> 'T)>() =
  static member inline Index(address : 'T) : int64 = address.Index
  static member inline Distance(startAddr : 'T, endAddr : 'T) : int64 = startAddr.Distance(endAddr)
  static member inline Increment(address : 'T) : 'T = address.Increment()
  static member inline Decrement(address : 'T) : 'T = address.Decrement()
  static member inline AsInt(address : 'T) = address.AsInt()
  static member inline AsInt64(address : 'T) = address.AsInt64()
  [<Obsolete("Generic address cannot enumerate a range from lo/hi, only vector/index can")>]
  static member GenerateRange (lo:'T, hi:'T) : seq<'T> = 
    if hi < lo then 
      seq { 
        let current = ref lo
        while hi <= !current do
          yield !current
          current := current.Value.Decrement()
        } 
    else
      seq { 
        let current = ref lo
        while !current <= hi do
          yield !current
          current := current.Value.Increment()
        } 

  /// First valid address
  static member inline Zero with get() : 'T = new 'T()
  /// Invalid address in defined as zero address decremented once
  static member inline Invalid with get() : 'T = AddressHelper<_>.Zero.Decrement()
  
[<LiteralAttribute>]
let private int32Mask : int64 = 4294967295L // (1L <<< 32) - 1L
[<LiteralAttribute>]
let private positiveInt32Mask : int64 = 2147483647L // (1L <<< 31) - 1L

/// Represents a type used for addressing values in a linear vector/index
[<CustomEquality; CustomComparison>]
type Address = // linear address
  struct
    val private value : int64
    new(index1D:int64) = {value = index1D}
  end
  override x.Equals(yobj) =
    match yobj with
    | :? Address as y -> (x.value = y.value)
    | _ -> false
  override x.GetHashCode() = x.value.GetHashCode()
  interface System.IComparable<Address> with
    member x.CompareTo y = x.value.CompareTo(y.value)
  interface System.IComparable with
    member x.CompareTo other = 
      match other with
      | :? Address as y -> x.value.CompareTo(y.value)
      | _ -> invalidArg "other" "Cannot compare values of different types"

  member private x.Index: int64 = x.value
  
  interface IAddress<Address> with
    member x.Index with get() : int64 = x.Index
    member x.Distance(endAddress): int64 = endAddress.Index - x.Index + 1L
    member x.Decrement() = Address(x.value - 1L)
    member x.Increment() = Address(x.value + 1L)
    member x.AsInt(): int = int x.value
    member x.AsInt64(): int64 = x.value

  // int32/64 cast is compatible with previous `type Address = int64` definition

  static member op_Explicit(addr: Address) : int64 = addr.value
  static member op_Explicit(value: int64) : Address = Address(value)
  static member op_Explicit(addr: Address) : int = int addr.value
    //if addr.value >= -1L then int addr.value else invalidOp "Cannot convert 2D address to int32"
  static member op_Explicit(value: int) : Address = Address(int64 value)

 type private AddressHelper = AddressHelper<Address>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let invalid : Address = AddressHelper.Invalid
  let zero : Address = AddressHelper.Zero
  [<ObsoleteAttribute("Makes sense only for linear address")>]
  let inline distance (startAddr:Address) (endAddr:Address) = AddressHelper.Distance(startAddr, endAddr)
  let inline asInt (x:Address) = AddressHelper.AsInt(x)
  let inline asInt64 (x:Address) : int64 = AddressHelper.AsInt64(x)
  let inline ofInt (x:int) : Address = Address(int64 x)
  let inline ofInt64 (x:int64) : Address = Address(x)
  let inline increment (x:Address) = AddressHelper.Increment(x)
  let inline decrement (x:Address) = AddressHelper.Decrement(x)
  [<ObsoleteAttribute("Abstract address cannot enumerate a range from lo/hi, only vector/index can")>]
  let generateRange (lo:Address, hi:Address) : seq<Address> = AddressHelper.GenerateRange(lo, hi)

#else


/// Represents a type used for addressing values in a vector
type Address = int64

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Address =
  let invalid = -1L
  let zero : Address = 0L
  let inline distance (startAddr:Address) (endAddr:Address) = endAddr - startAddr + 1L
  let inline asInt (x:Address) = int x
  let inline asInt64 (x:Address) : int64 = x
  let inline ofInt (x:int) : Address = int64 x
  let inline ofInt64 (x:int64) : Address = x
  let inline increment (x:Address) = (x + 1L)
  let inline decrement (x:Address) = (x - 1L)
  let generateRange (lo:Address, hi:Address) = 
    if hi < lo then seq { lo .. -1L .. hi } else seq { lo .. hi }


#endif