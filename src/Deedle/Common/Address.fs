/// An `Address` value is used as an interface between vectors and indices. The index maps
/// keys of various types to address, which is then used to get a value from the vector.
/// In the current implementation, the address is just `int64`, but most address-related
/// functionality is in a separate module to make this easy to change.
module Deedle.Addressing
  #nowarn "77" // for Helper's `static member inline OfInt64<'T>(value : int64)` : due to monstrous type constraint 'T is never built-in primitive

  open System
  open System.Reflection
  open System.Linq.Expressions

  // Address definition:
  /// An address is a comparable value type with default constructor.
  /// Default constructor returns the smallest possible (zero) address.
  /// Decrementing zero address results in an invalid address.
  /// Any transformation or operation on invalid address results in an invalid address or invalidOp.
  /// Address type must implement explicit conversion to and from int64.
  /// Zero address is always 0L, invalid address is always -1L.
  type IAddress<'T when 'T :> IAddress<'T>
                  and 'T : comparison 
                  and 'T : struct 
                  and 'T : (new : unit -> 'T)
                >= 
    interface
      /// Next possible address (not next address with a value)
      abstract Increment : unit -> 'T
      /// Previous possible address (not next address with a value)
      abstract Decrement : unit -> 'T
    end


  // Implement generic helper type similar to Address module.
  // When IAddress is a structure there is no boxing while accessing properties (at least true for C#)
  // http://stackoverflow.com/a/1289537/801189
  [<AbstractClass>]
  type private AddressHelper<'T when 'T :> IAddress<'T>
                        and 'T : comparison 
                        and 'T : struct 
                        and 'T : (new : unit -> 'T)
                        and 'T : (static member op_Explicit: int64 -> 'T)
                        and 'T : (static member op_Explicit: 'T -> int64)
                    >() =
    static let zero = new 'T()
    static let invalid = zero.Decrement()
    static member inline Increment(address : 'T) : 'T = address.Increment()
    static member inline Decrement(address : 'T) : 'T = address.Decrement()
    /// Invalid address in defined as zero address decremented once
    static member inline Invalid with get() : 'T = invalid
    static member inline AsInt64<'T>(addr : 'T) : int64 = int64 addr
    static member inline OfInt64<'T>(value : int64) : 'T = 
      ( ^T : (static member op_Explicit : int64 -> 'T) value) 

   
  /// Represents a type used for addressing values in a linear vector/index
  [<CustomEquality; CustomComparison>]
  type Address = // linear address
    struct
      val private value : int64 // TODO Private
      new(index:int64) = {value = index}
    end
    override x.Equals(yobj) =
      match yobj with
      | :? Address as y -> (x.value = y.value)
      | _ -> false
    override x.GetHashCode() = x.value.GetHashCode()
    override x.ToString() = x.value.ToString()
    interface System.IComparable<Address> with
      member x.CompareTo y = x.value.CompareTo(y.value)
    interface System.IComparable with
      member x.CompareTo other = 
        match other with
        | :? Address as y -> x.value.CompareTo(y.value)
        | _ -> invalidArg "other" "Cannot compare values of different types"

    static member op_Explicit(addr: Address) : int64 = addr.value
    static member op_Explicit(value: int64) : Address = Address(value)

    interface IAddress<Address> with
      member x.Decrement() = Address(x.value - 1L)
      member x.Increment() = Address(x.value + 1L)

  type AddressHelper = AddressHelper<Address>

        
  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Address =
    let invalid : Address = AddressHelper.Invalid
    //[<ObsoleteAttribute("This must be used only in linear vector/index and ordinal idx.")>]
    let inline asInt64 (x:Address) : int64 = AddressHelper.AsInt64(x)
    //[<ObsoleteAttribute("This must be used only in linear vector/index and ordinal idx.")>]
    let inline asInt (x:Address) : int = int <| AddressHelper.AsInt64(x)
    //[<ObsoleteAttribute("This must be used only in linear vector/index and ordinal idx.")>]
    let inline ofInt64 (x:int64) : Address = AddressHelper.OfInt64(x)
    //[<ObsoleteAttribute("This must be used only in linear vector/index and ordinal idx.")>]
    let inline ofInt (x:int) : Address = AddressHelper.OfInt64(int64 x)
    let inline increment (x:Address) = AddressHelper.Increment(x)
    let inline decrement (x:Address) = AddressHelper.Decrement(x)


//  // In fsi a, b and c show the same timing (40-50 msec after fsi reset), d is more than 20 times slower (c.1100 msec after fsi reset)
//  let a = Array.init 10000000 (fun i -> Address.ofInt64(int64 i)) |> Array.length
//  let b = Array.init 10000000 (fun i -> Address(int64 i)) |> Array.length
//  let c = Array.init 10000000 (fun i -> (int64 i)) |> Array.length
//  let d = Array.init 10000000 (fun i -> Activator.CreateInstance<Address>()) |> Array.length