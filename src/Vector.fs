// --------------------------------------------------------------------------------------
// Interface (generic & non-generic) for representing vectors
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open FSharp.DataFrame.Common

/// Provides a way to get the data of a vector. This is a concrete type used by 
/// functions that operate on vectors (like Series.sum, etc.). The vector may choose
/// to return the data as IReadOnlyList (with or without N/A values) which is more
/// efficient to use or as a lazy sequence (slower, but more general).
type VectorData<'TAddress> = 
  | DenseList of IReadOnlyList<'TAddress>
  | SparseList of IReadOnlyList<OptionalValue<'TAddress>>
  | Sequence of seq<OptionalValue<'TAddress>>

/// Represents an (untyped) vector that stores some values and provides access
/// to the values via a generic address. For convenience, the vector exposes
/// type of elements as System.Type.
type IVector<'TAddress> = 
  abstract ElementType : System.Type
  abstract GetObject : 'TAddress -> OptionalValue<obj>

/// A generic typed vector. Represents mapping from addresses 'TAddress to 
/// values of type 'TValue. It is possible to get all data using the Data member.
type IVector<'TAddress, 'TValue> = 
  inherit IVector<'TAddress>
  abstract GetValue : 'TAddress -> OptionalValue<'TValue>
  abstract Data : VectorData<'TValue>
  // TODO: Not entirely happy with these two being here... 
  abstract Map : ('TValue -> 'TNewValue) -> IVector<'TAddress, 'TNewValue>
  abstract MapMissing : (OptionalValue<'TValue> -> OptionalValue<'TNewValue>) -> IVector<'TAddress, 'TNewValue>

[<AutoOpen>]
module VectorExtensions = 
  type IVector<'TAddress, 'TValue> with
    /// Returns the data of the vector as a lazy sequence. (This preserves the 
    /// order of elements in the vector and so it also returns N/A values.)
    member x.DataSequence = 
      match x.Data with
      | Sequence s -> s
      | SparseList s -> upcast s
      | DenseList s -> Seq.map (fun v -> OptionalValue(v)) s

module internal VectorHelpers =
  /// Pretty printer for vectors. This uses the 'Data' property
  let prettyPrintVector (vector:IVector<'TAddress, 'T>) = 
    let printSequence kind (input:seq<string>) dots = 
      let sb = Text.StringBuilder(kind + " [")
      for it in input |> Seq.takeAtMost PrettyPrint.ItemCount do 
        sb.Append(" ").Append(it).Append(";") |> ignore
      sb.Remove(sb.Length - 1, 1) |> ignore
      if dots then sb.Append("; ... ]").ToString() 
      else sb.Append(" ]").ToString()
    match vector.Data with
    | DenseList list -> printSequence "dense" (Seq.map (fun v -> v.ToString()) list) (list.Count > PrettyPrint.ItemCount)
    | SparseList list -> printSequence "sparse" (Seq.map (fun v -> v.ToString()) list) (list.Count > PrettyPrint.ItemCount)
    | Sequence list -> printSequence "seq" (Seq.map (fun v -> v.ToString()) list) (Seq.length list > PrettyPrint.ItemCount)

  /// Create a new vector that delegates all functionality to a ref vector
  let delegatedVector (vector:IVector<'TAddress, 'TValue> ref) =
    { new IVector<'TAddress, 'TValue> with
        member x.GetValue(a) = vector.Value.GetValue(a)
        member x.Data = vector.Value.Data
        member x.Map(f) = vector.Value.Map(f)
        member x.MapMissing(f) = vector.Value.MapMissing(f)
      interface IVector<'TAddress> with
        member x.ElementType = vector.Value.ElementType
        member x.GetObject(i) = vector.Value.GetObject(i) }


  type VectorCallSite<'TAddress, 'R> =
    abstract Invoke<'T> : IVector<'TAddress, 'T> -> 'R

  let createDispatcher<'TAddress, 'R> (callSite:VectorCallSite<'TAddress, 'R>) =
    let dict = lazy Dictionary<_, System.Func<IVector<'TAddress>, 'R>>()

    let doubleCode = typeof<float>.TypeHandle.Value
    let intCode = typeof<int>.TypeHandle.Value
    let stringCode = typeof<string>.TypeHandle.Value

    let invoke (vect:IVector<'TAddress>) = 
      let code = vect.ElementType.TypeHandle.Value
      if code = doubleCode then callSite.Invoke<float>(vect :?> IVector<'TAddress, float>)
      elif code = intCode then callSite.Invoke<int>(vect :?> IVector<'TAddress, int>)
      elif code = stringCode then callSite.Invoke<string>(vect :?> IVector<'TAddress, string>)
      else
        match dict.Value.TryGetValue(code) with
        | true, f -> f.Invoke(vect)
        | _ ->
            let mi = typeof<VectorCallSite<'TAddress, 'R>>.GetMethod("Invoke").MakeGenericMethod(vect.ElementType)
            let par = Expression.Parameter(typeof<IVector<'TAddress>>)
            let ty = typedefof<IVector<_, _>>.MakeGenericType(typeof<'TAddress>, vect.ElementType)
            let expr =
              Expression.Lambda<System.Func<IVector<'TAddress>, 'R>>
                ( Expression.Call(mi, [Expression.Convert(par, ty) :> Expression]), [ par ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke vect
    invoke 

// --------------------------------------------------------------------------------------
// Types related to vectors that should not be exposed too directly
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame.Vectors

open FSharp.DataFrame
open FSharp.DataFrame.Common

/// Represents a range inside a vector
type VectorRange<'TAddress> = 'TAddress * 'TAddress

/// Representes a "variable" in the mini-DSL below
type VectorHole = int

/// A "mini-DSL" that describes construction of a vector. Vector can be constructed
/// from arrays of values, from existing vector value (of an unknown representation)
/// or by various range operations (relocate, drop, slicing, appending)
type VectorConstruction<'TAddress> =

  // Convert an existing vector to another representation
  // (ReturnAsOrdinal creates a vector with "default" ordering)
  | Return of VectorHole
  | ReturnAsOrdinal of VectorConstruction<'TAddress>
  
  // Reorders elements of the vector. Carries a new required vector range and a list
  // of relocations (each pair of addresses specifies that an element at an old address 
  // should be moved to a new address). THe addresses may be out of range!
  | Relocate of VectorConstruction<'TAddress> * VectorRange<'TAddress> * seq<'TAddress * 'TAddress>

  // Drop part of range & get subrange & append multiple vectors
  | DropRange of VectorConstruction<'TAddress> * VectorRange<'TAddress> 
  | GetRange of VectorConstruction<'TAddress> * VectorRange<'TAddress>
  | Append of VectorConstruction<'TAddress> * VectorConstruction<'TAddress>


/// Represents an object that can construct vector values by processing 
// the "mini-DSL" representation `VectorConstruction<'TAddress, 'TValue>`
type IVectorBuilder<'TAddress> = 
  // Create a vector from array containing values or optional values
  abstract CreateNonOptional : 'TValue[] -> IVector<'TAddress, 'TValue>
  abstract CreateOptional : OptionalValue<'TValue>[] -> IVector<'TAddress, 'TValue>
  // Apply a vector construction to a given vector
  abstract Build<'TValue> : VectorConstruction<'TAddress> * IVector<'TAddress, 'TValue>[] -> IVector<'TAddress, 'TValue>



