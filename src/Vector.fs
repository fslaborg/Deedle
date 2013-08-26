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
  abstract Select : ('TValue -> 'TNewValue) -> IVector<'TAddress, 'TNewValue>
  abstract SelectMissing : (OptionalValue<'TValue> -> OptionalValue<'TNewValue>) -> IVector<'TAddress, 'TNewValue>

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
        member x.Select(f) = vector.Value.Select(f)
        member x.SelectMissing(f) = vector.Value.SelectMissing(f)
      interface IVector<'TAddress> with
        member x.ElementType = vector.Value.ElementType
        member x.GetObject(i) = vector.Value.GetObject(i) }


  type VectorCallSite1<'TAddress, 'R> =
    abstract Invoke<'T> : IVector<'TAddress, 'T> -> 'R
  type VectorCallSite2<'TAddress, 'R> =
    abstract Invoke<'T> : IVector<'TAddress, 'T> * IVector<'TAddress, 'T> -> 'R

  let createDispatcher<'TAddress, 'R> (callSite:VectorCallSite1<'TAddress, 'R>) =
    let dict = lazy Dictionary<_, System.Func<VectorCallSite1<'TAddress, 'R>, IVector<'TAddress>, 'R>>()

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
        | true, f -> f.Invoke(callSite, vect)
        | _ ->
            let mi = typeof<VectorCallSite1<'TAddress, 'R>>.GetMethod("Invoke").MakeGenericMethod(vect.ElementType)
            let inst = Expression.Parameter(typeof<VectorCallSite1<'TAddress, 'R>>)
            let par = Expression.Parameter(typeof<IVector<'TAddress>>)
            let ty = typedefof<IVector<_, _>>.MakeGenericType(typeof<'TAddress>, vect.ElementType)
            let expr =
              Expression.Lambda<System.Func<VectorCallSite1<'TAddress, 'R>, IVector<'TAddress>, 'R>>
                ( Expression.Call(inst, mi, Expression.Convert(par, ty)), [ inst; par ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke(callSite, vect)
    invoke 

  let createTwoArgDispatcher<'TAddress, 'R> (callSite:VectorCallSite2<'TAddress, 'R>) =
    let dict = lazy Dictionary<_, System.Func<VectorCallSite2<'TAddress, 'R>, IVector<'TAddress>, IVector<'TAddress>, 'R>>()

    let doubleCode = typeof<float>.TypeHandle.Value
    let intCode = typeof<int>.TypeHandle.Value
    let stringCode = typeof<string>.TypeHandle.Value

    let invoke (vect1:IVector<'TAddress>, vect2:IVector<'TAddress>) = 
      let code = vect1.ElementType.TypeHandle.Value
      if vect2.ElementType.TypeHandle.Value <> code then 
        invalidOp "createTwoArgDispatcher: Both arguments should have the same element type"
      if code = doubleCode then callSite.Invoke<float>(vect1 :?> IVector<'TAddress, float>, vect2 :?> IVector<'TAddress, float>)
      elif code = intCode then callSite.Invoke<int>(vect1 :?> IVector<'TAddress, int>, vect2 :?> IVector<'TAddress, int>)
      elif code = stringCode then callSite.Invoke<string>(vect1 :?> IVector<'TAddress, string>, vect2 :?> IVector<'TAddress, string>)
      else
        match dict.Value.TryGetValue(code) with
        | true, f -> f.Invoke(callSite, vect1, vect2)
        | _ ->
            let mi = typeof<VectorCallSite2<'TAddress, 'R>>.GetMethod("Invoke").MakeGenericMethod(vect1.ElementType)
            let inst = Expression.Parameter(typeof<VectorCallSite2<'TAddress, 'R>>)
            let par1 = Expression.Parameter(typeof<IVector<'TAddress>>)
            let par2 = Expression.Parameter(typeof<IVector<'TAddress>>)
            let ty = typedefof<IVector<_, _>>.MakeGenericType(typeof<'TAddress>, vect1.ElementType)
            let expr =
              Expression.Lambda<System.Func<VectorCallSite2<'TAddress, 'R>, IVector<'TAddress>, IVector<'TAddress>, 'R>>
                ( Expression.Call(inst, mi, Expression.Convert(par1, ty), Expression.Convert(par2, ty)), [ inst; par1; par2 ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke(callSite, vect1, vect2)
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

type IVectorValueTransform =
  abstract GetFunction<'T> : unit -> (OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>)

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

  // Combines two aligned vectors. The function specifies how to merge values.
  | Combine of VectorConstruction<'TAddress> * VectorConstruction<'TAddress> * IVectorValueTransform


/// Represents an object that can construct vector values by processing 
// the "mini-DSL" representation `VectorConstruction<'TAddress, 'TValue>`
type IVectorBuilder<'TAddress> = 
  // Create a vector from array containing values or optional values
  abstract CreateNonOptional : 'TValue[] -> IVector<'TAddress, 'TValue>
  abstract CreateOptional : OptionalValue<'TValue>[] -> IVector<'TAddress, 'TValue>
  // Apply a vector construction to a given vector
  abstract Build<'TValue> : VectorConstruction<'TAddress> * IVector<'TAddress, 'TValue>[] -> IVector<'TAddress, 'TValue>



type VectorValueTransform =
  static member Create<'T>(operation:OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>) = 
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> -> OptionalValue<'R> -> OptionalValue<'R>> (box operation) }
  static member FillNA =
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> if l.HasValue then l else r) }
  static member LeftOrRight =
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue && r.HasValue then invalidOp "Combining vectors failed - both vectors have a value."
          if l.HasValue then l else r) }
