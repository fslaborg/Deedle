namespace FSharp.DataFrame.Vectors

open FSharp.DataFrame
open System.Collections.Generic

/// Provides a way to get the data of a vector. This is a concrete type used by 
/// functions that operate on vectors (like Series.sum, etc.). The vector may choose
/// to return the data as IReadOnlyList (with or without N/A values) which is more
/// efficient to use or as a lazy sequence (slower, but more general).
[<RequireQualifiedAccess>]
type VectorData<'T> = 
  | DenseList of IReadOnlyList<'T>
  | SparseList of IReadOnlyList<OptionalValue<'T>>
  | Sequence of seq<OptionalValue<'T>>

// --------------------------------------------------------------------------------------
// Interface (generic & non-generic) for representing vectors
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open FSharp.DataFrame.Common
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.Addressing

/// Represents an (untyped) vector that stores some values and provides access
/// to the values via a generic address. For convenience, the vector exposes
/// type of elements as System.Type.
type IVector = 
  abstract ElementType : System.Type
  abstract SuppressPrinting : bool
  abstract GetObject : Address -> OptionalValue<obj>

/// A generic typed vector. Represents mapping from addresses to 
/// values of type 'TValue. It is possible to get all data using the Data member.
type IVector<'T> = 
  inherit IVector 
  abstract GetValue : Address -> OptionalValue<'T>
  abstract Data : VectorData<'T>
  // TODO: Not entirely happy with these two being here... 
  abstract Select : ('T -> 'TNew) -> IVector<'TNew>
  abstract SelectOptional : (OptionalValue<'T> -> OptionalValue<'TNew>) -> IVector<'TNew>

[<AutoOpen>]
module VectorExtensions = 
  type IVector<'TValue> with
    /// Returns the data of the vector as a lazy sequence. (This preserves the 
    /// order of elements in the vector and so it also returns N/A values.)
    member x.DataSequence = 
      match x.Data with
      | VectorData.Sequence s -> s
      | VectorData.SparseList s -> upcast s
      | VectorData.DenseList s -> Seq.map (fun v -> OptionalValue(v)) s

module internal VectorHelpers =
  /// Pretty printer for vectors. This uses the 'Data' property
  let prettyPrintVector (vector:IVector<'T>) = 
    let printSequence kind (input:seq<string>) dots = 
      let sb = Text.StringBuilder(kind + " [")
      for it in input |> Seq.takeAtMost Formatting.ItemCount do 
        sb.Append(" ").Append(it).Append(";") |> ignore
      sb.Remove(sb.Length - 1, 1) |> ignore
      if dots then sb.Append("; ... ]").ToString() 
      else sb.Append(" ]").ToString()
    match vector.Data with
    | VectorData.DenseList list -> printSequence "dense" (Seq.map (fun v -> v.ToString()) list) (list.Count > Formatting.ItemCount)
    | VectorData.SparseList list -> printSequence "sparse" (Seq.map (fun v -> v.ToString()) list) (list.Count > Formatting.ItemCount)
    | VectorData.Sequence list -> printSequence "seq" (Seq.map (fun v -> v.ToString()) list) (Seq.length list > Formatting.ItemCount)

  /// Create a new vector that delegates all functionality to a ref vector
  let delegatedVector (vector:IVector<'TValue> ref) =
    { new IVector<'TValue> with
        member x.GetValue(a) = vector.Value.GetValue(a)
        member x.Data = vector.Value.Data
        member x.Select(f) = vector.Value.Select(f)
        member x.SelectOptional(f) = vector.Value.SelectOptional(f)
      interface IVector with
        member x.SuppressPrinting = vector.Value.SuppressPrinting
        member x.ElementType = vector.Value.ElementType
        member x.GetObject(i) = vector.Value.GetObject(i) }


  type ValueCallSite1<'R> =
    abstract Invoke<'T> : 'T -> 'R
  type VectorCallSite1<'R> =
    abstract Invoke<'T> : IVector<'T> -> 'R
  type VectorCallSite2<'R> =
    abstract Invoke<'T> : IVector<'T> * IVector<'T> -> 'R

  let doubleCode = typeof<float>.TypeHandle.Value
  let intCode = typeof<int>.TypeHandle.Value
  let stringCode = typeof<string>.TypeHandle.Value

  let createValueDispatcher<'R> (callSite:ValueCallSite1<'R>) =
    let dict = lazy Dictionary<_, System.Func<ValueCallSite1<'R>, obj, 'R>>()
    fun (value:obj) ->
      let ty = value.GetType()
      let code = ty.TypeHandle.Value
      if code = doubleCode then callSite.Invoke<float>(value :?> float)
      elif code = intCode then callSite.Invoke<int>(value :?> int)
      elif code = stringCode then callSite.Invoke<string>(value :?> string)
      else
        match dict.Value.TryGetValue(code) with
        | true, f -> f.Invoke(callSite, value)
        | _ ->
            let mi = typeof<ValueCallSite1<'R>>.GetMethod("Invoke").MakeGenericMethod(ty)
            let inst = Expression.Parameter(typeof<ValueCallSite1<'R>>)
            let par = Expression.Parameter(typeof<obj>)
            let expr =
              Expression.Lambda<System.Func<ValueCallSite1<'R>, obj, 'R>>
                ( Expression.Call(inst, mi, Expression.Convert(par, ty)), [ inst; par ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke(callSite, value)


  let createDispatcher<'R> (callSite:VectorCallSite1<'R>) =
    let dict = lazy Dictionary<_, System.Func<VectorCallSite1<'R>, IVector, 'R>>()
    fun (vect:IVector) ->
      let code = vect.ElementType.TypeHandle.Value
      if code = doubleCode then callSite.Invoke<float>(vect :?> IVector<float>)
      elif code = intCode then callSite.Invoke<int>(vect :?> IVector<int>)
      elif code = stringCode then callSite.Invoke<string>(vect :?> IVector<string>)
      else
        match dict.Value.TryGetValue(code) with
        | true, f -> f.Invoke(callSite, vect)
        | _ ->
            let mi = typeof<VectorCallSite1<'R>>.GetMethod("Invoke").MakeGenericMethod(vect.ElementType)
            let inst = Expression.Parameter(typeof<VectorCallSite1<'R>>)
            let par = Expression.Parameter(typeof<IVector>)
            let ty = typedefof<IVector<_>>.MakeGenericType(vect.ElementType)
            let expr =
              Expression.Lambda<System.Func<VectorCallSite1<'R>, IVector, 'R>>
                ( Expression.Call(inst, mi, Expression.Convert(par, ty)), [ inst; par ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke(callSite, vect)

  let createTwoArgDispatcher<'R> (callSite:VectorCallSite2<'R>) =
    let dict = lazy Dictionary<_, System.Func<VectorCallSite2<'R>, IVector, IVector, 'R>>()
    fun (vect1:IVector, vect2:IVector) ->
      let code = vect1.ElementType.TypeHandle.Value
      if vect2.ElementType.TypeHandle.Value <> code then 
        invalidOp "createTwoArgDispatcher: Both arguments should have the same element type"
      if code = doubleCode then callSite.Invoke<float>(vect1 :?> IVector<float>, vect2 :?> IVector<float>)
      elif code = intCode then callSite.Invoke<int>(vect1 :?> IVector<int>, vect2 :?> IVector<int>)
      elif code = stringCode then callSite.Invoke<string>(vect1 :?> IVector<string>, vect2 :?> IVector<string>)
      else
        match dict.Value.TryGetValue(code) with
        | true, f -> f.Invoke(callSite, vect1, vect2)
        | _ ->
            let mi = typeof<VectorCallSite2<'R>>.GetMethod("Invoke").MakeGenericMethod(vect1.ElementType)
            let inst = Expression.Parameter(typeof<VectorCallSite2<'R>>)
            let par1 = Expression.Parameter(typeof<IVector>)
            let par2 = Expression.Parameter(typeof<IVector>)
            let ty = typedefof<IVector<_>>.MakeGenericType(vect1.ElementType)
            let expr =
              Expression.Lambda<System.Func<VectorCallSite2<'R>, IVector, IVector, 'R>>
                ( Expression.Call(inst, mi, Expression.Convert(par1, ty), Expression.Convert(par2, ty)), [ inst; par1; par2 ])
            let func = expr.Compile()
            dict.Value.[code] <- func
            func.Invoke(callSite, vect1, vect2)

// --------------------------------------------------------------------------------------
// Types related to vectors that should not be exposed too directly
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame.Vectors

open FSharp.DataFrame
open FSharp.DataFrame.Common
open FSharp.DataFrame.Addressing

/// Represents a range inside a vector
type VectorRange = Address * Address

/// Representes a "variable" in the mini-DSL below
type VectorHole = int

type IVectorValueTransform =
  abstract GetFunction<'T> : unit -> (OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>)

/// A "mini-DSL" that describes construction of a vector. Vector can be constructed
/// from arrays of values, from existing vector value (of an unknown representation)
/// or by various range operations (relocate, drop, slicing, appending)
type VectorConstruction =

  // Convert an existing vector to another representation
  | Return of VectorHole

  // Reorders elements of the vector. Carries a new required vector range and a list
  // of relocations (each pair of addresses specifies that an element at a new address 
  // should be filled with an element from an old address). THe addresses may be out of range!
  | Relocate of VectorConstruction * VectorRange * seq<Address * Address>

  // Drop part of range & get subrange & append multiple vectors
  | DropRange of VectorConstruction * VectorRange 
  | GetRange of VectorConstruction * VectorRange
  | Append of VectorConstruction * VectorConstruction

  // Combines two aligned vectors. The function specifies how to merge values.
  | Combine of VectorConstruction * VectorConstruction * IVectorValueTransform

  | CustomCommand of list<VectorConstruction> * (list<IVector> -> IVector)

/// Represents an object that can construct vector values by processing 
// the "mini-DSL" representation `VectorConstruction`
type IVectorBuilder = 
  // Create a vector from array containing values or optional values
  abstract CreateNonOptional : 'TValue[] -> IVector<'TValue>
  abstract CreateOptional : OptionalValue<'TValue>[] -> IVector<'TValue>
  // Apply a vector construction to a given vector
  abstract Build<'TValue> : VectorConstruction * IVector<'TValue>[] -> IVector<'TValue>



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

module VectorHelpers =
  // A "generic function" that boxes all values of a vector (IVector<int, 'T> -> IVector<int, obj>)
  let boxVector<'T> = 
    { new VectorHelpers.VectorCallSite1<IVector<obj>> with
        override x.Invoke<'T>(col:IVector<'T>) = col.Select(box) }
    |> VectorHelpers.createDispatcher

  // A "generic function" that transforms a generic vector using specified transformation
  let transformColumn (vectorBuilder:IVectorBuilder) rowCmd = 
    { new VectorHelpers.VectorCallSite1<IVector> with
        override x.Invoke<'T>(col:IVector<'T>) = 
          vectorBuilder.Build<'T>(rowCmd, [| col |]) :> IVector }
    |> VectorHelpers.createDispatcher