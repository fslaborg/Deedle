// --------------------------------------------------------------------------------------
// Internal helpers for working with vectors
// --------------------------------------------------------------------------------------

/// A module with various utilities for working with vectors. 
module internal Deedle.VectorHelpers

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open Deedle
open Deedle.Internal
open Deedle.Vectors
open Deedle.Addressing

// --------------------------------------------------------------------------------------
// Various
// --------------------------------------------------------------------------------------

/// Pretty printer for vectors. This uses the 'Data' property
let prettyPrintVector (vector:IVector<'T>) = 
  let printSequence kind (input:seq<string>) = 
    let sb = Text.StringBuilder(kind + " [")
    for it in input |> Seq.startAndEnd Formatting.StartItemCount Formatting.EndItemCount do
      match it with 
      | Choice1Of3(v) | Choice3Of3(v) -> 
          sb.Append(" ").Append(v).Append(";") |> ignore
      | Choice2Of3() -> sb.Append(" ... ") |> ignore
    sb.Append(" ]").ToString()
  match vector.Data with
  | VectorData.DenseList list -> printSequence "dense" (Seq.map (fun v -> v.ToString()) list) 
  | VectorData.SparseList list -> printSequence "sparse" (Seq.map (fun v -> v.ToString()) list) 
  | VectorData.Sequence list -> printSequence "seq" (Seq.map (fun v -> v.ToString()) list) 

/// Create a new vector that delegates all functionality to a ref vector
let delegatedVector (vector:IVector<'TValue> ref) =
  { new IVector<'TValue> with
      member x.GetValue(a) = vector.Value.GetValue(a)
      member x.Data = vector.Value.Data
      member x.Select(f) = vector.Value.Select(f)
      member x.SelectMissing(f) = vector.Value.SelectMissing(f)
    interface IVector with
      member x.ObjectSequence = vector.Value.ObjectSequence
      member x.SuppressPrinting = vector.Value.SuppressPrinting
      member x.ElementType = vector.Value.ElementType
      member x.GetObject(i) = vector.Value.GetObject(i) }


// --------------------------------------------------------------------------------------
// Generic operations 
// --------------------------------------------------------------------------------------

/// Represents a generic function `\forall.'T.('T -> 'R)`. The function can be 
/// generically invoked on an argument of type `obj` using `createValueDispatcher`
type ValueCallSite1<'R> =
  abstract Invoke<'T> : 'T -> 'R

/// Represents a generic function `\forall.'T.(IVector<'T> -> 'R)`. The function can be 
/// generically invoked on an argument of type `IVector` using `createVectorDispatcher`
type VectorCallSite1<'R> =
  abstract Invoke<'T> : IVector<'T> -> 'R

/// Represents a generic function `\forall.'T.(IVector<'T> * IVector<'T> -> 'R)`. The function 
/// can be generically invoked on a pair of `IVector` values using `createTwoVectorDispatcher`
type VectorCallSite2<'R> =
  abstract Invoke<'T> : IVector<'T> * IVector<'T> -> 'R

/// Type code of the `float` type for efficient type equality test
let doubleCode = typeof<float>.TypeHandle.Value
/// Type code of the `int` type for efficient type equality test
let intCode = typeof<int>.TypeHandle.Value
/// Type code of the `string` type for efficient type equality test
let stringCode = typeof<string>.TypeHandle.Value

/// Creates a function `obj -> 'R` that dynamically invokes to 
/// a generic `Invoke` method of the provided `ValueCallSite1<'R>`
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

/// Creates a function `IVector -> 'R` that dynamically invokes to 
/// a generic `Invoke` method of the provided `VectorCallSite1<'R>`
let createVectorDispatcher<'R> (callSite:VectorCallSite1<'R>) =
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

/// Creates a function `IVector * IVector -> 'R` that dynamically invokes to 
/// a generic `Invoke` method of the provided `VectorCallSite2<'R>`
let createTwoVectorDispatcher<'R> (callSite:VectorCallSite2<'R>) =
  let dict = lazy Dictionary<_, System.Func<VectorCallSite2<'R>, IVector, IVector, 'R>>()
  fun (vect1:IVector, vect2:IVector) ->
    let code = vect1.ElementType.TypeHandle.Value
    if vect2.ElementType.TypeHandle.Value <> code then 
      invalidOp "createTwoVectorDispatcher: Both arguments should have the same element type"
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

/// A type that implements common vector value transformations and 
/// a helper method for creating transformation on values of known types
type VectorValueTransform =
  /// Creates a transformation that applies the specified function on `'T` values 
  static member Create<'T>(operation:OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>) = 
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> -> OptionalValue<'R> -> OptionalValue<'R>> (box operation) }
  /// Creates a transformation that applies the specified function on `'T` values 
  static member CreateLifted<'T>(operation:'T -> 'T -> 'T) = 
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue && r.HasValue then OptionalValue((unbox<'R -> 'R -> 'R> operation) l.Value r.Value)
          else OptionalValue.Missing )}
  /// A generic transformation that prefers the left value (if it is not missing)
  static member LeftIfAvailable =
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue then l else r) }
  /// A generic transformation that prefers the left value (if it is not missing)
  static member RightIfAvailable =
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if r.HasValue then r else l) }
  /// A generic transformation that works when at most one value is defined
  static member LeftOrRight =
    { new IVectorValueTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue && r.HasValue then invalidOp "Combining vectors failed - both vectors have a value."
          if l.HasValue then l else r) }

type VectorValueListTransform =
  /// Creates a transformation that applies the specified function on `'T` values list
  static member Create<'T>(operation:OptionalValue<'T> list -> OptionalValue<'T>) = 
    { new IVectorValueListTransform with
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> list -> OptionalValue<'R>> (box operation) }
  /// A generic transformation that works when at most one value is defined
  static member AtMostOne =
    { new IVectorValueListTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R> list) ->
          l |> List.fold (fun s v -> 
            if s.HasValue && v.HasValue then invalidOp "Combining vectors failed - more than one vector has a value."
            if v.HasValue then v else s) OptionalValue.Missing) }

// A "generic function" that boxes all values of a vector (IVector<int, 'T> -> IVector<int, obj>)
let boxVector () = 
  { new VectorCallSite1<IVector<obj>> with
      override x.Invoke<'T>(col:IVector<'T>) = col.Select(box) }
  |> createVectorDispatcher

// A "generic function" that transforms a generic vector using specified transformation
let transformColumn (vectorBuilder:IVectorBuilder) rowCmd = 
  { new VectorCallSite1<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        vectorBuilder.Build<'T>(rowCmd, [| col |]) :> IVector }
  |> createVectorDispatcher

// A "generic function" that changes the type of vector elements
let changeType<'R> : IVector -> IVector<'R> = 
  { new VectorCallSite1<IVector<'R>> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        col.Select(Convert.changeType<'R>) }
  |> createVectorDispatcher

// A "generic function" that tries to change the type of vector elements
let tryChangeType<'R> : IVector -> OptionalValue<IVector<'R>> = 
  let shouldBeConvertible (o:obj) = o <> null && o :? IConvertible
  { new VectorCallSite1<OptionalValue<IVector<'R>>> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        // Check the first non-missing value to see if we should even try doing the conversion
        let first = 
          col.DataSequence |> Seq.choose OptionalValue.asOption |> Seq.headOrNone 
          |> Option.map (box >> shouldBeConvertible)
        if first = Some(false) then OptionalValue.Missing
        else 
          // We still cannot be sure that it will actually work
          try OptionalValue(col.Select(fun v -> Convert.changeType<'R> v))
          with :? InvalidCastException | :? FormatException -> OptionalValue.Missing }
  |> createVectorDispatcher

// A "generic function" that tries to cast the type of vector elements
let tryCastType<'R> : IVector -> OptionalValue<IVector<'R>> = 
  let shouldBeConvertible (o:obj) = o <> null && o :? 'R
  { new VectorCallSite1<OptionalValue<IVector<'R>>> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        // Check the first non-missing value to see if we should even try doing the conversion
        let first = 
          col.DataSequence |> Seq.choose OptionalValue.asOption |> Seq.headOrNone 
          |> Option.map (box >> shouldBeConvertible)
        if first = Some(false) then OptionalValue.Missing
        else 
          // We still cannot be sure that it will actually work
          try OptionalValue(col.Select(fun v -> unbox<'R> v))
          with :? InvalidCastException -> OptionalValue.Missing }
  |> createVectorDispatcher

// A "generic function" that drops 
let getVectorRange (builder:IVectorBuilder) range = 
  { new VectorCallSite1<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        let cmd = VectorConstruction.GetRange(VectorConstruction.Return 0, range)
        builder.Build(cmd, [| col |]) :> IVector }
  |> createVectorDispatcher

// A "generic function" that fills NA values
let fillNA (def:obj) : IVector -> IVector = 
  { new VectorCallSite1<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        col.SelectMissing(function
          | OptionalValue.Missing -> OptionalValue(unbox def)
          | OptionalValue.Present v -> OptionalValue(v)) :> IVector }
  |> createVectorDispatcher

/// Helper type that is used via reflection
type TryValuesHelper =
  /// Turns IVector<TryValue<'T>> into TryValue<IVector<'T>> by aggregating all exceptions
  /// (used via reflection by the `tryValues` function below)
  static member TryValues<'T>(vector:IVector<'T tryval>) = 
    let exceptions = vector.DataSequence |> Seq.choose OptionalValue.asOption |> Seq.choose (fun tv -> 
      if tv.HasValue then None else Some tv.Exception) |> List.ofSeq
    if List.isEmpty exceptions then TryValue.Success (vector.Select(fun v -> v.Value) :> IVector)
    else TryValue.Error (new AggregateException(exceptions))

/// Given an IVector, check if the vector contains `'T tryval` values and if it does,
/// turn that into a vector of just `'T` values, or return aggregated exception
let tryValues (vect:IVector) =
  let elty = vect.ElementType
  // Does the specified vector represent 'tryval' column?
  if elty.IsGenericType && elty.GetGenericTypeDefinition() = typedefof<_ tryval> then
    let tyarg = elty.GetGenericArguments().[0]
    let mi = typeof<TryValuesHelper>.GetMethod("TryValues").MakeGenericMethod [|tyarg|]        
    mi.Invoke(null, [| vect |]) :?> TryValue<IVector>
  else TryValue.Success vect

/// Helper functions and active patterns for type inference
module Inference = 
  /// Classsify type as one of the supported primitives
  let (|Top|Bottom|String|Int|Float|) ty =
    if ty = null then Top
    elif ty = typeof<int> || ty = typeof<int16> || ty = typeof<byte> then Int
    elif ty = typeof<string> || ty = typeof<char> then String
    elif ty = typeof<float32> || ty = typeof<float> then Float
    else Bottom

  /// System.Type representing bottom
  let Bottom = typeof<obj>
  /// System.Type representing top
  let Top : System.Type = null

  /// Given two types, find their common supertype
  let commonSupertype t1 t2 = 
    match t1, t2 with
    // Top and anything is the other thing
    | Top, t | t, Top -> t
    // Bottom and anything is Bottom
    | Bottom, _ | _, Bottom -> Bottom
    // A type with itself is a type
    | String, String -> typeof<string>
    | Int, Int -> typeof<int>
    | Float, Float -> typeof<float>
    // Ints can be converted to floats
    | Int, Float | Float, Int -> typeof<float>
    // String cannot be implicitly converted to anything else
    | String, _ | _, String -> Bottom

/// Helper object called by createTypedVector via reflection
type CreateTypedVectorHelper = 
  static member Create<'T>(builder:IVectorBuilder, data:obj[]) =
    builder.Create(Array.map unbox<'T> data)

/// Given object array, create a typed vector of the best possible type
let createTypedVector (builder:IVectorBuilder) (data:obj[]) =
  // Infer the vector type
  let vectorType = 
    data |> Array.map (fun v -> if v = null then Inference.Top else v.GetType()) 
         |> Seq.reduce Inference.commonSupertype
  let vectorType = if vectorType = Inference.Top then Inference.Bottom else vectorType

  // Create specialized method info and invoke it
  let flags = System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Static
  let createMi = typeof<CreateTypedVectorHelper>.GetMethod("Create", flags).MakeGenericMethod [| vectorType |]
  createMi.Invoke(null, [| builder; data |]) :?> IVector

/// Substitute variable hole for another in a vector construction
let rec substitute ((oldVar, newVar) as subst) = function
  | Return v when v = oldVar -> Return newVar
  | Return v -> Return v
  | Empty -> Empty
  | FillMissing(vc, d) -> FillMissing(substitute subst vc, d)
  | Relocate(vc, r, l) -> Relocate(substitute subst vc, r, l)
  | DropRange(vc, r) -> DropRange(substitute subst vc, r)
  | GetRange(vc, r) -> GetRange(substitute subst vc, r)
  | Append(l, r) -> Append(substitute subst l, substitute subst r)
  | Combine(l, r, c) -> Combine(substitute subst l, substitute subst r, c)
  | CustomCommand(vcs, f) -> CustomCommand(List.map (substitute subst) vcs, f)
  | AsyncCustomCommand(vcs, f) -> AsyncCustomCommand(List.map (substitute subst) vcs, f)
