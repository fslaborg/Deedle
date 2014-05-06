// --------------------------------------------------------------------------------------
// Internal helpers for working with vectors
// --------------------------------------------------------------------------------------

/// A module with various utilities for working with vectors. 
module internal Deedle.VectorHelpers

open System
open System.Reflection
open System.Linq 
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


// --------------------------------------------------------------------------------------
// Derived/wrapped implementations of the IVector<'T> interface
// --------------------------------------------------------------------------------------

/// Represents a vector containing objects, that has been created by "boxing" a vector
/// containing values of any (likely more specific type). Given a boxed vector, we can 
/// get the original vector containing original values via the 'UnboxedVector' property
type IBoxedVector = 
  inherit IVector<obj>
  abstract UnboxedVector : IVector

/// Creates a boxed vector - returns IBoxedVector that delegates all functionality to 
/// the vector specified as an argument and boxes all values on the fly
let createBoxedVector (vector:IVector<'TValue>) = 
  { new System.Object() with
      member x.Equals(another) = vector.Equals(another)
      member x.GetHashCode() = vector.GetHashCode()
    interface IBoxedVector with
      member x.UnboxedVector = vector :> IVector
    interface IVector<obj> with
      member x.GetValue(a) = vector.GetObject(a)
      member x.Data = 
        match vector.Data with
        | VectorData.DenseList list -> 
            VectorData.DenseList(ReadOnlyCollection.map box list)
        | VectorData.SparseList list ->
            VectorData.SparseList(ReadOnlyCollection.map (OptionalValue.map box) list)
        | VectorData.Sequence list ->
            VectorData.Sequence(Seq.map (OptionalValue.map box) list)
      member x.Select(f) = vector.Select(f)
      member x.SelectMissing(f) = vector.SelectMissing(OptionalValue.map box >> f)
    interface IVector with
      member x.ObjectSequence = vector.ObjectSequence
      member x.SuppressPrinting = vector.SuppressPrinting
      member x.ElementType = typeof<obj>
      member x.GetObject(i) = vector.GetObject(i) 
      member x.Invoke(site) = 
        // Note: This means that the call site will be invoked on the 
        // underlying (more precisely typed) vector of this boxed vector!
        vector.Invoke(site) }

// --------------------------------------------------------------------------------------
// Generic operations 
// --------------------------------------------------------------------------------------

/// Represents a generic function `\forall.'T.('T -> 'R)`. The function can be 
/// generically invoked on an argument of type `obj` using `createValueDispatcher`
type ValueCallSite<'R> =
  abstract Invoke<'T> : 'T -> 'R

/// Type code of the `float` type for efficient type equality test
let doubleCode = typeof<float>.TypeHandle.Value
/// Type code of the `int` type for efficient type equality test
let intCode = typeof<int>.TypeHandle.Value
/// Type code of the `string` type for efficient type equality test
let stringCode = typeof<string>.TypeHandle.Value

/// Creates a function `obj -> 'R` that dynamically invokes to 
/// a generic `Invoke` method of the provided `ValueCallSite<'R>`
let createValueDispatcher<'R> (callSite:ValueCallSite<'R>) =
  let dict = lazy Dictionary<_, System.Func<ValueCallSite<'R>, obj, 'R>>()
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
          let mi = typeof<ValueCallSite<'R>>.GetMethod("Invoke").MakeGenericMethod(ty)
          let inst = Expression.Parameter(typeof<ValueCallSite<'R>>)
          let par = Expression.Parameter(typeof<obj>)
          let expr =
            Expression.Lambda<System.Func<ValueCallSite<'R>, obj, 'R>>
              ( Expression.Call(inst, mi, Expression.Convert(par, ty)), [ inst; par ])
          let func = expr.Compile()
          dict.Value.[code] <- func
          func.Invoke(callSite, value)


/// A type that implements common vector value transformations and 
/// a helper method for creating transformation on values of known types
type VectorValueTransform =
  /// Creates a transformation that applies the specified function on `'T` values 
  static member inline Create<'T>(operation:OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>) = 
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
        member vt.GetBinaryFunction<'R>() : option<OptionalValue<'R> * _ -> _> = None
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> list -> OptionalValue<'R>> (box operation) }
  /// A generic transformation that works when at most one value is defined
  static member AtMostOne =
    { new IVectorValueListTransform with
        member vt.GetBinaryFunction<'R>() = Some(fun (l:OptionalValue<'R>, r:OptionalValue<'R>) ->
          if l.HasValue && r.HasValue then invalidOp "Combining vectors failed - both vectors have a value."
          if l.HasValue then l else r)
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R> list) ->
          l |> List.fold (fun s v -> 
            if s.HasValue && v.HasValue then invalidOp "Combining vectors failed - more than one vector has a value."
            if v.HasValue then v else s) OptionalValue.Missing) }

// A "generic function" that boxes all values of a vector (IVector<'T> -> IVector<obj>)
let boxVector (vector:IVector) =
  { new VectorCallSite<IVector<obj>> with
      override x.Invoke<'T>(col:IVector<'T>) = createBoxedVector(col) :> IVector<obj> }
  |> vector.Invoke

/// Given a vector, check whether it is `IBoxedVector` and if so, return the 
/// underlying unboxed vector (see `IBoxedVector` for more information)
let inline unboxVector (v:IVector) = 
  match v with 
  | :? IBoxedVector as vec -> vec.UnboxedVector
  | vec -> vec 

// A "generic function" that transforms a generic vector using specified transformation
let transformColumn (vectorBuilder:IVectorBuilder) rowCmd (vector:IVector) = 
  { new VectorCallSite<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        vectorBuilder.Build<'T>(rowCmd, [| col |]) :> IVector }
  |> vector.Invoke

// A "generic function" that changes the type of vector elements
let changeType<'R> (vector:IVector) = 
  match unboxVector vector with
  | :? IVector<'R> as res -> res
  | vector ->
      { new VectorCallSite<IVector<'R>> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            col.Select(Convert.changeType<'R>) }
      |> vector.Invoke

// A "generic function" that tries to change the type of vector elements
let tryChangeType<'R> (vector:IVector) : OptionalValue<IVector<'R>> = 
  let shouldBeConvertible (o:obj) = o :? 'R || o :? IConvertible
  match unboxVector vector with
  | :? IVector<'R> as res -> OptionalValue(res)
  | vector ->
      { new VectorCallSite<OptionalValue<IVector<'R>>> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            // Check the first non-missing value to see if we should even try doing the conversion
            let first = 
              col.DataSequence 
              |> Seq.choose (fun v -> 
                  if v.HasValue && (box v.Value) <> null 
                  then Some (box v.Value) else None) 
              |> Seq.headOrNone 
              |> Option.map shouldBeConvertible
            if first = Some(false) then OptionalValue.Missing
            else 
              // We still cannot be sure that it will actually work
              try OptionalValue(col.Select(fun v -> Convert.changeType<'R> v))
              with :? InvalidCastException | :? FormatException -> OptionalValue.Missing }
      |> vector.Invoke

// A "generic function" that tries to cast the type of vector elements
let tryCastType<'R> (vector:IVector) : OptionalValue<IVector<'R>> = 
  let shouldBeCastable (o:obj) = o :? 'R
  match unboxVector vector with
  | :? IVector<'R> as res -> OptionalValue(res)
  | vector ->
      { new VectorCallSite<OptionalValue<IVector<'R>>> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            // Check the first non-missing value to see if we should even try doing the conversion
            let first = 
              col.DataSequence 
              |> Seq.choose (fun v -> 
                  if v.HasValue && (box v.Value) <> null 
                  then Some (box v.Value) else None) 
              |> Seq.headOrNone 
              |> Option.map shouldBeCastable
            if first = Some(false) then OptionalValue.Missing
            else 
              // We still cannot be sure that it will actually work
              try OptionalValue(col.Select(fun v -> unbox<'R> v))
              with :? InvalidCastException -> OptionalValue.Missing }
      |> vector.Invoke

/// A "generic function" that drops a specified range from any vector
let getVectorRange (builder:IVectorBuilder) range (vector:IVector) = 
  { new VectorCallSite<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        let cmd = VectorConstruction.GetRange(VectorConstruction.Return 0, range)
        builder.Build(cmd, [| col |]) :> IVector }
  |> vector.Invoke

/// Active pattern that calls the `tryChangeType<float>` function
let (|AsFloatVector|_|) v : option<IVector<float>> = 
  OptionalValue.asOption (tryChangeType v)

/// A virtual vector for reading "row" of a data frame. The virtual vector accesses
/// internal representation of the frame (specified by `data` and `columnCount`).
/// The type is generic and automatically converts the values from the underlying
/// (untyped) vector to the specified type.
type RowReaderVector<'T>(data:IVector<IVector>, builder:IVectorBuilder, columnCount:int64, rowAddress) =

  // Comparison and get hash code follows the ArrayVector implementation
  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'T> as another -> 
        Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() = vector.DataSequence |> Seq.structuralHash

  member private vector.DataArray =
    Array.init (int columnCount) (fun addr -> (vector :> IVector<_>).GetValue(Address.ofInt addr))
      
  // In the generic vector implementation, we
  // read data as objects and perform conversion
  interface IVector<'T> with
    member x.GetValue(columnAddress) = 
      let vector = data.GetValue(columnAddress)
      if not vector.HasValue then OptionalValue.Missing
      else vector.Value.GetObject(rowAddress) |> OptionalValue.map (Convert.changeType<'T>)

    member vector.Data = 
      vector.DataArray |> ReadOnlyCollection.ofArray |> VectorData.SparseList 

    member vector.Select(f) = 
      (vector :> IVector<_>).SelectMissing(OptionalValue.map f)

    member vector.SelectMissing(f) = 
      let isNA = MissingValues.isNA<'TNewValue>() 
      let flattenNA (value:OptionalValue<_>) = 
        if value.HasValue && isNA value.Value then OptionalValue.Missing else value
      let data = vector.DataArray |> Array.map (f >> flattenNA)
      builder.CreateMissing(data)

  // Non-generic interface is fully implemented as "virtual"   
  interface IVector with
    member x.ObjectSequence = x.DataArray |> Seq.map (OptionalValue.map box)
    member x.SuppressPrinting = false
    member x.ElementType = typeof<'T>
    member x.GetObject(i) = OptionalValue.map box ((unbox<IVector<'T>> x).GetValue(i))
    member x.Invoke(site) = site.Invoke(unbox<IVector<'T>> x)


/// Creates a virtual vector for reading "row" of a data frame. 
// For more information, see the `RowReaderVector<'T>` type.
let inline createRowReader (data:IVector<IVector>) (builder:IVectorBuilder) columnCount rowAddress =
  RowReaderVector<'T>(data, builder, columnCount, rowAddress) :> IVector<'T>
 
/// The same as `createRowReader`, but returns `obj` vector as the result
let inline createObjRowReader data builder colmap addr : IVector<obj> = 
  createRowReader data builder colmap addr

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

/// Return data from a (column-major) vector of vectors as 2D array of a specified type
/// If value is missing, `defaultValue` is used (which may throw an exception)
let toArray2D<'R> rowCount colCount (data:IVector<IVector>) (defaultValue:Lazy<'R>) =
    let res = Array2D.zeroCreate rowCount colCount 
    data.DataSequence
    |> Seq.iteri (fun c vector ->
      if vector.HasValue then
        changeType(vector.Value).DataSequence
        |> Seq.iteri (fun r v -> 
            res.[r,c] <- if v.HasValue then v.Value else defaultValue.Value )
      else for r = 0 to rowCount - 1 do res.[r, c] <- defaultValue.Value )
    res

/// Helper functions and active patterns for type inference
module Inference = 

  // When we get multiple primitive values that could be converted to a common
  // type, we choose 'int', 'int64', 'float' (for numbers) or 'string' (for strings and characters)
  // In principle, we could do better and find "least upper bound" of the conversion relation
  // but choosing one of the common types seems to be good enough.
  let inline isType types t = if List.exists ((=) t) types then Some() else None
  let intTypes = [ typeof<byte>; typeof<sbyte>; typeof<int16>; typeof<uint16>; typeof<int> ]
  let int64Types = intTypes @ [ typeof<uint32>; typeof<int64> ]
  let floatTypes = int64Types @ [ typeof<decimal>; typeof<uint64>; typeof<float32>; typeof<float> ]
  let stringTypes = [ typeof<char>; typeof<string> ]

  /// Classsify type as one of the supported primitives
  let (|Top|_|) (ty:System.Type) = if ty = null then Some() else None
  let (|Bottom|_|) ty = if ty = typeof<obj> then Some() else None
  let (|AsInt|_|) ty = isType intTypes ty
  let (|AsInt64|_|) ty = isType int64Types ty
  let (|AsFloat|_|) ty = isType floatTypes ty
  let (|AsString|_|) ty = isType stringTypes ty

  /// System.Type representing bottom
  let Bottom = typeof<obj>
  /// System.Type representing top
  let Top : System.Type = null

  /// Given two types, find their common supertype
  let commonSupertype t1 t2 = 
    match t1, t2 with
    // Top (null) and anything is the other thing
    | Top, t | t, Top -> t
    // When they are the same type, just return it
    | _ when t1 = t2 -> t1
    // Bottom and anything is Bottom (one is 'obj')
    | Bottom, _ | _, Bottom -> Bottom
    // When they are subtypes according to .NET, use that
    | _ when t1.IsAssignableFrom(t2) -> t1
    | _ when t2.IsAssignableFrom(t1) -> t2

    // Both can be converted to the same primitive type
    | AsString, AsString -> typeof<string>
    | AsInt, AsInt -> typeof<int>
    | AsInt64, AsInt64 -> typeof<int64>
    | AsFloat, AsFloat -> typeof<float>

    // No conversion is possible, so return bottom
    | _, _ -> Bottom

/// Helper object called by createTypedVector via reflection
type CreateTypedVectorHelper = 
  static member Create<'T>(builder:IVectorBuilder, data:obj[]) =
    builder.Create(Array.map Convert.changeType<'T> data)

/// Given object array, create a typed vector of the best possible type
let createTypedVector (builder:IVectorBuilder) (vectorType:System.Type) (data:obj[]) =
  let flags = System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Static
  let createMi = typeof<CreateTypedVectorHelper>.GetMethod("Create", flags).MakeGenericMethod [| vectorType |]
  createMi.Invoke(null, [| builder; data |]) :?> IVector

/// Find common super type of the specified .NET types
/// (This also allows implicit conversions between primitive values, so casting values
/// to the common super type would fail, but Convert.changeType will work fine)
let findCommonSupertype types = 
  let ty = types |> Seq.fold Inference.commonSupertype Inference.Top
  if ty = Inference.Top then Inference.Bottom else ty

/// Given object array, create a typed vector of the best possible type
let createInferredTypeVector (builder:IVectorBuilder) (data:obj[]) =
  let vectorType = data |> Seq.map (fun v -> 
    if v = null then Inference.Top else v.GetType()) |> findCommonSupertype
  createTypedVector builder vectorType data

/// Substitute variable hole for another in a vector construction
let rec substitute ((oldVar, newVar) as subst) = function
  | Return v when v = oldVar -> Return newVar
  | Return v -> Return v
  | Empty size -> Empty size
  | FillMissing(vc, d) -> FillMissing(substitute subst vc, d)
  | Relocate(vc, r, l) -> Relocate(substitute subst vc, r, l)
  | DropRange(vc, r) -> DropRange(substitute subst vc, r)
  | GetRange(vc, r) -> GetRange(substitute subst vc, r)
  | Append(l, r) -> Append(substitute subst l, substitute subst r)
  | Combine(l, r, c) -> Combine(substitute subst l, substitute subst r, c)
  | CombineN(lst, c) -> CombineN(List.map (substitute subst) lst, c)
  | CustomCommand(vcs, f) -> CustomCommand(List.map (substitute subst) vcs, f)
  | AsyncCustomCommand(vcs, f) -> AsyncCustomCommand(List.map (substitute subst) vcs, f)

/// Matches when the vector command represents a combination
/// of N relocated vectors (that is CombineN [Relocate ..; Relocate ..; ...])
let (|CombinedRelocations|_|) = function
  | CombineN(list, op) ->
      if op.GetBinaryFunction<unit>().IsNone then None else
      if list |> List.forall (function Relocate _ -> true | _ -> false) then
        let parts = list |> List.map (function Relocate(a,b,c) -> (a,b,c) | _ -> failwith "logic error")
        Some(parts, op)
      else None
  | _ -> None