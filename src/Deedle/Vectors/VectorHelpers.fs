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

module RangeRestriction =
  /// Creates a `Custom` range from a sequence of indices
  let ofSeq count (indices : seq<_>) =
    { new IRangeRestriction<Address> with
        member x.Count = count
      interface seq<Address> with 
        member x.GetEnumerator() = (indices :> seq<_>).GetEnumerator() 
      interface System.Collections.IEnumerable with
        member x.GetEnumerator() = indices.GetEnumerator() :> _ } 
    |> RangeRestriction.Custom

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
      member x.GetValueAtLocation(l) = vector.GetValueAtLocation(l) |> OptionalValue.map box
      member x.Data = 
        match vector.Data with
        | VectorData.DenseList list -> 
            VectorData.DenseList(ReadOnlyCollection.map box list)
        | VectorData.SparseList list ->
            VectorData.SparseList(ReadOnlyCollection.map (OptionalValue.map box) list)
        | VectorData.Sequence list ->
            VectorData.Sequence(Seq.map (OptionalValue.map box) list)
      member x.Select(f) = vector.Select(fun loc v -> f loc (OptionalValue.map box v))
      member x.Convert(f, g) = vector.Convert(box >> f, g >> unbox)
    interface IVector with
      member x.AddressingScheme = vector.AddressingScheme
      member x.Length = vector.Length
      member x.ObjectSequence = vector.ObjectSequence
      member x.SuppressPrinting = vector.SuppressPrinting
      member x.ElementType = typeof<obj>
      member x.GetObject(i) = vector.GetObject(i) 
      member x.Invoke(site) = 
        // Note: This means that the call site will be invoked on the 
        // underlying (more precisely typed) vector of this boxed vector!
        vector.Invoke(site) }


/// Used to mark vectors that are just light-weight wrappers over some computation
/// When vector builders perform operations on those, they might want to use the
/// fully evaluated unwrapped value so that they can e.g. check for 
/// interface implementations
type IWrappedVector<'T> = 
  inherit IVector<'T>
  abstract UnwrapVector : unit -> IVector<'T>

/// Creates a vector that lazily applies the specified projection `f` on 
/// the values of the source `vector`. In general, Deedle does not secretly delay
/// computations, so this should be used with care. Currently, we only use this
/// to avoid allocations in `df.Rows`.
let lazyMapVector (f:'TValue -> 'TResult) (vector:IVector<'TValue>) : IVector<'TResult> = 
  let unwrapVector = lazy vector.Select(f)
  { new System.Object() with
      member x.Equals(another) = vector.Equals(another)
      member x.GetHashCode() = vector.GetHashCode()
    interface IVector<'TResult> with
      member x.GetValue(a) = vector.GetValue(a) |> OptionalValue.map f
      member x.GetValueAtLocation(l) = vector.GetValueAtLocation(l) |> OptionalValue.map f
      member x.Data = 
        match vector.Data with
        | VectorData.DenseList list -> 
            VectorData.DenseList(ReadOnlyCollection.map f list)
        | VectorData.SparseList list ->
            VectorData.SparseList(ReadOnlyCollection.map (OptionalValue.map f) list)
        | VectorData.Sequence list ->
            VectorData.Sequence(Seq.map (OptionalValue.map f) list)
      member x.Select(g) = vector.Select(fun loc v -> g loc (OptionalValue.map f v))
      member x.Convert(h, g) = invalidOp "lazyMapVector: Conversion is not supported"
    interface IWrappedVector<'TResult> with
      member x.UnwrapVector() = unwrapVector.Value
    interface IVector with
      member x.AddressingScheme = vector.AddressingScheme
      member x.Length = vector.Length
      member x.ObjectSequence = vector.ObjectSequence
      member x.SuppressPrinting = vector.SuppressPrinting
      member x.ElementType = typeof<'TResult>
      member x.GetObject(i) = vector.GetObject(i) 
      member x.Invoke(site) = invalidOp "lazyMapVector: Invocation is not supported" }

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
type BinaryTransform =
  /// Creates a transformation that applies the specified function on `'T` values 
  static member inline Create<'T>(operation:OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>) = 
    { new IBinaryTransform with
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> -> OptionalValue<'R> -> OptionalValue<'R>> (box operation) 
        member vt.IsMissingUnit = false } 
    |> VectorListTransform.Binary

  /// Creates a transformation that applies the specified function on `'T` values 
  static member inline CreateLifted<'T>(operation:'T -> 'T -> 'T) = 
    { new IBinaryTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue && r.HasValue then OptionalValue((unbox<'R -> 'R -> 'R> (box operation)) l.Value r.Value)
          else OptionalValue.Missing )
        member vt.IsMissingUnit = false }
    |> VectorListTransform.Binary

  /// A generic transformation that prefers the left value (if it is not missing)
  static member LeftIfAvailable =
    { new IBinaryTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue then l else r) 
        member vt.IsMissingUnit = true }
    |> VectorListTransform.Binary

  /// A generic transformation that prefers the left value (if it is not missing)
  static member RightIfAvailable =
    { new IBinaryTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if r.HasValue then r else l)
        member vt.IsMissingUnit = true }
    |> VectorListTransform.Binary

  /// A generic transformation that works when at most one value is defined
  static member AtMostOne =
    { new IBinaryTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R>) (r:OptionalValue<'R>) -> 
          if l.HasValue && r.HasValue then invalidOp "Combining vectors failed - both vectors have a value."
          if l.HasValue then l else r)
        member vt.IsMissingUnit = true }
    |> VectorListTransform.Binary

type NaryTransform =
  /// Creates a transformation that applies the specified function on `'T` values list
  static member Create<'T>(operation:OptionalValue<'T> list -> OptionalValue<'T>) = 
    { new INaryTransform with
        member vt.GetFunction<'R>() = 
          unbox<OptionalValue<'R> list -> OptionalValue<'R>> (box operation) }
    |> VectorListTransform.Nary

  /// A generic transformation that works when at most one value is defined
  static member AtMostOne =
    { new INaryTransform with
        member vt.GetFunction<'R>() = (fun (l:OptionalValue<'R> list) ->
          l |> List.fold (fun s v -> 
            if s.HasValue && v.HasValue then invalidOp "Combining vectors failed - more than one vector has a value."
            if v.HasValue then v else s) OptionalValue.Missing) }
    |> VectorListTransform.Nary

type VectorListTransform with
  /// Returns a function that can aggregate a list of values. This is either the
  /// original N-ary reduce function or binary function extended using List.reduce
  member x.GetFunction<'T>() = 
    match x with
    | VectorListTransform.Nary n -> n.GetFunction<'T>()
    | VectorListTransform.Binary b -> let f = b.GetFunction<'T>() in List.reduce f

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
let transformColumn (vectorBuilder:IVectorBuilder) scheme rowCmd (vector:IVector) = 
  { new VectorCallSite<IVector> with
      override x.Invoke<'T>(col:IVector<'T>) = 
        vectorBuilder.Build<'T>(scheme, rowCmd, [| col |]) :> IVector }
  |> vector.Invoke

// A generic vector operation that converts the elements of the 
// vector to the specified type using the specified kind of conversion.
let convertType<'R> conversionKind (vector:IVector) = 
  match unboxVector vector with
  | :? IVector<'R> as res -> res
  | vector ->
      { new VectorCallSite<IVector<'R>> with
          override x.Invoke<'T>(col:IVector<'T>) = 
            col.Convert(Convert.convertType<'R> conversionKind, Convert.convertType<'T> conversionKind) }
      |> vector.Invoke

// Store MethodInfo of generic 'convertType' function
let private convertTypeMethod = Lazy.Create(fun () ->
  let typ = typeof<Deedle.OptionalValue<int>>.Assembly.GetType("Deedle.VectorHelpers")
  typ.GetMethod("convertType", BindingFlags.NonPublic ||| BindingFlags.Static) )

// Calls `convertType` dynamically for a specified runtime type
let convertTypeDynamic typ conversionKind (vector:IVector) = 
  let mi = convertTypeMethod.Value.MakeGenericMethod([| typ |])
  mi.Invoke(conversionKind, [| conversionKind; vector |]) :?> IVector

// A generic vector operation that attempts to convert the elements of the 
// vector to the specified type using the specified kind of conversion.
let tryConvertType<'R> conversionKind (vector:IVector) : OptionalValue<IVector<'R>> = 
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
              |> Option.map (Convert.canConvertType<'R> conversionKind)

            if first = Some(false) then OptionalValue.Missing
            else 
              // We still cannot be sure that it will actually work
              try OptionalValue(col.Select(fun v -> Convert.convertType<'R> conversionKind v))
              with :? InvalidCastException | :? FormatException -> OptionalValue.Missing }
      |> vector.Invoke


/// Active pattern that calls the `tryChangeType<float>` function
let (|AsFloatVector|_|) v : option<IVector<float>> = 
  OptionalValue.asOption (tryConvertType ConversionKind.Flexible v)


/// A virtual vector for reading "row" of a data frame. The virtual vector accesses
/// internal representation of the frame (specified by `data` and `columnCount`).
/// The type is generic and automatically converts the values from the underlying
/// (untyped) vector to the specified type.
type RowReaderVector<'T>(data:IVector<IVector>, builder:IVectorBuilder, rowAddress:Address, colAddressAt) =
  
  // Comparison and get hash code follows the ArrayVector implementation
  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'T> as another -> 
        Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() = vector.DataSequence |> Seq.structuralHash

  member private vector.DataArray =
    Array.init (int data.Length) (fun index -> 
      let v = (vector :> IVector<_>)
      v.GetValue(colAddressAt (int64 index)))
      
  // In the generic vector implementation, we
  // read data as objects and perform conversion
  interface IVector<'T> with
    member x.GetValue(columnAddress) = 
      let vector = data.GetValue(columnAddress)
      if not vector.HasValue then OptionalValue.Missing
      else vector.Value.GetObject(rowAddress) |> OptionalValue.map (Convert.convertType<'T> ConversionKind.Flexible)
    
    member x.GetValueAtLocation(loc) = 
      (x :> IVector<_>).GetValue(loc.Address)

    member vector.Data = 
      vector.DataArray |> ReadOnlyCollection.ofArray |> VectorData.SparseList 

    member vector.Select(f) =
      let isNA = MissingValues.isNA<'TNewValue>() 
      let flattenNA (value:OptionalValue<_>) = 
        if value.HasValue && isNA value.Value then OptionalValue.Missing else value
      let data = 
        vector.DataArray 
        |> Array.mapi (fun idx v -> f (KnownLocation(colAddressAt (int64 idx), int64 idx)) v |> flattenNA)
      builder.CreateMissing(data)

    member vector.Convert(f, _) = (vector :> IVector<_>).Select(f)
      
  // Non-generic interface is fully implemented as "virtual"   
  interface IVector with
    member x.AddressingScheme = data.AddressingScheme
    member x.Length = data.Length
    member x.ObjectSequence = x.DataArray |> Seq.map (OptionalValue.map box)
    member x.SuppressPrinting = false
    member x.ElementType = typeof<'T>
    member x.GetObject(i) = OptionalValue.map box ((unbox<IVector<'T>> x).GetValue(i))
    member x.Invoke(site) = site.Invoke(unbox<IVector<'T>> x)


/// Creates a virtual vector for reading "row" of a data frame. 
// For more information, see the `RowReaderVector<'T>` type.
let inline createRowReader (data:IVector<IVector>) (builder:IVectorBuilder) rowAddress colAddressAt =
  RowReaderVector<'T>(data, builder, rowAddress, colAddressAt) :> IVector<'T>
 
/// The same as `createRowReader`, but returns `obj` vector as the result
let inline createObjRowReader data builder addr colAddressAt : IVector<obj> = 
  createRowReader data builder addr colAddressAt

/// Helper type that is used via reflection
type TryValuesHelper =
  /// Turns IVector<TryValue<'T>> into TryValue<IVector<'T>> by aggregating all exceptions
  /// (used via reflection by the `tryValues` function below)
  static member TryValues<'T>(vector:IVector<'T tryval>) = 
    let exceptions = vector.DataSequence |> Seq.choose OptionalValue.asOption |> Seq.choose (fun tv -> 
      if tv.HasValue then None else Some tv.Exception) |> List.ofSeq
    if List.isEmpty exceptions then TryValue.Success (vector.Select(fun (v:tryval<_>) -> v.Value) :> IVector)
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
        (convertType ConversionKind.Flexible vector.Value).DataSequence
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
type mapFrameRowVector = 
  static member Create<'T>(builder:IVectorBuilder, data:obj[]) =
    builder.Create(Array.map (Convert.convertType<'T> ConversionKind.Flexible) data)

/// Given object array, create a typed vector of the best possible type
let createTypedVector (builder:IVectorBuilder) (vectorType:System.Type) (data:obj[]) =
  let flags = System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Static
  let createMi = typeof<mapFrameRowVector>.GetMethod("Create", flags).MakeGenericMethod [| vectorType |]
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

// --------------------------------------------------------------------------------------
// Implementing interface using Reflection Emit
// --------------------------------------------------------------------------------------

open System.Reflection
open System.Reflection.Emit

/// Creates a vector of typed rows `IVector<'TRow>` from frame data `IVector<IVector>`.
/// The returned vector uses the specified delegate `ctor` to construct `'TRow` values.
/// (the `ctor` function takes data and address of the row to be wrapped)
let mapFrameRowVector 
    (ctor:System.Func<IVector[], Addressing.Address, 'TRow>) 
    length (addressAt:int64 -> Address) 
    (data:IVector[])  =
  { new IVector<'TRow> with
      member x.GetValue(a) = OptionalValue(ctor.Invoke(data, a))
      member x.GetValueAtLocation(l) = OptionalValue(ctor.Invoke(data, l.Address))
      member x.Data = 
        seq { for i in Seq.range 0L (length-1L) -> (x :> IVector<_>).GetValue(addressAt i) }
        |> VectorData.Sequence
      member x.Select(g) =  failwith "mapFrameRowVector: Select not supported"
      member x.Convert(h, g) = failwith "mapFrameRowVector: Convert not supported"
    interface IVector with
      member x.AddressingScheme = 
        data |> Seq.map (fun v -> v.AddressingScheme) |> Seq.reduce (fun a b ->
          if a <> b then failwith "mapFrameRowVector: Addressing scheme mismatch" else a )
      member x.Length = length
      member x.ObjectSequence = seq { for i in Seq.range 0L (length-1L) -> x.GetObject(addressAt i) }
      member x.SuppressPrinting = false
      member x.ElementType = typeof<'TRow>
      member x.GetObject(i) = (x :?> IVector<'TRow>).GetValue(i) |> OptionalValue.map box
      member x.Invoke(site) = failwith "mapFrameRowVector: Invoke not supported" }

#if DEBUG_TYPED_ROWS
let name = new AssemblyName("TypedRowAssembly")
let asmBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.RunAndSave)
let private typedRowModule = Lazy.Create(fun _ ->
  asmBuilder.DefineDynamicModule(name.Name, name.Name+".dll"))
#else 
/// Dynamic assembly & module for storing generated types
let private typedRowModule = Lazy.Create(fun _ -> 
  let name = new AssemblyName("TypedRowAssembly")
  let asmBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.RunAndCollect)
  asmBuilder.DefineDynamicModule(name.Name, name.Name+".dll"))
#endif

/// Helper module with various MemberInfo and similar values
module private Reflection = 
  let objCtor = typeof<obj>.GetConstructor([| |])
  let addrTyp = typeof<Addressing.Address>
  let optTyp = typedefof<OptionalValue<_>>

/// Counter of generated types to avoid name clashes
let private typeCounter = ref 0

/// Cache for optimizing 'createTypedRowBuilder' 
let private createdTypedRowsCache = Dictionary<Type * string list, obj * list<string * Type>>() 

/// Uses Reflection.Emit to create an efficient implementation of the `'TRow` interface.
let createTypedRowCreator<'TRow> columnKeys = 
  let rowType = typeof<'TRow>
  match createdTypedRowsCache.TryGetValue( (rowType, columnKeys) ) with 
  | true, (ctor, meta) -> 
      unbox<Func<IVector[], Address, 'TRow>> ctor, meta
  | false, _ -> 
      // Check that the interface has only property getters
      for m in rowType.GetMethods() do
        if not m.IsSpecialName || not (m.Name.StartsWith("get_")) then
          raise (new InvalidOperationException("Only readonly properties are supported in the interface!"))

      // Define a type named ImpleIInterface@1 
      incr typeCounter
      let typeName = sprintf "Impl%s@%d" rowType.Name typeCounter.Value
      let rowImpl = 
        typedRowModule.Value.DefineType
          (typeName, TypeAttributes.Public, typeof<obj>, [| rowType |])

      // For every property of type `T`, define a field of type `IVector<T>`
      // (this stores reference to the typed vector) and define a field
      // `address` storing the current row address of the `'TRow` value.
      //
      // When the property has a type `OptionalValue<T>` then we expect that
      // the column has a type `T`, but the user wants to see missing values
      let columnTypes = 
        [ for m in rowType.GetMethods() ->
            if m.ReturnType.IsGenericType && m.ReturnType.GetGenericTypeDefinition() = Reflection.optTyp 
              then true, typedefof<IVector<_>>.MakeGenericType(m.ReturnType.GetGenericArguments().[0])
              else false, typedefof<IVector<_>>.MakeGenericType(m.ReturnType) ]
            
      let vecFields = 
        columnTypes |> List.mapi (fun i (_, vecTy) ->
            rowImpl.DefineField(sprintf "vector_%d" i, vecTy, FieldAttributes.Private))
      let addrField = rowImpl.DefineField("address", typeof<Addressing.Address>, FieldAttributes.Private)

      // Define constructor which takes address & column vectors and stores them:
      //
      //    new(addr, vec1, ..., vecN) =
      //      base()
      //      this.address <- addr
      //      this.vector_1 <- vec1
      //      (...)
      ///
      let ctor = 
        rowImpl.DefineConstructor
          ( MethodAttributes.Public, CallingConventions.Standard, 
            Reflection.addrTyp::(List.map snd columnTypes) |> Array.ofSeq)
      let ilgen = ctor.GetILGenerator()
      ilgen.Emit(OpCodes.Ldarg_0)
      ilgen.Emit(OpCodes.Callvirt, Reflection.objCtor)

      ilgen.Emit(OpCodes.Ldarg_0)
      ilgen.Emit(OpCodes.Ldarg_1)
      ilgen.Emit(OpCodes.Stfld, addrField)
  
      vecFields |> List.iteri (fun i vecField -> 
        ilgen.Emit(OpCodes.Ldarg_0)
        ilgen.Emit(OpCodes.Ldarg, 1 (*this*) + 1 (*address*) + i)
        ilgen.Emit(OpCodes.Stfld, vecField) )
  
      ilgen.Emit(OpCodes.Ret)


      // For every property in the interface, define a method that overrides the getter
      for m, ((isOptional, typ), fld) in Seq.zip (rowType.GetMethods()) (Seq.zip columnTypes vecFields) do
        // if `isOptional`, i.e. the return type is `OptionalValue<T>` then
        //
        //   override this.get_Something() =
        //     this.vector_i.GetValue(this.address)
        //
        // Otherwise, we call `get_Value()` at the end (which fails for missing values):
        //
        //   override this.get_Something() =
        //     let local = this.vector_i.GetValue(this.address)
        //     local.get_Value()
        //
        let impl = 
          rowImpl.DefineMethod
            ( m.Name, MethodAttributes.Public ||| MethodAttributes.Virtual 
              ||| MethodAttributes.SpecialName, m.ReturnType, [| |])
        let ilgen = impl.GetILGenerator()

        ilgen.Emit(OpCodes.Ldarg_0)
        ilgen.Emit(OpCodes.Ldfld, fld)
        ilgen.Emit(OpCodes.Ldarg_0)
        ilgen.Emit(OpCodes.Ldfld, addrField)
        ilgen.Emit(OpCodes.Callvirt, fld.FieldType.GetMethod("GetValue"))

        if not isOptional then
          let optTyp = Reflection.optTyp.MakeGenericType(typ.GetGenericArguments().[0])
          let localOpt = ilgen.DeclareLocal(optTyp)
          ilgen.Emit(OpCodes.Stloc, localOpt)
          ilgen.Emit(OpCodes.Ldloca_S, localOpt)
          ilgen.Emit(OpCodes.Call, optTyp.GetProperty("Value").GetGetMethod())
        ilgen.Emit(OpCodes.Ret)
  
        rowImpl.DefineMethodOverride(impl, m)

      // Finish building the type
      let rowImplType = rowImpl.CreateType()
      #if DEBUG_TYPED_ROWS
      asmBuilder.Save("TypedRowAssembly.dll")
      #endif

      // Next, we create a delegate `Func<IVector<IVector>, Address, 'TRow>` that 
      // we can pass to `mapFrameRowVector` in order to build the resulting vector
      let args = [| typeof<IVector[]>; typeof<Address> |]
      let makeRow = DynamicMethod("Make" + rowType.Name, rowType, args)
      let ilgen = makeRow.GetILGenerator()

      // fun data address ->
      //   let vecOpt0 = data.[0] ) 
      //   (...)
      //   let vecOptN = data.[N]
      //
      //   new ImpleIInterface@1( vecOpt1.Value :?> IVector<'T1>, ...
      //                          vecOptN.Value :?> IVector<'TN> )
      let locals = 
        columnTypes |> List.mapi (fun i (_, ty) -> 
            let localOpt = ilgen.DeclareLocal(typeof<IVector>)
            ilgen.Emit(OpCodes.Ldarg_0)
            ilgen.Emit(OpCodes.Ldc_I4, i)
            ilgen.Emit(OpCodes.Ldelem, typeof<IVector>)
            ilgen.Emit(OpCodes.Stloc, localOpt)
            localOpt, ty )

      ilgen.Emit(OpCodes.Ldarg_1)
      for loc, vecTy in locals do 
        ilgen.Emit(OpCodes.Ldloc, loc)
        ilgen.Emit(OpCodes.Castclass, vecTy)

      let ctor = rowImplType.GetConstructors().[0]
      ilgen.Emit(OpCodes.Newobj, ctor)
      ilgen.Emit(OpCodes.Ret)

      // Build the delegate and get it as Systme.Func we can call
      let createRowImpl : Func<IVector[], Address, 'TRow> = 
        unbox (makeRow.CreateDelegate(typeof<Func<IVector[], Address, 'TRow>>))

      // We return information about the structure - a list of property names
      // & types that we are expecting in the incoming IVector<IVector>
      let meta =            
        Seq.zip (rowType.GetMethods()) columnTypes
        |> Seq.map (fun (m, (_, t)) -> m.Name.Substring(4), t.GetGenericArguments().[0])
        |> List.ofSeq

      createdTypedRowsCache.Add( (rowType, columnKeys), (box createRowImpl, meta) )
      createRowImpl, meta

/// Creates a typed vector of `IVector<'TRow>` for a given interface `'TRow`
/// (which is expected to have only read-only properties). 
let createTypedRowReader<'TRow> 
    columnKeys (columnIndex:string -> Address) size 
    addressAt (data:IVector<IVector>) = 
  let ctor, meta = createTypedRowCreator<'TRow> columnKeys
  let subData = 
    [| for name, typ in meta ->
         let colVector = data.GetValue(columnIndex name)
         if colVector.Value.ElementType = typ then colVector.Value
         else convertTypeDynamic typ ConversionKind.Flexible colVector.Value |]
  mapFrameRowVector ctor size addressAt subData

// --------------------------------------------------------------------------------------
// Vector constructions
// --------------------------------------------------------------------------------------

/// Substitute variable hole for another in a vector construction
let rec substitute ((oldVar, newVect) as subst) = function
  | Return v when v = oldVar -> newVect
  | Return v -> Return v
  | Empty size -> Empty size
  | FillMissing(vc, d) -> FillMissing(substitute subst vc, d)
  | Relocate(vc, r, l) -> Relocate(substitute subst vc, r, l)
  | DropRange(vc, r) -> DropRange(substitute subst vc, r)
  | GetRange(vc, r) -> GetRange(substitute subst vc, r)
  | Append(l, r) -> Append(substitute subst l, substitute subst r)
  | Combine(l, lst, c) -> Combine(l, List.map (substitute subst) lst, c)
  | CustomCommand(lst, f) -> CustomCommand(List.map (substitute subst) lst, f)
  | AsyncCustomCommand(lst, f) -> AsyncCustomCommand(List.map (substitute subst) lst, f)

/// Matches when the vector command represents a combination
/// of N relocated vectors (that is Combine [Relocate ..; Relocate ..; ...])
let (|CombinedRelocations|_|) = function
  | Combine(l, list, VectorListTransform.Binary op) ->
      if list |> List.forall (function Relocate _ -> true | _ -> false) then
        let parts = list |> List.map (function Relocate(a,b,c) -> (a,b,c) | _ -> failwith "logic error")
        Some(l, parts, op)
      else None
  | _ -> None
