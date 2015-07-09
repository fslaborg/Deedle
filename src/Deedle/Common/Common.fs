#nowarn "86" // Let me redefine <, >, <=, >= locally using comparator
namespace Deedle

open System
open System.Runtime.CompilerServices

/// Represents different kinds of type conversions that can be used by Deedle internally.
/// This is used, for example, when converting `ObjectSeries<'K>` to `Series<'K, 'T>` - 
/// The conversion kind can be specified as an argument to allow certain conversions.
///
/// [category:Parameters and results of various operations]
type ConversionKind = 
  /// Specifies that the type has to match exactly and no conversions are performed.
  | Exact = 0
  /// Allows conversions that widen numeric types, but nothing else. This includes
  /// conversions on decimals `decimal -> float32 -> float` and also from integers 
  /// to floating points (`int -> decimal` etc.)
  | Safe = 1
  /// Allows the use of arbitrary .NET conversions. This uses `Convert.ChangeType`, which
  /// performs numerical conversions, parsing of strings, uses `IConvertable` and more.
  | Flexible = 2


/// Thrown when a value at the specified index does not exist in the data frame or series.
/// This exception is thrown only when the key is defined, but the value is not available,
/// in other situations `KeyNotFoundException` is thrown
///
/// [category:Primitive types and values]
type MissingValueException(key:obj, message) =
  inherit Exception(message)
  /// The key that has been accessed
  member x.Key = key

/// Value type that represents a potentially missing value. This is similar to 
/// `System.Nullable<T>`, but does not restrict the contained value to be a value
/// type, so it can be used for storing values of any types. When obtained from
/// `DataFrame<R, C>` or `Series<K, T>`, the `Value` will never be `Double.NaN` or `null`
/// (but this is not, in general, checked when constructing the value).
///
/// The type is only used in C#-friendly API. F# operations generally use expose
/// standard F# `option<T>` type instead. However, there the `OptionalValue` module
/// contains helper functions for using this type from F# as well as `Missing` and
/// `Present` active patterns.
///
/// [category:Primitive types and values]
[<Struct; CustomEquality; NoComparison>]
type OptionalValue<'T> private (hasValue:bool, value:'T) = 
  /// Gets a value indicating whether the current `OptionalValue<T>` has a value
  member x.HasValue = hasValue

  /// Returns the value stored in the current `OptionalValue<T>`. 
  /// Exceptions:
  ///   `InvalidOperationException` - Thrown when `HasValue` is `false`.
  member x.Value = 
    if hasValue then value
    else invalidOp "OptionalValue.Value: Value is not available" 
  
  /// Returns the value stored in the current `OptionalValue<T>` or 
  /// the default value of the type `T` when a value is not present.
  member x.ValueOrDefault = value

  /// Creates a new instance of `OptionalValue<T>` that contains  
  /// the specified `T` value .
  new (value:'T) = OptionalValue(true, value)

  /// Returns a new instance of `OptionalValue<T>` that does not contain a value.
  static member Missing = OptionalValue(false, Unchecked.defaultof<'T>)

  /// Prints the value or "<null>" when the value is present, but is `null`
  /// or "<missing>" when the value is not present (`HasValue = false`).
  override x.ToString() = 
    if hasValue then 
      if Object.Equals(null, value) then "<null>"
      else value.ToString() 
    else "<missing>"

  /// Support structural equality      
  override x.GetHashCode() = 
    match box x.ValueOrDefault with null -> 0 | o -> o.GetHashCode()

  /// Support structural equality      
  override x.Equals(y) =
    match y with 
    | null -> false
    | :? OptionalValue<'T> as y -> 
        match x.HasValue, y.HasValue with
        | true, true -> Object.Equals(x.ValueOrDefault, y.ValueOrDefault)
        | false, false -> true
        | _ -> false
    | _ -> false
   
/// Non-generic type that makes it easier to create `OptionalValue<T>` values
/// from C# by benefiting the type inference for generic method invocations.
///
/// [category:Primitive types and values]
type OptionalValue =
  /// Creates an `OptionalValue<T>` from a nullable value of type `T?`
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member OfNullable(v:Nullable<'T>) =  
    if v.HasValue then OptionalValue(v.Value) else OptionalValue<'T>.Missing
  
  /// Creates an `OptionalValue<'T>` that contains a value `v`
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member Create(v) = 
    OptionalValue(v)
  
  /// Creates an `OptionalValue<'T>` that does not contain a value
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member Empty<'T>() = OptionalValue<'T>.Missing

/// Represents a value or an exception. This type is used by functions such as
/// `Series.tryMap` and `Frame.tryMap` to capture the result of a lambda function,
/// which may be either a value or an exception. The type is a discriminated union,
/// so it can be processed using F# pattern matching, or using `Value`, `HasValue`
/// and `Exception` properties
///
/// [category:Primitive types and values]
type TryValue<'T> =
  | Success of 'T
  | Error of exn

  /// Returns the value of `TryValue<T>` when the value is present; 
  /// otherwise, throws an exception that was captured
  member x.Value =
    match x with Success v -> v | Error exn -> raise exn

  /// Returns `true` when the `TryValue<T>` object represents a 
  /// successfully calculated value 
  member x.HasValue = 
    match x with Success _ -> true | _ -> false

  /// Returns the exception captured by this value. When `HasValue = true`, 
  /// accessing the property throws `InvalidOperationException`.
  member x.Exception = 
    match x with Success _ -> invalidOp "The TryValue<T> does not represent an exception" | Error exn -> exn

  /// Returns the string representation of the underlying value or `<error>`
  override x.ToString() = 
    match x with Success v -> v.ToString() | _ -> "<error>"

/// A type alias for the `TryValue<T>` type. The type alias can be used
/// to make F# type declarations that explcitly handle exceptions more succinct.
///
/// [category:Primitive types and values]
type 'T tryval = TryValue<'T>

/// A type alias for the `OptionalValue<T>` type. The type alias can be used
/// to make F# type definitions that use optional values directly more succinct.
///
/// [category:Primitive types and values]
type 'T opt = OptionalValue<'T>


/// Represents different behaviors of key lookup in series. For unordered series,
/// the only available option is `Lookup.Exact` which finds the exact key - methods
/// fail or return missing value if the key is not available in the index. For ordered
/// series `Lookup.Greater` finds the first greater key (e.g. later date) with
/// a value. `Lookup.Smaller` searches for the first smaller key. The options
/// `Lookup.ExactOrGreater` and `Lookup.ExactOrSmaller` finds the exact key (if it is
/// present) and otherwise search for the nearest larger or smaller key, respectively.
///
/// [category:Parameters and results of various operations]
[<System.Flags>]
type Lookup = 
  /// Lookup a value associated with the exact specified key. 
  /// If the key is not available, then fail or return missing value. 
  | Exact = 1

  /// Lookup a value associated with the specified key or with the nearest
  /// greater key that has a value available. Fails (or returns missing value)
  /// only when the specified key is greater than all available keys.
  | ExactOrGreater = 3

  /// Lookup a value associated with the specified key or with the nearest
  /// smaller key that has a value available. Fails (or returns missing value)
  /// only when the specified key is smaller than all available keys.
  | ExactOrSmaller = 5

  /// Lookup a value associated with a key that is greater than the specified one.
  /// Fails (or returns missing value) when the specified key is greater or equal
  /// to the greatest available key.
  | Greater = 2

  /// Lookup a value associated with a key that is smaller than the specified one.
  /// Fails (or returns missing value) when the specified key is smaller or equal
  /// to the smallest available key.
  | Smaller = 4


/// Specifies in which direction should we look when performing operations such as
/// `Series.Pairwise`. 
///
/// ## Example
///
///     let abc = 
///       [ 1 => "a"; 2 => "b"; 3 => "c" ]
///       |> Series.ofObservations
///
///     // Using 'Forward' the key of the first element is used
///     abc.Pairwise(direction=Direction.Forward)
///     [fsi:[ 1 => ("a", "b"); 2 => ("b", "c") ]]
///
///     // Using 'Backward' the key of the second element is used
///     abc.Pairwise(direction=Direction.Backward)
///     [fsi:[ 2 => ("a", "b"); 3 => ("b", "c") ]]
///
///
/// [category:Parameters and results of various operations]
type Direction = 
  | Backward = 0
  | Forward = 1 

/// Represents boundary behaviour for operations such as floating window. The type
/// specifies whether incomplete windows (of smaller than required length) should be
/// produced at the beginning (`AtBeginning`) or at the end (`AtEnding`) or
/// skipped (`Skip`). For chunking, combinations are allowed too - to skip incomplete
/// chunk at the beginning, use `Boundary.Skip ||| Boundary.AtBeginning`.
///
/// [category:Parameters and results of various operations]
[<Flags>]
type Boundary =
  | AtBeginning = 1
  | AtEnding = 2
  | Skip = 4

/// Represents a kind of `DataSegment<T>`. See that type for more information.
///
/// [category:Parameters and results of various operations]
type DataSegmentKind = Complete | Incomplete

/// Represents a segment of a series or sequence. The value is returned from 
/// various functions that aggregate data into chunks or floating windows. The 
/// `Complete` case represents complete segment (e.g. of the specified size) and
/// `Boundary` represents segment at the boundary (e.g. smaller than the required
/// size). 
///
/// ## Example
///
/// For example (using internal `windowed` function):
///
///     open Deedle.Internal
///
///     Seq.windowedWithBounds 3 Boundary.AtBeginning [ 1; 2; 3; 4 ]
///     [fsi:  [| DataSegment(Incomplete, [| 1 |])         ]
///     [fsi:       DataSegment(Incomplete, [| 1; 2 |])    ]
///     [fsi:       DataSegment(Complete [| 1; 2; 3 |])    ]
///     [fsi:       DataSegment(Complete [| 2; 3; 4 |]) |] ]
///
/// If you do not need to distinguish the two cases, you can use the `Data` property
/// to get the array representing the segment data.
///
/// [category:Parameters and results of various operations]
type DataSegment<'T> = 
  | DataSegment of DataSegmentKind * 'T
  /// Returns the data associated with the segment
  /// (for boundary segment, this may be smaller than the required window size)
  member x.Data = let (DataSegment(_, data)) = x in data
  /// Return the kind of this segment
  member x.Kind = let (DataSegment(kind, _)) = x in kind
  /// Format data segment nicely
  override x.ToString() = 
    let s = x.Data.ToString()
    if s.StartsWith("series [") then
      let repl = match x.Kind with Complete -> "complete-segment [" | _ -> "incomplete-segment ["
      s.Replace("series [", repl)
    else sprintf "DataSegment(%s, %s)" (if x.Kind = Complete then "Complete" else "Incomplete") s
    

/// Provides helper functions and active patterns for working with `DataSegment` values
///
/// [category:Parameters and results of various operations]
module DataSegment = 
  /// A complete active pattern that extracts the kind and data from a `DataSegment`
  /// value. This makes it easier to write functions that only need data:
  ///
  ///    let sumAny = function DataSegment.Any(_, data) -> Stats.sum data
  ///
  let (|Any|) (ds:DataSegment<'T>) = ds.Kind, ds.Data
  
  /// Complete active pattern that makes it possible to write functions that behave 
  /// differently for complete and incomplete segments. For example, the following 
  /// returns zero for incomplete segments:
  ///
  ///     let sumSegmentOrZero = function
  ///       | DataSegment.Complete(value) -> Stats.sum value
  ///       | DataSegment.Incomplete _ -> 0.0
  ///
  let (|Complete|Incomplete|) (ds:DataSegment<_>) =
    if ds.Kind = DataSegmentKind.Complete then Complete(ds.Data)
    else Incomplete(ds.Data)

  /// Returns the data property of the specified `DataSegment<T>`
  [<CompiledName("GetData")>]
  let data (ds:DataSegment<_>) = ds.Data

  /// Returns the kind property of the specified `DataSegment<T>`
  [<CompiledName("GetKind")>]
  let kind (ds:DataSegment<_>) = ds.Kind


// --------------------------------------------------------------------------------------
// OptionalValue module (to be used from F#)
// --------------------------------------------------------------------------------------

/// Extension methods for working with optional values from C#. These make
/// it easier to provide default values and convert optional values to 
/// `Nullable` (when the contained value is value type)
///
/// [category:Primitive types and values]
[<Extension>]
type OptionalValueExtensions =
  
  /// Extension method that converts optional value containing a value type
  /// to a C# friendly `Nullable<T>` or `T?` type.
  [<Extension>]
  static member AsNullable(opt:OptionalValue<'T>) = 
    if opt.HasValue then Nullable(opt.Value) else Nullable()

  /// Extension method that returns value in the specified optional value
  /// or the provided default value (the second argument).
  [<Extension>]
  static member OrDefault(opt:OptionalValue<'T>, defaultValue) = 
    if opt.HasValue then opt.Value else defaultValue

/// Provides various helper functions for using the `OptionalValue<T>` type from F#
/// (The functions are similar to those in the standard `Option` module).
///
/// [category:Primitive types and values]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module OptionalValue = 

  /// If the `OptionalValue<T>` does not contain a value, then returns a new 
  /// `OptionalValue<R>.Empty`. Otherwise, returns the result of applying the 
  /// function `f` to the value contained in the provided optional value.
  [<CompiledName("Bind")>]
  let inline bind f (input:OptionalValue<'T>) : OptionalValue<'R> = 
    if input.HasValue then f input.Value
    else OptionalValue.Missing

  /// If the `OptionalValue<T>` does not contain a value, then returns a new 
  /// `OptionalValue<R>.Empty`. Otherwise, returns the result `OptionalValue<R>`
  /// containing the result of applying the function `f` to the value contained 
  /// in the provided optional value.
  [<CompiledName("Map")>]
  let inline map f (input:OptionalValue<'T>) : OptionalValue<'R> = 
    if input.HasValue then OptionalValue(f input.Value)
    else OptionalValue.Missing

  /// If both of the arguments contain value, apply the specified function to their
  /// values and return `OptionalValue<R>` with the result. Otherwise return
  /// `OptionalValue.Missing`.
  [<CompiledName("Map2")>]
  let inline map2 f (input1:OptionalValue<'T1>) (input2:OptionalValue<'T2>): OptionalValue<'R> = 
    if input1.HasValue && input2.HasValue then 
      OptionalValue(f input1.Value input2.Value)
    else OptionalValue.Missing

  /// Creates `OptionalValue<T>` from a tuple of type `bool * 'T`. This function
  /// can be used with .NET methods that use `out` arguments. For example:
  ///
  ///     Int32.TryParse("42") |> OptionalValue.ofTuple
  ///
  [<CompiledName("OfTuple")>]
  let inline ofTuple (b, value:'T) : 'T opt =
    if b then OptionalValue(value) else OptionalValue.Missing

  /// Creates `OptionalValue<T>` from a .NET `Nullable<T>` type.
  [<CompiledName("OfNullable")>]
  let inline ofNullable (value:Nullable<'T>) : 'T opt =
    if value.HasValue then OptionalValue(value.Value) else OptionalValue.Missing

  /// Turns the `OptionalValue<T>` into a corresponding standard F# `option<T>` value
  let inline asOption (value:'T opt) = 
    if value.HasValue then Some value.Value else None

  /// Turns a standard F# `option<T>` value into a corresponding `OptionalValue<T>`
  let inline ofOption (opt:option<'T>) : 'T opt = 
    match opt with
    | None -> OptionalValue.Missing
    | Some v -> OptionalValue(v)

  /// Complete active pattern that can be used to pattern match on `OptionalValue<T>`.
  /// For example:
  ///
  ///     let optVal = OptionalValue(42)
  ///     match optVal with
  ///     | OptionalValue.Missing -> printfn "Empty"
  ///     | OptionalValue.Present(v) -> printfn "Contains %d" v
  ///
  let (|Missing|Present|) (optional:'T opt) =
    if optional.HasValue then Present(optional.Value)
    else Missing

  /// Get the value stored in the specified optional value. If a value is not
  /// available, throws an exception. (This is equivalent to the `Value` property)
  let inline get (optional:'T opt) = optional.Value
    
  /// Returns the value `def` when the argument is missing, otherwise returns its value
  let inline defaultArg def (optional:'T opt) = 
    if optional.HasValue then optional.Value else def


// --------------------------------------------------------------------------------------
// Internals - working with missing values   
// --------------------------------------------------------------------------------------

namespace Deedle.Internal

open System
open Deedle
open System.Collections.Generic
open System.Collections.ObjectModel

/// An internal exception that is used to handle the case when comparison fails
/// (even though the type implements IComparable and everything...)
///
/// [omit]
type ComparisonFailedException() =
  inherit Exception() 

/// Simple helper functions for throwing exceptions
///
/// [omit]
[<AutoOpen>]
module internal ExceptionHelpers =
  /// Throws `MissingValueException` with a nicely formatted error message for the specified key
  let inline missingVal key = 
    raise (new MissingValueException(key, sprintf "Value at the key %O is missing" key))

  /// Throws `KeyNotFoundException` with a nicely formatted error message for the specified key
  let inline keyNotFound key = 
    raise (new KeyNotFoundException(sprintf "The key %O is not present in the index" key))

/// Pattern matching helpers
/// [omit]
[<AutoOpen>]
module internal MatchingHelpers =
  /// Helper that lets us define parameters in pattern matching; for example 
  /// "Let 42 (answer, input)" binds "answer=42" and propagates input
  let (|Let|) arg input = (arg, input)

/// Utility functions for identifying missing values. The `isNA` function 
/// can be used to test whether a value represents a missing value - this includes
/// the `null` value, `Nullable<T>` value with `HasValue = false` and 
/// `Single.NaN` as well as `Double.NaN`.
///
/// The functions in this module are not intended to be called directly.
module MissingValues =

  // TODO: Possibly optimize (in some cases) using static member constraints?

  let inline isNA<'T> () =
    let ty = typeof<'T>
    let isNullable = ty.IsGenericType && (ty.GetGenericTypeDefinition() = typedefof<Nullable<_>>)
    let nanTest : 'T -> bool =
      if ty = typeof<float> then unbox Double.IsNaN
      elif ty = typeof<float32> then unbox Single.IsNaN
      elif ty.IsValueType && not isNullable then (fun _ -> false)
      else (fun v -> Object.Equals(null, box v))
    nanTest

  let flattenNA<'T> () =
    let isNaOfT = isNA<'T>()
    fun (value:OptionalValue<_>) ->
      if value.HasValue && isNaOfT value.Value then OptionalValue.Missing else value

  let inline containsNA (data:'T[]) = 
    let isNA = isNA<'T>() 
    Array.exists isNA data

  let inline containsMissingOrNA (data:OptionalValue<'T>[]) = 
    let isNA = isNA<'T>() 
    data |> Array.exists (fun v -> not v.HasValue || isNA v.Value)

  let inline createNAArray (data:'T[]) =   
    let isNA = isNA<'T>() 
    data |> Array.map (fun v -> if isNA v then OptionalValue.Missing else OptionalValue(v))

  let inline createMissingOrNAArray (data:OptionalValue<'T>[]) =   
    let isNA = isNA<'T>() 
    data |> Array.map (fun v -> 
      if not v.HasValue || isNA v.Value then OptionalValue.Missing else OptionalValue(v.Value))


// --------------------------------------------------------------------------------------
// Internals - various functions for working with collections
// --------------------------------------------------------------------------------------

/// [omit]
[<AutoOpen>]
module ReadOnlyCollectionExtensions = 
  type ReadOnlyCollection<'T> with 
    member x.GetSlice(lo, hi) =
      let lo, hi = defaultArg lo 0, defaultArg hi (x.Count - 1)
      if lo < 0 then invalidArg "lo" "Must be greater than zero"
      if lo > hi then invalidOp "Slice must be from lower to higher index"
      if hi >= x.Count then invalidArg "hi" "Must be lower than the length"
      let arr = Array.init (hi - lo + 1) (fun i -> x.[i + lo])
      new ReadOnlyCollection<_>(arr)

/// Provides helper functions for working with `ReadOnlyCollection<T>` similar to those 
/// in the `Array` module. Most importantly, F# 3.0 does not know that array implements
/// `IList<T>`.
module ReadOnlyCollection =
  /// Converts an array to ReadOnlyCollection. 
  let inline ofArray (array:'T[]) : ReadOnlyCollection<'T> = Array.AsReadOnly(array)

  /// Converts a lazy sequence to fully evaluated ReadOnlyCollection
  let inline ofSeq (seq:seq<'T>) : ReadOnlyCollection<'T> = Array.AsReadOnly(Array.ofSeq seq)

  /// Transform all elements of ReadOnlyCollection using the specified function
  let inline map f (list:ReadOnlyCollection<'T>) = 
    let res = Array.zeroCreate list.Count
    for i in 0 .. list.Count - 1 do res.[i] <- f list.[i]
    Array.AsReadOnly(res)
  
  /// Count elements of the ReadOnlyCollection
  let inline length (list:ReadOnlyCollection<'T>) = list.Count
  
  /// Reduce elements of the ReadOnlyCollection
  let inline reduce op (list:ReadOnlyCollection<'T>) = 
    let mutable res = list.[0]
    for i in 1 .. list.Count - 1 do res <- op res list.[i]
    res

  /// Reduce elements of the ReadOnlyCollection, skipping over missing values
  let inline reduceOptional op (list:ReadOnlyCollection<OptionalValue<'T>>) = 
    let mutable res = None
    for i in 0 .. list.Count - 1 do 
      match res, list.[i] with
      | Some r, OptionalValue.Present v -> res <- Some (op r v)
      | None, OptionalValue.Present v -> res <- Some v
      | _ -> ()
    res |> OptionalValue.ofOption

  /// Fold elements of the ReadOnlyCollection
  let inline fold op init (list:ReadOnlyCollection<'T>) = 
    let mutable res = init
    for i in 0 .. list.Count - 1 do res <- op res list.[i]
    res

  /// Fold elements of the ReadOnlyCollection, skipping over missing values
  let inline foldOptional op init (list:ReadOnlyCollection<OptionalValue<'T>>) = 
    let mutable res = init
    for i in 0 .. list.Count - 1 do 
      match list.[i] with
      | OptionalValue.Present v -> res <- op res v
      | _ -> ()
    res

  /// Returns empty readonly collection
  let empty<'T> = new ReadOnlyCollection<'T>([||])


/// This module contains additional functions for working with arrays. 
/// `Deedle.Internals` is opened, it extends the standard `Array` module.
module Array = 

  /// Drop a specified range from a given array. The operation is inclusive on
  /// both sides. Given [ 1; 2; 3; 4 ] and indices (1, 2), the result is [ 1; 4 ]
  let inline dropRange first last (data:'T[]) =
    if last < first then invalidOp "The first index must be smaller than or equal to the last."
    if first < 0 || last >= data.Length then invalidArg "first" "The index must be within the array range."
    let part1 = if first = 0 then [| |] else data.[.. first - 1]
    let part2 = if last = data.Length - 1 then [| |] else data.[last + 1 ..]
    Array.append part1 part2

  /// Implementation of binary search
  let inline private binarySearch key (comparer:System.Collections.Generic.IComparer<'T>) (array:ReadOnlyCollection<'T>) =
    let rec search (lo, hi) =
      if lo = hi then lo else
      let mid = (lo + hi) / 2
      match comparer.Compare(key, array.[mid]) with 
      | 0 -> mid
      | n when n < 0 -> search (lo, max lo (mid - 1))
      | _ -> search (min hi (mid + 1), hi) 
    search (0, array.Count - 1) 

  /// Returns the index of 'key' or the index of immediately following value.
  /// If the specified key is greater than all keys in the array, None is returned.
  ///
  /// When 'inclusive' is false, the function returns the index of strictry greater value.
  /// Note that the function expects that the array contains distinct values
  /// (which is fine because LinearIndex does not support duplicate keys)
  let binarySearchNearestGreater key (comparer:System.Collections.Generic.IComparer<'T>) inclusive (array:ReadOnlyCollection<'T>) =
    if array.Count = 0 then None else
    let loc = binarySearch key comparer array
    let comp = comparer.Compare(array.[loc], key)
    if (comp = 0 && inclusive) || comp > 0 then Some loc
    elif loc + 1 < array.Count && comparer.Compare(array.[loc + 1], key) >= 1 then Some (loc + 1)
    else None

  /// Returns the index of 'key' or the index of immediately preceeding value.
  /// If the specified key is smaller than all keys in the array, None is returned.
  ///
  /// When 'inclusive' is false, the function returns the index of strictry smaller value.
  /// Note that the function expects that the array contains distinct values
  /// (which is fine because LinearIndex does not support duplicate keys)
  let binarySearchNearestSmaller key (comparer:System.Collections.Generic.IComparer<'T>) inclusive (array:ReadOnlyCollection<'T>) =
    if array.Count = 0 then None else
    let loc = binarySearch key comparer array
    let comp = comparer.Compare(array.[loc], key)
    if (comp = 0 && inclusive) || comp < 0 then Some loc
    elif loc - 1 >= 0 && comparer.Compare(array.[loc - 1], key) <= 0 then Some (loc - 1)
    else None

  /// Returns a new array containing only the elements for which the specified function returns `Some`.
  /// The predicate is called with the index in the source array and the element.
  let inline choosei f (array:_[]) = 
    let res = new System.Collections.Generic.List<_>() // ResizeArray
    for i = 0 to array.Length - 1 do 
        let x = f i (array.[i])
        if Option.isSome x then res.Add(x.Value)
    res.ToArray()


/// This module contains additional functions for working with lists. 
module List =
  /// Returns an option value that is Some when the specified function 'f'
  /// succeeds for all values from the input list. Otherwise returns None. 
  let tryChooseBy f input = 
    let rec loop acc = function
      | [] -> Some (List.rev acc)
      | x::xs ->
          match f x with
          | Some v -> loop (v::acc) xs
          | None -> None
    loop [] input

/// This module contains additional functions for working with sequences. 
/// `Deedle.Internals` is opened, it extends the standard `Seq` module.
module Seq = 

  /// If the projection returns the same value for all elements, then it
  /// returns the value. Otherwise it throws an exception.
  let uniqueBy f input =
    input |> Seq.map f |> Seq.reduce (fun a b ->  
      if a <> b then failwith "uniqueBy: Elements are not the same" else a)


  /// Returns the last element and the length of a sequence
  /// (using just a single iteration over the sequence)
  let inline lastAndLength (input:seq<_>) = 
    let mutable last = Unchecked.defaultof<_>
    let mutable count = 0
    use en = input.GetEnumerator()
    while en.MoveNext() do
      last <- en.Current; count <- count + 1
    if count = 0 then invalidOp "Insufficient number of elements"
    last, count

  /// Generate infinite sequence using the specified function.
  /// The initial state is returned as the first element.
  let inline unreduce f start = seq {
    let state = ref start
    while true do
      yield state.Value
      state := f state.Value }

  /// Same as `Seq.choose` but passes 64 bit index (l stands for long) to the function
  let inline choosel f (input:seq<_>) = seq {
    let i = ref 0L
    for v in input do
      match f i.Value v with
      | Some res -> yield res 
      | _ -> ()
      i := !i + 1L }

  /// Same as `Seq.map` but passes 64 bit index (l stands for long) to the function
  let inline mapl f (input:seq<_>) = seq {
    let i = ref 0L
    for v in input do 
      yield f i.Value v 
      i := !i + 1L }

  /// Skip at most the specified number of elements. This is like
  /// `Seq.skip`, but it does not throw when the sequence is shorter.
  let skipAtMost count (input:seq<_>) = 
    seq { use en = input.GetEnumerator()
          for n in 0 .. count-1 do en.MoveNext() |> ignore
          while en.MoveNext() do
            yield en.Current }

  /// A helper function that generates a sequence for the specified range.
  /// (This takes the step and also an operator to use for checking at the end)
  let inline private rangeStepImpl (lo:^T) (hi:^T) (step:^T) geq = 
    { new IEnumerable< ^T > with
        member x.GetEnumerator() =
          let current = ref (lo - step)
          { new IEnumerator< ^T > with
              member x.Current = current.Value
            interface System.Collections.IEnumerator with
              member x.Current = box current.Value
              member x.MoveNext() = 
                if geq current.Value hi then false
                else current.Value <- current.Value + step; true
              member x.Reset() = current.Value <- lo - step
            interface System.IDisposable with
              member x.Dispose() = ()  }
      interface System.Collections.IEnumerable with
        member x.GetEnumerator() = (x :?> IEnumerable< ^T >).GetEnumerator() :> _ }

  /// A helper function that generates a sequence for the specified range of
  /// int or int64 values. This is notably faster than using `lo .. step .. hi`.
  let inline rangeStep (lo:^T) (step:^T) (hi:^T) = 
    if lo <= hi then rangeStepImpl lo hi step (>=)
    else rangeStepImpl lo hi step (<=)

  /// A helper function that generates a sequence for the specified range of
  /// int or int64 values. This is notably faster than using `lo .. hi`.
  let inline range (lo:^T) (hi:^T) = 
    if lo <= hi then rangeStepImpl lo hi LanguagePrimitives.GenericOne (>=)
    else rangeStepImpl lo hi LanguagePrimitives.GenericOne (<=)

  /// Comapre two sequences using the `Equals` method. Returns true
  /// when all their elements are equal and they have the same size.
  let structuralEquals (s1:seq<'T>) (s2:seq<'T>) = 
    let mutable result = None
    use en1 = s1.GetEnumerator()
    use en2 = s2.GetEnumerator()
    while result.IsNone do 
      let canNext1, canNext2 = en1.MoveNext(), en2.MoveNext()
      if canNext1 <> canNext2 then result <- Some false
      elif not canNext1 then result <- Some true
      elif not ((box en1.Current).Equals(en2.Current)) then result <- Some false
    result.Value

  /// Calculate hash code of a sequence, based on the values
  let structuralHash (s:seq<'T>) = 
    let combine h1 h2 = ((h1 <<< 5) + h1) ^^^ h2
    s |> Seq.map (fun v -> (box v).GetHashCode()) |> Seq.fold combine -1

  /// If the input is non empty, returns `Some(head)` where `head` is 
  /// the first value. Otherwise, returns `None`.
  let headOrNone (input:seq<_>) = 
    System.Linq.Enumerable.FirstOrDefault(input |> Seq.map Some)

  /// Returns the specified number of elements from the end of the sequence
  /// Note that this needs to store the specified number of elements in memory
  /// and it needs to iterate over the entire sequence.
  let lastFew count (input:seq<_>) = 
    let cache = Array.zeroCreate count 
    let mutable cacheCount = 0
    let mutable cacheIndex = 0
    for v in input do 
      cache.[cacheIndex] <- v
      cacheCount <- cacheCount + 1
      cacheIndex <- (cacheIndex + 1) % count
    let available = min cacheCount count
    cacheIndex <- (cacheIndex - available + count) % count
    let cacheIndex = cacheIndex
    seq { for i in range 0 (available - 1) do yield cache.[(cacheIndex + i) % count] }
    
  // lastFew 3 List.empty<int> |> List.ofSeq = []
  // lastFew 3 [ 1 .. 10 ]  |> List.ofSeq = [ 8; 9; 10]

  /// Calls the `GetEnumerator` method. Simple function to guide type inference.
  let getEnumerator (s:seq<_>) = s.GetEnumerator()

  /// Given a sequence, returns `startCount` number of elements at the beginning 
  /// of the sequence (wrapped in `Choice1Of3`) followed by one `Choice2Of2()` value
  /// and then followed by `endCount` number of elements at the end of the sequence
  /// wrapped in `Choice3Of3`. If the input is shorter than `startCount + endCount`,
  /// then all values are returned and wrapped in `Choice1Of3`.
  let startAndEnd startCount endCount input = seq { 
    let lastItems = Array.zeroCreate endCount
    let lastPointer = ref 0
    let written = ref 0
    let skippedAny = ref false
    let writeNext(v) = 
      if !written < endCount then incr written; 
      lastItems.[!lastPointer] <- v; lastPointer := (!lastPointer + 1) % endCount
    let readNext() = let p = !lastPointer in lastPointer := (!lastPointer + 1) % endCount; lastItems.[p]
    let readRest() = 
      lastPointer := (!lastPointer + endCount - !written) % endCount
      seq { for i in range 1 !written -> readNext() }

    use en = getEnumerator input 
    let rec skipToEnd() = 
      if en.MoveNext() then 
        writeNext(en.Current)
        skippedAny := true
        skipToEnd()
      else seq { if skippedAny.Value then 
                   yield Choice2Of3()
                   for v in readRest() -> Choice3Of3 v 
                 else for v in readRest() -> Choice1Of3 v }
    let rec fillRest count = 
      if count = endCount then skipToEnd()
      elif en.MoveNext() then 
        writeNext(en.Current)
        fillRest (count + 1)
      else seq { for v in readRest() -> Choice1Of3 v }
    let rec yieldFirst count = seq { 
      if count = 0 then yield! fillRest 0
      elif en.MoveNext() then 
        yield Choice1Of3 en.Current
        yield! yieldFirst (count - 1) }
    yield! yieldFirst startCount }


  /// Generate floating windows from the input sequence. New floating window is 
  /// started for each element. To find the end of the window, the function calls
  /// the provided argument `f` with the first and the last elements of the window
  /// as arguments. A window ends when `f` returns `false`.
  let windowedWhile (f:'T -> 'T -> bool) input = seq {
    let windows = System.Collections.Generic.LinkedList()
    for v in input do
      windows.AddLast( (v, []) ) |> ignore
      // Walk over all windows; use 'f' to determine if the item
      // should be added - if so, add it, otherwise yield window
      let win = ref windows.First
      while win.Value <> null do 
        let start, items = win.Value.Value
        let next = win.Value.Next
        if f start v then win.Value.Value <- start, v::items
        else 
          yield items |> List.rev |> Array.ofList
          windows.Remove(win.Value)
        win := next
    for _, win in windows do
      yield win |> List.rev |> Array.ofList }

  
  /// Generate non-verlapping chunks from the input sequence. A chunk is started 
  /// at the beginning and then immediately after the end of the previous chunk.
  /// To find the end of the chunk, the function calls the provided argument `f` 
  /// with the first and the last elements of the chunk as arguments. A chunk 
  /// ends when `f` returns `false`.
  let chunkedWhile f input = seq {
    let chunk = ref None
    for v in input do
      match chunk.Value with 
      | None -> chunk := Some(v, [v])
      | Some(start, items) ->
          if f start v then chunk := Some(start, v::items)
          else
            yield items |> List.rev |> Array.ofList
            chunk := Some(v, [v])
    match chunk.Value with
    | Some (_, items) -> yield items |> List.rev |> Array.ofList
    | _ -> () }

      
  /// A version of `Seq.windowed` that allows specifying more complex boundary
  /// behaviour. The `boundary` argument can specify one of the following options:
  /// 
  ///  * `Boundary.Skip` - only full windows are returned (like `Seq.windowed`)
  ///  * `Boundary.AtBeginning` - incomplete windows (smaller than the required
  ///    size) are returned at the beginning.
  ///  * `Boundary.AtEnding` - incomplete windows are returned at the end.
  ///
  /// The result is a sequence of `DataSegnebt<T>` values, which makes it 
  /// easy to distinguish between complete and incomplete windows.
  let windowedWithBounds size boundary (input:seq<'T>) = seq {
    let windows = Array.create size []
    let currentWindow = ref 0
    for v in input do
      for i in 0 .. windows.Length - 1 do windows.[i] <- v::windows.[i]
      let win = windows.[currentWindow.Value] |> Array.ofList |> Array.rev
      // If the window is smaller, we yield it as Boundary only when
      // the required behaviour is to yield boundary at the beginning
      if win.Length < size then
        if boundary = Boundary.AtBeginning then yield DataSegment(Incomplete, win)
      else yield DataSegment(Complete, win)
      windows.[currentWindow.Value] <- []
      currentWindow := (!currentWindow + 1) % size
    // If we are supposed to generate boundary at the end, do it now
    if boundary = Boundary.AtEnding then
      for _ in 1 .. size - 1 do
        yield DataSegment(Incomplete, windows.[currentWindow.Value] |> Array.ofList |> Array.rev)
        currentWindow := (!currentWindow + 1) % size }


  /// Similar to `Seq.windowedWithBounds`, but generates non-overlapping chunks
  /// rather than floating windows. See that function for detailed documentation.
  /// The function may iterate over the sequence repeatedly.
  let chunkedWithBounds size (boundary:Boundary) input = seq {
    // If the user wants incomplete chunk at the beginning, we 
    // need to know the length of the whole sequence..
    let tail = ref input
    if boundary.HasFlag(Boundary.AtBeginning) then 
      let size = (Seq.length input) % size
      if size <> 0 && not (boundary.HasFlag(Boundary.Skip)) then
        yield DataSegment(Incomplete, Seq.take size input |> Array.ofSeq)
      tail := input |> Seq.skip size
    
    // Process the main part of the sequence
    let currentChunk = ref []
    let currentChunkSize = ref 0
    for v in !tail do
      currentChunk := v::currentChunk.Value
      incr currentChunkSize
      if !currentChunkSize = size then
        yield DataSegment(Complete, !currentChunk |> Array.ofList |> Array.rev)
        currentChunk := []
        currentChunkSize := 0 
        
    // If we want to yield incomplete chunks at the end and we got some, yield now
    if boundary.HasFlag(Boundary.AtEnding) && !currentChunk <> [] &&
       not (boundary.HasFlag(Boundary.Skip)) then
       yield DataSegment(Incomplete, !currentChunk |> Array.ofList |> Array.rev) }


  /// Generates addresses of chunks in a collection of size 'length'. For example, consider
  /// a collection with 7 elements (and indices 0 .. 6) and the requirement to create chunks
  /// of length 4: 
  ///
  ///    0 1 2 3 4 5 6   
  ///
  ///    s
  ///    s s
  ///    s s s
  ///    w w w w
  ///      w w w w
  ///        w w w w
  ///          w w w w
  ///            e e e 
  ///              e e
  ///                e
  ///
  /// The windows 's' are returned when `boundary = Boundary.AtBeginning` and the windows
  /// 'e' are returned when `boundary = Boundary.AtEnding`. The middle is returned always.
  /// The windows are specified by *inclusive* indices, so, e.g. the first window is returned
  /// as a pair (0, 0).
  let windowRangesWithBounds size boundary length = seq { 
    // If we want incomplete windows at the beginning, 
    // generate "size - 1" windows always starting from 0
    if boundary = Boundary.AtBeginning then
      for i in 1L .. size - 1L do yield DataSegmentKind.Incomplete, 0L, i - 1L
    // Generate all windows in the middle. There is always length - size + 1 of those
    for i in 0L .. length - size do yield DataSegmentKind.Complete, i, i + size - 1L 
    // If we want incomplete windows at the ending
    // gneerate "size - 1" windows, always ending with length-1
    if boundary = Boundary.AtEnding then
      for i in 1L .. size - 1L do yield DataSegmentKind.Incomplete, length - size + i, length - 1L }


  /// Generates addresses of windows in a collection of size 'length'. For example, consider
  /// a collection with 7 elements (and indices 0 .. 6) and the requirement to create windows
  /// of length 3: 
  ///
  ///    0 1 2 3 4 5 6   
  ///
  /// When the `AtEnding` flag is set for `boundary`:
  ///
  ///    c c c
  ///          c c c
  ///                d
  ///
  /// The two chunks marked as 'c' are returned always. The incomplete chunk at the end is
  /// returned unless the `Skip` flag is set for `boundary`. When the `AtBeginning` flag is
  /// set, the incomplete chunk is (when not `Skip`) returned at the beginning:
  ///
  ///    d
  ///      c c c 
  ///            c c c 
  ///
  /// The chunks are specified by *inclusive* indices, so, e.g. the first chunk in 
  /// the second example above is returned as a pair (0, 0).
  let chunkRangesWithBounds size (boundary:Boundary) length = seq { 
    if boundary.HasFlag(Boundary.AtBeginning) && boundary.HasFlag(Boundary.AtEnding) then
      invalidOp "Only one kind of boundary must be specified (either AtBeginning or AtEnding)"

    // How many chunk do we return? What is the length of the incomplete one?
    let chunkCount = length / size
    let incompleteSize = length % size
    if boundary.HasFlag(Boundary.AtBeginning) then
      // Generate one incomplete chunk if it is required
      // and then chunkCount times chunks starting from incompleteSize
      if not (boundary.HasFlag(Boundary.Skip)) && incompleteSize <> 0L then
        yield DataSegmentKind.Incomplete, 0L, incompleteSize - 1L
      for i in 0L .. chunkCount - 1L do 
        yield DataSegmentKind.Complete, incompleteSize + i * size, incompleteSize + (i + 1L) * size - 1L
    else // Assuming Boundary.AtEnding
      // Generate chunkCount times chunks starting from zero
      // and then one incomplete chunk if it is required
      for i in 0L .. chunkCount - 1L do 
        yield DataSegmentKind.Complete, i * size, (i + 1L) * size - 1L
      if not (boundary.HasFlag(Boundary.Skip)) && incompleteSize <> 0L then
        yield DataSegmentKind.Incomplete, chunkCount * size, length - 1L }

  /// Generate floating windows from the input sequence. New floating window is 
  /// started for each element. To find the end of the window, the function calls
  /// the provided argument `f` with the first and the last elements of the window
  /// as arguments. A window ends when `f` returns `false`.
  /// The function returns the windows as pairs of their indices.
  let windowRangesWhile (f:'T -> 'T -> bool) input = seq {
    let windows = System.Collections.Generic.LinkedList()
    let index = ref -1L
    for v in input do
      index := !index + 1L
      windows.AddLast( (v, !index, !index) ) |> ignore
      // Walk over all windows; use 'f' to determine if the item
      // should be added - if so, add it, otherwise yield window
      let win = ref windows.First
      while win.Value <> null do 
        let value, startIdx, endIdx = win.Value.Value
        let next = win.Value.Next
        if f value v then win.Value.Value <- value, startIdx, !index
        else 
          yield startIdx, endIdx
          windows.Remove(win.Value)
        win := next
    for _, startIdx, endIdx in windows do
      yield startIdx, endIdx }

  
  /// Generate non-verlapping chunks from the input sequence. A chunk is started 
  /// at the beginning and then immediately after the end of the previous chunk.
  /// To find the end of the chunk, the function calls the provided argument `f` 
  /// with the first and the last elements of the chunk as arguments. A chunk 
  /// ends when `f` returns `false`.
  /// The function returns the chunks as pairs of their indices.
  let chunkRangesWhile f input = seq {
    let chunk = ref None
    let index = ref -1L
    for v in input do
      index := !index + 1L
      match chunk.Value with 
      | None -> chunk := Some(v, !index, !index)
      | Some(value, startIdx, endIdx) ->
          if f value v then chunk := Some(value, startIdx, !index)
          else
            yield startIdx, endIdx
            chunk := Some(v, !index, !index)
    match chunk.Value with
    | Some (_, startIdx, endIdx) -> yield startIdx, endIdx
    | _ -> () }


  /// Returns true if the specified sequence is sorted.
  let isSorted (data:seq<_>) (comparer:IComparer<_>) =
    let rec isSorted past (en:IEnumerator<'T>) =
      if not (en.MoveNext()) then true
      elif comparer.Compare(past, en.Current) > 0 then false
      else isSorted en.Current en
    let en = data.GetEnumerator()
    if not (en.MoveNext()) then true
    else isSorted en.Current en


  /// Returns the first and the last element from a sequence or 'None' if the sequence is empty
  let tryFirstAndLast (input:seq<_>) = 
    let mutable first = None
    let mutable last = None
    for v in input do 
      if first.IsNone then first <- Some v
      last <- Some v
    let last = last
    first |> Option.map (fun f -> f, last.Value)

  // ------------------------------------------------------------------------------------
  // Aligning sequences
  // ------------------------------------------------------------------------------------
  
  // The following functions take two or N, ordered or unordered sequences of keys.
  // They all produces a new array with the union of keys together with relocation
  // tables for each original sequence of keys. The relocation table is an array with
  // two numbers. The first number is the new location (index of a key K in newly 
  // returned array of keys) and the second is original location (index of the key K in
  // the original input sequence).
  // 
  // The functions generally perform union, meaning that the resulting array with keys
  // contains union of all the keys. For aligning two sequences, there is also a version
  // that performs intersection.

  /// Align two ordered sequences of keys (using the specified comparer)
  /// The resulting relocations are returned as two-element list for symmetry with other functions
  /// Throws ComparisonFailedException when the comparer fails.
  ///
  /// When `intersectionOnly = true`, the function only adds keys & relocations
  /// for keys that appear in both sequences (otherwise, it performs union)
  let alignOrdered (seq1:ReadOnlyCollection<'T>) (seq2:ReadOnlyCollection<'T>) (comparer:IComparer<'T>) intersectionOnly : 'T[] * list<(int64 * int64)[]> = 
    // Indices in the sequences seq1 and seq2
    let mutable index1 = if seq1.Count > 0 then 0 else -1
    let mutable index2 = if seq2.Count > 0 then 0 else -1
    // Index in the output list of keys
    let mutable outIndex = 0L
    let keys = ResizeArray<_>(seq1.Count + seq2.Count)
    // Arrays with relocations
    let res1 = ResizeArray<_>(seq1.Count)
    let res2 = ResizeArray<_>(seq2.Count)
    
    // Loop while there is a key in both inputs
    while index1 <> -1 && index2 <> -1 do
      let comparison = 
        try comparer.Compare(seq1.[index1], seq2.[index2])
        with _ -> raise <| ComparisonFailedException()
      // Add current smallest key to the list of keys
      if intersectionOnly then 
        if comparison = 0 then keys.Add(seq1.[index1])
      else
        if comparison <= 0 then keys.Add(seq1.[index1])
        else keys.Add(seq2.[index2])

      // Advance sequence(s) starting with the current key
      if comparison <= 0 then
        if comparison = 0 || not intersectionOnly then res1.Add( (outIndex, int64 index1) )
        index1 <- if index1 + 1 >= seq1.Count then -1 else index1 + 1
      if comparison >= 0 then
        if comparison = 0 || not intersectionOnly then res2.Add( (outIndex, int64 index2) )
        index2 <- if index2 + 1 >= seq2.Count then -1 else index2 + 1

      // If we added key, increment index (we add key when 
      // unioning or when interesecting & keys are in both)
      if not intersectionOnly || comparison = 0 then
        outIndex <- outIndex + 1L

    // Add remaining relocations for one or the other 
    if not intersectionOnly && index1 <> -1 then
      for i = index1 to seq1.Count - 1 do 
        res1.Add( (outIndex, int64 i) )
        keys.Add(seq1.[i])
        outIndex <- outIndex + 1L
    if not intersectionOnly && index2 <> -1 then
      for i = index2 to seq2.Count - 1 do 
        res2.Add( (outIndex, int64 i) )
        keys.Add(seq2.[i])
        outIndex <- outIndex + 1L

    // Produce results as arrays
    keys.ToArray(), [ res1.ToArray(); res2.ToArray() ]

  
  /// Align N ordered sequences of keys (using the specified comparer)
  /// This is the same as `alignOrdered` but for larger number of key sequences.
  /// Throws ComparisonFailedException when the comparer fails.
  /// (This performs union on the specified sequences)
  let alignAllOrderedMany (seqs:ReadOnlyCollection<'T>[]) (comparer:IComparer<'T>) : 'T[] * list<(int64 * int64)[]> = 
    
    // We maintain a set of indices into the original key sequences, starting at 0
    // An empty sequence is marked with an index of -1
    let current = seqs |> Array.map (fun s -> if s.Count > 0 then 0, s else -1, null)

    // Resize arrays for resulting keys and relocation tables
    let newkeys = ResizeArray<_>(seqs |> Array.sumBy (fun s -> s.Count))
    let results = seqs |> Array.map (fun s -> ResizeArray<_>(s.Count))

    // We maintain a heap structure to access the next smallest key along with the 
    // sequence it comes; allows delete-min/find-min in O(log n) time
    let mutable heap = 
      { new IComparer<_> with
          member x.Compare(a, b) = 
            try comparer.Compare(fst a, fst b) 
            with _ -> raise (new ComparisonFailedException()) }
      |> BinomialHeap.emptyCustom

    // initialize heap
    for i = 0 to current.Length - 1 do
      let idx, keys = current.[i]
      if idx <> -1 then   
        heap <- heap |> BinomialHeap.insert (keys.[idx], i)

    let mutable index = -1L
    let mutable seen = HashSet()   // because F# Set requires comparison ...

    // When there are no more elements to examine, we're done
    while not (BinomialHeap.isEmpty heap) do
      // pop the min key along w/sequence it came from
      let (k, i), htmp = BinomialHeap.removeMin heap
      let idx, keys = current.[i]
      if not <| seen.Contains(k) then
        // we haven't seen this key, so increment index into resulting keys array
        newkeys.Add(k) |> ignore
        seen.Add(k) |> ignore
        index <- index + 1L
      // store the relocation indexing
      results.[i].Add( (index, int64 idx) )
      if idx + 1 < keys.Count then 
        // there's another key to examine in the i'th sequence, so it on the heap
        current.[i] <- (idx + 1, keys)
        heap <- htmp |> BinomialHeap.insert (keys.[idx + 1], i)                
      else
        // no more keys in i'th sequence, allow heap to shrink
        heap <- htmp
    
    // Return results as arrays
    newkeys.ToArray(), [ for r in results -> r.ToArray() ]

  /// Align N ordered sequences of keys (using the specified comparer)
  /// This is the same as `alignOrdered` but for larger number of key sequences.
  /// Throws ComparisonFailedException when the comparer fails.
  /// (This performs union on the specified sequences)
  let alignAllOrderedFew (seqs:ReadOnlyCollection<'T>[]) (comparer:IComparer<'T>) : 'T[] * list<(int64 * int64)[]> = 
    
    // We keep an array with indices & original sequences
    // When we finish iterating over a sequence, we set it to 'null' and set the index to -1
    let current = seqs |> Array.map (fun s -> 
      if s.Count > 0 then 0, s else -1, null)

    // Resize arrays with keys and resulting relocation tables
    let keys = ResizeArray<_>(seqs |> Array.sumBy (fun s -> s.Count))
    let results = seqs |> Array.map (fun s -> ResizeArray<_>(s.Count))

    /// Returns the smallest key from the current position in all collections
    let smallestKey() = 
      let mutable k = Unchecked.defaultof<_>
      let mutable found = false
      for i = 0 to current.Length - 1 do
        let idx, keys = current.[i]
        if idx <> -1 then                   // else: No more values in this collection
          if found then                     // Get smaller of previous & current
            let k2 = keys.[idx]
            try k <- if comparer.Compare(k, k2) <= 0 then k else k2
            with _ -> raise (new ComparisonFailedException())
          else                              // We found our first key
            k <- keys.[idx]
            found <- true
      found, k

    /// For a given key, advance all input collections currently at the given key
    /// and add mapping to relocation table for them (also add key to the list of keys)
    let addAndAdvanceForKey index k =
      keys.Add(k)
      for i = 0 to current.Length - 1 do
        let idx, keys = current.[i]
        if idx <> -1 && comparer.Compare(keys.[idx], k) = 0 then
          current.[i] <- if idx + 1 >= keys.Count then -1, null else idx + 1, keys
          results.[i].Add( (index, int64 idx) )

    // While there is some key in any of the collections, call `addAndAdvanceForKey`
    let mutable index = 0L
    let mutable completed = false
    while not completed do
      match smallestKey() with
      | false, _ -> completed <- true
      | true, k -> addAndAdvanceForKey index k; index <- index + 1L
    // Return results as arrays
    keys.ToArray(), [ for r in results -> r.ToArray() ]


  /// Calls either `alignAllOrderedMany` or `alignAllOrderedFew` depending on the number
  /// of sequences to be aligned. Performance measurements suggest that 150 is the limit
  /// when the implementation using binomial heap is faster.
  let alignAllOrdered (collections:_[]) comparer = 
    if collections.Length > 150 then alignAllOrderedMany collections comparer
    else alignAllOrderedFew collections comparer
    

  /// Align two unordered sequences of keys (performs union of the keys)
  /// The resulting relocations are returned as two-element list for symmetry with other functions
  let alignUnorderedUnion (seq1:ReadOnlyCollection<'T>) (seq2:ReadOnlyCollection<'T>) = 
    let dict = Dictionary<_, _>(seq1.Count + seq2.Count)
    let keys = ResizeArray<_>(seq1.Count + seq2.Count)
    let res1 = ResizeArray<_>(seq1.Count)
    let res2 = ResizeArray<_>(seq1.Count)

    let mutable keyIndex = 0L
    for i = 0 to seq1.Count - 1 do
      let key = seq1.[i]
      keys.Add(key)
      res1.Add( (keyIndex, int64 i) )
      dict.[key] <- keyIndex
      keyIndex <- keyIndex + 1L

    for i = 0 to seq2.Count - 1 do
      let key = seq2.[i]
      match dict.TryGetValue(key) with
      | true, ki -> 
          res2.Add( (ki, int64 i) )
      | false, _ ->
          keys.Add(key)
          res2.Add( (keyIndex, int64 i) )
          keyIndex <- keyIndex + 1L
          // No need to add to 'dict' because it will not appear again in 'seq2'

    keys.ToArray(), [ res1.ToArray(); res2.ToArray() ]    


  /// Align two unordered sequences of keys (performs intersection of the keys)
  /// The resulting relocations are returned as two-element list for symmetry with other functions
  let alignUnorderedIntersection (seq1:ReadOnlyCollection<'T>) (seq2:ReadOnlyCollection<'T>) = 
    // Dictionary containing 'true' when the key is in both collections
    let dict = Dictionary<_, _>(seq1.Count + seq2.Count)
    for k in seq1 do dict.Add(k, false)
    for k in seq2 do if fst (dict.TryGetValue(k)) then dict.[k] <- true

    // Lookup with indices for keys & array of keys to return
    let keyIndices = Dictionary<_, _>(seq1.Count + seq2.Count)
    let keys = ResizeArray<_>()
    for (KeyValue(k, v)) in dict do
      if v then 
        keyIndices.Add(k, int64 keys.Count)
        keys.Add(k)

    // Build relocation tables
    let res1 = ResizeArray<_>(seq1.Count)
    let res2 = ResizeArray<_>(seq2.Count)
    for i = 0 to seq1.Count - 1 do 
      let succ, j = keyIndices.TryGetValue(seq1.[i])
      if succ then res1.Add( (j, int64 i) ) 
    for i = 0 to seq2.Count - 1 do 
      let succ, j = keyIndices.TryGetValue(seq2.[i])
      if succ then res2.Add( (j, int64 i) ) 

    // Return the results
    keys.ToArray(), [ res1.ToArray(); res2.ToArray() ]

  /// Align two unordered sequences of keys. Calls either
  /// `alignUnorderedUnion` or `alignUnorderedIntersection`, based 
  /// on the `intersectionOnly` parameter.
  let alignUnordered s1 s2 intersectionOnly = 
    if intersectionOnly then alignUnorderedIntersection s1 s2
    else alignUnorderedUnion s1 s2


  /// Align N unordered sequences of keys (performs union of the keys)
  let alignAllUnordered (seqs:ReadOnlyCollection<'T>[]) = 
    let capacity = seqs |> Array.sumBy (fun s -> s.Count)
    let dict = Dictionary<_, _>(capacity)
    let keys = ResizeArray(capacity)
    let mutable keyIndex = 0L
    let relocs = seqs |> Array.map (fun c -> ResizeArray<_>(c.Count))

    for i = 0 to seqs.Length - 1 do
      let seq = seqs.[i]
      for j = 0 to seq.Count - 1 do
        let key = seq.[j]
        match dict.TryGetValue(key) with
        | true, ki -> relocs.[i].Add( (ki, int64 j) )
        | false, _ ->
            keys.Add(key)
            dict.Add(key, keyIndex)
            relocs.[i].Add( (keyIndex, int64 j) )
            keyIndex <- keyIndex + 1L
            
    keys.ToArray(), [ for r in relocs -> r.ToArray() ] 

// --------------------------------------------------------------------------------------
// Misc - formatting
// --------------------------------------------------------------------------------------
  
/// [omit]
/// An interface implemented by types that support nice formatting for F# Interactive
/// (The `FSharp.DataFrame.fsx` file registers an FSI printer using this interface.)
type IFsiFormattable =
  abstract Format : unit -> string

/// [omit]
/// Contains helper functions and configuration constants for pretty printing
module Formatting = 
  /// Maximal number of items to be printed at the beginning of a series/frame
  let StartItemCount = 15
  /// Maximal number of items to be printed at the end of a series/frame
  let EndItemCount = 15

  /// Maximal number of items to be printed at the beginning of an inline formatted series/frame
  let StartInlineItemCount = 5
  /// Maximal number of items to be printed at the end of an inline formatted series/frame
  let EndInlineItemCount = 1

  open System
  open System.IO
  open System.Text

  // Simple functions that pretty-print series and frames
  // (to be integrated as ToString and with F# Interactive)
  let formatTable (data:string[,]) =
    let sb = StringBuilder()
    use wr = new StringWriter(sb)

    let rows = data.GetLength(0)
    let columns = data.GetLength(1)
    let widths = Array.zeroCreate columns 
    data |> Array2D.iteri (fun r c str ->
      widths.[c] <- max (widths.[c]) (str.Length))
    for r in 0 .. rows - 1 do
      for c in 0 .. columns - 1 do
        wr.Write(data.[r, c].PadRight(widths.[c] + 1))
      wr.WriteLine()

    sb.ToString()

// --------------------------------------------------------------------------------------
// Type conversions
// --------------------------------------------------------------------------------------

/// [omit]
module Convert =
  let private nullableType = typedefof<System.Nullable<_>>

  /// Conversions that are "safe" as a list of "source -> target" types
  let private safeConversions =
    [ // Conversions from smaller integers to larger integers
      typeof<uint8>, typeof<int16>
      typeof<int8>, typeof<int16>
      typeof<uint16>, typeof<int32>
      typeof<int16>, typeof<int32>
      typeof<uint32>, typeof<int64>
      typeof<int32>, typeof<int64>
      // Conversions from u/int64 to decimal are legal too
      typeof<int64>, typeof<decimal>
      typeof<uint64>, typeof<decimal>
      // Conversions decimal -> float32 -> float are allowed
      typeof<decimal>, typeof<float32>
      typeof<float32>, typeof<float> ]

  /// Dictionary that maps target type (e.g. 'float32') to all the source types that can be
  /// safely converted to it (e.g. 'decimal,int64,unit64,int32,uint32,int16,uint16,int8,uint8')
  let private sourcesByTarget =
    let lookupFromTarget =        
      safeConversions    
      |> Seq.groupBy snd
      |> Seq.map (fun (k, s) -> k, [ for f, _ in s -> f ])
      |> dict

    // For each target type, find all the allowed sources (recursively)
    let rec allSourcesFor typ = seq {
      match lookupFromTarget.TryGetValue(typ) with
      | false, _ -> ()
      | true, sources ->
          yield! sources
          for sourceTyp in sources do
            yield! allSourcesFor sourceTyp }

    [ for _, target in safeConversions ->
        target, dict [ for s in allSourcesFor target -> s, true] ] |> dict

  /// Helper function that converts value to a specified type. The conversion
  /// is done using the specified conversion kind, which specifies the level
  /// of flexibility (Exact - cast, Safe - according to 'sourcesByTarget', 
  /// Flexible - anything that System.Convert allows)
  let convertType<'T> conversionKind (value:obj) =
    match conversionKind with
    | ConversionKind.Flexible ->
        // Check if we can cast first (one would think that System.Convert
        // should handle this, but it fails to convert nullables e.g. bool to bool?)
        if value :? 'T then value :?> 'T
        else System.Convert.ChangeType(value, typeof<'T>) :?> 'T
    | ConversionKind.Safe ->
        if value :? 'T then value :?> 'T
        elif value <> null then
          match sourcesByTarget.TryGetValue(typeof<'T>) with
          | true, sources when sources.ContainsKey(value.GetType()) ->
              System.Convert.ChangeType(value, typeof<'T>) :?> 'T
          | _ -> raise <| InvalidCastException(sprintf "Cannot safely convert %s to %s" (value.GetType().Name) (typeof<'T>.Name))
        else raise <| InvalidCastException(sprintf "Cannot safely convert null to %s" (typeof<'T>.Name))
    | ConversionKind.Exact -> value :?> 'T
    | _ -> invalidArg "conversionKind" "Invalid value"

  /// A function that returns `true` when `convertType<'T>` is expected
  /// to succeed on the specified value (this is approximation - it may
  /// return 'true' even if the conversion fails, but not the other way round)
  let canConvertType<'T> conversionKind (value:obj) =
    match conversionKind with
    | ConversionKind.Flexible ->
        value :? 'T || value :? IConvertible
    | ConversionKind.Safe ->
        if value :? 'T then true
        elif value :? IConvertible then
          match sourcesByTarget.TryGetValue(typeof<'T>) with
          | true, sources -> sources.ContainsKey(value.GetType())
          | _ -> false
        else false
    | ConversionKind.Exact -> value :? 'T
    | _ -> invalidArg "conversionKind" "Invalid value"

// --------------------------------------------------------------------------------------
// Support for C# dynamic
// --------------------------------------------------------------------------------------

/// [omit]
/// Module that implements various helpers for supporting C# dynamic type.
/// (this takes care of some of the complexity around building `DynamicMetaObject`
/// and it is used by `SeriesBuilder` and `Frame`)
module DynamicExtensions =
  open System.Dynamic
  open System.Linq.Expressions
  open Microsoft.FSharp.Quotations

  /// A C# expression tree, embedded in a value (in a quotation)
  type WrappedExpression(expr:Expression) =
    member x.Expression = expr

  /// Wrap C# expressionm tree into an F# quotation
  type System.Linq.Expressions.Expression with
    member x.Wrap() = Expr.Value(WrappedExpression x, typeof<obj>)
    member x.Wrap<'T>() = Expr.Value(WrappedExpression (Expression.Convert(x, typeof<'T>)), typeof<'T>)

  /// Translate simple F# quotation to C# expression & handle wrapped values
  let rec private asExpr : _ -> Expression = function
    | Patterns.Coerce(expr, typ) -> upcast Expression.Convert(asExpr expr, typ)
    | Patterns.NewObject(ci, args) -> upcast Expression.New(ci, Seq.map asExpr args)
    | Patterns.Value((:? WrappedExpression as w), _) -> w.Expression
    | Patterns.Sequential(e1, e2) ->
        Expression.Block(asExpr e1, asExpr e2) :> Expression
    | Patterns.Value(v, typ) -> upcast Expression.Constant(v, typ) 
    | Patterns.Call(None, mi, args) -> 
        let args = (List.map asExpr args, mi.GetParameters()) ||> Seq.map2 (fun expr par ->
          Expression.Convert(expr, par.ParameterType) :> Expression)
        upcast Expression.Call(mi, args)
    | Patterns.Call(Some inst, mi, args) -> 
        let args = (List.map asExpr args, mi.GetParameters()) ||> Seq.map2 (fun expr par ->
          Expression.Convert(expr, par.ParameterType) :> Expression)
        upcast Expression.Call(asExpr inst, mi, args)
    | expr -> failwithf "ExpressionHelpers.asExpr: Not supported expression!\n%A" expr

  type Microsoft.FSharp.Quotations.Expr with
    member x.AsExpression() = asExpr x

  type GetterWrapper<'T>(f:'T -> string -> obj) =
    member x.Invoke(owner, name) = f owner name

  type SetterWrapper<'T>(f:'T -> string -> obj -> unit) =
    member x.Invoke(owner, name, value) = f owner name value

  /// This can be used when getter/setter are generic (in some way) - the caller
  /// is responsible for generating the right expression tree (this cannot easily
  /// be done using quotations)
  let createPropertyMetaObject expr (owner:'T) getter setter =
    { new System.Dynamic.DynamicMetaObject(expr, System.Dynamic.BindingRestrictions.Empty, owner) with
        override x.BindGetMember(binder) = 
          let call : Expr = getter (x.Expression.Wrap<'T>()) binder.Name binder.ReturnType
          let restrictions = BindingRestrictions.GetTypeRestriction(x.Expression, x.LimitType);
          new DynamicMetaObject(call.AsExpression(), restrictions)

        override x.BindSetMember(binder, value) = 
          let call : Expr = setter (x.Expression.Wrap<'T>()) binder.Name value.LimitType (value.Expression.Wrap())
          let call = 
            if binder.ReturnType = typeof<System.Void> then call
            elif binder.ReturnType = typeof<obj> then <@@ %%call; new obj() @@>
            else failwith "createPropertyMetaObject: Expected void or object return type"              
          let restrictions = BindingRestrictions.GetTypeRestriction(x.Expression, x.LimitType);
          new DynamicMetaObject(call.AsExpression(), restrictions) }

  /// This can be used when the setter is a simple non-generic function that 
  /// takes the name as string & argument as object (and returns nothing)
  let createGetterAndSetterFromFunc expr (owner:'T) (getter:'T -> string -> obj) (setter:'T -> string -> obj -> unit) =
    { new System.Dynamic.DynamicMetaObject(expr, System.Dynamic.BindingRestrictions.Empty, owner) with
        override x.BindGetMember(binder) =
          if binder.ReturnType <> typeof<obj> then failwith "createGetterAndSetterFromFunc: Expected object return type"
          let getter = GetterWrapper<'T>(getter)
          let call = <@@ getter.Invoke(%%(x.Expression.Wrap<'T>()), %%(Expr.Value(binder.Name))) @@>
          let restrictions = BindingRestrictions.GetTypeRestriction(x.Expression, x.LimitType)
          new DynamicMetaObject(call.AsExpression(), restrictions)

        override x.BindSetMember(binder, value) = 
          if binder.ReturnType <> typeof<obj> then failwith "createGetterAndSetterFromFunc: Expected object return type"
          let setter = SetterWrapper<'T>(setter)
          let call = <@@ setter.Invoke(%%(x.Expression.Wrap<'T>()), %%(Expr.Value(binder.Name)), %%(value.Expression.Wrap())); null @@>
          let restrictions = BindingRestrictions.GetTypeRestriction(x.Expression, x.LimitType);
          new DynamicMetaObject(call.AsExpression(), restrictions) }
