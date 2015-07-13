namespace Deedle 

open Deedle.Internal
open Deedle.Vectors
open Deedle.Vectors.ArrayVector

// ------------------------------------------------------------------------------------------------
// F# friendly operations for creating vectors
// ------------------------------------------------------------------------------------------------

/// Set concrete IVectorBuilder implementation
///
/// [category:Vectors and indices]
[<AutoOpen>]
module ``F# VectorBuilder implementation`` =
  type VectorBuilder = 
    /// Returns concrete implementation for IVectorBuilder
    static member Instance = ArrayVectorBuilder.Instance

/// Defines non-generic `Vector` type that provides functions for building vectors
/// (hard-bound to `ArrayVectorBuilder` type). In F#, the module is automatically opened
/// using `AutoOpen`. The methods are not designed for the use from C#.
///
/// [category:Vectors and indices]
[<AutoOpen>]
module ``F# Vector extensions`` =
  open System

  /// Type that provides a simple access to creating vectors represented
  /// using the built-in `ArrayVector` type that stores the data in a 
  /// continuous block of memory.
  type Vector = 
    /// Creates a vector that stores the specified data in an array.
    /// Values such as `null` and `Double.NaN` are turned into missing values.
    static member inline ofValues<'T>(data:'T[]) = 
      VectorBuilder.Instance.Create(data)

    /// Creates a vector that stores the specified data in an array.
    /// Values such as `null` and `Double.NaN` are turned into missing values.
    static member inline ofValues<'T>(data:seq<'T>) = 
      VectorBuilder.Instance.Create(Array.ofSeq data)

    /// Creates a vector that stores the specified data in an array.
    /// Missing values can be specified explicitly as `None`, but other values 
    /// such as `null` and `Double.NaN` are turned into missing values too.
    static member inline ofOptionalValues<'T>(data:seq<option<'T>>) = 
      VectorBuilder.Instance.CreateMissing(data |> Seq.map OptionalValue.ofOption |> Array.ofSeq)

    /// Missing values can be specified explicitly as `OptionalValue.Missing`, but 
    /// other values such as `null` and `Double.NaN` are turned into missing values too.
    static member inline ofOptionalValues<'T>(data:seq<OptionalValue<'T>>) = 
      VectorBuilder.Instance.CreateMissing(data |> Array.ofSeq)

// ------------------------------------------------------------------------------------------------

namespace Deedle.Vectors

open Deedle
open Deedle.VectorHelpers
open Deedle.Vectors.ArrayVector

// ------------------------------------------------------------------------------------------------
// Additional vector helpers that have to be defined after ArrayVector
// ------------------------------------------------------------------------------------------------

[<AutoOpen>]
module internal VectorHelperExtensions =
  open Deedle.Internal

  /// Returns `OptionalValue<obj>` which is a boxed version of `OptionalValue<IVector<obj>>`
  /// (where the vector contains values from the specified list of values)
  let private rowReaderFunc (values:OptionalValue<obj> list) : OptionalValue<obj> =
    OptionalValue(box (ArrayVectorBuilder.Instance.CreateMissing(Array.ofList values)))

  /// Transformation that creates a row reader. This also implements the
  /// `IRowReaderTransform` so that it can be replaced by a more optimial implementation
  type RowReaderTransform(colAddressAt) =
    interface IRowReaderTransform with
      member irt.ColumnAddressAt(idx) = colAddressAt idx
    interface INaryTransform with
      member vt.GetFunction<'R>() = 
        unbox<OptionalValue<'R> list -> OptionalValue<'R>> rowReaderFunc

  type NaryTransform with
    /// Returns the well-known `IRowReaderTransform` transformation
    static member RowReader(colAddressAt) = 
      RowReaderTransform(colAddressAt) :> INaryTransform
      |> VectorListTransform.Nary

  let createRowVector (vectorBuilder:IVectorBuilder) scheme rowKeyCount colKeyCount colAddressAt f (data:IVector<IVector>) =
    /// Create vector of row reader objects. This method creates 
    /// `IVector<obj>` where each `obj` is actually a boxed `IVector<obj>`
    /// that provides access to data of individual rows of the frame.
    let combinedRowVector =
      let vectors = [ for n in Seq.range 0L (colKeyCount-1L) -> Vectors.Return(int n) ]
      let cmd = Vectors.Combine(rowKeyCount, vectors, NaryTransform.RowReader(colAddressAt))
      let boxedData = [| for v in data.DataSequence -> boxVector v.Value |]
      vectorBuilder.Build(scheme, cmd, boxedData)

    // We get a vector containing boxed `IVector<obj>` - we turn it into a
    // vector containing what the caller specified, but lazily to avoid allocations
    combinedRowVector |> lazyMapVector (unbox<IVector<obj>> >> f)
    
// ------------------------------------------------------------------------------------------------
// C# frienly operations for creating vectors
// ------------------------------------------------------------------------------------------------

/// Type that provides access to creating vectors (represented as arrays)
type Vector = 
  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline Create<'T>(data:'T[]) = 
    VectorBuilder.Instance.Create(data)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline Create<'T>(data:seq<'T>) = 
    VectorBuilder.Instance.Create(Array.ofSeq data)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline CreateMissing<'T>(data:seq<OptionalValue<'T>>) = 
    VectorBuilder.Instance.CreateMissing(data |> Array.ofSeq)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline CreateMissing(data:seq<System.Nullable<'T>>) = 
    VectorBuilder.Instance.CreateMissing(data |> Array.ofSeq |> Array.map OptionalValue.ofNullable)