namespace Deedle 

open Deedle.Internal
open Deedle.Vectors
open Deedle.Vectors.ArrayVector

// ------------------------------------------------------------------------------------------------
// F# frienly operations for creating vectors
// ------------------------------------------------------------------------------------------------

/// Defines non-generic `Vector` type that provides functions for building vectors
/// (hard-bound to `ArrayVectorBuilder` type). In F#, the module is automatically opened
/// using `AutoOpen`. The methods are not designed for the use from C#.
[<AutoOpen>]
module FSharpVectorExtensions =
  open System

  /// Type that provides a simple access to creating vectors represented
  /// using the built-in `ArrayVector` type that stores the data in a 
  /// continuous block of memory.
  type Vector = 
    /// Creates a vector that stores the specified data in an array.
    /// Values such as `null` and `Double.NaN` are turned into missing values.
    static member inline ofValues<'T>(data:'T[]) = 
      ArrayVectorBuilder.Instance.Create(data)

    /// Creates a vector that stores the specified data in an array.
    /// Values such as `null` and `Double.NaN` are turned into missing values.
    static member inline ofValues<'T>(data:seq<'T>) = 
      ArrayVectorBuilder.Instance.Create(Array.ofSeq data)

    /// Creates a vector that stores the specified data in an array.
    /// Missing values can be specified explicitly as `None`, but other values 
    /// such as `null` and `Double.NaN` are turned into missing values too.
    static member inline ofOptionalValues<'T>(data:seq<option<'T>>) = 
      ArrayVectorBuilder.Instance.CreateMissing(data |> Seq.map OptionalValue.ofOption |> Array.ofSeq)

    /// Missing values can be specified explicitly as `OptionalValue.Missing`, but 
    /// other values such as `null` and `Double.NaN` are turned into missing values too.
    static member inline ofOptionalValues<'T>(data:seq<OptionalValue<'T>>) = 
      ArrayVectorBuilder.Instance.CreateMissing(data |> Array.ofSeq)

// ------------------------------------------------------------------------------------------------
// C# frienly operations for creating vectors
// ------------------------------------------------------------------------------------------------

namespace Deedle.Vectors

open Deedle
open Deedle.Vectors.ArrayVector

/// Type that provides access to creating vectors (represented as arrays)
type Vector = 
  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline Create<'T>(data:'T[]) = 
    ArrayVectorBuilder.Instance.Create(data)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline Create<'T>(data:seq<'T>) = 
    ArrayVectorBuilder.Instance.Create(Array.ofSeq data)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline CreateMissing<'T>(data:seq<OptionalValue<'T>>) = 
    ArrayVectorBuilder.Instance.CreateMissing(data |> Array.ofSeq)

  /// Creates a vector that stores the specified data in an array.
  /// Values such as `null` and `Double.NaN` are turned into missing values.
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member inline CreateMissing(data:seq<System.Nullable<'T>>) = 
    ArrayVectorBuilder.Instance.CreateMissing(data |> Array.ofSeq |> Array.map OptionalValue.ofNullable)
