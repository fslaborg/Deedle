namespace FSharp.DataFrame.Vectors

open FSharp.DataFrame
//open System.Collections.Generic
open System.Collections.ObjectModel

/// Provides a way to get the data of an arbitrary vector. This is a concrete type used 
/// by functions that operate on vectors (like `Series.sum`, etc.). The vector may choose
/// to return the data as `ReadOnlyCollection` (with or without N/A values) which is more
/// efficient to use or as a lazy sequence (slower, but more general).
[<RequireQualifiedAccess>]
type VectorData<'T> = 
  | DenseList of ReadOnlyCollection<'T>
  | SparseList of ReadOnlyCollection<OptionalValue<'T>>
  | Sequence of seq<OptionalValue<'T>>

// --------------------------------------------------------------------------------------
// Interfaces (generic & non-generic) for representing vectors
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame

open System
open System.Collections.Generic
open System.Collections.ObjectModel
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Vectors
open FSharp.DataFrame.Addressing

/// Represents an (untyped) vector that stores some values and provides access
/// to the values via a generic address. This type should be only used directly when
/// extending the DataFrame library and adding a new way of storing or loading data.
/// To allow invocation via Reflection, the vector exposes type of elements as `System.Type`.
type IVector = 
  /// Returns all values of the vector as a sequence of optional objects
  abstract ObjectSequence : seq<OptionalValue<obj>>

  /// Returns the type of elements stored in the current vector as `System.Type`.
  /// This member is mainly used for internal purposes (to invoke a generic function
  /// represented by `VectorCallSite1<R>` with the typed version of the current 
  /// vector as an argument.
  abstract ElementType : System.Type

  /// When `true`, the formatter in F# Interactive will not attempt to evaluate the
  /// vector to print it. This is useful when the vector contains lazily loaded data.
  abstract SuppressPrinting : bool

  /// Return value stored in the vector at a specified address. This is simply an
  /// untyped version of `GetValue` method on a typed vector.
  abstract GetObject : Address -> OptionalValue<obj>

/// A generic, typed vector. Represents mapping from addresses to values of type `T`. 
/// The vector provides a minimal interface that is required by series and can be
/// implemented in a number of ways to provide vector backed by database or an
/// alternative representation of data.
type IVector<'T> = 
  inherit IVector 
  /// Returns value stored in the vector at a specified address. 
  abstract GetValue : Address -> OptionalValue<'T>

  /// Returns all data of the vector in one of the supported formats. Depending
  /// on the vector, data may be returned as a continuous block of memory using
  /// `ReadOnlyCollection<T>` or as a lazy sequence `seq<T>`.
  abstract Data : VectorData<'T>

  /// Apply the specified function to all values stored in the vector and return
  /// a new vector (not necessarily of the same representation) with the results.
  abstract Select : ('T -> 'TNew) -> IVector<'TNew>

  /// Apply the specified function to all values stored in the vector and return
  /// a new vector (not necessarily of the same representation) with the results.
  /// The function handles missing values - it is called with optional values and
  /// may return a missing value as a result of the transformation.
  abstract SelectMissing : (OptionalValue<'T> -> OptionalValue<'TNew>) -> IVector<'TNew>


/// Module with extensions for generic vector type. Given `vec` of type `IVector<T>`, 
/// the extension property `vec.DataSequence` returns all data of the vector converted
/// to the "least common denominator" data structure - `IEnumerable<T>`.
[<AutoOpen>]
module VectorExtensions = 
  type IVector<'TValue> with
    /// Returns the data of the vector as a lazy sequence. (This preserves the 
    /// order of elements in the vector and so it also returns missing values.)
    member x.DataSequence = 
      match x.Data with
      | VectorData.Sequence s -> s
      | VectorData.SparseList s -> upcast s
      | VectorData.DenseList s -> Seq.map (fun v -> OptionalValue(v)) s

// --------------------------------------------------------------------------------------
// Types related to vectors that should not be exposed too directly
// --------------------------------------------------------------------------------------
namespace FSharp.DataFrame.Vectors

open FSharp.DataFrame
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Addressing

/// Represents a range inside a vector
type VectorRange = Address * Address

/// Representes a "variable" in the mini-DSL below
type VectorHole = int

/// Represent a transformation that is applied when combining two vectors
/// (because we are combining untyped `IVector` values, the transformation
/// is also untyped)
type IVectorValueTransform =
  /// Returns a function that combines two values stored in vectors into a new vector value.
  /// Although generic, this function will only be called with the `T` set to the
  /// type of vector that is being built. Since `VectorConstruction` is not generic,
  /// the type cannot be statically propagated.
  abstract GetFunction<'T> : unit -> (OptionalValue<'T> -> OptionalValue<'T> -> OptionalValue<'T>)

/// Specifies how to fill missing values in a vector (when using the 
/// `VectorConstruction.FillMissing` command). This can only fill missing
/// values using strategy that does not require access to index keys - 
/// either using constant or by propagating values.
type VectorFillMissing =
  | Direction of Direction
  | Constant of obj

/// A "mini-DSL" that describes construction of a vector. Vector can be constructed
/// from various range operations (relocate, drop, slicing, appending), by combination
/// of two vectors or by taking a vector from a list of variables.
///
/// Notably, vectors can only be constructed from other vectors of the same type 
/// (the `Combine` operation requires this - even though that one could be made more general).
/// This is an intentional choice to make the representation simpler.
///
/// Logically, when we apply some index operation, we should get back a polymorphic vector
/// construction (`\forall T. VectorConstruction<T>`) that can be applied to variuous 
/// different vector types. That would mean adding some more types, so we just model vector
/// construction as an untyped operation and the typing is resquired by the `Build` method
/// of the vector builder.
type VectorConstruction =
  /// When constructing vectors, we get an array of vectors to be used as "variables"
  /// - this element represent getting one of the variables.
  | Return of VectorHole

  /// Creates an empty vector of the requested type
  | Empty 

  /// Reorders elements of the vector. Carries a new required vector range and a list
  /// of relocations (each pair of addresses specifies that an element at a new address 
  /// should be filled with an element from an old address). THe addresses may be out of range!
  | Relocate of VectorConstruction * VectorRange * seq<Address * Address>

  /// Drop the specified range of addresses from the vector 
  /// and return a new vector that excludes the range
  | DropRange of VectorConstruction * VectorRange 

  /// Get the specified range of addresses from the vector and return it as a new vector
  | GetRange of VectorConstruction * VectorRange

  /// Append two vectors after each other
  | Append of VectorConstruction * VectorConstruction

  /// Combine two aligned vectors. The `IVectorValueTransform` object
  /// specifies how to merge values (in case there is a value at a given address
  /// in both of the vectors).
  | Combine of VectorConstruction * VectorConstruction * IVectorValueTransform

  /// Create a vector that has missing values filled using the specified direction
  /// (forward means that n-th value will contain (n-i)-th value where (n-i) is the
  /// first index that contains a value).
  | FillMissing of VectorConstruction * VectorFillMissing

  /// Apply a custom command to a vector - this can be used by special indices (e.g. index
  /// for a lazy vector) to provide a custom operations to be used. The first parameter
  /// is a list of sub-vectors to be combined (if as in e.g. `Append`) and the
  /// second argument is a function that will be called with evaluated vectors and is
  /// supposed to create the new vector.
  | CustomCommand of list<VectorConstruction> * (list<IVector> -> IVector)


/// Represents an object that can construct vector values by processing 
/// the "mini-DSL" representation `VectorConstruction`.
type IVectorBuilder = 
  /// Create a vector from an array containing values. The values may 
  /// still represent missing values and the vector should handle this.
  /// For example `Double.NaN` or `null` should be turned into a missing
  /// value in the returned vector.
  abstract Create : 'T[] -> IVector<'T>
  
  /// Create a vector from an array containing values that may be missing. 
  /// Even if a value is passed, it may be a missing value such as `Double.NaN`
  /// or `null`. The vector builder should hanlde this.
  abstract CreateMissing : OptionalValue<'T>[] -> IVector<'T>

  /// Apply a vector construction to a given vector. The second parameter
  /// is an array of arguments ("variables") that may be referenced from the
  /// `VectorConstruction` using the `Return 0` construct.
  abstract Build<'T> : VectorConstruction * IVector<'T>[] -> IVector<'T>