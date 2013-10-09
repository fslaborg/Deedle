namespace FSharp.DataFrame.Vectors.ArrayVector

/// --------------------------------------------------------------------------------------
/// ArrayVector - stores data of the vector in a continuous memory block. If the vector
/// contains missing values, then uses `OptionalValue<'T>[]`, otherwise uses just `'T[]`.
/// --------------------------------------------------------------------------------------

open FSharp.DataFrame
open FSharp.DataFrame.Addressing
open FSharp.DataFrame.Internal
open FSharp.DataFrame.Vectors

/// Internal representation of the ArrayVector. To make this more 
/// efficient, we distinguish between "sparse" vectors that have missing 
/// values and "dense" vectors without N/As.
type internal ArrayVectorData<'T> =
  | VectorOptional of OptionalValue<'T>[]
  | VectorNonOptional of 'T[]

// --------------------------------------------------------------------------------------

/// Implements a builder object (`IVectorBuilder`) for creating
/// vectors of type `ArrayVector<'T>`. This includes operations such as
/// appending, relocating values, creating vectors from arrays etc.
/// The vector builder automatically switches between the two possible
/// representations of the vector - when a missing value is present, it
/// uses `ArrayVectorData.VectorOptional`, otherwise it uses 
/// `ArrayVectorData.VectorNonOptional`.
type ArrayVectorBuilder() = 
  /// Instance of the vector builder
  static let vectorBuilder = ArrayVectorBuilder() :> IVectorBuilder
  
  /// A simple helper that creates IVector from ArrayVectorData
  let av data = ArrayVector(data) :> IVector<_>

  /// Treat vector as containing optionals
  let (|AsVectorOptional|) = function
    | VectorOptional d -> d
    | VectorNonOptional d -> Array.map (fun v -> OptionalValue v) d

  /// Builds a vector using the specified commands, ensures that the
  /// returned vector is ArrayVector (if no, it converts it) and then
  /// returns the internal representation of the vector
  member private builder.buildArrayVector<'T> (commands:VectorConstruction) (arguments:IVector<'T>[]) : ArrayVectorData<'T> = 
    let got = vectorBuilder.Build(commands, arguments)
    match got with
    | :? ArrayVector<'T> as av -> av.Representation
    | otherVector -> 
        match vectorBuilder.CreateMissing(Array.ofSeq otherVector.DataSequence) with
        | :? ArrayVector<'T> as av -> av.Representation
        | _ -> failwith "builder.buildArrayVector: Unexpected vector type"

  /// Provides a global access to an instance of the `ArrayVectorBuilder`
  static member Instance = vectorBuilder

  interface IVectorBuilder with
    member builder.Create(values) =
      // Check that there are no NaN values and create appropriate representation
      let hasNAs = MissingValues.containsNA values
      if hasNAs then av <| VectorOptional(MissingValues.createNAArray values)
      else av <| VectorNonOptional(values)

    member builder.CreateMissing(optValues) =
      // Check for both OptionalValue.Missing and OptionalValue.Value = NaN
      let hasNAs = MissingValues.containsMissingOrNA optValues
      if hasNAs then av <| VectorOptional(MissingValues.createMissingOrNAArray optValues)
      else av <| VectorNonOptional(optValues |> Array.map (fun v -> v.Value))

    /// Given a vector construction command(s) produces a new IVector
    /// (the result is typically ArrayVector, but this is not guaranteed)
    member builder.Build<'T>(command:VectorConstruction, arguments:IVector<'T>[]) = 
      match command with
      | Return vectorVar -> arguments.[vectorVar]
      | Empty -> vectorBuilder.Create [||]
      | Relocate(source, (IntAddress loRange, IntAddress hiRange), relocations) ->
          // Create a new array with specified size and move values from the
          // old array (source) to the new, according to 'relocations'
          let newData = Array.zeroCreate (hiRange - loRange + 1)
          match builder.buildArrayVector source arguments with 
          | VectorOptional data ->
              for IntAddress newIndex, IntAddress oldIndex in relocations do
                if oldIndex < data.Length && oldIndex >= 0 then
                  newData.[newIndex] <- data.[oldIndex]
          | VectorNonOptional data ->
              for IntAddress newIndex, IntAddress oldIndex in relocations do
                if oldIndex < data.Length && oldIndex >= 0 then
                  newData.[newIndex] <- OptionalValue(data.[oldIndex])
          vectorBuilder.CreateMissing(newData)

      | DropRange(source, (IntAddress loRange, IntAddress hiRange)) ->
          // Create a new array without the specified range. For Optional, call the 
          // builder recursively as this may turn Optional representation to NonOptional
          match builder.buildArrayVector source arguments with 
          | VectorOptional data -> 
              vectorBuilder.CreateMissing(Array.dropRange loRange hiRange data) 
          | VectorNonOptional data -> 
              VectorNonOptional(Array.dropRange loRange hiRange data) |> av

      | GetRange(source, (IntAddress loRange, IntAddress hiRange)) ->
          // Get the specified sub-range. For Optional, call the builder recursively 
          // as this may turn Optional representation to NonOptional
          if hiRange < loRange then VectorNonOptional [||] |> av else
          match builder.buildArrayVector source arguments with 
          | VectorOptional data -> 
              vectorBuilder.CreateMissing(data.[loRange .. hiRange])
          | VectorNonOptional data -> 
              VectorNonOptional(data.[loRange .. hiRange]) |> av

      | Append(first, second) ->
          // Convert both vectors to ArrayVectors and append them (this preserves
          // the kind of representation - Optional will stay Optional etc.)
          match builder.buildArrayVector first arguments, builder.buildArrayVector second arguments with
          | VectorNonOptional first, VectorNonOptional second -> 
              VectorNonOptional(Array.append first second) |> av
          | AsVectorOptional first, AsVectorOptional second ->
              VectorOptional(Array.append first second) |> av

      | Combine(left, right, op) ->
          // Convert both vectors to ArrayVectors and zip them
          match builder.buildArrayVector left arguments,builder.buildArrayVector right arguments with
          | AsVectorOptional left, AsVectorOptional right ->
              let merge = op.GetFunction<'T>()
              let filled = Array.init (max left.Length right.Length) (fun idx ->
                let lv = if idx >= left.Length then OptionalValue.Missing else left.[idx]
                let rv = if idx >= right.Length then OptionalValue.Missing else right.[idx]
                merge lv rv)
              vectorBuilder.CreateMissing(filled)

      | CustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          f vectors :?> IVector<_>

/// --------------------------------------------------------------------------------------

/// Vector that stores data in an array. The data is stored using the
/// `ArrayVectorData<'T>` type (discriminated union)
and ArrayVector<'T> internal (representation:ArrayVectorData<'T>) = 
  member internal vector.Representation = representation

  // To string formatting & equality support
  override vector.ToString() = VectorHelpers.prettyPrintVector vector
  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'T> as another -> Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() =
    vector.DataSequence |> Seq.structuralHash

  // Implement the untyped vector interface
  interface IVector with
    member x.ObjectSequence = 
      match representation with
      | VectorOptional opts -> opts |> Seq.map (OptionalValue.map box)
      | VectorNonOptional opts -> opts |> Seq.map (fun v -> OptionalValue(box v))
    member x.ElementType = typeof<'T>
    member x.SuppressPrinting = false

    member vector.GetObject(IntAddress index) = 
      match representation with
      | VectorOptional data when index < data.Length -> data.[index] |> OptionalValue.map box 
      | VectorNonOptional data when index < data.Length -> OptionalValue(box data.[index])
      | _ -> OptionalValue.Missing

  // Implement the typed vector interface
  interface IVector<'T> with
    member vector.GetValue(IntAddress index) = 
      match representation with
      | VectorOptional data when index < data.Length -> data.[index]
      | VectorNonOptional data when index < data.Length -> OptionalValue(data.[index])
      | _ -> OptionalValue.Missing
    member vector.Data = 
      match representation with 
      | VectorNonOptional data -> VectorData.DenseList (IReadOnlyList.ofArray data)
      | VectorOptional data -> VectorData.SparseList (IReadOnlyList.ofArray data)

    // A version of Select that can transform missing values to actual values (we always 
    // end up with array that may contain missing values, so use CreateMissing)
    member vector.SelectMissing<'TNewValue>(f) = 
      let isNA = MissingValues.isNA<'TNewValue>() 
      let flattenNA (value:OptionalValue<_>) = 
        if value.HasValue && isNA value.Value then OptionalValue.Missing else value
      let data = 
        match representation with
        | VectorNonOptional data ->
            data |> Array.map (fun v -> OptionalValue(v) |> f |> flattenNA)
        | VectorOptional data ->
            data |> Array.map (f >> flattenNA)
      ArrayVectorBuilder.Instance.CreateMissing(data)

    // Select function does not call 'f' on missing values.
    member vector.Select<'TNewValue>(f:'T -> 'TNewValue) = 
      (vector :> IVector<_>).SelectMissing(OptionalValue.map f)
