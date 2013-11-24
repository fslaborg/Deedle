namespace Deedle.Vectors.SolidVector

/// --------------------------------------------------------------------------------------
/// SolidVector - stores data of the vector in a Solid.Vector<'T> data structure. 
/// --------------------------------------------------------------------------------------

open Deedle
open Deedle.Addressing
open Deedle.Internal
open Deedle.Vectors

/// Internal representation of the SolidVector. To make this more 
/// efficient, we distinguish between "sparse" vectors that have missing 
/// values and "dense" vectors without N/As.
type internal SolidVectorData<'T> =
  | VectorOptional of Solid<OptionalValue<'T>>
  | VectorNonOptional of Solid<'T>

// --------------------------------------------------------------------------------------

/// Implements a builder object (`IVectorBuilder`) for creating
/// vectors of type `SolidVector<'T>`. This includes operations such as
/// appending, relocating values, creating vectors from arrays etc.
/// The vector builder automatically switches between the two possible
/// representations of the vector - when a missing value is present, it
/// uses `SolidVectorData.VectorOptional`, otherwise it uses 
/// `SolidVectorData.VectorNonOptional`.
type SolidVectorBuilder() = 
  /// Instance of the vector builder
  static let vectorBuilder = SolidVectorBuilder() :> IVectorBuilder
  
  /// A simple helper that creates IVector from SolidVectorData
  let sv data = SolidVector(data) :> IVector<_>

  /// Treat vector as containing optionals
  let (|AsVectorOptional|) = function
    | VectorOptional d -> d
    | VectorNonOptional d -> Solid.select (fun v -> OptionalValue v) d

  /// Builds a vector using the specified commands, ensures that the
  /// returned vector is SolidVector (if no, it converts it) and then
  /// returns the internal representation of the vector
  member private builder.buildSolidVector<'T> (commands:VectorConstruction) (arguments:IVector<'T>[]) : SolidVectorData<'T> = 
    let got = vectorBuilder.Build(commands, arguments)
    match got with
    | :? SolidVector<'T> as sv -> sv.Representation
    | otherVector -> 
        match vectorBuilder.CreateMissing(Array.ofSeq otherVector.DataSequence) with
        | :? SolidVector<'T> as sv -> sv.Representation
        | _ -> failwith "builder.buildSolidVector: Unexpected vector type"

  /// Provides a global access to an instance of the `SolidVectorBuilder`
  static member Instance = vectorBuilder

  interface IVectorBuilder with
    member builder.Create(values) =
      // Check that there are no NaN values and create appropriate representation
      let hasNAs = MissingValues.containsNA values
      if hasNAs then sv <| VectorOptional(Solid.ofArray (MissingValues.createNAArray values))
      else sv <| VectorNonOptional(Solid.ofArray values)

    member builder.CreateMissing(optValues) =
      // Check for both OptionalValue.Missing and OptionalValue.Value = NaN
      let hasNAs = MissingValues.containsMissingOrNA optValues
      if hasNAs then sv <| VectorOptional(Solid.ofArray (MissingValues.createMissingOrNAArray optValues))
      else sv <| VectorNonOptional(Solid.ofArray (optValues |> Array.map (fun v -> v.Value)))

    /// Asynchronous version - limited implementation for AsyncMaterialize
    member builder.AsyncBuild<'T>(command:VectorConstruction, arguments:IVector<'T>[]) = async {
      match command with
      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          let! res = f vectors
          return res :?> IVector<_>
      | cmd ->  
          // Fall back to the synchronous mode for anything more complicated
          return (builder :> IVectorBuilder).Build<'T>(cmd, arguments) }

    /// Given a vector construction command(s) produces a new IVector
    /// (the result is typically SolidVector, but this is not guaranteed)
    member builder.Build<'T>(command:VectorConstruction, arguments:IVector<'T>[]) = 
      match command with
      | Return vectorVar -> arguments.[vectorVar]
      | Empty -> vectorBuilder.Create [||]
      | FillMissing(source, dir) ->
          // The nice thing is that this is no-op on dense vectors!
          // On sparse vectors, we have some work to do...
          match builder.buildSolidVector source arguments, dir with
          | (VectorNonOptional _) as it, _ -> it |> sv
          | VectorOptional data, VectorFillMissing.Direction dir -> 
              // There may still be NAs, if the first value is missing..
              let mutable prev = OptionalValue.Missing
              let optionals : bool ref = ref false
              let rep (prev:Solid<OptionalValue<'T>>) (curr:OptionalValue<'T>) = 
                let value = if curr.HasValue then curr else (if prev.IsEmpty then OptionalValue.Missing else prev.Last)
                optionals := !optionals || (not curr.HasValue)
                prev <+ value

              let newData = 
                match dir with
                | Direction.Forward -> Seq.fold rep Solid.empty data
                | Direction.Backward -> (Seq.fold rep Solid.empty data.Backward).Backward |> Solid.ofSeq // nice property of Solid!             
                | _ -> failwith "Wrong direction"

              // Return as optional/non-optional, depending on if we filled everything
              if !optionals then sv <| VectorOptional(newData)
              else sv <| VectorNonOptional(newData |> Solid.select OptionalValue.get)

          | VectorOptional data, VectorFillMissing.Constant (:? 'T as fill) -> 
              sv <| VectorNonOptional(data |> Solid.select (fun v -> if v.HasValue then v.Value else fill))
          | VectorOptional data, VectorFillMissing.Constant _ -> 
              invalidOp "Type mismatch - cannot fill values of the vector!"
              

      | Relocate(source, (IntAddress loRange, IntAddress hiRange), relocations) ->
          // Create a new array with specified size and move values from the
          // old array (source) to the new, according to 'relocations'
          let newData = Array.zeroCreate (hiRange - loRange + 1)
          match builder.buildSolidVector source arguments with 
          | VectorOptional data ->
              for IntAddress newIndex, IntAddress oldIndex in relocations do
                if oldIndex < data.Count && oldIndex >= 0 then
                  newData.[newIndex] <- data.[oldIndex]
          | VectorNonOptional data ->
              for IntAddress newIndex, IntAddress oldIndex in relocations do
                if oldIndex < data.Count && oldIndex >= 0 then
                  newData.[newIndex] <- OptionalValue(data.[oldIndex])
          vectorBuilder.CreateMissing(newData)

      | DropRange(source, (IntAddress loRange, IntAddress hiRange)) ->
          // Create a new array without the specified range. For Optional, call the 
          // builder recursively as this may turn Optional representation to NonOptional
          match builder.buildSolidVector source arguments with 
          | VectorOptional data -> 
              vectorBuilder.CreateMissing(Array.dropRange loRange hiRange (data.ToArray())) // TODO highly inefficient, but rarely need to drop range in Solid
          | VectorNonOptional data -> 
              VectorNonOptional(Solid.ofArray (Array.dropRange loRange hiRange (data.ToArray()))) |> sv

      | GetRange(source, (IntAddress loRange, IntAddress hiRange)) ->
          // Get the specified sub-range. For Optional, call the builder recursively 
          // as this may turn Optional representation to NonOptional
          if hiRange < loRange then VectorNonOptional Solid.empty |> sv else
          match builder.buildSolidVector source arguments with 
          | VectorOptional data -> 
              vectorBuilder.CreateMissing(data.ToArray().[loRange .. hiRange])
          | VectorNonOptional data -> 
              VectorNonOptional(Solid.ofArray (data.ToArray().[loRange .. hiRange])) |> sv

      | Append(first, second) ->
          // Convert both vectors to SolidVectors and append them (this preserves
          // the kind of representation - Optional will stay Optional etc.)
          match builder.buildSolidVector first arguments, builder.buildSolidVector second arguments with
          | VectorNonOptional first, VectorNonOptional second -> 
              VectorNonOptional(first.AddLastRange(second)) |> sv
          | AsVectorOptional first, AsVectorOptional second ->
              VectorOptional(first.AddLastRange(second)) |> sv

      | Combine(left, right, op) ->
          // Convert both vectors to SolidVectors and zip them
          match builder.buildSolidVector left arguments,builder.buildSolidVector right arguments with
          | AsVectorOptional left, AsVectorOptional right ->
              let merge = op.GetFunction<'T>()
              let filled = Array.init (max left.Count right.Count) (fun idx ->
                let lv = if idx >= left.Count then OptionalValue.Missing else left.[idx]
                let rv = if idx >= right.Count then OptionalValue.Missing else right.[idx]
                merge lv rv)
              vectorBuilder.CreateMissing(filled)

      | CustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          f vectors :?> IVector<_>

      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          Async.RunSynchronously(f vectors) :?> IVector<_>

/// --------------------------------------------------------------------------------------

/// Vector that stores data in an array. The data is stored using the
/// `SolidVectorData<'T>` type (discriminated union)
and SolidVector<'T> internal (representation:SolidVectorData<'T>) = 
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
      | VectorOptional data when index < data.Count -> data.[index] |> OptionalValue.map box 
      | VectorNonOptional data when index < data.Count -> OptionalValue(box data.[index])
      | _ -> OptionalValue.Missing

  // Implement the typed vector interface
  interface IVector<'T> with
    member vector.GetValue(IntAddress index) = 
      match representation with
      | VectorOptional data when index < data.Count -> data.[index]
      | VectorNonOptional data when index < data.Count -> OptionalValue(data.[index])
      | _ -> OptionalValue.Missing
    member vector.Data = 
      match representation with 
      | VectorNonOptional data -> VectorData.DenseList data
      | VectorOptional data -> VectorData.SparseList data

    // A version of Select that can transform missing values to actual values (we always 
    // end up with array that may contain missing values, so use CreateMissing)
    member vector.SelectMissing<'TNewValue>(f) = 
      let isNA = MissingValues.isNA<'TNewValue>() 
      let flattenNA (value:OptionalValue<_>) = 
        if value.HasValue && isNA value.Value then OptionalValue.Missing else value
      let data = 
        match representation with
        | VectorNonOptional data ->
            data |> Solid.select (fun v -> OptionalValue(v) |> f |> flattenNA)
        | VectorOptional data ->
            data |> Solid.select  (f >> flattenNA)
      SolidVectorBuilder.Instance.CreateMissing(data.ToArray())

    // Select function does not call 'f' on missing values.
    member vector.Select<'TNewValue>(f:'T -> 'TNewValue) = 
      (vector :> IVector<_>).SelectMissing(OptionalValue.map f)
