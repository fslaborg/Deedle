namespace Deedle.Vectors.ArrayVector

/// --------------------------------------------------------------------------------------
/// ArrayVector - stores data of the vector in a continuous memory block. If the vector
/// contains missing values, then uses `OptionalValue<'T>[]`, otherwise uses just `'T[]`.
/// --------------------------------------------------------------------------------------

open Deedle
open Deedle.Addressing
open Deedle.Internal
open Deedle.Vectors

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
    /// (the result is typically ArrayVector, but this is not guaranteed)
    member builder.Build<'T>(command:VectorConstruction, arguments:IVector<'T>[]) = 
      match command with
      | Return vectorVar -> arguments.[vectorVar]
      | Empty -> vectorBuilder.Create [||]
      | FillMissing(source, dir) ->
          // The nice thing is that this is no-op on dense vectors!
          // On sparse vectors, we have some work to do...
          match builder.buildArrayVector source arguments, dir with
          | (VectorNonOptional _) as it, _ -> it |> av
          | VectorOptional data, VectorFillMissing.Direction dir -> 
              // There may still be NAs, if the first value is missing..
              let mutable prev = OptionalValue.Missing
              let mutable optionals = false
              let newData = Array.map id data

              if dir = Direction.Forward then
                for i in 0 .. newData.Length - 1 do
                  let it = newData.[i]
                  if it.HasValue then prev <- it
                  else newData.[i] <- prev
                  optionals <- optionals || (not newData.[i].HasValue)
              else 
                for i in newData.Length-1 .. -1 .. 0 do
                  let it = newData.[i]
                  if it.HasValue then prev <- it
                  else newData.[i] <- prev
                  optionals <- optionals || (not newData.[i].HasValue)

              // Return as optional/non-optional, depending on if we filled everything
              if optionals then av <| VectorOptional(newData)
              else av <| VectorNonOptional(newData |> Array.map OptionalValue.get)

          | VectorOptional data, VectorFillMissing.Constant (:? 'T as fill) -> 
              av <| VectorNonOptional(data |> Array.map (fun v -> if v.HasValue then v.Value else fill))
          | VectorOptional data, VectorFillMissing.Constant _ -> 
              invalidOp "Type mismatch - cannot fill values of the vector!"
              

      | Relocate(source, len, relocations) ->
          // Create a new array with specified size and move values from the
          // old array (source) to the new, according to 'relocations'
          let newData = Array.zeroCreate (int len)
          match builder.buildArrayVector source arguments with 
          | VectorOptional data ->
              for newIndex, oldIndex in relocations do
                let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
                if oldIndex < data.Length && oldIndex >= 0 then
                  newData.[newIndex] <- data.[oldIndex]
          | VectorNonOptional data ->
              for newIndex, oldIndex in relocations do
                let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
                if oldIndex < data.Length && oldIndex >= 0 then
                  newData.[newIndex] <- OptionalValue(data.[oldIndex])
          vectorBuilder.CreateMissing(newData)

      | DropRange(source, (loRange, hiRange)) ->
          // Create a new array without the specified range. For Optional, call the 
          // builder recursively as this may turn Optional representation to NonOptional
          let loRange, hiRange = Address.asInt loRange, Address.asInt hiRange
          match builder.buildArrayVector source arguments with 
          | VectorOptional data -> 
              vectorBuilder.CreateMissing(Array.dropRange loRange hiRange data) 
          | VectorNonOptional data -> 
              VectorNonOptional(Array.dropRange loRange hiRange data) |> av

      | GetRange(source, (loRange, hiRange)) ->
          // Get the specified sub-range. For Optional, call the builder recursively 
          // as this may turn Optional representation to NonOptional
          let loRange, hiRange = Address.asInt loRange, Address.asInt hiRange
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

      | VectorHelpers.CombinedRelocations(relocs, op) ->
          // OPTIMIZATION: Matches when we want to combine N vectors (as below) but
          // each vector is specified by a Relocate construction. In that case, we do
          // not need to build intermediate relocated vectors, but can directly build
          // the final vector (handling relocations during the process)
          // (This is useful when merging/aligning series or joining frames)
          //
          // NOTE: This can also only be used when the operation can be specified as a
          // binary function (as we add vectors as we go, rather than building full list)
          let data = relocs |> List.map (fun (v, _, r) -> 
            (|AsVectorOptional|) (builder.buildArrayVector v arguments), r)
          let merge = op.GetBinaryFunction<'T>().Value // IsSome=true is checked by CombinedRelocations 
          let count = relocs |> List.map (fun (_, l, _) -> l) |> List.max
          let filled = Array.create (int count) OptionalValue.Missing
          
          for vdata, vreloc in data do
            for newIndex, oldIndex in vreloc do
              let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
              if oldIndex < vdata.Length && oldIndex >= 0 then
                filled.[newIndex] <- merge (filled.[newIndex], vdata.[oldIndex])
          vectorBuilder.CreateMissing(filled)

      | CombineN(vectors, op) ->
          let data = 
            vectors 
            |> List.map (fun v -> builder.buildArrayVector v arguments) 
            |> List.map (function AsVectorOptional o -> o)

          let merge = op.GetFunction<'T>()
          let filled = Array.init (data |> List.map (fun v -> v.Length) |> List.reduce max) (fun idx ->
            data 
            |> List.map (fun v -> if idx > v.Length then OptionalValue.Missing else v.[idx]) 
            |> merge)  

          vectorBuilder.CreateMissing(filled)

      | CustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          f vectors :?> IVector<_>

      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(v, arguments) :> IVector) vectors
          Async.RunSynchronously(f vectors) :?> IVector<_>

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
    member vector.Invoke(site) = site.Invoke<'T>(vector)

    member vector.GetObject(index) = 
      let index = Address.asInt index
      match representation with
      | VectorOptional data when index < data.Length -> data.[index] |> OptionalValue.map box 
      | VectorNonOptional data when index < data.Length -> OptionalValue(box data.[index])
      | _ -> OptionalValue.Missing

  // Implement the typed vector interface
  interface IVector<'T> with
    member vector.GetValue(index) = 
      let index = Address.asInt index
      match representation with
      | VectorOptional data when index < data.Length -> data.[index]
      | VectorNonOptional data when index < data.Length -> OptionalValue(data.[index])
      | _ -> OptionalValue.Missing
    member vector.Data = 
      match representation with 
      | VectorNonOptional data -> VectorData.DenseList (ReadOnlyCollection.ofArray data)
      | VectorOptional data -> VectorData.SparseList (ReadOnlyCollection.ofArray data)

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
