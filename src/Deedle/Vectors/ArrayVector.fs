namespace Deedle.Vectors.ArrayVector

/// --------------------------------------------------------------------------------------
/// ArrayVector - stores data of the vector in a continuous memory block. If the vector
/// contains missing values, then uses `OptionalValue<'T>[]`, otherwise uses just `'T[]`.
/// --------------------------------------------------------------------------------------

open Deedle
open Deedle.Addressing
open Deedle.Internal
open Deedle.Vectors
open Deedle.VectorHelpers

/// LinearIndex + ArrayVector use linear addressing (address is just an offset)
module Address = LinearAddress

/// Internal representation of the ArrayVector. To make this more 
/// efficient, we distinguish between "sparse" vectors that have missing 
/// values and "dense" vectors without N/As.
type internal ArrayVectorData<'T> =
  | VectorOptional of OptionalValue<'T>[]
  | VectorNonOptional of 'T[]
  member x.Length = 
    match x with VectorNonOptional n -> n.Length | VectorOptional n -> n.Length

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
  let asVectorOptional = function
    | VectorOptional d -> d
    | VectorNonOptional d -> Array.map (fun v -> OptionalValue v) d

  /// Treat vector as containing optionals
  let (|AsVectorOptional|) = asVectorOptional

  /// Builds a vector using the specified commands, ensures that the
  /// returned vector is ArrayVector (if no, it converts it) and then
  /// returns the internal representation of the vector
  member private builder.buildArrayVector<'T> (commands:VectorConstruction) (arguments:IVector<'T>[]) : ArrayVectorData<'T> = 
    let got = vectorBuilder.Build(Addressing.LinearAddressingScheme.Instance, commands, arguments)
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
    member builder.AsyncBuild<'T>(scheme, command:VectorConstruction, arguments:IVector<'T>[]) = async {
      // Check that the expected scheme matches what we can build
      if scheme <> Addressing.LinearAddressingScheme.Instance then
        failwith "ArrayVector.AsyncBuild: Can only build vectors with linear addressing scheme!"
      match command with
      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(scheme, v, arguments) :> IVector) vectors
          let! res = f vectors
          return res :?> IVector<_>
      | cmd ->  
          // Fall back to the synchronous mode for anything more complicated
          return (builder :> IVectorBuilder).Build<'T>(scheme, cmd, arguments) }

    /// Given a vector construction command(s) produces a new IVector
    /// (the result is typically ArrayVector, but this is not guaranteed)
    member builder.Build<'T>(scheme, command:VectorConstruction, arguments:IVector<'T>[]) = 
      // Check that the expected scheme matches what we can build
      if scheme <> Addressing.LinearAddressingScheme.Instance then
        failwith "ArrayVector.Build: Can only build vectors with linear addressing scheme!"
      match command with
      | Return vectorVar -> 
          // Ensure that the result has the right addressing scheme!
          match arguments.[vectorVar] with
          | linear when linear.AddressingScheme = scheme -> linear
          | otherVector -> vectorBuilder.CreateMissing(Array.ofSeq otherVector.DataSequence)

      | Empty 0L -> vectorBuilder.Create [||]
      | Empty size -> vectorBuilder.CreateMissing (Array.create (int size) OptionalValue.Missing)

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

      | DropRange(source, range) ->
          let built = builder.buildArrayVector source arguments
          match range.AsAbsolute(int64 built.Length) with
          | Choice1Of2(loRange, hiRange) ->
              // Create a new array without the specified range. For Optional, call the 
              // builder recursively as this may turn Optional representation to NonOptional
              let loRange, hiRange = Address.asInt loRange, Address.asInt hiRange
              match built with 
              | VectorOptional data -> 
                  vectorBuilder.CreateMissing(Array.dropRange loRange hiRange data) 
              | VectorNonOptional data -> 
                  VectorNonOptional(Array.dropRange loRange hiRange data) |> av
          
          | Choice2Of2 range ->
              // NOTE: This is never currently needed in Deedle 
              // (DropRange is only used when dropping a single item at the moment)
              failwith "DropRange does not support Custom ranges at he moment"

      | GetRange(source, range) ->
          let built = builder.buildArrayVector source arguments
          match range.AsAbsolute(int64 built.Length) with
          | Choice1Of2(loRange, hiRange) ->
              // Get the specified sub-range. For Optional, call the builder recursively 
              // as this may turn Optional representation to NonOptional
              let loRange, hiRange = Address.asInt loRange, Address.asInt hiRange
              if hiRange < loRange then VectorNonOptional [||] |> av else
              match built with 
              | VectorOptional data -> 
                  vectorBuilder.CreateMissing(data.[loRange .. hiRange])
              | VectorNonOptional data -> 
                  VectorNonOptional(data.[loRange .. hiRange]) |> av

          | Choice2Of2(indices) ->
              // Get vector with the specified indices. Optional may turn to 
              // NonOptional, but NonOptional will stay NonOptional
              match builder.buildArrayVector source arguments with 
              | VectorOptional data ->
                  [| for address in indices -> data.[Address.asInt address] |] |> vectorBuilder.CreateMissing 
              | VectorNonOptional data ->
                  [| for address in indices -> data.[Address.asInt address] |] |> VectorNonOptional |> av


      | Append(first, second) ->
          // Convert both vectors to ArrayVectors and append them (this preserves
          // the kind of representation - Optional will stay Optional etc.)
          match builder.buildArrayVector first arguments, builder.buildArrayVector second arguments with
          | VectorNonOptional first, VectorNonOptional second -> 
              VectorNonOptional(Array.append first second) |> av
          | AsVectorOptional first, AsVectorOptional second ->
              VectorOptional(Array.append first second) |> av


      | CombinedRelocations(count, relocs, op) ->
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
          let merge = op.GetFunction<'T>()
          let filled : OptionalValue<_>[] = Array.create (int count.Value) OptionalValue.Missing

          if op.IsMissingUnit then
              // If the Missing value is unit of the operation, we can start with 
              // Missing and skip applying the function for values not in the vector
              // (This gives notable speedup for merging of vectors)
              for vdata, vreloc in data do
                for newIndex, oldIndex in vreloc do
                  let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
                  if oldIndex < vdata.Length && oldIndex >= 0 then
                    filled.[newIndex] <- merge filled.[newIndex] vdata.[oldIndex]
          
          else                    
              // Handle first vector differently - just write the values into the
              // 'filled' array without merging; all values are initialized to missing
              let head, rest = List.head data, List.tail data
              let vdata, vreloc = head
              for newIndex, oldIndex in vreloc do
                let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
                if oldIndex < vdata.Length && oldIndex >= 0 then
                  filled.[newIndex] <- vdata.[oldIndex]

              // For "2 .. " vectors, merge present values; then iterate over all 
              // values that have not been accessed (typically when the vector is smaller)
              // and merge the existing value with missing - this is important e.g. 
              // because 1 + N/A = N/A
              let accessed = Array.create (int count.Value) false 
              for vdata, vreloc in rest do
                for newIndex, oldIndex in vreloc do
                  let newIndex, oldIndex = Address.asInt newIndex, Address.asInt oldIndex
                  if oldIndex < vdata.Length && oldIndex >= 0 then
                    accessed.[newIndex] <- true
                    filled.[newIndex] <- merge filled.[newIndex] vdata.[oldIndex]
                // Merge all values not accessed & reset the accessed array
                for i = 0 to accessed.Length - 1 do
                  if not accessed.[i] then filled.[i] <- merge filled.[i] OptionalValue.Missing
                  accessed.[i] <- false

          filled |> vectorBuilder.CreateMissing


      | Combine(length, vectors, VectorListTransform.Nary (:? IRowReaderTransform as irt)) ->
          // OPTIMIZATION: The `IRowReaderTransform` interface is a marker telling us that 
          // we are creating `IVector<obj`> where `obj` is a boxed `IVector<obj>` 
          // representing the row formed by all of the specified vectors combined.
          //
          // We short-circuit the default implementation (which actually allocates the 
          // vectors) and we create a vector of virtualized "row readers".
          let data = 
            vectors 
            |> List.map (fun v -> vectorBuilder.Build(scheme, v, arguments) :> IVector)
            |> Array.ofSeq

          // Using `createObjRowReader` to get a row reader for a specified address
          let frameData = vectorBuilder.Create data
          let getRow addr = createObjRowReader frameData vectorBuilder addr irt.ColumnAddressAt

          // Because Build is `IVector<'T>[] -> IVector<'T>`, there is some nasty boxing.
          // This case is only called with `'T = obj` and so we create `IVector<obj>` containing 
          // `obj = IVector<obj>` as the row readers (the caller in Rows then unbox this)
          let rowCount = int length.Value
          let rows = Array.init rowCount (fun a -> box (getRow (Address.ofInt a)))
          VectorNonOptional(rows) |> av |> unbox


      | Combine(length, vectors, op) ->
          // Handles all remaining Combine cases - construct the vectors to be combined 
          // recursively and then combine them using a `'T list -> 'T` function (which is
          // either the specified one or `List.reduce` applied to a binary function)
          let merge = op.GetFunction<'T>()
          let data = vectors |> List.map (fun v -> 
            asVectorOptional (builder.buildArrayVector v arguments))
          let filled = 
            Array.init (int length.Value) (fun idx ->
              data 
              |> List.map (fun v -> if idx > v.Length then OptionalValue.Missing else v.[idx]) 
              |> merge)  
          vectorBuilder.CreateMissing(filled)

      | CustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(scheme, v, arguments) :> IVector) vectors
          f vectors :?> IVector<_>

      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> vectorBuilder.Build(scheme, v, arguments) :> IVector) vectors
          Async.RunSynchronously(f vectors) :?> IVector<_>

/// --------------------------------------------------------------------------------------

/// Vector that stores data in an array. The data is stored using the
/// `ArrayVectorData<'T>` type (discriminated union)
and ArrayVector<'T> internal (representation:ArrayVectorData<'T>) = 
  member internal vector.Representation = representation

  // To string formatting & equality support
  override vector.ToString() = prettyPrintVector vector
  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'T> as another -> Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() =
    vector.DataSequence |> Seq.structuralHash

  // Implement the untyped vector interface
  interface IVector with
    member x.AddressingScheme = Addressing.LinearAddressingScheme.Instance
    member x.Length = 
      match representation with
      | VectorOptional opts -> int64 opts.Length
      | VectorNonOptional opts -> int64 opts.Length
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
    member vector.GetValue(address) = 
      let index = Address.asInt address
      match representation with
      | VectorOptional data when index < data.Length -> data.[index]
      | VectorNonOptional data when index < data.Length -> OptionalValue(data.[index])
      | _ -> OptionalValue.Missing
    member x.GetValueAtLocation(loc) = 
      (x :> IVector<_>).GetValue(loc.Address)
    member vector.Data = 
      match representation with 
      | VectorNonOptional data -> VectorData.DenseList (ReadOnlyCollection.ofArray data)
      | VectorOptional data -> VectorData.SparseList (ReadOnlyCollection.ofArray data)

    // A version of Select that can transform missing values to actual values (we always 
    // end up with array that may contain missing values, so use CreateMissing)
    member vector.Select<'TNewValue>(f) = 
      let flattenNA = MissingValues.flattenNA<'TNewValue>() 
      let data = 
        match representation with
        | VectorNonOptional data ->
            data |> Array.mapi (fun i v -> f (KnownLocation(Address.ofInt i, int64 i)) (OptionalValue v) |> flattenNA)
        | VectorOptional data ->
            data |> Array.mapi (fun i v -> f (KnownLocation(Address.ofInt i, int64 i)) v |> flattenNA)
      ArrayVectorBuilder.Instance.CreateMissing(data)

    // Conversion on array vectors does not deleay
    member vector.Convert(f, _) = (vector :> IVector<'T>).Select(f)