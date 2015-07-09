namespace Deedle.Vectors.Virtual

// ------------------------------------------------------------------------------------------------
// Virtual vector - implements a virtualized data storage for Deedle (together with 
// VirtualIndex.fs). The actual data store needs to implement an interface `IVirtualVectorSource`, 
// which represents essentially a key-value mapping with some lookup capabilities
// ------------------------------------------------------------------------------------------------

open System
open Deedle
open Deedle.Ranges
open Deedle.Addressing
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Internal
open System.Runtime.CompilerServices


/// A helper type used by non-generic `IVirtualVectorSource` to invoke generic 
/// operations that require generic `IVirtualVectorSource<'T>` as an argument.
type IVirtualVectorSourceOperation<'R> =
  /// Generic method that is invoked by the non-generic interface with the correct `'T`
  abstract Invoke<'T> : IVirtualVectorSource<'T> -> 'R


/// Non-generic part of the `IVirtualVectorSource<'V>` interface, which 
/// provides some basic information about the virtualized data source
and IVirtualVectorSource =
  /// Identifies the addressing scheme associated with this source. This should be shared
  /// by all sources for which the same addresses are valid (e.g. all columns in the same table)
  abstract AddressingSchemeID : string

  /// Returns the type of elements - essentially typeof<'V> for IVirtualVectorSource<'V>
  abstract ElementType : System.Type

  /// Returns the length of the source - by design, Big Deedle always 
  /// needs to know the length of the source (e.g. for binary search)
  abstract Length : int64

  /// Returns the addressing implementation associated with this vector source
  abstract AddressOperations : IAddressOperations
  
  /// Invoke a generic operation (wrapped in an interface) 
  /// that takes a generic version of this type
  abstract Invoke<'R> : IVirtualVectorSourceOperation<'R> -> 'R


/// Represents a data source for Big Deedle. The interface is used both as a representation
/// of data source for `VirtualVector` (this file) and `VirtualIndex` (another file). The 
/// index uses `Length` and `ValueAt` to perform binary search when looking for a key; the
/// vector simply provides an access to values using `ValueAt`. 
and IVirtualVectorSource<'V> = 
  inherit IVirtualVectorSource

  /// Returns the value at the specifid address. We assume that the address is in range 
  /// [0L, Length-1L] and that each location has a value (or has a missing value, but is valid)
  abstract ValueAt : IVectorLocation -> OptionalValue<'V>

  /// Find a range (continuous or a sequence of indices) such that all values in the range are 
  /// the specified value. This is used, for example, when filtering frame based on column
  /// value (say column "PClass" has a value "1").
  ///
  /// If the data source has some "clever" representation of the range, it can return
  /// `Custom of IVectorRange` - which is then passed to `GetSubVector`.
  abstract LookupRange : 'V -> RangeRestriction<Address>

  /// Find the address associated with the specified value. This is used by the 
  /// index and it has the same signature as `IIndex<'K>.Lookup` (see `Index.fs`).
  abstract LookupValue : 'V * Lookup * Func<Addressing.Address, bool> -> OptionalValue<'V * Addressing.Address> 

  /// Returns a virtual source for the specified range (used when performing splicing on the 
  /// frame/series, both using address or using keys - which are obtained using Lookup)
  abstract GetSubVector : RangeRestriction<Address> -> IVirtualVectorSource<'V>

  /// Merge the current source with a list of other sources
  /// (used by functions such as `Frame.merge` and `Frame.mergeAll`)
  abstract MergeWith : seq<IVirtualVectorSource<'V>> -> IVirtualVectorSource<'V>


/// Represents an addressing scheme associated to virtual vectors. The addresses
/// may be partitioned differently (for different data sources), so this carries
/// an "id" of the data source (to make sure we don't try to mix mismatching data
/// sources)
type VirtualAddressingScheme = 
  | VirtualAddressingScheme of string
  interface IAddressingScheme


// ------------------------------------------------------------------------------------------------
// RangeAddressOperations - when using Ranges<T> for merging/slicing, address ops are for free
// ------------------------------------------------------------------------------------------------


/// In BigDeedle, we often use `Ranges<'T>` to represent the address range obtained as a result
/// of slicing and merging frames & series. This implements `IAddressOperations` for `Ranges<'T>`.
///
/// ## Parameters
///  - `ranges` - ranges containing any key that we are wrapping
///  - `asAddress` - converts the key to an address
///  - `ofAddress` - converts address to a key
///
type RangesAddressOperations<'TKey when 'TKey : equality>
      ( ranges:Ranges<'TKey>, 
        asAddress:System.Func<'TKey, Address>,
        ofAddress:System.Func<Address, 'TKey> ) =

  /// Provides access to the underlying ranges  
  member x.Ranges = ranges

  // Equality is based on ranges and operations objects
  override x.GetHashCode() =
    hash (ranges.Ranges, ranges.Operations)
  override x.Equals(other) =
    match other with 
    | :? RangesAddressOperations<'TKey> as other -> 
        other.Ranges.Ranges = ranges.Ranges &&
        other.Ranges.Operations = ranges.Operations
    | _ -> false

  // Call Ranges functions and convert to/from address as needed
  interface IAddressOperations with
    member x.AdjustBy(addr, offset) = 
      asAddress.Invoke (ranges.Operations.IncrementBy(ofAddress.Invoke addr, offset))
    member x.AddressOf(offset) = asAddress.Invoke(ranges.KeyAtOffset(offset))
    member x.OffsetOf(addr) = ranges.OffsetOfKey(ofAddress.Invoke(addr))
    member x.FirstElement = asAddress.Invoke(ranges.FirstKey)
    member x.LastElement = asAddress.Invoke(ranges.LastKey)
    member x.Range = ranges.Keys |> Seq.map asAddress.Invoke 

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

/// Represents a vector location that calculates the offset using
/// address operations as needed (typically, we want to avoid this
/// because it might be slow)
type DelayedLocation(addr, addrOps:IAddressOperations) = 
  let mutable offs = None
  interface IVectorLocation with
    member x.Address = addr
    member x.Offset = 
      if offs.IsNone then offs <- Some(addrOps.OffsetOf(addr))
      offs.Value


/// Module that implements various helper operations over `IVirtualVectorSource` type
module VirtualVectorSource = 
  
  /// Marks IVirtualVectorSource that has been created by boxing another source
  type IBoxedVectorSource<'T> =
    abstract Source : IVirtualVectorSource<'T> 

  /// Marks IVirtualVectorSource that has been created by combining another sources
  type ICombinedVectorSource<'T> =
    abstract Sources : IVirtualVectorSource<'T> list

  /// Marks IVirtualVectorSource that has been created by mapping from source
  type IMappedVectorSource<'T, 'R> = 
    abstract Source : IVirtualVectorSource<'T>
    abstract Function : unit // TODO: Check that the applied function is the same


  /// Creates a new vector source that boxes all values
  /// The result also implements IBoxedVectorSource
  let rec boxSource (source:IVirtualVectorSource<'T>) =
    { new IVirtualVectorSource<obj> with
        member x.ValueAt(address) = source.ValueAt(address) |> OptionalValue.map box
        member x.LookupRange(search) = failwith "Search not implemented on boxed vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on boxed vector" 
        member x.GetSubVector(range) = boxSource (source.GetSubVector(range))
        member x.MergeWith(sources) = 
          let sources = sources |> List.ofSeq |> List.tryChooseBy (function
            | :? IBoxedVectorSource<'T> as src -> Some(src.Source)
            | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge with vectors that are not created by boxing."
          | Some sources -> boxSource (source.MergeWith(sources))
      
      interface IBoxedVectorSource<'T> with
        member x.Source = source

      interface IVirtualVectorSource with
        member x.AddressingSchemeID = source.AddressingSchemeID
        member x.ElementType = typeof<obj>
        member x.Length = source.Length
        member x.AddressOperations = source.AddressOperations 
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<obj>) }


  /// Creates a new vector that combines (zips) vectors using a given function
  /// The result also implements ICombinedVectorSource
  let rec combine (f:OptionalValue<'T> list -> OptionalValue<'R>) (sources:IVirtualVectorSource<'T> list) = 
    { new IVirtualVectorSource<'R> with
        member x.ValueAt(address) = f [ for s in sources -> s.ValueAt(address)  ]
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = combine f [ for s in sources -> s.GetSubVector(range) ]
        member x.MergeWith(sources) = 
          let sources = Seq.append [x] sources |> List.ofSeq |> List.tryChooseBy (function
            | :? ICombinedVectorSource<'T> as src -> Some(src.Sources |> Array.ofSeq)
            | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge frames or series not created by combine"
          | Some sources ->
              // Make sure that all sources are combinations of the same number of vectors 
              let length = sources |> Seq.uniqueBy Array.length 
              let toCombine =
                [ for i in 0 .. length - 1 ->
                    // Merge each of the original vector sources with the corresponding 
                    // vector source from the ICombinedVectorSource that we got as argument
                    let sh, st = 
                      match [ for s in sources -> s.[i] ] with 
                      | sh::st -> sh, st | _ -> failwith "Merge requires one or more sources" 
                    sh.MergeWith(st) ]
              combine f toCombine 
      interface ICombinedVectorSource<'T> with
        member x.Sources = sources
      interface IVirtualVectorSource with
        member x.AddressingSchemeID = sources |> Seq.uniqueBy (fun s -> s.AddressingSchemeID)
        member x.AddressOperations = sources |> Seq.uniqueBy (fun s -> s.AddressOperations)
        member x.ElementType = typeof<'R>
        member x.Length = sources |> Seq.uniqueBy (fun s -> s.Length) 
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<'R>) }


  /// Creates a new vector that transforms source values using a given function
  /// The result also implements IMappedVectorSource
  let rec map rev f (source:IVirtualVectorSource<'V>) = 
    
    /// 'rev' aka reverse lookup (converting back) is required by some operations
    /// this helper fails when 'rev' is not available and calls 'op' otherwise
    let withReverseLookup op = 
      match rev with 
      | None -> failwith "Cannot lookup on virtual vector without reverse lookup"
      | Some g -> op g

    /// Caputre function for handling NA values for more efficient processing
    let flattenNA = MissingValues.flattenNA<'TNew>()

    { new IVirtualVectorSource<'TNew> with
        member x.ValueAt(location) = 
          f location (source.ValueAt(location)) |> flattenNA

        member x.MergeWith(sources) = 
          let sources = sources |> List.ofSeq |> List.tryChooseBy (function
              | :? IMappedVectorSource<'V, 'TNew> as src -> Some(src.Source) | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge frames or series not created by combine"
          | Some sources -> map rev f (source.MergeWith(sources))

        member x.LookupRange(search) = 
          // If we have reverse mapping, we can call the original source
          // otherwise, we need to perform full scan over the indices
          match rev with
          | Some f -> source.LookupRange(f search)
          | None ->
              let scanFunc loc ov = 
                match f loc ov with
                | OptionalValue.Present(rv) -> Object.Equals(search, rv)
                | _ -> false
          
              let scanIndices = 
                source.AddressOperations.Range
                |> Seq.choosel (fun idx addr ->
                    let loc = KnownLocation(addr, idx)
                    if scanFunc loc (source.ValueAt(loc)) then Some(addr)
                    else None)
                |> Array.ofSeq

              RangeRestriction.ofSeq (int64 scanIndices.Length) scanIndices

        member x.LookupValue(v, l, c) = 
          // Lookup can only be performed if we have a revse mapping function
          // However, we no longer know the offset, so we need 'DelayedLocation' here
          withReverseLookup (fun g ->
              source.LookupValue(g v, l, c)
              |> OptionalValue.bind (fun (v, a) ->
                  f (DelayedLocation(a, source.AddressOperations)) (OptionalValue v)
                  |> OptionalValue.map (fun v -> v, a))  )

        member x.GetSubVector(range) = map rev f (source.GetSubVector(range))
      
      interface IMappedVectorSource<'V, 'TNew> with
        member x.Source = source
        member x.Function = ()

      interface IVirtualVectorSource with
        member x.AddressingSchemeID = source.AddressingSchemeID
        member x.AddressOperations = source.AddressOperations
        member x.ElementType = typeof<'TNew>
        member x.Length = source.Length
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<'TNew>) }

  /// Creates a "row reader vector" - that is a vector that provides source for the series
  /// returned by frame.Rows. This is combined from a number of sources, but we do not
  /// fully materialize anything to create the vector. The first parameter is a ctor
  /// for `VirtualVector` which is defined below in the file.
  let rec createRowReader 
      (vectorCtor:_ -> IVector) (builder:IVectorBuilder) (irt:IRowReaderTransform) 
      (vectors:IVector<IVector>) (sources:IVirtualVectorSource<'T> list) : IVirtualVectorSource<IVector<obj>> = 
    { new IVirtualVectorSource<IVector<obj>> with
        member x.ValueAt(loc) = 
          OptionalValue(RowReaderVector<_>(vectors, builder, loc.Address, irt.ColumnAddressAt) :> IVector<_>)
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = 
          let subSources = [ for s in sources -> s.GetSubVector(range) ]
          let subVectors = Vector.ofValues [ for s in subSources -> vectorCtor s ]
          createRowReader vectorCtor builder irt subVectors subSources
        member x.MergeWith(sources) = 
          // For every source, we get a list of sources that were used to produce it
          let sources = 
            Seq.append [x] sources |> List.ofSeq |> List.tryChooseBy (function
            | :? ICombinedVectorSource<'T> as src -> Some(src.Sources |> Array.ofSeq)
            | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge frames or series not created by combine"
          | Some sources ->
              // Make sure that all sources are combinations of the same number of vectors 
              let length = sources |> Seq.uniqueBy Array.length
              let toCombine =
                [ for i in 0 .. length - 1 ->
                    let sh,st = 
                      match [ for s in sources -> s.[i] ] with 
                      | sh::st -> sh, st | _ -> failwith "Merge requires one or more sources"
                    sh.MergeWith(st) ]
              let data = Vector.ofValues [ for s in toCombine -> vectorCtor s ]
              createRowReader vectorCtor builder irt data toCombine

      interface ICombinedVectorSource<'T> with
        member x.Sources = sources
      interface IVirtualVectorSource with
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<obj>)
        member x.ElementType = typeof<IVector<obj>>
        member x.Length = sources |> Seq.uniqueBy (fun s -> s.Length)
        member x.AddressOperations = sources |> Seq.uniqueBy (fun s -> s.AddressOperations)
        member x.AddressingSchemeID = sources |> Seq.uniqueBy (fun s -> s.AddressingSchemeID) }

// ------------------------------------------------------------------------------------------------
// VirtualVector - represents vector created from IVirtualVectorSource
// ------------------------------------------------------------------------------------------------

/// Creates an `IVector<'T>` implementation that provides operations for accessing
/// data in `IVirtualVectorSource`. This mostly just calls `ValueAt` to read data.
type VirtualVector<'V>(source:IVirtualVectorSource<'V>) = 

  /// Returns the wrapped source of the vector
  member vector.Source = source

  // Equality checks `DataSequence` (this is lazy, so it can fail before evaluating everything)
  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'V> as another -> Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() =
    vector.DataSequence |> Seq.structuralHash

  interface IVector with
    member val ElementType = typeof<'V>
    member vector.Invoke(site) = site.Invoke<'V>(vector)
    member vector.AddressingScheme = VirtualAddressingScheme(source.AddressingSchemeID) :> _
    member vector.Length = source.Length
    member vector.SuppressPrinting = false
    member vector.GetObject(addr) = 
      source.ValueAt(DelayedLocation(addr, source.AddressOperations)) |> OptionalValue.map box
    member vector.ObjectSequence = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> 
          source.ValueAt(KnownLocation(addr, idx)) |> OptionalValue.map box )
 
  interface IVector<'V> with
    member vector.GetValueAtLocation(loc) = 
      source.ValueAt(loc)
    member vector.GetValue(address) = 
      source.ValueAt(DelayedLocation(address, source.AddressOperations))
    member vector.Data = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> source.ValueAt(KnownLocation(addr, idx)))
      |> VectorData.Sequence

    // Use 'VirtualVectorSource.map' to transform the vector
    member vector.Select<'TNew>(f:IVectorLocation -> OptionalValue<'V> -> OptionalValue<'TNew>) = 
      VirtualVector(VirtualVectorSource.map None f source) :> _
    member vector.Convert(f, g) = 
      VirtualVector(VirtualVectorSource.map (Some g) (fun _ -> OptionalValue.map f) source) :> _


// ------------------------------------------------------------------------------------------------
// VirtualVectorBuilder - constructs VirtualVectors based on VectorConstrusruction specifications
// generated by VirtualIndexBuilder (or by ArrayIndexBuilder when materializing the vector)
// ------------------------------------------------------------------------------------------------

/// Implements a builder object (`IVectorBuilder`) for creating vectors of type `VirtualVector<'T>`.
/// This can do a few things without evaluating vectors (merging, slicing). For other operations
/// the builder needs to materialize the vector and call `ArrayVectorBuilder`.
type VirtualVectorBuilder() = 

  /// If the specified vector is boxed (IBoxedVector created by accessing frame.Rows), 
  /// then box the *source* and create new normal VirtualVector
  let rec unboxVector (vector:IVector<'V>) : IVector<'V> =
    match vector with
    | :? IBoxedVector as boxed -> 
        { new VectorCallSite<IVector<'V>> with
            member x.Invoke<'T>(typed:IVector<'T>) = 
              match typed with
              | :? VirtualVector<'T> as vt -> 
                  // We know that 'V = obj because IVector<'V> is IBoxedVector
                  unbox<IVector<'V>> (VirtualVector(VirtualVectorSource.boxSource vt.Source)) 
              | _ -> vector } 
        |> boxed.UnboxedVector.Invoke
    | vec -> vec

  /// Evaluate vector that is wrapped (and delayed)
  /// (this is used for example by `Rows` to evaluate the row series)
  let unwrapVector (vector:IVector<'T>) = 
    match vector with
    | :? IWrappedVector<'T> as w -> w.UnwrapVector() | v -> v

  // Base builder is array vector builder, used as a fallback
  let baseBuilder = VectorBuilder.Instance
  static let instance = VirtualVectorBuilder()
  
  /// Returns an instance of VirtualVectorBuilder  
  static member Instance = instance

  interface IVectorBuilder with

    // Create operations always create in-memory vector
    member builder.Create(values) = baseBuilder.Create(values)
    member builder.CreateMissing(optValues) = baseBuilder.CreateMissing(optValues)

    member builder.AsyncBuild(scheme, cmd, args) = 
      // If the required result is linearly-addressed, call base builder
      // otherwise we cannot perform the operation so fail
      if scheme = LinearAddressingScheme.Instance then
        baseBuilder.AsyncBuild<'T>(scheme, cmd, args)
      else
        failwith "VirtualVectorBuilder.AsyncBuild: Not supported for virtual vectors"
    
    member builder.Build<'T>(topLevelScheme, cmd, args) = 

      /// Succeeds when the required scheme is any or Linear
      let (|AnyOrLinear|_|) = function
        | None -> Some ()
        | Some scheme when scheme = LinearAddressingScheme.Instance -> Some()
        | _ -> None

      /// Succeeds when the required scheme is any or virtual
      let (|AnyOrVirtual|_|) : IAddressingScheme option -> _ = function
        | None -> Some ()
        | Some(:? VirtualAddressingScheme) -> Some()
        | _ -> None

      /// Recursively walks over the VectorConstruction command and builds the 
      /// vector. We also need to keep the required scheme - when this is linear,
      /// we return materialized vectors. For some operations (mainly Relocate), we do
      /// not care and so the scheme is optional
      let rec build schemeOpt cmd = 

        // Any recursive call to 'build' will also unwrap and unbox the result
        let build schemeOpt cmd = 
          unwrapVector (unboxVector (build schemeOpt cmd))

        match schemeOpt, cmd with 
        | AnyOrLinear, Relocate(source, length, relocations) ->
            // Create a new ArrayVector by collecting the specified indices 
            // (this can work on any source vector, as long as the indices are right)
            let source = build None source
            let newData = Array.zeroCreate (int length)
            for newIndex, oldAddress in relocations do
              let newIndex = LinearAddress.asInt newIndex
              newData.[newIndex] <- source.GetValue(oldAddress)
            baseBuilder.CreateMissing(newData)

        | schemeOpt, Return vectorVar ->             
            let vector = args.[vectorVar] 
            // If the vector has the right scheme (or we do not care), then return it
            if schemeOpt = None || schemeOpt = Some(vector.AddressingScheme) then vector
            // if we need to materialize it, then call the base builder
            elif schemeOpt = Some(LinearAddressingScheme.Instance) then 
              baseBuilder.CreateMissing(Array.ofSeq vector.DataSequence)
            else failwith "VirtualVectorBuilder.Build: Can only create linearly addressed vectors"

        | AnyOrVirtual, GetRange(source, range) ->
            match build schemeOpt source with
            | vector & (:? VirtualVector<'T> as source) when 
                  // Also make sure that the addressing scheme is the same (if it is specified)
                  schemeOpt = None || Some(vector.AddressingScheme) = schemeOpt ->
                let subSource = source.Source.GetSubVector(range)
                VirtualVector<'T>(subSource) :> IVector<'T>
            | _ -> 
                failwith "VirtualVectorBuilder.Build: GetRange - vector does not use matching virtual addressing"

        | AnyOrVirtual, Combine(count, vectors, ((VectorListTransform.Nary (:? IRowReaderTransform as irt)) as transform)) ->
            // OPTIMIZATION: The `IRowReaderTransform` interface is a marker telling us that 
            // we are creating `IVector<obj`> where `obj` is a boxed `IVector<obj>` 
            // representing the row formed by all of the specified vectors combined.
            //
            // We short-circuit the default implementation (which actually allocates the 
            // vectors) and we create a vector of virtualized "row readers".
            let built = vectors |> List.map (build schemeOpt)
            let virtualSources = built |> List.tryChooseBy (fun vec ->
              match vec with :? VirtualVector<'T> as v -> Some v.Source | _ -> None)
            match virtualSources with
            | Some sources ->
                let data = Vector.ofValues [ for v in built -> v :> IVector ]
                let newSource = 
                    VirtualVectorSource.createRowReader 
                      (fun s -> VirtualVector s :> _) builder irt data sources
            
                // Because Build is `IVector<'T>[] -> IVector<'T>`, there is some nasty boxing.
                // This case is only called with `'T = obj` and so we create `IVector<obj>` containing 
                // `obj = IVector<obj>` as the row readers (the caller in Rows then unbox this)
                VirtualVector(VirtualVectorSource.map None (fun _ -> OptionalValue.map box) newSource) |> unbox<IVector<'T>>
            | _ ->
              failwith "VirtualVectorBuilder.Build: Cannot return virtual vector from Combine"


        | AnyOrVirtual, Combine(length, sources, transform) ->
            // Build all sources and make sure they are all virtual
            let virtualSources =
              sources |> List.tryChooseBy (fun source -> 
                match build schemeOpt source with
                | :? VirtualVector<'T> as vv -> Some vv.Source | _ -> None)
            match virtualSources with
            | Some sources ->
                let func = transform.GetFunction<'T>()
                let newSource = VirtualVectorSource.combine func sources
                VirtualVector(newSource) :> _
            | _ ->
              failwith "VirtualVectorBuilder.Build: Cannot return virtual vector from Combine"

        | AnyOrVirtual, Append(first, second) ->
            // Merge sources of the two vectors, provided that they are both virtual
            match build schemeOpt first, build schemeOpt second with
            | (:? VirtualVector<'T> as first), (:? VirtualVector<'T> as second) ->
                let newSource = first.Source.MergeWith([second.Source])
                VirtualVector(newSource) :> _
            | _ ->
                failwith "VirtualVectorBuilder.Build: Cannot return virtual vector from Append"

        | _, CustomCommand(vectors, f) ->
            let vectors = List.map (fun v -> build schemeOpt v :> IVector) vectors
            f vectors :?> IVector<_>

        | _, AsyncCustomCommand(vectors, f) ->
            let vectors = List.map (fun v -> build schemeOpt v :> IVector) vectors
            Async.RunSynchronously(f vectors) :?> IVector<_>

        | AnyOrLinear, command ->
            // For any other command, we can use the 
            // base builder if we want linear vector back
            baseBuilder.Build(LinearAddressingScheme.Instance, command, args)

        | _ ->
            // If we wanted virtual vector back, then we cannot do that
            failwith "VirtualVectorBuilder.Build: Unexpected situation"

      // Start the recursive processing with initial required schema
      build (Some topLevelScheme) cmd
