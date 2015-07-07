namespace Deedle.Vectors.Virtual

// ------------------------------------------------------------------------------------------------
// Virtual vector - implements a virtualized data storage for Deedle (together with 
// VirtualIndex.fs). The actual data store needs to implement an interface `IVirtualVectorSource`, 
// which represents essentially a key-value mapping with some lookup capabilities
// ------------------------------------------------------------------------------------------------

open System
open Deedle
open Deedle.Addressing
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Internal
open System.Runtime.CompilerServices

/// Various BigDeedle implementations can use different schemes for working with addresses 
/// (for example, address can be just a global offset, or it can be pair of `int32` values
/// that store partition and offset in a partition). This interface represents a specific
/// address range and abstracts operations that BigDeedle needs to perform on addresses
/// (within the specified range)
type IAddressOperations =
  /// Returns the first address of the range
  abstract FirstElement : Address

  /// Returns the last address of the range
  abstract LastElement : Address

  /// Returns a sequence that iterates over `FirstElement .. LastElement`
  abstract Range : seq<Address>

  /// Given an address, return the absolute offset of the address in the range
  /// This might be tricky for partitioned ranges. For example if you have two 
  /// partitions with 10 values addressed by (0,0)..(0,9); (1,0)..(1,9), the the
  /// offset of address (1, 5) is 15.
  abstract OffsetOf : Address -> int64

  /// Return the address of a value at the specified absolute offset.
  /// (See the comment for `OffsetOf` for more info about partitioning)
  abstract AddressOf : int64 -> Address


/// A helper type used by non-generic `IVirtualVectorSource` to invoke generic 
/// operations that require generic `IVirtualVectorSource<'T>` as an argument.
type IVirtualVectorSourceOperation<'R> =
  /// Generic method that is invoked by the non-generic interface with the correct `'T`
  abstract Invoke<'T> : IVirtualVectorSource<'T> -> 'R


/// Non-generic part of the `IVirtualVectorSource<'V>` interface, which 
/// provides some basic information about the virtualized data source
and IVirtualVectorSource =
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

// ------------------------------------------------------------------------------------------------
// Common utilities
// ------------------------------------------------------------------------------------------------

open Deedle.Ranges

/// In BigDeedle, we often use `Ranges<'T>` to represent the address range obtained as a result
/// of slicing and merging frames & series. This implements `IAddressOperations` for `Ranges<'T>`.
type RangesAddressOperations<'TKey when 'TKey : equality>
      ( ranges:Ranges<'TKey>, 
        asAddress:System.Func<'TKey, Address>,
        ofAddress:System.Func<Address, 'TKey> ) =
  
  member x.Ranges = ranges

  override x.GetHashCode() =
    hash (ranges.Ranges, ranges.Operations)

  override x.Equals(other) =
    match other with 
    | :? RangesAddressOperations<'TKey> as other -> 
        other.Ranges.Ranges = ranges.Ranges &&
        other.Ranges.Operations = ranges.Operations
    | _ -> false

  interface IAddressOperations with
    member x.AddressOf(offset) = asAddress.Invoke(ranges.KeyAtOffset(offset))
    member x.OffsetOf(addr) = ranges.OffsetOfKey(ofAddress.Invoke(addr))
    member x.FirstElement = asAddress.Invoke(ranges.FirstKey)
    member x.LastElement = asAddress.Invoke(ranges.LastKey)
    member x.Range = ranges.Keys |> Seq.map asAddress.Invoke 

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

module Location =
  let delayed(addr, addrOps:IAddressOperations) = 
    let offs = ref None
    { new IVectorLocation with
        member x.Address = addr
        member x.Offset = 
          if offs.Value.IsNone then offs := Some(addrOps.OffsetOf(addr))
          offs.Value.Value }

module VirtualVectorSource = 
  type IBoxedVectorSource<'T> =
    abstract Source : IVirtualVectorSource<'T> 

  type ICombinedVectorSource<'T> =
    abstract Sources : IVirtualVectorSource<'T> list
    abstract Function : unit // TODO: Check that the applied function is the same

  type IMappedVectorSource<'T, 'R> = 
    abstract Source : IVirtualVectorSource<'T>
    abstract Function : unit // TODO: Check that the applied function is the same

  let rec boxSource (source:IVirtualVectorSource<'T>) =
    { new IVirtualVectorSource<obj> with
        member x.ValueAt(address) = source.ValueAt(address) |> OptionalValue.map box
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = boxSource (source.GetSubVector(range))
        member x.MergeWith(sources) = 
          let sources = 
            sources |> List.ofSeq |> List.tryChooseBy (function
            | :? IBoxedVectorSource<'T> as src -> Some(src.Source)
            | _ -> None)
          match sources with 
          | None -> failwith "Cannot box frames or series not created by combine"
          | Some sources ->
              boxSource (source.MergeWith(sources))
      
      interface IBoxedVectorSource<'T> with
        member x.Source = source
      interface IVirtualVectorSource with
        member x.ElementType = typeof<obj>
        member x.Length = source.Length
        member x.AddressOperations = source.AddressOperations 
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<obj>) }

  let rec combine (f:OptionalValue<'T> list -> OptionalValue<'R>) (sources:IVirtualVectorSource<'T> list) : IVirtualVectorSource<'R> = 
    { new IVirtualVectorSource<'R> with
        member x.ValueAt(address) = f [ for s in sources -> s.ValueAt(address)  ]
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = combine f [ for s in sources -> s.GetSubVector(range) ]
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
              let length = sources |> Seq.map Array.length |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a)
              let toCombine =
                [ for i in 0 .. length - 1 ->
                    let sh,st = match [ for s in sources -> s.[i] ] with sh::st -> sh, st | _ -> failwith "Merge requires one or more sources"  // ....
                    sh.MergeWith(st) ]
              combine f toCombine 
      interface ICombinedVectorSource<'T> with
        member x.Sources = sources
        member x.Function = () 
      interface IVirtualVectorSource with
        member x.AddressOperations = 
          sources 
          |> Seq.map (fun s -> s.AddressOperations)
          |> Seq.reduce (fun a b ->  
              if a <> b then failwith "Address mismatch" else a)
        member x.ElementType = typeof<'R>
        member x.Length = sources |> Seq.map (fun s -> s.Length) |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a) 
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<'R>)
    }

  let rec map rev f (source:IVirtualVectorSource<'V>) = 
    let withReverseLookup op = 
      match rev with 
      | None -> failwith "Cannot lookup on virtual vector without reverse lookup"
      | Some g -> op g

    let flattenNA = MissingValues.flattenNA<'TNew>()

    { new IVirtualVectorSource<'TNew> with
        member x.ValueAt(location) = 
          f location (source.ValueAt(location))
          |> flattenNA

        member x.MergeWith(sources) = 
          let sources = sources |> List.ofSeq |> List.tryChooseBy (function
              | :? IMappedVectorSource<'V, 'TNew> as src -> Some(src.Source) | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge frames or series not created by combine"
          | Some sources ->
              map rev f (source.MergeWith(sources)) // TODO: Check that the function is the same

        member x.LookupRange(search) = 
          let scanFunc loc ov = 
              match f loc ov with
              | OptionalValue.Present(rv) -> Object.Equals(search, rv) // TODO: Object.Equals is not so good here
              | _ -> false
          
          let scanIndices : Address array = 
            source.AddressOperations.Range
            |> Seq.choosel (fun idx addr ->
                let loc = Location.known(addr, idx)
                if scanFunc loc (source.ValueAt(loc)) then Some(addr)
                else None)
            |> Array.ofSeq

          { new IRangeRestriction<Address> with
              member x.Count = scanIndices |> Seq.length |> int64 // TODO: SLOW!
            interface seq<Address> with 
              member x.GetEnumerator() = (scanIndices :> seq<_>).GetEnumerator() // TODO: SLow
            interface System.Collections.IEnumerable with
              member x.GetEnumerator() = scanIndices.GetEnumerator() } //TODO: Slow
          |> RangeRestriction.Custom

        member x.LookupValue(v, l, c) = 
          withReverseLookup (fun g ->
              source.LookupValue(g v, l, c)
              |> OptionalValue.bind (fun (v, a) ->  // TODO: Make me nice and readable!
                  f (Location.delayed(a, source.AddressOperations)) (OptionalValue v)
                  |> OptionalValue.map (fun v -> v, a))  )

        member x.GetSubVector(range) = map rev f (source.GetSubVector(range))
      
      interface IMappedVectorSource<'V, 'TNew> with
        member x.Source = source
        member x.Function = ()
      interface IVirtualVectorSource with
        member x.AddressOperations = source.AddressOperations
        member x.ElementType = typeof<'TNew>
        member x.Length = source.Length
        member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<'TNew>) }


type VirtualVector<'V>(source:IVirtualVectorSource<'V>) = 
  member vector.Source = source

  override vector.Equals(another) = 
    match another with
    | null -> false
    | :? IVector<'V> as another -> Seq.structuralEquals vector.DataSequence another.DataSequence
    | _ -> false
  override vector.GetHashCode() =
    vector.DataSequence |> Seq.structuralHash

  interface IVector with
    member val ElementType = typeof<'V>
    member vector.Length = source.Length
    member vector.SuppressPrinting = false
    member vector.GetObject(addr) = source.ValueAt(Location.delayed(addr, source.AddressOperations)) |> OptionalValue.map box
    member vector.ObjectSequence = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> source.ValueAt(Location.known(addr, idx)) |> OptionalValue.map box )

    member vector.Invoke(site) = site.Invoke<'V>(vector)
  
  interface IVector<'V> with
    member vector.GetValue(address) = source.ValueAt(Location.delayed(address, source.AddressOperations))
    member x.GetValueAtLocation(loc) = source.ValueAt(loc)
    member vector.Data = 
      source.AddressOperations.Range
      |> Seq.mapl (fun idx addr -> source.ValueAt(Location.known(addr, idx)))
      |> VectorData.Sequence

    member vector.Select<'TNew>(f:IVectorLocation -> OptionalValue<'V> -> OptionalValue<'TNew>) = 
      VirtualVector(VirtualVectorSource.map None f source) :> _
    member vector.Convert(f, g) = 
      VirtualVector(VirtualVectorSource.map (Some g) (fun _ -> OptionalValue.map f) source) :> _

module VirtualVectorHelpers = 
  let rec unboxVector (vector:IVector<'V>) : IVector<'V> =
    match vector with
    | :? IBoxedVector as boxed -> 
        { new VectorCallSite<IVector<'V>> with
            member x.Invoke<'T>(typed:IVector<'T>) = 
              match typed with
              | :? VirtualVector<'T> as vt -> unbox<IVector<'V>> (VirtualVector(VirtualVectorSource.boxSource vt.Source)) // 'V = obj
              | _ -> vector } 
        |> boxed.UnboxedVector.Invoke
    | vec -> vec

type VirtualVectorBuilder() = 
  let baseBuilder = VectorBuilder.Instance
  static let instance = VirtualVectorBuilder()
  
  let build cmd args = (instance :> IVectorBuilder).Build(cmd, args)
  static member Instance = instance

  interface IVectorBuilder with
    member builder.Create(values) = baseBuilder.Create(values)
    member builder.CreateMissing(optValues) = baseBuilder.CreateMissing(optValues)
(*
    member builder.InitMissing<'T>(size, f) = 
      let isNa = MissingValues.isNA<'T> ()
      let rec createSource lo hi =
        { new IVirtualVectorSource<'T> with
            member x.ValueAt(idx) = 
              let v = f (lo + idx)
              if v.HasValue && not (isNa v.Value) then v
              else OptionalValue.Missing 
            member x.LookupValue(v, l, c) = failwith "cannot lookup on vector created by init"
            member x.LookupRange(lookup) = 
              match lookup with 
              | LookupKind.Scan sf -> 
                  let scanIndices = 
                    seq {
                      for i in 0L .. size - 1L do // TODO: .. is slow
                        let v = f (Address.ofInt64 i)
                        if sf (Address.ofInt64 i) v then 
                          yield i } |> Array.ofSeq

                  { new IVectorRange with
                      member x.Count = scanIndices |> Seq.length |> int64 // TODO: SLOW!
                    interface seq<int64> with 
                      member x.GetEnumerator() = (scanIndices :> seq<_>).GetEnumerator() // TODO: SLow
                    interface System.Collections.IEnumerable with
                      member x.GetEnumerator() = scanIndices.GetEnumerator() } //TODO: Slow
                  |> VectorRange.Custom

              | _ -> failwith "cannot lookup on vector created by init"

            member x.GetSubVector(sub) =
              match sub with 
              | Range(nlo, nhi) -> createSource (lo+nlo) (lo+nhi)
              | Custom indices -> failwith "cannot create subvector from custom inidices"

          interface IVirtualVectorSource with
            member x.ElementType = typeof<'T>
            member x.GetSubVector(sub) = (x :?> IVirtualVectorSource<'T>).GetSubVector(sub) :> _
            member x.Length = (hi-lo+1L) }
      VirtualVector(createSource 0L (size-1L)) :> _
*)
    member builder.AsyncBuild(cmd, args) = 
      failwith "VirtualVectorBuilder.AsyncBuild: Avoid implicit materialization"
      //baseBuilder.AsyncBuild<'T>(cmd, args)
    
    member builder.Build<'T>(cmd, args) = 
      match cmd with 
      | Return vectorVar -> 
          match args.[vectorVar] with
          | :? IWrappedVector<'T> as w -> w.UnwrapVector()
          | v -> v

      | GetRange(source, range) ->
          let restrictRange (vector:IVector<'T2>) =
            match vector with
            | :? VirtualVector<'T2> as source ->
                let subSource = source.Source.GetSubVector(range)
                VirtualVector<'T2>(subSource) :> IVector<'T2>
            | source -> 
                //let cmd = GetRange(Return 0, range)
                //baseBuilder.Build(cmd, [| source |])
                failwith "VirtualVectorBuilder.Build: Avoid implicit materialization (GetRange)"

          match build source args with
          | :? IBoxedVector as boxed -> // 'T = obj
              { new VectorCallSite<IVector<'T>> with
                  member x.Invoke(underlying:IVector<'U>) = 
                    boxVector (restrictRange underlying) |> unbox<IVector<'T>> }
               |> boxed.UnboxedVector.Invoke
          | :? IWrappedVector<'T> as vector -> restrictRange (vector.UnwrapVector())
          | vector -> restrictRange vector 

      | Combine(count, vectors, ((VectorListTransform.Nary (:? IRowReaderTransform as irt)) as transform)) ->
          // OPTIMIZATION: The `IRowReaderTransform` interface is a marker telling us that 
          // we are creating `IVector<obj`> where `obj` is a boxed `IVector<obj>` 
          // representing the row formed by all of the specified vectors combined.
          //
          // We short-circuit the default implementation (which actually allocates the 
          // vectors) and we create a vector of virtualized "row readers".
          let builtSources = vectors |> List.map (fun source -> VirtualVectorHelpers.unboxVector (build source args)) |> Array.ofSeq
          let allVirtual = builtSources |> Array.forall (fun vec -> vec :? VirtualVector<'T>)
          if allVirtual then
            let sources = builtSources |> Array.map (function :? VirtualVector<'T> as v -> v.Source | _ -> failwith "assertion failed") |> List.ofSeq

            let rec createRowReader (vectors:IVector<IVector>) (sources:IVirtualVectorSource<'T> list) : IVirtualVectorSource<IVector<obj>> = 
              { new IVirtualVectorSource<IVector<obj>> with
                  member x.ValueAt(loc) = 
                    OptionalValue(RowReaderVector<_>(vectors, builder, loc.Address, irt.ColumnAddressAt) :> IVector<_>)
                  member x.LookupRange(search) = failwith "Search not implemented on combined vector"
                  member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
                  member x.GetSubVector(range) = 
                    let subSources = [ for s in sources -> s.GetSubVector(range) ]
                    let subVectors = Vector.ofValues [ for s in subSources -> VirtualVector(s) :> IVector ]
                    createRowReader subVectors subSources
                  member x.MergeWith(sources) = 
                    // For every source, we get a list of sources that were used to produce it
                    let sources = 
                      Seq.append [x] sources |> List.ofSeq |> List.tryChooseBy (function
                      | :? VirtualVectorSource.ICombinedVectorSource<'T> as src -> Some(src.Sources |> Array.ofSeq)
                      | _ -> None)
                    match sources with 
                    | None -> failwith "Cannot merge frames or series not created by combine"
                    | Some sources ->
                        // Make sure that all sources are combinations of the same number of vectors 
                        let length = sources |> Seq.map Array.length |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a)
                        let toCombine =
                          [ for i in 0 .. length - 1 ->
                              let sh,st = match [ for s in sources -> s.[i] ] with sh::st -> sh, st | _ -> failwith "Merge requires one or more sources"  // ....
                              sh.MergeWith(st) ]
                        let data = Vector.ofValues [ for s in toCombine -> VirtualVector(s) :> IVector ]
                        createRowReader data toCombine

                interface VirtualVectorSource.ICombinedVectorSource<'T> with
                  member x.Sources = sources
                  member x.Function = () 
                interface IVirtualVectorSource with
                  member x.Invoke(op) = op.Invoke(x :?> IVirtualVectorSource<obj>)
                  member x.ElementType = typeof<IVector<obj>>
                  member x.Length = 
                    sources 
                    |> Seq.map (fun s -> s.Length) 
                    |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a) 

                  member x.AddressOperations = 
                    sources 
                    |> Seq.map (fun s -> s.AddressOperations)
                    |> Seq.reduce (fun a b ->  
                        if a <> b then failwith "Address mismatch" else a) }

            let data = Vector.ofValues [ for v in builtSources -> v :> IVector ]
            let newSource = createRowReader data sources
            
            // Because Build is `IVector<'T>[] -> IVector<'T>`, there is some nasty boxing.
            // This case is only called with `'T = obj` and so we create `IVector<obj>` containing 
            // `obj = IVector<obj>` as the row readers (the caller in Rows then unbox this)
            VirtualVector(VirtualVectorSource.map None (fun _ -> OptionalValue.map box) newSource) |> unbox<IVector<'T>>
          else
            //let cmd = Combine(count, [ for i in 0 .. builtSources.Length-1 -> Return i ], transform)
            //baseBuilder.Build(cmd, builtSources)
            failwith "VirtualVectorBuilder.Build: Avoid implicit materialization (Combine - row reader)"

      | Combine(length, sources, transform) ->
          let builtSources = sources |> List.map (fun source -> VirtualVectorHelpers.unboxVector (build source args)) |> Array.ofSeq
          let allVirtual = builtSources |> Array.forall (fun vec -> vec :? VirtualVector<'T>)
          if allVirtual then
            let sources = builtSources |> Array.map (function :? VirtualVector<'T> as v -> v.Source | _ -> failwith "assertion failed") |> List.ofSeq
            let func = transform.GetFunction<'T>()
            let newSource = VirtualVectorSource.combine func sources
            VirtualVector(newSource) :> _
          else
            failwith "VirtualVectorBuilder.Build: Avoid implicit materialization (Combine)"
            //let cmd = Combine(length, [ for i in 0 .. builtSources.Length-1 -> Return i ], transform)
            //baseBuilder.Build(cmd, builtSources)

      | Append(first, second) ->
          match build first args, build second args with
          | (:? VirtualVector<'T> as first), (:? VirtualVector<'T> as second) ->
              let newSource = first.Source.MergeWith([second.Source])
              VirtualVector(newSource) :> _
          | _ ->
              failwith "Append would materialize vectors"

      | Materialize(Relocate(source, length, relocations)) ->
          let source = VirtualVectorHelpers.unboxVector (build source args)
          let newData = Array.zeroCreate (int length)
          for newIndex, oldAddress in relocations do
            let newIndex = LinearAddress.asInt newIndex
            newData.[newIndex] <- source.GetValue(oldAddress)
          baseBuilder.CreateMissing(newData)

      | Relocate _ ->
          failwith "VirtualVectorBuilder.Build: Avoid implicit materialization (Relocate)"

      | DropRange(source, range) -> 
          match build source args with
          | :? VirtualVector<'T> as source -> 
              failwith "VectorBuilder.Build - DropRange not implemented"
          | _ -> //baseBuilder.Build<'T>(cmd, args)
              failwith "VirtualVectorBuilder.Build: Avoid implicit materialization (DropRange)"

      | FillMissing _ -> 
          failwith "VectorBuilder.Build - FillMissing not implemented"

      | Materialize(source) ->
          match baseBuilder.Build<'T>(source, args) with
          | :? Vectors.ArrayVector.ArrayVector<'T> as av -> upcast av
          | otherVector -> Vectors.ArrayVector.ArrayVectorBuilder.Instance.CreateMissing(Array.ofSeq otherVector.DataSequence)

      | Empty _ ->
          failwith "VectorBuilder.Build - Empty not implemented"

      // Hopefully the caller knows what they're doing    
      | CustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> (builder :> IVectorBuilder).Build(v, args) :> IVector) vectors
          f vectors :?> IVector<_>

      | AsyncCustomCommand(vectors, f) ->
          let vectors = List.map (fun v -> (builder :> IVectorBuilder).Build(v, args) :> IVector) vectors
          Async.RunSynchronously(f vectors) :?> IVector<_>
