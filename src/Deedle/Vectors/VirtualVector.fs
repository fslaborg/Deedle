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

/// Non-generic part of the `IVirtualVectorSource<'V>` interface, which 
/// provides some basic information about the virtualized data source
type IVirtualVectorSource =
  /// Returns the type of elements - essentially typeof<'V> for IVirtualVectorSource<'V>
  abstract ElementType : System.Type

  /// Returns the length of the source - by design, Big Deedle always 
  /// needs to know the length of the source (e.g. for binary search)
  abstract Length : int64


/// Represents a data source for Big Deedle. The interface is used both as a representation
/// of data source for `VirtualVector` (this file) and `VirtualIndex` (another file). The 
/// index uses `Length` and `ValueAt` to perform binary search when looking for a key; the
/// vector simply provides an access to values using `ValueAt`. 
type IVirtualVectorSource<'V> = 
  inherit IVirtualVectorSource

  /// Returns the value at the specifid address. We assume that the address is in range 
  /// [0L, Length-1L] and that each location has a value (or has a missing value, but is valid)
  abstract ValueAt : int64 -> OptionalValue<'V>

  /// Find a range (continuous or a sequence of indices) such that all values in the range are 
  /// the specified value. This is used, for example, when filtering frame based on column
  /// value (say column "PClass" has a value "1").
  ///
  /// If the data source has some "clever" representation of the range, it can return
  /// `Custom of IVectorRange` - which is then passed to `GetSubVector`.
  abstract LookupRange : 'V -> VectorRange

  /// Find the address associated with the specified value. This is used by the 
  /// index and it has the same signature as `IIndex<'K>.Lookup` (see `Index.fs`).
  abstract LookupValue : 'V * Lookup * Func<Addressing.Address, bool> -> OptionalValue<'V * Addressing.Address> 

  /// Returns a virtual source for the specified range (used when performing splicing on the 
  /// frame/series, both using address or using keys - which are obtained using Lookup)
  abstract GetSubVector : VectorRange -> IVirtualVectorSource<'V>

  /// Merge the current source with a list of other sources
  /// (used by functions such as `Frame.merge` and `Frame.mergeAll`)
  abstract MergeWith : IVirtualVectorSource<'V> list -> IVirtualVectorSource<'V>

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

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
        member x.ValueAt(idx) = source.ValueAt(idx) |> OptionalValue.map box
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = boxSource (source.GetSubVector(range))
        member x.MergeWith(sources) = 
          let sources = 
            sources |> List.tryChooseBy (function
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
        member x.Length = source.Length }

  let rec combine (f:OptionalValue<'T> list -> OptionalValue<'R>) (sources:IVirtualVectorSource<'T> list) : IVirtualVectorSource<'R> = 
    { new IVirtualVectorSource<'R> with
        member x.ValueAt(idx) = f [ for s in sources -> s.ValueAt(idx)  ]
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = combine f [ for s in sources -> s.GetSubVector(range) ]
        member x.MergeWith(sources) = 
          // For every source, we get a list of sources that were used to produce it
          let sources = 
            (x::sources) |> List.tryChooseBy (function
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
        member x.ElementType = typeof<'R>
        member x.Length = sources |> Seq.map (fun s -> s.Length) |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a) }

  let rec map rev f (source:IVirtualVectorSource<'V>) = 
    let withReverseLookup op = 
      match rev with 
      | None -> failwith "Cannot lookup on virtual vector without reverse lookup"
      | Some g -> op g

    { new IVirtualVectorSource<'TNew> with
        member x.ValueAt(idx) = f (Address.ofInt64 idx) (source.ValueAt(idx)) // TODO: Are we calculating the address correctly here??
        member x.MergeWith(sources) = 
          let sources = sources |> List.tryChooseBy (function
              | :? IMappedVectorSource<'V, 'TNew> as src -> Some(src.Source) | _ -> None)
          match sources with 
          | None -> failwith "Cannot merge frames or series not created by combine"
          | Some sources ->
              map rev f (source.MergeWith(sources)) // TODO: Check that the function is the same

        member x.LookupRange(search) = 
          (*
          // TODO: Test me
          match search with
          | LookupKind.Scan sf -> source.LookupRange(LookupKind.Scan(fun a ov -> sf a (f a ov))) // TODO: explain me
          | LookupKind.Lookup lv -> source.LookupRange(LookupKind.Scan(fun a ov ->
              match f a ov with
              | OptionalValue.Present r -> Object.Equals(r, lv)
              | _ -> false))
              *)
          let scanFunc a ov = 
                match f a ov with
                | OptionalValue.Present(rv) -> Object.Equals(search, rv) // TODO: Object.Equals is not so good here
                | _ -> false
          
          let scanIndices = 
            Seq.range 0L (source.Length-1L)
            |> Seq.filter (fun i -> 
                scanFunc (Address.ofInt64 i) (source.ValueAt(i)) )
            |> Array.ofSeq

          { new IVectorRange with
              member x.Count = scanIndices |> Seq.length |> int64 // TODO: SLOW!
            interface seq<int64> with 
              member x.GetEnumerator() = (scanIndices :> seq<_>).GetEnumerator() // TODO: SLow
            interface System.Collections.IEnumerable with
              member x.GetEnumerator() = scanIndices.GetEnumerator() } //TODO: Slow
          |> VectorRange.Custom

        member x.LookupValue(v, l, c) = 
          withReverseLookup (fun g ->
              source.LookupValue(g v, l, c)
              |> OptionalValue.bind (fun (v, a) ->  // TODO: Make me nice and readable!
                  f a (OptionalValue v)
                  |> OptionalValue.map (fun v -> v, a))  )

        member x.GetSubVector(range) = map rev f (source.GetSubVector(range))
      interface IMappedVectorSource<'V, 'TNew> with
        member x.Source = source
        member x.Function = ()
      interface IVirtualVectorSource with
        member x.ElementType = typeof<'TNew>
        member x.Length = source.Length }


[<Extension>]
type VirtualVectorSourceExtensions =
  [<Extension>]
  static member Select<'T, 'R>(source:IVirtualVectorSource<'T>, func:Func<Address, OptionalValue<'T>, OptionalValue<'R>>) =
    VirtualVectorSource.map None (fun a v -> func.Invoke(a, v))
  [<Extension>]
  static member Select<'T, 'R>(source:IVirtualVectorSource<'T>, func:Func<Address, OptionalValue<'T>, OptionalValue<'R>>, rev:Func<'R, 'T>) =
    VirtualVectorSource.map (Some rev.Invoke) (fun a v -> func.Invoke(a, v))


type VirtualVector<'V>(source:IVirtualVectorSource<'V>) = 
  member vector.Source = source
  interface IVector with
    member val ElementType = typeof<'V>
    member vector.Length = source.Length
    member vector.SuppressPrinting = false
    member vector.GetObject(index) = source.ValueAt(index) |> OptionalValue.map box
    member vector.ObjectSequence = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) |> OptionalValue.map box }
    member vector.Invoke(site) = site.Invoke<'V>(vector)
  interface IVector<'V> with
    member vector.GetValue(index) = source.ValueAt(index)
    member vector.Data = seq { for i in Seq.range 0L (source.Length-1L) -> source.ValueAt(i) } |> VectorData.Sequence
    member vector.SelectMissing<'TNew>(f:Address -> OptionalValue<'V> -> OptionalValue<'TNew>) = 
      VirtualVector(VirtualVectorSource.map None f source) :> _
    member vector.Select(f) = 
      (vector :> IVector<_>).SelectMissing(fun _ -> OptionalValue.map f)
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
    member builder.AsyncBuild<'T>(cmd, args) = baseBuilder.AsyncBuild<'T>(cmd, args)
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
                let cmd = GetRange(Return 0, range)
                baseBuilder.Build(cmd, [| source |])

          match build source args with
          | :? IBoxedVector as boxed -> // 'T = obj
              { new VectorCallSite<IVector<'T>> with
                  member x.Invoke(underlying:IVector<'U>) = 
                    boxVector (restrictRange underlying) |> unbox<IVector<'T>> }
               |> boxed.UnboxedVector.Invoke
          | :? IWrappedVector<'T> as vector -> restrictRange (vector.UnwrapVector())
          | vector -> restrictRange vector 

      | Combine(vectors, ((VectorListTransform.Nary (:? IRowReaderTransform)) as transform)) ->
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
                  member x.ValueAt(idx) = 
                    OptionalValue(RowReaderVector<_>(vectors, builder, Address.ofInt64 idx) :> IVector<_>)
                  member x.LookupRange(search) = failwith "Search not implemented on combined vector"
                  member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
                  member x.GetSubVector(range) = 
                    let subSources = [ for s in sources -> s.GetSubVector(range) ]
                    let subVectors = Vector.ofValues [ for s in subSources -> VirtualVector(s) :> IVector ]
                    createRowReader subVectors subSources
                  member x.MergeWith(sources) = 
                    // For every source, we get a list of sources that were used to produce it
                    let sources = 
                      (x::sources) |> List.tryChooseBy (function
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
                  member x.ElementType = typeof<IVector<obj>>
                  member x.Length = sources |> Seq.map (fun s -> s.Length) |> Seq.reduce (fun a b -> if a <> b then failwith "Length mismatch" else a) }

            let data = Vector.ofValues [ for v in builtSources -> v :> IVector ]
            let newSource = createRowReader data sources
            
            // Because Build is `IVector<'T>[] -> IVector<'T>`, there is some nasty boxing.
            // This case is only called with `'T = obj` and so we create `IVector<obj>` containing 
            // `obj = IVector<obj>` as the row readers (the caller in Rows then unbox this)
            VirtualVector(VirtualVectorSource.map None (fun _ -> OptionalValue.map box) newSource) |> unbox<IVector<'T>>
          else
            let cmd = Combine([ for i in 0 .. builtSources.Length-1 -> Return i ], transform)
            baseBuilder.Build(cmd, builtSources)

      | Combine(sources, transform) ->
          let builtSources = sources |> List.map (fun source -> VirtualVectorHelpers.unboxVector (build source args)) |> Array.ofSeq
          let allVirtual = builtSources |> Array.forall (fun vec -> vec :? VirtualVector<'T>)
          if allVirtual then
            let sources = builtSources |> Array.map (function :? VirtualVector<'T> as v -> v.Source | _ -> failwith "assertion failed") |> List.ofSeq
            let func = transform.GetFunction<'T>()
            let newSource = VirtualVectorSource.combine func sources
            VirtualVector(newSource) :> _
          else
            let cmd = Combine([ for i in 0 .. builtSources.Length-1 -> Return i ], transform)
            baseBuilder.Build(cmd, builtSources)

      | Append(first, second) ->
          match build first args, build second args with
          | (:? VirtualVector<'T> as first), (:? VirtualVector<'T> as second) ->
              let newSource = first.Source.MergeWith([second.Source])
              VirtualVector(newSource) :> _
          | _ ->
              failwith "Append would materialize vectors"
      | _ ->    
          //failwith "VectorBuilder.Build - this would be slow"
          baseBuilder.Build<'T>(cmd, args)
