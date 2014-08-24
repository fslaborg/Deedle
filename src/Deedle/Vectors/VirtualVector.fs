// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

namespace Deedle.Vectors.Virtual

open System
open Deedle
open Deedle.Addressing
open Deedle.Vectors
open Deedle.VectorHelpers
open Deedle.Internal
open System.Runtime.CompilerServices

type LookupKind<'V> = 
  | Scan of (Address -> OptionalValue<'V> -> bool)
  | Lookup of 'V

type IVirtualVectorSource =
  abstract ElementType : System.Type
  abstract Length : int64

type IVirtualVectorSource<'V> = 
  inherit IVirtualVectorSource
  abstract ValueAt : int64 -> OptionalValue<'V>
  abstract GetSubVector : VectorRange -> IVirtualVectorSource<'V>
  abstract LookupRange : LookupKind<'V> -> VectorRange
  abstract LookupValue : 'V * Lookup * Func<Addressing.Address, bool> -> OptionalValue<'V * Addressing.Address> 

// ------------------------------------------------------------------------------------------------
// Virtual vectors
// ------------------------------------------------------------------------------------------------

module VirtualVectorSource = 
  let rec boxSource (source:IVirtualVectorSource<'T>) =
    { new IVirtualVectorSource<obj> with
        member x.ValueAt(idx) = source.ValueAt(idx) |> OptionalValue.map box
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = boxSource (source.GetSubVector(range))
      interface IVirtualVectorSource with
        member x.ElementType = typeof<obj>
        member x.Length = source.Length }

  let rec combine (f:OptionalValue<'T> list -> OptionalValue<'R>) (sources:IVirtualVectorSource<'T> list) : IVirtualVectorSource<'R> = 
    { new IVirtualVectorSource<'R> with
        member x.ValueAt(idx) = f [ for s in sources -> s.ValueAt(idx)  ]
        member x.LookupRange(search) = failwith "Search not implemented on combined vector"
        member x.LookupValue(v, l, c) = failwith "Lookup not implemented on combined vector" 
        member x.GetSubVector(range) = combine f [ for s in sources -> s.GetSubVector(range) ]
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
          let scanFunc = 
            match search with
            | LookupKind.Scan sf -> fun a ov -> sf a (f a ov)
            | LookupKind.Lookup lv -> fun a ov -> 
                match f a ov with
                | OptionalValue.Present(rv) -> Object.Equals(lv, rv) // TODO: Object.Equals is not so good here
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

      | _ ->    
        baseBuilder.Build<'T>(cmd, args)
