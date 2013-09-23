namespace FSharp.DataFrame

type ICustomKey<'K> = 
  abstract Matches : ICustomKey<'K> -> bool

type HierarchicalLookup<'K> = 
  HL of 'K

/// 
type IMultiKey = 
  abstract Keys : option<obj> * option<obj>

type MultiKey<'K1, 'K2> = 
  internal | MK of option<'K1> * option<'K2>
  interface IMultiKey with
    member x.Keys = 
      let (MK(l, r)) = x in Option.map box l, Option.map box r
  override x.ToString() = 
    match x with
    | MK(None, None) -> "_"
    | MK(None, Some k) -> sprintf "(_, %A)" k
    | MK(Some k, None) -> sprintf "(%A, _)" k
    | MK(Some k1, Some k2) -> sprintf "%A" (k1, k2)
  interface ICustomKey<MultiKey<'K1, 'K2>> with
    member x.Matches(another) = 
      let (|IMultiKey|_|) (obj:obj) =
        match obj with 
        | :? IMultiKey as mk -> Some(mk.Keys)
        | _ -> None
      let rec matches template value = 
        match template, value with
        | IMultiKey(Some v1, _), IMultiKey(Some v2, None) 
        | IMultiKey(_, Some v1), IMultiKey(None, Some v2)
        | IMultiKey(Some v1, None), IMultiKey(Some v2, _) 
        | IMultiKey(None, Some v1), IMultiKey(_, Some v2) -> matches v1 v2
        | IMultiKey(None, None), _ 
        | _, IMultiKey(None, None) -> true
        | IMultiKey(Some l1, Some r1), IMultiKey(Some l2, Some r2) -> matches l1 l2 && matches r1 r2
        | o1, o2 -> o1.Equals(o2)
      matches x another

[<AutoOpen>]
module MultiKeyExtensions =
  let MultiKey(l, r) = MK(Some l, Some r)
  let (|MultiKey2|) = function
    | MK(Some l, Some r) -> l, r
    | _ -> failwith "Multi key contains a hole"
  let (|MultiKey3|) = function
    | MK(Some v1, Some (MK(Some v2, Some v3))) -> v1,v2,v3
    | _ -> failwith "Multi key contains a hole"
  let (|MultiKey4|) = function
    | MK(Some v1, Some (MK(Some v2, Some (MK(Some v3, Some v4))))) -> v1,v2,v3,v4
    | _ -> failwith "Multi key contains a hole"
  let Level1Of2 k = HL(MK(Some k, None))
  let Level2Of2 k = HL(MK(None, Some k))
  let Level1Of3 k = HL(MK(Some k, Some(MK(None, None))))
  let Level2Of3 k = HL(MK(None, Some(MK(Some k, None))))
  let Level3Of3 k = HL(MK(None, Some(MK(None, Some k))))

module Key =
  let key1Of2(MultiKey2(v, _)) = v
  let key2Of2(MultiKey2(_, v)) = v
  let key1Of3(MultiKey3(v, _, _)) = v
  let key2Of3(MultiKey3(_, v, _)) = v
  let key3Of3(MultiKey3(_, _, v)) = v