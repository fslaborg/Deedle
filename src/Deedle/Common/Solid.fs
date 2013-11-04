namespace Deedle

open System
open Solid
// use this type instead of arrays in the SolidVector implementation
// more details: https://github.com/BlueMountainCapital/Deedle/issues/51
type Solid<'T> = Vector<'T> // name not to mix with SolidVector<'T> implementation of IVector



module Solid =
    // copied from module Solid.FSharp.Solid to avoid conflic with Deedle's Solid module

    ///O(1). Gets if the Solid is empty.
    let isEmpty (target:_ Solid) = 
        target.IsEmpty
    ///O(logn); immediate. Gets the first element.
    let inline first (target : Solid<'a>)  = 
        target.First
    ///O(logn); immediate. Gets the last element.
    let last (target :  Solid<'a>) = 
        target.Last

    ///O(logn), fast. Returns a Solid without the last element.
    let dropLast (target :Solid<'a>) = 
        target.DropLast()
    ///O(logn); immediate. Returns the element with the specified index.
    let lookup  (index : int) (target :Solid<'a>) =
        target.[index]

    ///O(logn), fast. Updates the element with the specified index.
    let update (index,item) (target :Solid<'a>) = 
        target.Set(index,item)

    ///O(1). Gets the length of the Solid.
    let inline length (target:  Solid<'a>) = 
        target.Count

    ///O(n). Returns a Solid containing the elements that fulfill the predicate.
    let filter (f : 'a -> bool) (target : Solid<'a>) = 
        target.Where(Func<'a,bool>(f))
    ///O(n). Applies the specified function on every element in the Solid.
    let select (f : 'a -> 'b) (target : Solid<'a>) = 
        target.Select(Func<'a,'b>(f))
    ///O(n). Iterates over the Solid, from first to last.
    let iter (f : 'a -> unit) (target : Solid<'a>) = 
        target.ForEach(Action<'a>(f))
    ///O(n). Iterates over the Solid, from last to first.
    let iterBack (f:_->unit) (target:_ Solid) =
        target.ForEachBack(Action<_>(f))

    

    ///O(logn), fast. Returns a new Solid consisting of the first several elements.
    let take count (target : Solid<'a>) = 
        target.Take(count)

    ///Gets the empty Solid.
    let empty<'a> = Solid<'a>.Empty

    let choose (f : 'a -> 'b option) (target :Solid<'a>) = 
        let new_target = ref (empty : Solid<'b>)
        target |> iter (fun v -> match f v with | None -> () | Some u -> new_target := (!new_target <+ u))
    
    ///Applies an accumulator over the Solid.
    let fold (initial : 'state) f (target : Solid<'item>) = 
        target.Fold(initial,f)

    let zip (left : #seq<_>) (right:#seq<_>) = 
        empty<_> <++ Seq.zip left right

    ///O(n). Constructs a Solid from a sequence.
    let ofSeq (xs : seq<_>) = empty<_>.AddLastRange(xs)
    


    ///O(n). Checks if any item fulfilling the predicate exists in the Solid.
    let exists (f : _ -> bool) (l : _ Solid) =
        l.IndexOf(Func<_,bool>(f)).HasValue

    let ofItem x = empty <+ x

    ///O(n). Returns the index of the first item that fulfills the predicate, or None.
    let indexOf(f : _ -> bool) (l :_ Solid)=
        let res = l.IndexOf(Func<_,_>(f))
        if res.HasValue then Some res.Value else None

    ///O(n). Returns the first item that fulfills the predicate, or None.
    let find(f : _ -> bool) (l :_ Solid)=
        let res = l.IndexOf(Func<_,_>(f))
        if res.HasValue then Some l.[res.Value] else None

    // MEMBERS SMAE AS IN READONLY COOLLECTION MODULE

    // ofSeq done above

    ///O(?). Constructs a Solid from an array.
    let inline ofArray (array:'T[]) = Convertion.ToVector(array)

    /// Sum elements of the Solid
    let inline sum (list:Solid<'T>) = 
        let mutable total = LanguagePrimitives.GenericZero
        for i in 0 .. list.Count - 1 do total <- total + list.[i]
        total

    /// Return the smallest element of the Solid
    let inline min (list:Solid<'T>) = 
        let mutable res = list.[0]
        for i in 1 .. list.Count - 1 do res <- min res list.[i]
        res

    /// Return the greatest element of the Solid
    let inline max (list:Solid<'T>) = 
        let mutable res = list.[0]
        for i in 1 .. list.Count - 1 do res <- max res list.[i]
        res

    /// Reduce elements of the Solid
    let inline reduce op (list:Solid<'T>) = 
        let mutable res = list.[0]
        for i in 1 .. list.Count - 1 do res <- op res list.[i]
        res

    // length done above

    /// Average elements of the Solid
    let inline average (list:Solid<'T>) = 
        let mutable total = LanguagePrimitives.GenericZero
        for i in 0 .. list.Count - 1 do total <- total + list.[i]
        LanguagePrimitives.DivideByInt total list.Count

    /// Sum elements of the Solid, skipping over missing values
    let inline sumOptional (list:Solid<OptionalValue<'T>>) = 
        let mutable total = LanguagePrimitives.GenericZero
        for i in 0 .. list.Count - 1 do 
            if list.[i].HasValue then total <- total + list.[i].Value
        total

    /// Reduce elements of the Solid, skipping over missing values
    let inline reduceOptional op (list:Solid<OptionalValue<'T>>) = 
        let mutable res = None
        for i in 0 .. list.Count - 1 do 
            match res, list.[i] with
            | Some r, OptionalValue.Present v -> res <- Some (op r v)
            | None, OptionalValue.Present v -> res <- Some v
            | _ -> ()
        res |> OptionalValue.ofOption

    /// Average elements of the Solid, skipping over missing values
    let inline averageOptional (list:Solid<OptionalValue< ^T >>) = 
        let mutable total = LanguagePrimitives.GenericZero
        let mutable count = 0 
        for i in 0 .. list.Count - 1 do 
            if list.[i].HasValue then 
                total <- total + list.[i].Value
                count <- count + 1
        LanguagePrimitives.DivideByInt total count

    /// Count elements of the Solid that are not missing
    let inline lengthOptional (list:Solid<OptionalValue<'T>>) = 
        let mutable total = 0
        for i in 0 .. list.Count - 1 do if list.[i].HasValue then total <- total + 1
        total

    /// Return the smallest element, skipping over missing values
    let inline minOptional (list:Solid<OptionalValue< ^T >>) = 
        reduceOptional Operators.min list

    /// Return the greatest element, skipping over missing values
    let inline maxOptional (list:Solid<OptionalValue< ^T >>) = 
        reduceOptional Operators.max list