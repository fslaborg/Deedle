(*

Copyright 2005-2009 Microsoft Corporation
Copyright 2013 Jack Pappas

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*)

namespace ExtCore.Collections

open System.Collections.Generic

#nowarn "21" // recursive initialization
#nowarn "40" // recursive initialization

[<AutoOpen>]
module ExtCoreExtensions = 
    /// Determines if a reference is a null reference.
    [<CompiledName("IsNull")>]
    let inline isNull< ^T when ^T : not struct> (x : ^T) =
        System.Object.ReferenceEquals (null, x)

    /// Determines if a reference is a null reference, and if it is, throws an ArgumentNullException.
    [<CompiledName("CheckNonNull")>]
    let inline checkNonNull< ^T when ^T : not struct> argName (value : ^T) =
        if isNull value then
            nullArg argName

    /// Raises a System.ArgumentOutOfRangeException.
    [<CompiledName("RaiseArgumentOutOfRangeException")>]
    let argOutOfRange (paramName : string) (message : string) : 'T =
        match System.String.IsNullOrEmpty paramName, System.String.IsNullOrEmpty message with
        | false, false ->
            raise <| System.ArgumentOutOfRangeException (paramName, message)
        | false, true ->
            raise <| System.ArgumentOutOfRangeException (paramName)
        | true, true ->
            raise <| System.ArgumentOutOfRangeException ()
        | true, false ->
            raise <| System.ArgumentOutOfRangeException ("(Unspecified parameter)", message)


(* OPTIMIZE :   The code below could likely be simplified and optimized by using System.Lazy<'T>
                ("lazy" in F#) and Choice<_,_> instead of LazyCellStatus<'T>.
                Basically, the Delayed and Value cases are replaced by Lazy<'T> and Choice1Of2,
                and the Exception case is replaced by Choice2Of2.
                We'll still need LazyListCell<'T> though, since that's the actual list representation. *)

//
[<NoEquality; NoComparison>]
type internal LazyCellStatus<'T> =
    //
    | Delayed of (unit -> LazyListCell<'T>)
    //
    | Value of LazyListCell<'T>
    //
    | Exception of exn

//
and [<NoEquality; NoComparison>]
    [<CompilationRepresentation(CompilationRepresentationFlags.UseNullAsTrueValue)>]
    internal LazyListCell<'T> =
    //
    | Empty
    //
    | Cons of 'T * LazyList<'T>    

/// LazyLists are possibly-infinite, cached sequences.  See also IEnumerable/Seq for
/// uncached sequences. LazyLists normally involve delayed computations without 
/// side-effects.  The results of these computations are cached and evaluations will be 
/// performed only once for each element of the lazy list.  In contrast, for sequences 
/// (IEnumerable) recomputation happens each time an enumerator is created and the sequence 
/// traversed.
///
/// LazyLists can represent cached, potentially-infinite computations.  Because they are 
/// cached they may cause memory leaks if some active code or data structure maintains a 
/// live reference to the head of an infinite or very large lazy list while iterating it, 
/// or if a reference is maintained after the list is no longer required.
///
/// Lazy lists may be matched using the LazyList.Cons and LazyList.Nil active patterns. 
/// These may force the computation of elements of the list.
and [<NoEquality; NoComparison; Sealed>]
//[<StructuredFormatDisplay("")>]
    LazyList<'T> internal (initialStatus) =
    //
    static let emptyList = LazyList (Value Empty)
    
    /// The status for undefined values.
    static let undefinedValue =
        System.InvalidOperationException "The value of the LazyList cell is undefined."
        :> exn
        |> Exception

    //
    let mutable status : LazyCellStatus<'T> = initialStatus

    /// The empty LazyList.
    static member internal Empty
        with get () = emptyList
    
    member internal this.Value =
        match status with
        | Value value ->
            value
        | _ ->
            lock this <| fun () ->
                match status with
                | Delayed f ->
                    status <- undefinedValue
                    try
                        let res = f ()
                        status <- Value res
                        res
                    with ex ->
                        status <- Exception ex
                        reraise ()
                | Value value ->
                    value
                | Exception ex ->
                    raise ex

    /// Test if a list is empty.
    /// Forces the evaluation of the first element of the stream if it is not already evaluated.
    member this.IsEmpty
        with get () =
            match this.Value with
            | Empty -> true
            | Cons _ -> false

    /// Return the first element of the list.
    /// Forces the evaluation of the first cell of the list if it is not already evaluated.
    member this.Head () =
        match this.Value with
        | Cons (hd, _) -> hd
        | Empty ->
            invalidOp "The list is empty."

    /// Return the list corresponding to the remaining items in the sequence.
    /// Forces the evaluation of the first cell of the list if it is not already evaluated.
    member this.Tail () =
        match this.Value with
        | Cons (_, tl) -> tl
        | Empty ->
            invalidOp "The list is empty."

    /// Get the first cell of the list.
    member this.TryHeadTail () =
        match this.Value with
        | Empty ->
            None
        | Cons (hd, tl) ->
            Some (hd, tl)

    /// Creates a sequence which enumerates the values in the LazyList.
    member this.ToSeq () =
        this
        |> Seq.unfold (fun list ->
            match list.Value with
            | Empty ->
                None
            | Cons (hd, tl) ->
                Some (hd, tl))

    //
    static member internal CreateLazy cellCreator : LazyList<'T> =
        LazyList (Delayed cellCreator)

    /// Return a new list which contains the given item followed by the given list.
    static member internal Cons (value, list) : LazyList<'T> =
        // Preconditions
        checkNonNull "list" list

        LazyList<_>.CreateLazy <| fun () ->
            Cons (value, list)

    /// Return a new list which on consumption contains the given item 
    /// followed by the list returned by the given computation.
    static member internal ConsDelayed (value, creator : unit -> LazyList<'T>) : LazyList<'T> =
        LazyList<_>.CreateLazy <| fun () ->
            Cons (value, LazyList<_>.CreateLazy <| fun () ->
                (creator ()).Value)

    /// Return a list that is -- in effect -- the list returned by the given computation.
    /// The given computation is not executed until the first element on the list is consumed.
    static member internal Delayed (creator : unit -> LazyList<'T>) : LazyList<'T> =
        LazyList (Delayed <| fun () ->
            (creator ()).Value)
            
    interface IEnumerable<'T> with
        member this.GetEnumerator () =
            this.ToSeq().GetEnumerator ()

    interface System.Collections.IEnumerable with
        override this.GetEnumerator () =
            this.ToSeq().GetEnumerator ()
            :> System.Collections.IEnumerator


/// Functional operators on LazyLists.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module LazyList =
    //open OptimizedClosures

    /// The empty LazyList.
    [<CompiledName("Empty")>]
    let empty<'T> : LazyList<'T> =
        LazyList<'T>.Empty
    
    /// Get the first cell of the list.
    [<CompiledName("TryGet")>]
    let inline tryGet (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        list.TryHeadTail ()

    /// Return the first element of the list.
    /// Forces the evaluation of the first cell of the list if it is not already evaluated.
    [<CompiledName("Head")>]
    let inline head (list : LazyList<'T>) : 'T =
        // Preconditions
        checkNonNull "list" list
        
        list.Head ()

    /// Return the list corresponding to the remaining items in the sequence.
    /// Forces the evaluation of the first cell of the list if it is not already evaluated.
    [<CompiledName("Tail")>]
    let inline tail (list : LazyList<'T>) : LazyList<'T> =
        // Preconditions
        checkNonNull "list" list
        
        list.Tail ()

    /// Test if a list is empty.
    /// Forces the evaluation of the first element of the stream if it is not already evaluated.
    [<CompiledName("IsEmpty")>]
    let inline isEmpty (list : LazyList<'T>) : bool =
        // Preconditions
        checkNonNull "list" list

        list.IsEmpty

    /// Return a new list which contains the given item followed by the given list.
    [<CompiledName("Cons")>]
    let cons value (list : LazyList<'T>) =
        // Preconditions checked by the implementation.
        LazyList<_>.Cons (value, list)
    
    /// Return a new list which on consumption contains the given item
    /// followed by the list returned by the given computation.
    [<CompiledName("ConsDelayed")>]
    let consDelayed (value : 'T) creator =
        LazyList<_>.ConsDelayed (value, creator)

    /// Return a list that is -- in effect -- the list returned by the given computation.
    /// The given computation is not executed until the first element on the list is consumed.
    [<CompiledName("Delayed")>]
    let delayed creator : LazyList<'T> =
        LazyList<_>.Delayed creator

    /// Creates a LazyList containing the given value.
    [<CompiledName("Singleton")>]
    let singleton value : LazyList<'T> =
        LazyList<_>.Cons (value, LazyList.Empty)

    /// Returns a new LazyListCell created by adding a value to the end of the given LazyList.
    /// This is simply a curried form of the Cons constructor.
    let inline private consCell value (list : LazyList<'T>) =
        assert (not <| isNull list)
        Cons (value, list)

    /// Alias for LazyList.CreateLazy.
    let inline private lzy cellCreator : LazyList<'T> =
        LazyList<_>.CreateLazy cellCreator

    /// Return the length of the list.
    [<CompiledName("Length")>]
    let length (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        let rec lengthAux acc (list : LazyList<'T>) =
            match list.Value with
            | Empty ->
                int acc
            | Cons (_, tl) ->
                lengthAux (acc + 1u) tl

        lengthAux 0u list

    /// Return the list which on consumption will consist of an
    /// infinite sequence of the given item.
    [<CompiledName("Repeat")>]
    let repeat value : LazyList<'T> =
        let rec s = cons value (delayed (fun () -> s))
        s

    /// Return the list which contains on demand the elements of the
    /// first list followed by the elements of the second list.
    [<CompiledName("Append")>]
    let rec append (list1 : LazyList<'T>) (list2 : LazyList<'T>) =
        // Preconditions
        checkNonNull "list1" list1
        checkNonNull "list2" list2

        lzy <| fun () ->
            appendCell list1 list2

    and private appendCell list1 list2 =
        match list1.Value with
        | Empty ->
            list2.Value
        | Cons (hd, tl) ->
            consCell hd (append tl list2)

    /// Return the list which contains on demand the list of elements of the list of lazy lists.
    [<CompiledName("Concat")>]
    let rec concat (lists : LazyList<LazyList<'T>>) =
        // Preconditions
        checkNonNull "lists" lists

        lzy <| fun () ->
            match lists.Value with
            | Empty ->
                Empty
            | Cons (hd, tl) ->
                appendCell hd (concat tl)

    /// Apply the given function to successive elements of the list, returning the first
    /// result where function returns <c>Some(x)</c> for some x.
    /// If the function never returns true, 'None' is returned.
    [<CompiledName("TryFind")>]
    let rec tryFind predicate (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        match list.Value with
        | Empty ->
            None
        | Cons (hd, tl) ->
            if predicate hd then Some hd
            else tryFind predicate tl

    /// Return the first element for which the given function returns <c>true</c>.
    /// Raise <c>KeyNotFoundException</c> if no such element exists.
    [<CompiledName("Find")>]
    let find predicate (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        match tryFind predicate list with
        | Some value ->
            value
        | None ->
            raise <| KeyNotFoundException "An element satisfying the predicate was not found in the collection."

    /// Return a list that contains the elements returned by the given computation.
    /// The given computation is not executed until the first element on the list is
    /// consumed.  The given argument is passed to the computation.  Subsequent elements
    /// in the list are generated by again applying the residual 'b to the computation.
    [<CompiledName("Unfold")>]
    let rec unfold (generator : 'State -> ('T * 'State) option) state =
        lzy <| fun () ->
            match generator state with
            | None ->
                Empty
            | Some (value, state) ->
                Cons (value, unfold generator state)

    /// Build a new collection whose elements are the results of applying
    /// the given function to each of the elements of the collection.
    [<CompiledName("Map")>]
    let rec map (mapping : 'T -> 'U) (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        lzy <| fun () ->
            match list.Value with
            | Empty ->
                Empty
            | Cons (hd, tl) ->
                consCell (mapping hd) (map mapping tl)

    /// Build a new collection whose elements are the results of applying the given function
    /// to the corresponding elements of the two collections pairwise.
    [<CompiledName("Map2")>]
    let rec map2 (mapping : 'T1 -> 'T2 -> 'U) (list1 : LazyList<'T1>) (list2 : LazyList<'T2>) =
        // Preconditions
        checkNonNull "list1" list1
        checkNonNull "list2" list2

        lzy <| fun () ->
            match list1.Value, list2.Value with
            | Cons (hd1, tl1), Cons (hd2, tl2) ->
                consCell (mapping hd1 hd2) (map2 mapping tl1 tl2)
            | _ -> Empty

    /// Return the list which contains on demand the pair of elements of the first and second list.
    // OPTIMIZE : Why not just re-implement this based on the 'map2' function?
    [<CompiledName("Zip")>]
    let rec zip (list1 : LazyList<'T>) (list2 : LazyList<'T>) =
        // Preconditions
        checkNonNull "list1" list1
        checkNonNull "list2" list2

        lzy <| fun () ->
            match list1.Value, list2.Value with
            | Cons (hd1, tl1), Cons (hd2, tl2) ->
                consCell (hd1, hd2) (zip tl1 tl2)
            | _ -> Empty

    /// Return a new collection which on consumption will consist of only the
    /// elements of the collection for which the given predicate returns "true".
    [<CompiledName("Filter")>]
    let rec filter predicate (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        lzy <| fun () ->
            filterCell predicate list

    and private filterCell predicate list =
        match list.Value with
        | Empty ->
            Empty
        | Cons (hd, tl) ->
            if predicate hd then
                consCell hd (filter predicate tl)
            else
                filterCell predicate tl

    /// Return a new list consisting of the results of applying the
    /// given accumulating function to successive elements of the list.
    [<CompiledName("Scan")>]
    let rec scan (folder : 'State -> 'T -> 'State) (state : 'State) (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        lzy <| fun () ->
            match list.Value with
            | Empty ->
                consCell state empty
            | Cons (hd, tl) ->
                let state' = folder state hd
                consCell state (scan folder state' tl)

    /// Return the list which on consumption will consist of
    /// at most 'count' elements of the input list.
    [<CompiledName("Take")>]
    let rec take count (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list
        if count < 0 then
            argOutOfRange "count" "Cannot take a negative number of elements."

        lzy <| fun () ->
            if count = 0 then
                Empty
            else
                match list.Value with
                | Cons (hd, tl) ->
                    consCell hd (take (count - 1) tl)
                | Empty ->
                    let msg = sprintf "There are not enough items in the list. (Count = %i)" count
                    invalidArg "count" msg

    let rec private skipCell count (list : LazyList<'T>) =
        if count = 0 then
            list.Value
        else
            match list.Value with
            | Cons (_, tl) ->
                skipCell (count - 1) tl
            | Empty ->
                let msg = sprintf "There are not enough items in the list. (Count = %i)" count
                invalidArg "count" msg

    /// Return the list which on consumption will skip the first 'count' elements of the input list.
    [<CompiledName("Skip")>]
    let skip count (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list
        if count < 0 then
            argOutOfRange "count" "Cannot skip a negative number of elements."

        lzy <| fun () ->
            skipCell count list

    /// Apply the given function to each element of the collection.
    [<CompiledName("Iterate")>]
    let rec iter (action : 'T -> unit) (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        match list.Value with
        | Empty -> ()
        | Cons (hd, tl) ->
            action hd
            iter action tl

    // NOTE : This function doesn't dispose the IEnumerator until it reaches
    // the end of the enumeration; this function can therefore cause memory leaks
    // if not used carefully.
    let rec private ofFreshIEnumerator (e : IEnumerator<'T>) =
        lzy <| fun () ->
            if e.MoveNext () then
                consCell e.Current (ofFreshIEnumerator e)
            else
               e.Dispose ()
               Empty

    /// Build a LazyList from the given sequence of elements.
    [<CompiledName("OfSeq")>]
    let ofSeq (sequence : seq<'T>) =
        // Preconditions
        checkNonNull "sequence" sequence

        sequence.GetEnumerator ()
        |> ofFreshIEnumerator

    /// Create a LazyList containing the elements of the given list.
    [<CompiledName("OfList")>]
    let rec ofList (list : 'T list) =
        // Preconditions
        checkNonNull "list" list

        lzy <| fun () ->
            match list with
            | [] ->
                Empty
            | hd :: tl ->
                consCell hd (ofList tl)

    /// Creates a LazyList from an array by copying elements from the array into the LazyList.
    let rec private copyFrom index (array : 'T[]) =
        lzy <| fun () ->
            if index >= Array.length array then
                Empty
            else
                copyFrom (index + 1) array
                |> consCell array.[index]

    /// Create a LazyList containing the elements of the given array.
    [<CompiledName("OfArray")>]
    let ofArray (array : 'T[]) =
        // Preconditions
        checkNonNull "array" array

        copyFrom 0 array

    /// Return a view of the collection as an enumerable object.
    [<CompiledName("ToSeq")>]
    let inline toSeq (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        list.ToSeq ()

    /// Build a non-lazy list from the given collection. This function will eagerly
    /// evaluate the entire list (and thus may not terminate).
    [<CompiledName("ToList")>]
    let toList (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        let rec loop acc (list : LazyList<'T>) =
            match list.Value with
            | Empty ->
                List.rev acc
            | Cons (hd, tl) ->
                loop (hd :: acc) tl
        loop [] list

    /// Build an array from the given LazyList. This function will eagerly
    /// evaluate the entire list (and thus may not terminate).
    [<CompiledName("ToArray")>]
    let toArray (list : LazyList<'T>) =
        // Preconditions
        checkNonNull "list" list

        // Iterate over the LazyList and copy its elements into a ResizeArray.
        let elements = ResizeArray ()
        iter elements.Add list
        elements.ToArray()


type LazyList<'T> with
    /// Appends the second LazyList to the end of the first.
    static member op_PlusPlus (list1 : LazyList<'T>, list2 : LazyList<'T>) : LazyList<'T> =
        // Preconditions
        checkNonNull "list1" list1
        checkNonNull "list2" list2

        LazyList.append list1 list2


/// Active patterns for deconstructing lazy lists.
[<AutoOpen>]
module LazyListPatterns =
    // Active pattern for deconstructing lazy lists.
    let inline (|Nil|Cons|) (list : LazyList<'T>) =
        match list.TryHeadTail () with
        | None ->
            Nil
        | Some hd_tl ->
            Cons hd_tl