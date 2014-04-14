namespace Deedle

open System
open System.Collections
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.InteropServices


[<SerializableAttribute>]
type Deque<'V>
    public (capacity:int) as this=

    //#region Main private constructor

    // View Index - external index of SortedList, internally used as "index"
    // Buffer Index - index in buffer arrays, internally used as "idx"

        
    [<DefaultValue>] val mutable internal values : 'V array 

    [<DefaultValue>] val mutable internal offset : int
    [<DefaultValue>] val mutable internal size : int
    [<DefaultValue>] val mutable internal version : int
    [<DefaultValue>] val mutable internal isReadOnly : bool
    [<DefaultValue>] val mutable internal capacity : int
    let maxSize = 2146435071

    let syncObject = new Object()

    do
        this.values <- Array.zeroCreate capacity
        this.capacity <- capacity
        //this.Capacity <- capacity
    //#endregion

    //#region Private & Internal members

    member private this.PostIncrement(value) =
        let ret = this.offset
        this.offset <- (this.offset + value) % capacity
        ret

    member private this.PreDecrement(value) =
        this.offset <- this.offset - value
        if this.offset < 0 then this.offset <- this.offset + capacity
        this.offset

    /// Calculates buffer index from view index
    member private this.IndexToBufferIndex(index) = 
        (index + this.offset) % capacity

    member internal this.GetByViewIndex(index) = 
        let idx = this.IndexToBufferIndex(index)
        this.values.[idx]

    member internal this.SetByViewIndex(index, v) =
        //use lock = makeLock this.SyncRoot
        let idx = this.IndexToBufferIndex(index)
        this.values.[idx] <- v
        this.version <- this.version + 1
        ()
    
    member internal this.AddToBack(v) =
        Debug.Assert(not this.IsFull)
        //use lock = makeLock this.SyncRoot
        let idx = this.IndexToBufferIndex(this.size)
        this.values.[idx] <- v
        this.size <- this.size + 1
        this.version <- this.version + 1
        ()

    member internal this.AddToFront(v) =
        Debug.Assert(not this.IsFull)
        //use lock = makeLock this.SyncRoot
        let idx = this.PreDecrement(1)
        this.values.[idx] <- v
        this.size <- this.size + 1
        this.version <- this.version + 1
        ()

    member internal this.InsertAtIndex(index, v) =
        this.EnsureCapacity()
        Debug.Assert(1 + this.size <= capacity)
        if this.IsEmpty || index = this.size then
            this.AddToBack(v)
        elif index = 0 then
            this.AddToFront(v)
        else
            //use lock = makeLock this.SyncRoot
            if index < this.size / 2 then
                let copyCount = index
                let writeIndex = capacity - 1
                for j in 0..copyCount-1 do
                    this.values.[this.IndexToBufferIndex(writeIndex + j)] <- this.values.[this.IndexToBufferIndex(j)]
                this.PreDecrement(1) |> ignore
            else
                let copyCount = this.size - index
                let writeIndex = index + 1
                for j in copyCount-1..0 do
                    this.values.[this.IndexToBufferIndex(writeIndex + j)] <- this.values.[this.IndexToBufferIndex(index + j)]
            let idx = this.IndexToBufferIndex(index)
            this.values.[idx] <- v
            this.version <- this.version + 1
            this.size <- this.size + 1

    member internal this.RemoveFromBack() : 'V =
        Debug.Assert(not this.IsEmpty)
        //use lock = makeLock this.SyncRoot
        let idx = this.IndexToBufferIndex(this.size - 1)
        this.size <- this.size - 1
        this.version <- this.version + 1
        this.values.[idx]

    member internal this.RemoveFromFront() : 'V =
        Debug.Assert(not this.IsEmpty)
        //use lock = makeLock this.SyncRoot
        this.size <- this.size - 1
        let idx = this.PostIncrement(1)
        this.version <- this.version + 1
        this.values.[idx]

    member internal this.RemoveAtIndex(index)=
        //use lock = makeLock this.SyncRoot
        if index = 0 then
            this.PostIncrement(1) |> ignore
        elif index = this.size - 1 then
            ()
        else
            if index < this.size / 2 then
                let copyCount = index
                for j in copyCount-1..0 do
                    this.values.[this.IndexToBufferIndex(j + 1)] <- this.values.[this.IndexToBufferIndex(j)]
                this.PostIncrement(1) |> ignore
            else
                let copyCount = this.size - index - 1
                let readIndex = index + 1
                for j in 0..copyCount-1 do
                    this.values.[this.IndexToBufferIndex(j + index)] <- this.values.[this.IndexToBufferIndex(readIndex + j)]
        this.version <- this.version + 1
        this.size <- this.size - 1

    member internal this.EnsureCapacity(?min) =
        let mutable num = this.values.Length * 2
        if num > maxSize then num <- maxSize
        if min.IsSome && num < min.Value then num <- min.Value
        if this.size = this.values.Length then this.Capacity <- num

    
    member internal this.GetValueByIndex(index) = 
        if index < 0 || index >= this.size then 
            raise (ArgumentOutOfRangeException("index"))
        this.values.[this.IndexToBufferIndex(index)]

    member internal this.IsFull with get() = this.size = capacity

    member internal this.IsSplit with get() = this.offset > (capacity - this.size)

    member internal this.IsReadOnly with get() = this.isReadOnly

    member internal this.Offset with get() = this.offset

    member internal this.SyncRoot with get() = syncObject

    member internal this.Version with get() = this.version

    /// Removes first element to free space for new element if the map is full. 
    member internal this.AddAndRemoveFisrtIfFull(value) =
        //use lock = makeLock this.SyncRoot
        if this.IsFull then this.RemoveFromFront() |> ignore
        this.Add(value)
        ()

    //#endregion

    //#region Public members

    ///Gets or sets the capacity. This value must always be greater than zero, and this property cannot be set to a value less than this.size/>.
    member this.Capacity 
        with get() = capacity
        and set(value) =
            //use lock = makeLock this.SyncRoot
            match value with
            | c when c = this.values.Length -> ()
            | c when c < this.size -> raise (ArgumentOutOfRangeException("Small capacity"))
            | c when c > 0 -> 
                let vArr : 'V array = Array.zeroCreate c
                if this.IsSplit then
                    let len = capacity - this.offset
                    // Values
                    Array.Copy(this.values, this.offset, vArr, 0, len);
                    Array.Copy(this.values, 0, vArr, len, this.size - len);
                else
                    Array.Copy(this.values, this.offset, vArr, 0, this.size)
                this.values <- vArr
                this.offset <- 0
                //this.version <- this.version + 1
                this.capacity <- value
            | _ -> ()


    member this.Clear()=
        this.version <- this.version + 1
        Array.Clear(this.values, 0, this.size)
        this.offset <- 0
        this.size <- 0

    member this.Count with get() = this.size

    member this.IsEmpty with get() = this.size = 0

    member this.Values 
        with get() : IList<'V> =
            { new IList<'V> with
                member x.Count with get() = this.size
                member x.IsReadOnly with get() = true
                member x.Item 
                    with get index : 'V = this.GetValueByIndex(index)
                    and set index value = raise (NotSupportedException("Values colelction is read-only"))
                member x.Add(k) = raise (NotSupportedException("Values colelction is read-only"))
                member x.Clear() = raise (NotSupportedException("Values colelction is read-only"))
                member x.Contains(value) = this.ContainsValue(value)
                member x.CopyTo(array, arrayIndex) = 
                    Array.Copy(this.values, 0, array, arrayIndex, this.size)
                member x.IndexOf(value:'V) = this.IndexOfValue(value)
                member x.Insert(index, value) = raise (NotSupportedException("Values colelction is read-only"))
                member x.Remove(value:'V) = raise (NotSupportedException("Values colelction is read-only"))
                member x.RemoveAt(index:int) = raise (NotSupportedException("Values colelction is read-only"))
                member x.GetEnumerator() = x.GetEnumerator() :> IEnumerator
                member x.GetEnumerator() : IEnumerator<'V> = 
                    let index = ref 0
                    let eVersion = ref this.version
                    let currentValue : 'V ref = ref Unchecked.defaultof<'V>
                    { new IEnumerator<'V> with
                        member e.Current with get() = currentValue.Value
                        member e.Current with get() = box e.Current
                        member e.MoveNext() = 
                            if eVersion.Value <> this.version then
                                raise (InvalidOperationException("Collection changed during enumeration"))
                            if index.Value < this.size then
                                currentValue := this.values.[index.Value]
                                index := index.Value + 1
                                true
                            else
                                index := this.size + 1
                                currentValue := Unchecked.defaultof<'V>
                                false
                        member e.Reset() = 
                            if eVersion.Value <> this.version then
                                raise (InvalidOperationException("Collection changed during enumeration"))
                            index := 0
                            currentValue := Unchecked.defaultof<'V>
                        member e.Dispose() = 
                            index := 0
                            currentValue := Unchecked.defaultof<'V>
                    }
            }

    member this.First 
        with get()=
            //use lock = makeLock this.SyncRoot
            if this.IsEmpty then OptionalValue.Missing
            else OptionalValue<'V>(this.GetByViewIndex(0)) 
            // TODO OMG this opt(k, opt v) nested opts are going deeper, will be too late to redesign!

    member this.Last 
        with get() =
            //use lock = makeLock this.SyncRoot
            if this.IsEmpty then OptionalValue.Missing
            else OptionalValue(this.GetByViewIndex(this.Count - 1))

    member this.ContainsValue(value) = this.IndexOfValue(value) >= 0

    member this.IndexOfValue(value:'V) : int =
        //use lock = makeLock this.SyncRoot
        let mutable res = 0
        let mutable found = false
        let valueComparer = Comparer<'V>.Default;
        while not found do
            if valueComparer.Compare(value,this.values.[res]) = 0 then
                found <- true
            else res <- res + 1
        if found then res else -1
   
    member this.GetEnumerator() : IEnumerator<'V> =
        let index = ref -1
        let pVersion = ref this.Version
        let currentValue : 'V ref = ref Unchecked.defaultof<'V>
        { new IEnumerator<'V> with
            member p.Current with get() = currentValue.Value

            member p.Current with get() = box p.Current

            member p.MoveNext() = 
                if pVersion.Value <> this.Version then
                    raise (InvalidOperationException("IEnumerable changed during MoveNext"))
                //use lock = makeLock this.SyncRoot
                if index.Value + 1 < this.Count then
                    index := index.Value + 1
                    currentValue := this.Values.[index.Value]
                    true
                else
                    index := this.Count + 1
                    currentValue := Unchecked.defaultof<'V>
                    false

            member p.Reset() = 
                if pVersion.Value <> this.Version then
                    raise (InvalidOperationException("IEnumerable changed during Reset"))
                index := 0
                currentValue := Unchecked.defaultof<'V>

            member p.Dispose() = 
                index := 0
                currentValue := Unchecked.defaultof<'V>

        }

    //#endregion


    //#region Virtual members

    abstract Add : v:'V -> unit
    default this.Add(value) =
        if (box value) = null then raise (ArgumentNullException("value"))
        //use lock = makeLock this.SyncRoot
        this.EnsureCapacity()
        this.AddToBack(value)
        ()

    abstract AddLast : v:'V -> unit
    default this.AddLast(value) = this.Add(value)

    abstract AddFirst : v:'V -> unit
    default this.AddFirst(value) =
        if (box value) = null then raise (ArgumentNullException("value"))
        //use lock = makeLock this.SyncRoot
        this.EnsureCapacity()
        this.AddToFront(value)
        ()

        
    abstract Item : int -> 'V with get, set
    default this.Item
        with get index =
            //use lock = makeLock this.SyncRoot
            this.GetValueByIndex(index)
        and set index v =
            //use lock = makeLock this.SyncRoot
            if index < 0 || index >= this.size then
                this.Add(v)
            else
                this.SetByViewIndex(index, v)

    abstract Remove: int -> bool
    default this.Remove(index) =
        //use lock = makeLock this.SyncRoot
        if index >= 0 then this.RemoveAtIndex(index)
        index >= 0

    abstract RemoveLast : unit -> 'V
    default this.RemoveLast() = 
        //use lock = makeLock this.SyncRoot
        if this.IsEmpty then raise (InvalidOperationException("Deque is empty"))
        this.RemoveFromBack()

    abstract RemoveFirst : unit -> 'V
    default this.RemoveFirst() =
        //use lock = makeLock this.SyncRoot
        if this.IsEmpty then raise (InvalidOperationException("Deque is empty"))
        this.RemoveFromFront()


    //#endregion

    //#region Interfaces

    interface IEnumerable with
        member this.GetEnumerator() = this.GetEnumerator() :> IEnumerator

    interface IEnumerable<'V> with
        member this.GetEnumerator() = this.GetEnumerator()

    interface ICollection  with
        member this.SyncRoot = this.SyncRoot
        member this.CopyTo(array, arrayIndex) =
            if array = null then raise (ArgumentNullException("array"))
            if arrayIndex < 0 || arrayIndex > array.Length then raise (ArgumentOutOfRangeException("arrayIndex"))
            if array.Length - arrayIndex < this.Count then raise (ArgumentException("ArrayPlusOffTooSmall"))
            for index in 0..this.size do
                let kvp = this.values.[index]
                array.SetValue(kvp, arrayIndex + index)
        member this.Count = this.Count
        member this.IsSynchronized with get() = false

    //#endregion

    //#region Constructors

    new() = Deque(8)
    //new(capacity:int) = Deque(capacity)

    //#endregion