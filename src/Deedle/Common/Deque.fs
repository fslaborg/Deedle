namespace Deedle

open System
open System.Collections
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.InteropServices

[<SerializableAttribute>]
type Deque<'T>(initCapacity : int) =
    let mutable capacity = initCapacity
    // The circular array holding the items.
    let mutable buffer : 'T array = Array.zeroCreate capacity
    
    // The first element offset from the beginning of the data array.
    let mutable startOffset = 0

    let mutable count = 0
    let mutable capacityClosestPowerOfTwoMinusOne = 0

    /// Creates a new Deque with the default capacity
    new() = Deque<'T>(16)

    /// Gets the total number of elements the internal array can hold without resizing.
    member this.Capacity = buffer.Length

    /// Gets the number of elements contained in the Deque.
    member this.Count = count

    member private this.CopyArray(size) =
        let newArray = Array.zeroCreate size

        if 0 <> startOffset && startOffset + this.Count >= this.Capacity then
            let lengthFromStart = this.Capacity - startOffset
            let lengthFromEnd = this.Count - lengthFromStart            

            Array.Copy(buffer, startOffset, newArray, 0, lengthFromStart)

            Array.Copy(buffer, 0, newArray, lengthFromStart, lengthFromEnd)
        else
            Array.Copy(buffer, startOffset, newArray, 0, this.Count)
        newArray

    /// Sets the total number of elements the internal array can hold without resizing.
    member private this.DoubleCapacity() =
        if capacity < 0 then
            raise <| new ArgumentOutOfRangeException("value", "Capacity is less than 0.")

        if capacity < this.Count then
            raise <| new InvalidOperationException("Capacity cannot be set to a value less than Count")

        if capacity <= buffer.Length then () else

        // Set up to use the new buffer.
        capacity <- capacity * 2
        buffer <- this.CopyArray(capacity)
        capacityClosestPowerOfTwoMinusOne <- capacity - 1
        startOffset <- 0

    member private this.shiftStartOffset(value) =
        startOffset <- this.toBufferIndex(value)
        startOffset

    member private this.preShiftStartOffset(value) =
        let offset = startOffset
        this.shiftStartOffset(value) |> ignore
        offset

    /// Gets whether or not the Deque is filled to capacity.
    member this.IsFull = this.Count >= this.Capacity

    /// Gets whether or not the Deque is empty.
    member this.IsEmpty = this.Count = 0

    member this.toBufferIndex index =
        (index + startOffset) &&& capacityClosestPowerOfTwoMinusOne

    member this.iterate() =
        // The below is done for performance reasons.
        // Rather than doing bounds checking and modulo arithmetic
        // that would go along with calls to Get(index), we can skip
        // all of that by referencing the underlying array.
        seq {
            if startOffset + this.Count > this.Capacity then
            
                for i in startOffset..this.Capacity-1 do
                    yield buffer.[i]

                let endIndex = this.toBufferIndex(this.Count - startOffset - 1)

                for i in 0..endIndex-1 do
                    yield buffer.[i]           
            else            
                let endIndex = startOffset + this.Count
                for i in startOffset .. endIndex-1 do
                    yield buffer.[i]
        }

    /// Adds the provided item to the back of the Deque.
    member this.Add(item) =
        if this.Count >= this.Capacity then
            this.DoubleCapacity()

        buffer.[this.toBufferIndex(startOffset + this.Count)] <- item
        count <- count + 1

    /// Removes an item from the front of the Deque and returns it.
    member this.RemoveFirst() =
        if this.IsEmpty then
            raise <| new InvalidOperationException("The Deque is empty")

        let item = buffer.[this.preShiftStartOffset(1)]
        count <- count - 1
        item

    /// Removes an item from the back of the Deque and returns it.
    member this.RemoveLast() =
        if this.IsEmpty then
            raise <| new InvalidOperationException("The Deque is empty")

        count <- count - 1
        buffer.[this.toBufferIndex(this.Count)]

    /// Gets the value at the specified index of the Deque
    member this.Get(index) =
        if index >= this.Count then
            raise <| new IndexOutOfRangeException("The supplied index is greater than the Count")

        buffer.[this.toBufferIndex(index)]

    /// Gets the element at the front
    member this.First =
        if count = 0 then
            raise <| new InvalidOperationException("The Deque is empty")

        buffer.[this.toBufferIndex(0)]

    /// Gets the element at the end
    member this.Last =
        if count = 0 then
            raise <| new InvalidOperationException("The Deque is empty")

        buffer.[this.toBufferIndex(count - 1)]

    interface System.Collections.Generic.IEnumerable<'T> with
        member this.GetEnumerator() =
          this.iterate().GetEnumerator()

    interface System.Collections.IEnumerable with
        member this.GetEnumerator () =
          (this.iterate().GetEnumerator())
            :> System.Collections.IEnumerator        
