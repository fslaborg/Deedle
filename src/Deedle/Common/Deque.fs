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
    
    let copyArray size = 
        let newArray = Array.zeroCreate size
        if 0 <> startOffset && startOffset + count >= capacity then 
            let lengthFromStart = capacity - startOffset
            let lengthFromEnd = count - lengthFromStart
            Array.Copy(buffer, startOffset, newArray, 0, lengthFromStart)
            Array.Copy(buffer, 0, newArray, lengthFromStart, lengthFromEnd)
        else Array.Copy(buffer, startOffset, newArray, 0, count)
        newArray
    
    /// Sets the total number of elements the internal array can hold without resizing.
    let doubleCapacity() = 
        if capacity < 0 then raise <| new ArgumentOutOfRangeException("value", "Capacity is less than 0.")
        if capacity < count then 
            raise <| new InvalidOperationException("Capacity cannot be set to a value less than Count")
        if capacity <= buffer.Length then ()
        else 
            // Set up to use the new buffer.
            capacity <- capacity * 2
            buffer <- copyArray (capacity)
            capacityClosestPowerOfTwoMinusOne <- capacity - 1
            startOffset <- 0
    
    let toBufferIndex index = (index + startOffset) &&& capacityClosestPowerOfTwoMinusOne
    
    let iterate() = 
        seq { 
            if startOffset + count > capacity then 
                for i in startOffset..capacity - 1 do
                    yield buffer.[i]
                let endIndex = toBufferIndex (count - startOffset - 1)
                for i in 0..endIndex - 1 do
                    yield buffer.[i]
            else 
                let endIndex = startOffset + count
                for i in startOffset..endIndex - 1 do
                    yield buffer.[i]
        }
    
    let preShiftStartOffset value = 
        let offset = startOffset
        startOffset <- toBufferIndex value
        offset
    
    /// Creates a new Deque with the default capacity
    new() = Deque<'T>(16)
    
    /// Gets the total number of elements the internal array can hold without resizing.
    member this.Capacity = buffer.Length
    
    /// Gets the number of elements contained in the Deque.
    member this.Count = count
    
    /// Gets whether or not the Deque is filled to capacity.
    member this.IsFull = count >= capacity
    
    /// Gets whether or not the Deque is empty.
    member this.IsEmpty = count = 0
    
    /// Adds the provided item to the back of the Deque.
    member this.Add(item) = 
        if count >= capacity then doubleCapacity()
        buffer.[toBufferIndex (startOffset + count)] <- item
        count <- count + 1
    
    /// Removes an item from the front of the Deque and returns it.
    member this.RemoveFirst() = 
        if count = 0 then raise <| new InvalidOperationException("The Deque is empty")
        let item = buffer.[preShiftStartOffset 1]
        count <- count - 1
        item
    
    /// Removes an item from the back of the Deque and returns it.
    member this.RemoveLast() = 
        if count = 0 then raise <| new InvalidOperationException("The Deque is empty")
        count <- count - 1
        buffer.[toBufferIndex count]
    
    /// Gets the value at the specified index of the Deque
    member this.Get(index) = 
        if index >= count then raise <| new IndexOutOfRangeException("The supplied index is greater than the Count")
        buffer.[toBufferIndex index]
    
    /// Gets the element at the front
    member this.First = 
        if count = 0 then raise <| new InvalidOperationException("The Deque is empty")
        buffer.[toBufferIndex 0]
    
    /// Gets the element at the end
    member this.Last = 
        if count = 0 then raise <| new InvalidOperationException("The Deque is empty")
        buffer.[toBufferIndex (count - 1)]
    
    interface System.Collections.Generic.IEnumerable<'T> with
        member this.GetEnumerator() = iterate().GetEnumerator()
    
    interface System.Collections.IEnumerable with
        member this.GetEnumerator() = (iterate().GetEnumerator()) :> System.Collections.IEnumerator
