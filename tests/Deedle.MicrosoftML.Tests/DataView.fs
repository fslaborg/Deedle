module Deedle.MicrosoftML.Tests.DataView

open System
open NUnit.Framework
open Deedle
open Deedle.MicrosoftML
open Microsoft.ML
open Microsoft.ML.Data

// ── helpers ──────────────────────────────────────────────────────────────────

let private mlc = MLContext(seed = 1)

let private numericFrame () : Frame<int, string> =
    Frame.ofColumns [
        "A", Series.ofValues [ 1.0; 2.0; 3.0 ] :> ISeries<int>
        "B", Series.ofValues [ 4.0; 5.0; 6.0 ] :> ISeries<int>
    ]

let private stringFrame () : Frame<int, string> =
    Frame.ofColumns [
        "Label", Series.ofValues [ "x"; "y"; "z" ] :> ISeries<int>
    ]

// ── Frame → IDataView ─────────────────────────────────────────────────────────

[<Test>]
let ``toDataView produces correct schema for numeric frame`` () =
    let dv     = Frame.toDataView (numericFrame())
    let schema = dv.Schema
    Assert.That(Seq.length schema, Is.EqualTo(2))
    Assert.That(schema.[0].Name, Is.EqualTo("A"))
    Assert.That(schema.[1].Name, Is.EqualTo("B"))
    Assert.That(schema.[0].Type, Is.EqualTo(NumberDataViewType.Double))
    Assert.That(schema.[1].Type, Is.EqualTo(NumberDataViewType.Double))

[<Test>]
let ``toDataView returns correct row count`` () =
    let dv = Frame.toDataView (numericFrame())
    Assert.That(dv.GetRowCount(), Is.EqualTo(Nullable<int64>(3L)))

[<Test>]
let ``toDataView cursor reads correct float values`` () =
    let dv     = Frame.toDataView (numericFrame())
    let schema = dv.Schema
    use cursor = dv.GetRowCursor(schema |> Seq.toArray, null)

    let aGetter = cursor.GetGetter<float>(schema.["A"])
    let bGetter = cursor.GetGetter<float>(schema.["B"])
    let read () =
        let mutable a = 0.0
        let mutable b = 0.0
        aGetter.Invoke(&a)
        bGetter.Invoke(&b)
        a, b

    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo((1.0, 4.0)))
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo((2.0, 5.0)))
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo((3.0, 6.0)))
    Assert.That(cursor.MoveNext(), Is.False)

[<Test>]
let ``toDataView produces Text column for string data`` () =
    let dv     = Frame.toDataView (stringFrame())
    let schema = dv.Schema
    Assert.That(schema.["Label"].Type, Is.EqualTo(TextDataViewType.Instance))

[<Test>]
let ``toDataView string cursor reads correct values`` () =
    let dv     = Frame.toDataView (stringFrame())
    let schema = dv.Schema
    use cursor = dv.GetRowCursor(schema |> Seq.toArray, null)
    let g = cursor.GetGetter<ReadOnlyMemory<char>>(schema.["Label"])
    let read () =
        let mutable v = ReadOnlyMemory<char>()
        g.Invoke(&v)
        v.ToString()

    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo("x"))
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo("y"))
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo("z"))

// ── IDataView → Frame ─────────────────────────────────────────────────────────

[<Test>]
let ``ofDataView round-trips numeric frame correctly`` () =
    let original = numericFrame()
    let result   = Frame.ofDataView (Frame.toDataView original)

    Assert.That(result.RowCount,    Is.EqualTo(original.RowCount))
    Assert.That(result.ColumnCount, Is.EqualTo(original.ColumnCount))

    let a = result.GetColumn<float>("A")
    let b = result.GetColumn<float>("B")
    Assert.That(a.[0], Is.EqualTo(1.0))
    Assert.That(a.[1], Is.EqualTo(2.0))
    Assert.That(a.[2], Is.EqualTo(3.0))
    Assert.That(b.[0], Is.EqualTo(4.0))
    Assert.That(b.[1], Is.EqualTo(5.0))
    Assert.That(b.[2], Is.EqualTo(6.0))

[<Test>]
let ``ofDataView round-trips string column correctly`` () =
    let result = Frame.ofDataView (Frame.toDataView (stringFrame()))
    let labels = result.GetColumn<string>("Label")
    Assert.That(labels.[0], Is.EqualTo("x"))
    Assert.That(labels.[1], Is.EqualTo("y"))
    Assert.That(labels.[2], Is.EqualTo("z"))

[<Test>]
let ``toDataView handles empty frame`` () =
    let empty : Frame<int, string> = Frame.ofColumns ([] : (string * Series<int, float>) list)
    let dv = Frame.toDataView empty
    Assert.That(dv.GetRowCount(), Is.EqualTo(Nullable<int64>(0L)))

[<Test>]
let ``toDataView preserves missing values as NaN`` () =
    let s  = Series.ofOptionalObservations [ 0, Some 1.0; 1, None; 2, Some 3.0 ]
    let df = Frame.ofColumns [ "X", s :> ISeries<int> ]
    let dv = Frame.toDataView df

    let schema = dv.Schema
    use cursor = dv.GetRowCursor(schema |> Seq.toArray, null)
    let g = cursor.GetGetter<float>(schema.["X"])
    let read () =
        let mutable v = 0.0
        g.Invoke(&v)
        v

    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo(1.0))
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(Double.IsNaN(read()), Is.True)
    Assert.That(cursor.MoveNext(), Is.True)
    Assert.That(read(), Is.EqualTo(3.0))

[<Test>]
let ``FrameDataView exposes RowCount property`` () =
    let dv = Frame.toDataView (numericFrame())
    match dv with
    | :? FrameDataView as fdv ->
        Assert.That(fdv.RowCount, Is.EqualTo(3))
    | _ ->
        Assert.Fail("Expected FrameDataView")
