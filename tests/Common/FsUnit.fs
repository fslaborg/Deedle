module FsUnit

open System.Diagnostics
open NUnit.Framework
open NUnit.Framework.Constraints

[<DebuggerNonUserCode>]
let should (f : 'a -> #Constraint) x (y : obj) =
    let c = f x
    let y =
        match y with
        | :? (unit -> unit) -> box (new TestDelegate(y :?> unit -> unit))
        | _                 -> y
    Assert.That(y, c)

let equal x = new EqualConstraint(x)

// like "should equal", but validates same-type
let shouldEqual (x: 'a) (y: 'a) = Assert.AreEqual(x, y, sprintf "Expected: %A\nActual: %A" x y)

let notEqual x = new NotConstraint(new EqualConstraint(x))

let contain x = new ContainsConstraint(x)

let haveLength n = Has.Length.EqualTo(n)

let haveCount n = Has.Count.EqualTo(n)

let be = id

let Null = new NullConstraint()

let Empty = new EmptyConstraint()

let EmptyString = new EmptyStringConstraint()

let NullOrEmptyString = new NullOrEmptyStringConstraint()

let True = new TrueConstraint()

let False = new FalseConstraint()

let sameAs x = new SameAsConstraint(x)

let throw = Throws.TypeOf

type Recorder<'T>() =
  let mutable xs = []
  member recorder.Record(x:'T) = xs <- x :: xs
  member recorder.Values = xs

let inline spy1 (recorder:Recorder<'T>) f = fun p -> recorder.Record(p); f p
let inline spy2 (recorder:Recorder<'T1 * 'T2>) f = fun p1 p2 -> recorder.Record( (p1, p2) ); f p1 p2 
