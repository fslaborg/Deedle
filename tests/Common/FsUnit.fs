// TODO : Add license header

namespace FsUnit

open NUnit.Framework
open NUnit.Framework.Constraints
open System.Diagnostics

type Recorder<'T>() =
  let mutable xs = []
  member recorder.Record(x:'T) = xs <- x :: xs
  member recorder.Values = xs

[<AutoOpen>]
module Extensions =
  // like "should equal", but validates same-type
  let shouldEqual (x: 'a) (y: 'a) = Assert.AreEqual(x, y, sprintf "Expected: %A\nActual: %A" x y)
  let shouldThrow<'T> f = 
    try 
      f() 
      Assert.Fail("Expected failure, but the operation succeeded.")
    with e ->
      if e.GetType() <> typeof<'T> then
        Assert.Fail(sprintf "Expected exception '%s' but got '%s'." typeof<'T>.Name (e.GetType().Name))

  let notEqual x = new NotConstraint(new EqualConstraint(x))

  type Range = Within of float * float
  let (+/-) (a:float) b = Within(a, b)

  let beWithin (Within(x, within)) =
    (new NUnit.Framework.Constraints.EqualConstraint(x)).Within(within)

  let inline spy1 (recorder:Recorder<'T>) f = fun p -> recorder.Record(p); f p
  let inline spy2 (recorder:Recorder<'T1 * 'T2>) f = fun p1 p2 -> recorder.Record( (p1, p2) ); f p1 p2 
