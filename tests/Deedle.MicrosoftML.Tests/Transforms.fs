module Deedle.MicrosoftML.Tests.Transforms

open NUnit.Framework
open Deedle
open Deedle.MicrosoftML
open Microsoft.ML

// ── helpers ──────────────────────────────────────────────────────────────────

let private mlc = MLContext(seed = 1)

let private normFrame () : Frame<int, string> =
    Frame.ofColumns [
        "A", Series.ofValues [ 0.0; 5.0; 10.0 ] :> ISeries<int>
        "B", Series.ofValues [ 1.0; 3.0;  5.0 ] :> ISeries<int>
    ]

// ── Pipeline helpers ─────────────────────────────────────────────────────────

[<Test>]
let ``fitTransform adds output column to result frame`` () =
    let pipe   = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let result = Pipeline.fitTransform pipe (normFrame())
    Assert.That(result.ColumnKeys |> Seq.contains "A_norm", Is.True)
    Assert.That(result.ColumnKeys |> Seq.contains "A",      Is.True)

[<Test>]
let ``fitTransform NormalizeMinMax produces values in [0, 1]`` () =
    let pipe   = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let result = Pipeline.fitTransform pipe (normFrame())
    let col    = result.GetColumn<float>("A_norm")
    for v in col.Values do
        Assert.That(v, Is.GreaterThanOrEqualTo(0.0))
        Assert.That(v, Is.LessThanOrEqualTo(1.0))

[<Test>]
let ``fitEstimator returns transformer that can be applied separately`` () =
    let pipe        = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let transformer = Pipeline.fitEstimator pipe (normFrame())
    let result      = Pipeline.applyTransformer transformer (normFrame())
    Assert.That(result.ColumnKeys |> Seq.contains "A_norm", Is.True)

[<Test>]
let ``applyTransformer uses the fitted range from training data`` () =
    let train = normFrame()    // A in [0..10]
    let test  : Frame<int, string> =
        Frame.ofColumns [
            "A", Series.ofValues [ 2.5; 7.5 ] :> ISeries<int>
            "B", Series.ofValues [ 2.0; 4.0 ] :> ISeries<int>
        ]
    let pipe        = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let transformer = Pipeline.fitEstimator pipe train
    let result      = Pipeline.applyTransformer transformer test
    // With min=0, max=10: 2.5 → 0.25, 7.5 → 0.75
    let col = result.GetColumn<float>("A_norm")
    Assert.That(col.[0], Is.EqualTo(0.25).Within(1e-9))
    Assert.That(col.[1], Is.EqualTo(0.75).Within(1e-9))

[<Test>]
let ``fitTransformOn uses training frame for fit, test frame for transform`` () =
    let train = normFrame()
    let test  : Frame<int, string> =
        Frame.ofColumns [
            "A", Series.ofValues [ 0.0; 10.0 ] :> ISeries<int>
            "B", Series.ofValues [ 1.0;  5.0 ] :> ISeries<int>
        ]
    let pipe   = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let result = Pipeline.fitTransformOn pipe train test
    let col    = result.GetColumn<float>("A_norm")
    Assert.That(col.[0], Is.EqualTo(0.0).Within(1e-9))
    Assert.That(col.[1], Is.EqualTo(1.0).Within(1e-9))

// ── Extension methods ─────────────────────────────────────────────────────────

[<Test>]
let ``ToDataView extension produces correct schema`` () =
    let dv     = normFrame().ToDataView()
    Assert.That(Seq.length dv.Schema, Is.EqualTo(2))

[<Test>]
let ``FitTransform extension method works`` () =
    let pipe   = mlc.Transforms.NormalizeMinMax("A_norm", "A")
    let result = normFrame().FitTransform(pipe)
    Assert.That(result.ColumnKeys |> Seq.contains "A_norm", Is.True)
