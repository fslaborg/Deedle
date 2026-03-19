/// Helpers for applying ML.NET transforms and estimators to Deedle Frames.
///
/// <code>
/// open Deedle
/// open Deedle.MicrosoftML
/// open Microsoft.ML
///
/// let mlc = MLContext(seed = 1)
/// let pipe = mlc.Transforms.NormalizeMinMax("A")
///
/// // Fit and transform in one step
/// let normalised = Frame.fitTransform mlc.Transforms pipe myFrame
/// </code>
namespace Deedle.MicrosoftML

open Microsoft.ML
open Microsoft.ML.Data
open Deedle

/// Static helpers that extend the <c>Frame</c> concept with ML.NET pipeline operations.
/// Open <c>Deedle.MicrosoftML</c> to use these alongside the standard Deedle API.
///
/// <category>ML.NET pipeline integration</category>
type Pipeline =

    /// <summary>
    /// Apply an already-fitted ML.NET <see cref="ITransformer"/> to a Deedle Frame,
    /// returning the transformed data as a new <c>Frame&lt;int, string&gt;</c>.
    /// </summary>
    /// <param name="transformer">A fitted ML.NET transformer.</param>
    /// <param name="frame">The input Deedle frame.</param>
    static member applyTransformer
            (transformer : ITransformer)
            (frame       : Frame<'R, string>) : Frame<int, string> =
        let dv     = Frame.toDataView frame
        let result = transformer.Transform(dv)
        Frame.ofDataView result

    /// <summary>
    /// Fit an ML.NET estimator on a Deedle Frame and return the fitted model
    /// without transforming.
    /// </summary>
    /// <param name="estimator">An ML.NET estimator.</param>
    /// <param name="frame">The training data as a Deedle frame.</param>
    static member fitEstimator
            (estimator : IEstimator<'T>)
            (frame     : Frame<'R, string>) : 'T =
        estimator.Fit(Frame.toDataView frame)

    /// <summary>
    /// Fit an ML.NET estimator on a Deedle Frame and immediately apply the
    /// resulting transform, returning the output as a new <c>Frame&lt;int, string&gt;</c>.
    /// </summary>
    /// <param name="estimator">An ML.NET estimator whose output transformer implements <see cref="ITransformer"/>.</param>
    /// <param name="frame">The training data as a Deedle frame.</param>
    static member fitTransform
            (estimator : IEstimator<'T>)
            (frame     : Frame<'R, string>) : Frame<int, string>
            when 'T :> ITransformer =
        let dv          = Frame.toDataView frame
        let transformer = estimator.Fit(dv) :> ITransformer
        Frame.ofDataView (transformer.Transform(dv))

    /// <summary>
    /// Fit an estimator on a training frame, then apply it to a separate
    /// prediction/test frame and return the result.
    /// </summary>
    /// <param name="estimator">An ML.NET estimator.</param>
    /// <param name="trainFrame">Training data.</param>
    /// <param name="testFrame">Data to transform using the fitted model.</param>
    static member fitTransformOn
            (estimator  : IEstimator<'T>)
            (trainFrame : Frame<'R, string>)
            (testFrame  : Frame<'S, string>) : Frame<int, string>
            when 'T :> ITransformer =
        let trainDv     = Frame.toDataView trainFrame
        let testDv      = Frame.toDataView testFrame
        let transformer = estimator.Fit(trainDv) :> ITransformer
        Frame.ofDataView (transformer.Transform(testDv))
