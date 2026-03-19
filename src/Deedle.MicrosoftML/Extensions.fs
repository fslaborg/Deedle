/// C#-friendly extension methods for Deedle Frames, exposing ML.NET IDataView integration.
///
/// These mirror the static helpers in <c>Deedle.MicrosoftML.Frame</c> but can be called
/// as instance methods from C# or F# using method-call syntax.
namespace Deedle.MicrosoftML

open System.Runtime.CompilerServices
open Microsoft.ML
open Microsoft.ML.Data
open Deedle

/// Extension methods on <c>Frame&lt;'R, string&gt;</c> for ML.NET integration.
///
/// <category>ML.NET IDataView integration</category>
[<Extension>]
type FrameMLExtensions =

    /// <summary>
    /// Convert this Deedle Frame to an ML.NET <see cref="IDataView"/>.
    /// Equivalent to <c>Frame.toDataView frame</c>.
    /// </summary>
    [<Extension>]
    static member ToDataView(frame : Frame<'R, string>) : IDataView =
        Frame.toDataView frame

    /// <summary>
    /// Apply an already-fitted ML.NET transformer to this Frame,
    /// returning the transformed result as a new <c>Frame&lt;int, string&gt;</c>.
    /// Equivalent to <c>Pipeline.applyTransformer transformer frame</c>.
    /// </summary>
    [<Extension>]
    static member Transform(frame : Frame<'R, string>, transformer : ITransformer) : Frame<int, string> =
        Pipeline.applyTransformer transformer frame

    /// <summary>
    /// Fit an ML.NET estimator on this Frame and transform it, returning
    /// the result as a new <c>Frame&lt;int, string&gt;</c>.
    /// Equivalent to <c>Pipeline.fitTransform estimator frame</c>.
    /// </summary>
    [<Extension>]
    static member FitTransform(frame : Frame<'R, string>, estimator : IEstimator<'T>) : Frame<int, string>
            when 'T :> ITransformer =
        Pipeline.fitTransform estimator frame
