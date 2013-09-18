namespace FSharp.DataFrame

open System
open System.Linq
open System.Collections.Generic
open System.Runtime.InteropServices
open FSharp.DataFrame.Indices
open System.Runtime.CompilerServices

[<AutoOpen>]
module FSharpSeriesExtensions =
  open System

  type Series = 
    static member ofObservations(observations) = 
      Series(Seq.map fst observations, Seq.map snd observations)
    static member ofValues(values) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values)
    static member ofNullables(values:seq<Nullable<_>>) = 
      let keys = values |> Seq.mapi (fun i _ -> i)
      Series(keys, values).Select(fun kvp -> kvp.Value.Value)


type Series =
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromObservations(observations:seq<KeyValuePair<'K, 'V>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofObservations
  [<CompilerMessage("This method is not intended for use from F#.", 10001, IsHidden=true, IsError=false)>]
  static member FromValues(values:seq<'T>) = Series(values |> Seq.mapi (fun i _ -> i), values)
    
[<Extension>]
type EnumerableExtensions =
  [<Extension>]
  static member ToSeries(observations:seq<KeyValuePair<'K, 'V>>) = 
    observations |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Series.ofObservations

type Series with
  /// Vector & index builders
  static member internal vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance
  static member internal indexBuilder = Indices.Linear.LinearIndexBuilder.Instance

  static member internal Create(data:seq<'V>) =
    let lookup = data |> Seq.mapi (fun i _ -> i)
    Series<int, 'V>(Index.Create(lookup), Vector.ofValues(data), Series.vectorBuilder, Series.indexBuilder)
  static member internal Create(index:seq<'K>, data:seq<'V>) =
    Series<'K, 'V>(Index.Create(index), Vector.ofValues(data), Series.vectorBuilder, Series.indexBuilder)
  static member internal Create(index:IIndex<'K>, data:IVector<'V>) = 
    Series<'K, 'V>(index, data, Series.vectorBuilder, Series.indexBuilder)
  static member internal CreateUntyped(index:IIndex<'K>, data:IVector<obj>) = 
    ObjectSeries<'K>(index, data, Series.vectorBuilder, Series.indexBuilder)

type SeriesBuilder<'K when 'K : equality>() = 
  let mutable keys = []
  let mutable values = []

  member x.Add<'V>(key:'K, value) =
    keys <- key::keys
    values <- (box value)::values
  
  member x.Series =
    Series.CreateUntyped(Index.Create (List.rev keys), Vector.ofValues(List.rev values))

  static member (?<-) (builder:SeriesBuilder<string>, name:string, value) =
    builder.Add(name, value)
  
[<Extension>]
type SeriesExtensions =
  [<Extension>]
  static member Between(series:Series<'K, 'V>, lowerInclusive, upperInclusive) = 
    series.GetSubrange
      ( Some(lowerInclusive, BoundaryBehavior.Inclusive),
        Some(upperInclusive, BoundaryBehavior.Inclusive) )

  [<Extension>]
  static member After(series:Series<'K, 'V>, lowerExclusive) = 
    series.GetSubrange( Some(lowerExclusive, BoundaryBehavior.Exclusive), None )

  [<Extension>]
  static member Before(series:Series<'K, 'V>, upperExclusive) = 
    series.GetSubrange( None, Some(upperExclusive, BoundaryBehavior.Exclusive) )

  [<Extension>]
  static member Log(series:Series<'K, float>) = log series

  [<Extension>]
  static member Shift(series:Series<'K, 'V>, offset) = Series.shift offset series

  [<Extension>]
  static member Sum(series:Series<'K, float>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, int>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, float32>) = Series.sum series

  [<Extension>]
  static member Sum(series:Series<'K, decimal>) = Series.sum series

  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, value:'T) = 
    Series.fillMissingWith value

  /// Fill missing values in the series with the nearest available value
  /// (using the specified direction). The default direction is `Direction.Backward`.
  /// Note that the series may still contain missing values after call to this 
  /// function. This operation can only be used on ordered series. 
  ///
  /// Example:
  ///
  ///     let sample = Series.ofValues [ Double.NaN; 1.0; Double.NaN; 3.0 ]
  ///
  ///     // Returns a series consisting of [1; 1; 3; 3]
  ///     sample.FillMissing(Direction.Backward)
  ///
  ///     // Returns a series consisting of [<missing>; 1; 1; 3]
  ///     sample.FillMissing(Direction.Forward)
  ///
  /// Parameters:
  ///  * `direction` - Specifies the direction used when searching for 
  ///    the nearest available value. `Backward` means that we want to
  ///    look for the first value with a smaller key while `Forward` searches
  ///    for the nearest greater key.
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, [<Optional>] direction) = 
    Series.fillMissing direction

  /// Fill missing values in the series using the specified function.
  ///
  /// Parameters:
  ///  * `filler` - A `Func` delegate that calculates the filling value
  ///    based on the key in the series.
  [<Extension>]
  static member FillMissing(series:Series<'K, 'T>, filler:Func<_, _>) = 
    Series.fillMissingUsing filler.Invoke

  /// Returns all keys from the sequence, together with the associated (optional)
  /// values. The values are returned using the `OptionalValue<T>` struct which
  /// provides `HasValue` for testing if the value is available.
  [<Extension>]
  static member GetAllObservations(series:Series<'K, 'T>) = seq {
    for key, address in series.Index.Mappings ->
      KeyValuePair(key, series.Vector.GetValue(address)) }

  /// Return observations with available values. The operation skips over 
  /// all keys with missing values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  [<Extension>]
  static member GetObservations(series:Series<'K, 'T>) = seq { 
    for key, address in series.Index.Mappings do
      let v = series.Vector.GetValue(address)
      if v.HasValue then yield KeyValuePair(key, v.Value) }

  /// Returns the total number of values in the specified series. This excludes
  /// missing values or not available values (such as values created from `null`,
  /// `Double.NaN`, or those that are missing due to outer join etc.).
  [<Extension>]
  static member CountValues(series:Series<'K, 'T>) = Series.countValues series

  /// Returns the total number of keys in the specified series. This returns
  /// the total length of the series, including keys for which there is no 
  /// value available.
  [<Extension>]
  static member CountKeys(series:Series<'K, 'T>) = Series.countKeys series


  // static member Where(series:Series<'K, 'T>, f:System.Func<KeyValuePair<'K, 'V>, bool>) = 


  [<Extension>]
  static member Diff(series:Series<'K, float>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, float32>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, decimal>, offset) = series |> Series.diff offset
  [<Extension>]
  static member Diff(series:Series<'K, int>, offset) = series |> Series.diff offset