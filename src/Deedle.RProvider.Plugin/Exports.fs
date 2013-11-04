namespace Deedle.RProvider.Plugin

open System
open Deedle
open RDotNet
open System.ComponentModel.Composition
open Deedle.RProvider.Plugin.Conversions
open global.RProvider
open global.RProvider.``base``

// ------------------------------------------------------------------------------------------------
// IDefaultConvertFromR - convert Deedle frame to R symexpr
// ------------------------------------------------------------------------------------------------

[<Export(typeof<IConvertToR<IFrame>>)>]
type DataFrameToR() =
  interface IConvertToR<IFrame> with
    member x.Convert(engine, input) =
      { new IFrameOperation<_> with
          member x.Invoke(frame) =
            let args = 
              [ for r, c in frame.Columns |> Series.observations do
                  yield convertKey r, box [| 0 |] ] 
            let rowNames = "row.names", box (frame.RowKeys |> Seq.map convertKey)
            R.data_frame(namedParams (rowNames::args)) }
      |> input.Apply

  
[<Export(typeof<IConvertToR<Series<int, double>>>)>]
type IndexedSeriesDoubleConverter() =
    interface IConvertToR<Series<int, double>> with
        member x.Convert(engine, series) =
            let v = engine.CreateNumericVector(series.Values)
            v:> SymbolicExpression
(*
[<Export(typeof<IConvertToR<DateTime>>)>]
type DateTimeConverter() =
    interface IConvertToR<DateTime> with        
        member this.Convert(engine: REngine, x: DateTime) =            
            R.as_POSIXct(String.Format("{0:u}", x.ToUniversalTime(), "UTC"))

[<Export(typeof<IConvertToR<seq<DateTime>>>)>]
type DateTimeSeqConverter() =
    interface IConvertToR<seq<DateTime>> with        
        member this.Convert(engine: REngine, xs: seq<DateTime>) =            
            let dts = xs |> Seq.map (fun dt -> String.Format("{0:u}", dt.ToUniversalTime()))
            R.as_POSIXct(dts, "UTC")
*)
// ------------------------------------------------------------------------------------------------
// IDefaultConvertFromR - convert R symexpr to some data frame and return
// ------------------------------------------------------------------------------------------------

[<Export(typeof<IDefaultConvertFromR>)>]
type DataFrameDefaultFromR() = 
  interface IDefaultConvertFromR with
    member x.Convert(symExpr) = createDefaultFrame symExpr

// ------------------------------------------------------------------------------------------------
// IDefaultConvertFromR - convert Deedle frame to R symexpr
// ------------------------------------------------------------------------------------------------

// String row

[<Export(typeof<IConvertFromR<Frame<string, string>>>)>]
type DataFrameStringStringFromR() =
  interface IConvertFromR<Frame<string, string>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<string, int>>>)>]
type DataFrameStringIntFromR() =
  interface IConvertFromR<Frame<string, int>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<string, DateTime>>>)>]
type DataFrameStringDateFromR() =
  interface IConvertFromR<Frame<string, DateTime>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<string, bool>>>)>]
type DataFrameStringBoolFromR() =
  interface IConvertFromR<Frame<string, bool>> with member x.Convert(symExpr) = tryCreateFrame symExpr

// Int row

[<Export(typeof<IConvertFromR<Frame<int, string>>>)>]
type DataFrameIntStringFromR() =
  interface IConvertFromR<Frame<int, string>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<int, int>>>)>]
type DataFrameIntIntFromR() =
  interface IConvertFromR<Frame<int, int>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<int, DateTime>>>)>]
type DataFrameIntDateFromR() =
  interface IConvertFromR<Frame<int, DateTime>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<int, bool>>>)>]
type DataFrameIntBoolFromR() =
  interface IConvertFromR<Frame<int, bool>> with member x.Convert(symExpr) = tryCreateFrame symExpr

// DateTime  row

[<Export(typeof<IConvertFromR<Frame<DateTime, string>>>)>]
type DataFrameDateStringFromR() =
  interface IConvertFromR<Frame<DateTime, string>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<DateTime, int>>>)>]
type DataFrameDateIntFromR() =
  interface IConvertFromR<Frame<DateTime, int>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<DateTime, DateTime>>>)>]
type DataFrameDateDateFromR() =
  interface IConvertFromR<Frame<DateTime, DateTime>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<DateTime, bool>>>)>]
type DataFrameDateBoolFromR() =
  interface IConvertFromR<Frame<DateTime, bool>> with member x.Convert(symExpr) = tryCreateFrame symExpr

// Bool row

[<Export(typeof<IConvertFromR<Frame<bool, string>>>)>]
type DataFrameBoolStringFromR() =
  interface IConvertFromR<Frame<bool, string>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<bool, int>>>)>]
type DataFrameBoolIntFromR() =
  interface IConvertFromR<Frame<bool, int>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<bool, DateTime>>>)>]
type DataFrameBoolDateFromR() =
  interface IConvertFromR<Frame<bool, DateTime>> with member x.Convert(symExpr) = tryCreateFrame symExpr

[<Export(typeof<IConvertFromR<Frame<bool, bool>>>)>]
type DataFrameBoolBoolFromR() =
  interface IConvertFromR<Frame<bool, bool>> with member x.Convert(symExpr) = tryCreateFrame symExpr

