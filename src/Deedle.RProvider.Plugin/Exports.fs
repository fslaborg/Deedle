namespace Deedle.RPlugin

open System
open System.Collections
open System.ComponentModel.Composition
open Deedle
open Deedle.RPlugin.Conversions
open RDotNet
open RProvider
open RProvider.``base``
open RProvider.zoo

// ------------------------------------------------------------------------------------------------
// IDefaultConvertFromR - convert Deedle frame & time series to R symexpr
// ------------------------------------------------------------------------------------------------

[<Export(typeof<IConvertToR<IFrame>>)>]
type DataFrameToR() =
  interface IConvertToR<IFrame> with
    member x.Convert(engine, input) =
      { new IFrameOperation<_> with
          member x.Invoke(frame) =
            let args = 
              [| for KeyValue(r, addr) in frame.ColumnIndex.Mappings do
                  let c = frame.Data.GetValue(addr)
                  if c.HasValue then 
                    let data = convertVector c.Value
                    yield data |] 
            
            let df = R.data_frame(paramArray=args)
            df.SetAttribute("names", frame.ColumnKeys |> Seq.map convertKey |> engine.CreateCharacterVector)
            df.SetAttribute("row.names", frame.RowKeys |> Seq.map convertKey |> engine.CreateCharacterVector)
            df }

      |> input.Apply

[<Export(typeof<IConvertToR<ISeries<DateTime>>>)>]
type TimeSeriesToR() =
  interface IConvertToR<ISeries<DateTime>> with
    member x.Convert(engine, series) = R.zoo(convertVector series.Vector, series.Index.Keys)

[<Export(typeof<IConvertToR<ISeries<DateTimeOffset>>>)>]
type TimeSerieOffsetsToR() =
  interface IConvertToR<ISeries<DateTimeOffset>> with
    member x.Convert(engine, series) = R.zoo(convertVector series.Vector, series.Index.Keys)

[<Export(typeof<IConvertToR<ISeries<int>>>)>]
type IntSeriesToR() =
  interface IConvertToR<ISeries<int>> with
    member x.Convert(engine, series) = R.zoo(convertVector series.Vector, series.Index.Keys)

[<Export(typeof<IConvertToR<ISeries<float>>>)>]
type FloatSeriesToR() =
  interface IConvertToR<ISeries<float>> with
    member x.Convert(engine, series) = R.zoo(convertVector series.Vector, series.Index.Keys)

[<Export(typeof<IConvertToR<ISeries<string>>>)>]
type StringSeriesToR() =
  interface IConvertToR<ISeries<string>> with
    member x.Convert(engine, series) = R.zoo(convertVector series.Vector, series.Index.Keys)

// ------------------------------------------------------------------------------------------------
// IDefaultConvertFromR - convert R symexpr to some data frame and return
// ------------------------------------------------------------------------------------------------

[<Export(typeof<IDefaultConvertFromR>)>]
type DataFrameDefaultFromR() = 
  interface IDefaultConvertFromR with
    member x.Convert(symExpr) = createDefaultFrame symExpr

[<Export(typeof<IDefaultConvertFromR>)>]
type SeriesDefaultFromR() = 
  interface IDefaultConvertFromR with
    member x.Convert(symExpr) = createDefaultSeries symExpr

// ------------------------------------------------------------------------------------------------
// IConvertFromR - convert Deedle series to R symexpr
// ------------------------------------------------------------------------------------------------

// Time series with DateTime keys

[<Export(typeof<IConvertFromR<Series<DateTime, DateTime>>>)>]
type SeriesDateDateromR() =
  interface IConvertFromR<Series<DateTime, DateTime>> with member x.Convert(symExpr) = tryCreateTimeSeries id symExpr

[<Export(typeof<IConvertFromR<Series<DateTime, int>>>)>]
type SeriesDateIntFromR() =
  interface IConvertFromR<Series<DateTime, int>> with member x.Convert(symExpr) = tryCreateTimeSeries id symExpr

[<Export(typeof<IConvertFromR<Series<DateTime, float>>>)>]
type SeriesDateFloatFromR() =
  interface IConvertFromR<Series<DateTime, float>> with member x.Convert(symExpr) = tryCreateTimeSeries id symExpr

[<Export(typeof<IConvertFromR<Series<DateTime, string>>>)>]
type SeriesDateStringFromR() =
  interface IConvertFromR<Series<DateTime, string>> with member x.Convert(symExpr) = tryCreateTimeSeries id symExpr

// Time series with DateTimeOffset keys

[<Export(typeof<IConvertFromR<Series<DateTimeOffset, DateTime>>>)>]
type SeriesDateOffsetDateromR() =
  interface IConvertFromR<Series<DateTimeOffset, DateTime>> with member x.Convert(symExpr) = tryCreateTimeSeries (fun dt -> DateTimeOffset(dt)) symExpr

[<Export(typeof<IConvertFromR<Series<DateTimeOffset, int>>>)>]
type SeriesDateOffsetIntFromR() =
  interface IConvertFromR<Series<DateTimeOffset, int>> with member x.Convert(symExpr) = tryCreateTimeSeries (fun dt -> DateTimeOffset(dt)) symExpr

[<Export(typeof<IConvertFromR<Series<DateTimeOffset, float>>>)>]
type SeriesDateOffsetFloatFromR() =
  interface IConvertFromR<Series<DateTimeOffset, float>> with member x.Convert(symExpr) = tryCreateTimeSeries (fun dt -> DateTimeOffset(dt)) symExpr

[<Export(typeof<IConvertFromR<Series<DateTimeOffset, string>>>)>]
type SeriesDateOffsetStringFromR() =
  interface IConvertFromR<Series<DateTimeOffset, string>> with member x.Convert(symExpr) = tryCreateTimeSeries (fun dt -> DateTimeOffset(dt)) symExpr

// Series with int keys

[<Export(typeof<IConvertFromR<Series<int, DateTime>>>)>]
type SeriesIntDateromR() =
  interface IConvertFromR<Series<int, DateTime>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<int, int>>>)>]
type SeriesIntIntFromR() =
  interface IConvertFromR<Series<int, int>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<int, float>>>)>]
type SeriesIntFloatFromR() =
  interface IConvertFromR<Series<int, float>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<int, string>>>)>]
type SeriesIntStringFromR() =
  interface IConvertFromR<Series<int, string>> with member x.Convert(symExpr) = tryCreateSeries symExpr

// Series with float keys

[<Export(typeof<IConvertFromR<Series<float, DateTime>>>)>]
type SeriesFloatDateromR() =
  interface IConvertFromR<Series<float, DateTime>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<float, int>>>)>]
type SeriesFloatIntFromR() =
  interface IConvertFromR<Series<float, int>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<float, float>>>)>]
type SeriesFloatFloatFromR() =
  interface IConvertFromR<Series<float, float>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<float, string>>>)>]
type SeriesFloatStringFromR() =
  interface IConvertFromR<Series<float, string>> with member x.Convert(symExpr) = tryCreateSeries symExpr

// Series with string keys

[<Export(typeof<IConvertFromR<Series<string, DateTime>>>)>]
type SeriesStringDateromR() =
  interface IConvertFromR<Series<string, DateTime>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<string, int>>>)>]
type SeriesStringIntFromR() =
  interface IConvertFromR<Series<string, int>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<string, float>>>)>]
type SeriesStringFloatFromR() =
  interface IConvertFromR<Series<string, float>> with member x.Convert(symExpr) = tryCreateSeries symExpr

[<Export(typeof<IConvertFromR<Series<string, string>>>)>]
type SeriesStringStringFromR() =
  interface IConvertFromR<Series<string, string>> with member x.Convert(symExpr) = tryCreateSeries symExpr

// ------------------------------------------------------------------------------------------------
// IConvertFromR - convert Deedle frame to R symexpr
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

// ------------------------------------------------------------------------------------------------
// Conversions for other primitive types 
// ------------------------------------------------------------------------------------------------

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

[<Export(typeof<IConvertToR<Decimal>>)>]
type DecimalConverter() =
  interface IConvertToR<Decimal> with        
    member this.Convert(engine: REngine, x: Decimal) =            
      upcast engine.CreateNumericVector [float x] 

[<Export(typeof<IConvertToR<seq<Decimal>>>)>]
type DecimalSeqConverter() =
  interface IConvertToR<seq<Decimal>> with        
    member this.Convert(engine: REngine, x: seq<Decimal>) =            
      upcast engine.CreateNumericVector (Seq.map float x)