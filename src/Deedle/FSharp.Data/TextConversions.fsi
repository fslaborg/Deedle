// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
namespace FSharp.Data

open System
open System.Globalization

[<Class>]
type internal TextConversions =
  static member AsBoolean : text:string -> bool option
  static member AsDateTime : cultureInfo:IFormatProvider -> text:string -> DateTime option
  static member AsDecimal : cultureInfo:IFormatProvider -> text:string -> decimal option
  static member AsFloat : missingValues:string [] -> useNoneForMissingValues:bool -> cultureInfo:IFormatProvider -> text:string -> float option
  static member AsGuid : text:string -> Guid option
  static member AsInteger : cultureInfo:IFormatProvider -> text:string -> int option
  static member AsInteger64 : cultureInfo:IFormatProvider -> text:string -> int64 option
  static member AsString : str:string -> string option
  static member DefaultCurrencyAdorners : Set<char>
  static member DefaultMissingValues : string []
  static member DefaultNonCurrencyAdorners : Set<char>