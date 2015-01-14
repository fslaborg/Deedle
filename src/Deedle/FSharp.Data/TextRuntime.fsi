// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
namespace FSharp.Data.Runtime

open System
open System.Globalization

[<Class>]
type internal TextRuntime =
  static member AsyncMap : valueAsync:Async<'T> * mapping:Func<'T,'R> -> Async<'R>
  static member ConvertBoolean : text:string option -> bool option
  static member ConvertBooleanBack : value:bool option * use0and1:bool -> string
  static member ConvertDateTime : cultureStr:string * text:string option -> DateTime option
  static member ConvertDateTimeBack : cultureStr:string * value:DateTime option -> string
  static member ConvertDecimal : cultureStr:string * text:string option -> decimal option
  static member ConvertDecimalBack : cultureStr:string * value:decimal option -> string
  static member ConvertFloat : cultureStr:string * missingValuesStr:string * text:string option -> float option
  static member ConvertFloatBack : cultureStr:string * missingValuesStr:string * value:float option -> string
  static member ConvertGuid : text:string option -> Guid option
  static member ConvertGuidBack : value:Guid option -> string
  static member ConvertInteger : cultureStr:string * text:string option -> int option
  static member ConvertInteger64 : cultureStr:string * text:string option -> int64 option
  static member ConvertInteger64Back : cultureStr:string * value:int64 option -> string
  static member ConvertIntegerBack : cultureStr:string * value:int option -> string
  static member ConvertString : text:string option -> string option
  static member ConvertStringBack : value:string option -> string
  static member GetCulture : cultureStr:string -> CultureInfo
  static member GetMissingValues : missingValuesStr:string -> string []
  static member GetNonOptionalValue : name:string * opt:'T option * originalValue:string option -> 'T
  static member NullableToOption : nullable:Nullable<'a> -> 'a option when 'a : (new : unit ->  'a) and 'a : struct and 'a :> ValueType
  static member OptionToNullable : opt:'b option -> Nullable<'b> when 'b : (new : unit ->  'b) and 'b : struct and 'b :> ValueType
