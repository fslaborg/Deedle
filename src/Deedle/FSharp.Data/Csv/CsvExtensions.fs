/// Unsafe extension methods that can be used to work with CsvRow in a less safe, but shorter way.
/// This module also provides the dynamic operator.
namespace FSharp.Data

open System
open System.Globalization
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open FSharp.Data
open FSharp.Data.Runtime

[<Extension>]
/// Extension methods with conversions from strings to other types
type StringExtensions =

  [<Extension>]
  static member AsInteger(x:String, [<Optional>] ?cultureInfo) = 
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    match TextConversions.AsInteger cultureInfo x with
    | Some i -> i
    | _ -> failwithf "Not an int: %s" x

  [<Extension>]
  static member AsInteger64(x:String, [<Optional>] ?cultureInfo) = 
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    match TextConversions.AsInteger64 cultureInfo x with
    | Some i -> i
    | _ -> failwithf "Not an int64: %s" x

  [<Extension>]
  static member AsDecimal(x:String, [<Optional>] ?cultureInfo) =
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    match TextConversions.AsDecimal cultureInfo x with
    | Some d -> d
    | _ -> failwithf "Not a decimal: %s" x

  [<Extension>]
  static member AsFloat(x:String, [<Optional>] ?cultureInfo, [<Optional>] ?missingValues) = 
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    let missingValues = defaultArg missingValues TextConversions.DefaultMissingValues
    match TextConversions.AsFloat missingValues (*useNoneForMissingValues*)false cultureInfo x with
    | Some f -> f
    | _ -> failwithf "Not a float: %s" x
  
  [<Extension>]
  static member AsBoolean(x:String) =
    match TextConversions.AsBoolean x with
    | Some b -> b
    | _ -> failwithf "Not a boolean: %s" x

  [<Extension>]
  static member AsDateTime(x:String, [<Optional>] ?cultureInfo) = 
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    match TextConversions.AsDateTime cultureInfo x with 
    | Some d -> d
    | _ -> failwithf "Not a datetime: %s" x

  [<Extension>]
  static member AsDateTimeOffset(x, [<Optional>] ?cultureInfo) = 
    let cultureInfo = defaultArg cultureInfo  CultureInfo.InvariantCulture
    match TextConversions.AsDateTimeOffset cultureInfo x with
    | Some d -> d
    | _ -> failwithf "Not a datetime offset: %s" <| x

  [<Extension>]
  static member AsTimeSpan(x:String, [<Optional>] ?cultureInfo) = 
    let cultureInfo = defaultArg cultureInfo CultureInfo.InvariantCulture
    match TextConversions.AsTimeSpan cultureInfo x with 
    | Some t -> t
    | _ -> failwithf "Not a time span: %s" x

  [<Extension>]
  static member AsGuid(x:String) =
    match x |> TextConversions.AsGuid with
    | Some g -> g
    | _ -> failwithf "Not a guid: %s" x

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides the dynamic operator for getting column values by name from CSV rows
module CsvExtensions =
  
  /// Get the value of a column by name from a CSV row
  let (?) (csvRow:CsvRow) (columnName:string) = csvRow.[columnName]