// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
namespace FSharp.Data

open System
open System.IO
open FSharp.Data.Runtime

type internal CsvRow =
  new : parent:CsvFile * columns:string [] -> CsvRow
  member GetColumn : index:int -> string
  member GetColumn : columnName:string -> string
  member Columns : string []
  member Item : index:int -> string with get
  member Item : columnName:string -> string with get

and [<Class>] internal CsvFile =
  inherit CsvFile<CsvRow>
  member internal GetColumnIndex : columnName:string -> int
  static member AsyncLoad : uri:string * ?separators:string * ?quote:char * ?hasHeaders:bool * ?ignoreErrors:bool * ?skipRows:int -> Async<CsvFile>
  static member Load : stream:Stream * ?separators:string * ?quote:char * ?hasHeaders:bool * ?ignoreErrors:bool * ?skipRows:int -> CsvFile
  static member Load : reader:TextReader * ?separators:string * ?quote:char * ?hasHeaders:bool * ?ignoreErrors:bool * ?skipRows:int -> CsvFile
  static member Load : uri:string * ?separators:string * ?quote:char * ?hasHeaders:bool * ?ignoreErrors:bool * ?skipRows:int -> CsvFile
  static member Parse : text:string * ?separators:string * ?quote:char * ?hasHeaders:bool * ?ignoreErrors:bool * ?skipRows:int -> CsvFile