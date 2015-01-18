// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
namespace FSharp.Data.Runtime

open System
open System.IO

module internal CsvReader = 
  val readCsvFile : reader:TextReader -> separators:string -> quote:char -> seq<string [] * int>

type internal CsvFile<'RowType> =
  interface IDisposable
  new : stringArrayToRow:Func<obj,string [],'RowType> * rowToStringArray:Func<'RowType,string []> * readerFunc:Func<TextReader> * separators:string * quote:char * hasHeaders:bool * ignoreErrors:bool * skipRows:int -> CsvFile<'RowType>
  member Cache : unit -> CsvFile<'RowType>
  member Filter : predicate:Func<'RowType,bool> -> CsvFile<'RowType>
  member Save : writer:TextWriter * ?separator:char * ?quote:char -> unit
  member Save : stream:Stream * ?separator:char * ?quote:char -> unit
  member Save : path:string * ?separator:char * ?quote:char -> unit
  member SaveToString : ?separator:char * ?quote:char -> string
  member Skip : count:int -> CsvFile<'RowType>
  member SkipWhile : predicate:Func<'RowType,bool> -> CsvFile<'RowType>
  member Take : count:int -> CsvFile<'RowType>
  member TakeWhile : predicate:Func<'RowType,bool> -> CsvFile<'RowType>
  member Truncate : count:int -> CsvFile<'RowType>
  member Headers : string [] option
  member NumberOfColumns : int
  member Quote : char
  member Rows : seq<'RowType>
  member Separators : string
  static member Create : stringArrayToRow:Func<obj,string [],'RowType> * rowToStringArray:Func<'RowType,string []> * reader:TextReader * separators:string * quote:char * hasHeaders:bool * ignoreErrors:bool * skipRows:int * cacheRows:bool -> CsvFile<'RowType>
