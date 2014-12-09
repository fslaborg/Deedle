// --------------------------------------------------------------------------------------
// Untyped CSV api
// --------------------------------------------------------------------------------------

namespace FSharp.Data

open System
open System.Globalization
open System.IO
open System.Runtime.InteropServices
open FSharp.Data.Runtime
open FSharp.Data.Runtime.IO

[<StructuredFormatDisplay("{Columns}")>]
/// Represents a CSV row.
type CsvRow(parent:CsvFile, columns:string[]) =

  /// The columns of the row
  member __.Columns = columns
  
  /// Gets a column by index
  member __.GetColumn index = columns.[index]
  /// Gets a column by name
  member __.GetColumn columnName = columns.[parent.GetColumnIndex columnName]  

  /// Gets a column by index
  member __.Item with get index = columns.[index]
  /// Gets a column by name
  member __.Item with get columnName = columns.[parent.GetColumnIndex columnName]

/// Represents a CSV file. The lines are read on demand from `reader`.
/// Columns are delimited by one of the chars passed by `separators` (defaults to just `,`), and
/// to escape the separator chars, the `quote` character will be used (defaults to `"`).
/// If `hasHeaders` is true (the default), the first line read by `reader` will not be considered part of data.
/// If `ignoreErrors` is true (the default is false), rows with a different number of columns from the header row
/// (or the first row if headers are not present) will be ignored.
/// The first `skipRows` lines will be skipped.
and CsvFile private (readerFunc:Func<TextReader>, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) as this =
  inherit CsvFile<CsvRow>(
    Func<_,_,_>(fun this columns -> CsvRow(this :?> CsvFile, columns)),
    Func<_,_>(fun row -> row.Columns),
    readerFunc, 
    defaultArg separators "", 
    defaultArg quote '"', 
    defaultArg hasHeaders true, 
    defaultArg ignoreErrors false,
    defaultArg skipRows 0)

  let headerDic = 
    match this.Headers with
    | Some headers ->
        headers
        |> Seq.mapi (fun index header -> header, index)
        |> dict
    | None -> [] |> dict

  member internal __.GetColumnIndex columnName = headerDic.[columnName]

  /// Parses the specified CSV content
  static member Parse(text, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) = 
    let readerFunc = Func<_>(fun () -> new StringReader(text) :> TextReader)
    new CsvFile(readerFunc, ?separators=separators, ?quote=quote, ?hasHeaders=hasHeaders, ?ignoreErrors=ignoreErrors, ?skipRows=skipRows)

  /// Loads CSV from the specified stream
  static member Load(stream:Stream, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) = 
    let firstTime = ref true
    let readerFunc = Func<_>(fun () -> 
      if firstTime.Value then firstTime := false
      else stream.Position <- 0L
      new StreamReader(stream) :> TextReader)
    new CsvFile(readerFunc, ?separators=separators, ?quote=quote, ?hasHeaders=hasHeaders, ?ignoreErrors=ignoreErrors, ?skipRows=skipRows)

  /// Loads CSV from the specified reader
  static member Load(reader:TextReader, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) = 
    let firstTime = ref true
    let readerFunc = Func<_>(fun () ->  
      if firstTime.Value then firstTime := false
      elif reader :? StreamReader then
        let sr = reader :?> StreamReader
        sr.BaseStream.Position <- 0L
        sr.DiscardBufferedData()
      else invalidOp "The underlying source stream is not re-entrant. Use the Cache method to cache the data."
      reader)
    new CsvFile(readerFunc, ?separators=separators, ?quote=quote, ?hasHeaders=hasHeaders, ?ignoreErrors=ignoreErrors, ?skipRows=skipRows)

  /// Loads CSV from the specified uri
  static member Load(uri:string, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) = 
    let separators = defaultArg separators ""    
    let separators = 
        if String.IsNullOrEmpty separators && uri.EndsWith(".tsv" , StringComparison.OrdinalIgnoreCase) 
        then "\t" else separators
    let readerFunc = Func<_>(fun () -> asyncReadTextAtRuntime false "" "" "CSV" "" uri |> Async.RunSynchronously)
    new CsvFile(readerFunc, separators, ?quote=quote, ?hasHeaders=hasHeaders, ?ignoreErrors=ignoreErrors, ?skipRows=skipRows)

  /// Loads CSV from the specified uri asynchronously
  static member AsyncLoad(uri:string, [<Optional>] ?separators, [<Optional>] ?quote, [<Optional>] ?hasHeaders, [<Optional>] ?ignoreErrors, [<Optional>] ?skipRows) = async {
    let separators = defaultArg separators ""    
    let separators = 
        if String.IsNullOrEmpty separators && uri.EndsWith(".tsv" , StringComparison.OrdinalIgnoreCase)
        then "\t" else separators
    let! reader = asyncReadTextAtRuntime false "" "" "CSV" "" uri
    let firstTime = ref true
    let readerFunc = Func<_>(fun () ->  
      if firstTime.Value then firstTime := false; reader
      else asyncReadTextAtRuntime false "" "" "CSV" "" uri |> Async.RunSynchronously)
    return new CsvFile(readerFunc, separators, ?quote=quote, ?hasHeaders=hasHeaders, ?ignoreErrors=ignoreErrors, ?skipRows=skipRows)
  }

