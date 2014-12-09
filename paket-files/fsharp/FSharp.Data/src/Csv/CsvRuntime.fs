// --------------------------------------------------------------------------------------
// CSV type provider - runtime components (parsing and type representing CSV)
// --------------------------------------------------------------------------------------

namespace FSharp.Data.Runtime

open System
open System.ComponentModel
open System.Collections.Generic
open System.IO
open System.Runtime.InteropServices
open System.Text
open FSharp.Data.Runtime
open FSharp.Data.Runtime.IO

// --------------------------------------------------------------------------------------

// Parser for the CSV format 
module internal CsvReader = 

  /// Lazily reads the specified CSV file using the specified separators
  /// (Handles most of the RFC 4180 - most notably quoted values and also
  /// quoted newline characters in columns)
  let readCsvFile (reader:TextReader) (separators:string) quote =

    let inline (|Char|) (n:int) = char n
    let inline (|Quote|_|) (n:int) = if char n = quote then Some() else None
    
    let separators = separators.ToCharArray()
    let inline (|Separator|_|) (n:int) =
      if separators.Length = 1 then 
        if (char n) = separators.[0] then Some() else None
      else
        if Array.exists ((=) (char n)) separators then Some() else None

    /// Read quoted string value until the end (ends with end of stream or
    /// the " character, which can be encoded using double ")
    let rec readString (chars:StringBuilder) = 
      match reader.Read() with
      | -1 -> chars
      | Quote when reader.Peek() = int quote ->
          reader.Read() |> ignore
          readString (chars.Append quote)
      | Quote -> chars
      | Char c -> readString (chars.Append c)

    /// Reads a line with data that are separated using specified separators
    /// and may be quoted. Ends with newline or end of input.
    let rec readLine data (chars:StringBuilder) = 
      match reader.Read() with
      | -1 | Char '\r' | Char '\n' -> 
          let item = chars.ToString()
          item::data
      | Separator -> 
          let item = chars.ToString()
          readLine (item::data) (StringBuilder())
      | Quote ->
          readLine data (readString chars)
      | Char c ->
          readLine data (chars.Append c)

    /// Reads multiple lines from the input, skipping newline characters
    let rec readLines lineNumber = seq {
      match reader.Peek() with
      | -1 -> ()
      | Char '\r' | Char '\n' -> 
          reader.Read() |> ignore
          yield! readLines lineNumber
      | _ -> 
          yield readLine [] (StringBuilder()) |> List.rev |> Array.ofList, lineNumber
          yield! readLines (lineNumber + 1) }

    readLines 0

// --------------------------------------------------------------------------------------

[<AutoOpen>]
module private CsvHelpers = 

  type ParsedCsvLines = 
    { FirstLine : string[] * int
      SecondLine : (string[] * int) option
      Headers : string[] option
      LineIterator : IEnumerator<string[] * int> 
      ColumnCount : int
      HasHeaders : bool
      Separators : string
      Quote : char }

  /// An enumerable that will return elements from the 'firstSeq' first time it
  /// is accessed and then will call 'nextSeq' each time for all future GetEnumerator calls
  type private ReentrantEnumerable<'T>(firstSeq:seq<'T>, nextSeq:unit -> seq<'T>) =
    let mutable first = true
    interface seq<'T> with
      member x.GetEnumerator() = 
        if first then 
          first <- false
          firstSeq.GetEnumerator()
        else nextSeq().GetEnumerator()
    interface System.Collections.IEnumerable with
      member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> System.Collections.IEnumerator
    
  let parseIntoLines newReader separators quote hasHeaders skipRows =

    // Get the first iterator and read the first line
    let firstReader : TextReader = newReader()

    let linesIterator = (CsvReader.readCsvFile firstReader separators quote).GetEnumerator()  

    for i = 1 to skipRows do
      linesIterator.MoveNext() |> ignore

    let firstLine =
      if linesIterator.MoveNext() then
        linesIterator.Current
      else
        // If it does not have any lines, that's wrong...
        linesIterator.Dispose()
        if hasHeaders then failwithf "Invalid CSV file: header row not found" 
        else failwithf "Invalid CSV file: no data rows found"

    let headers = 
      if not hasHeaders then None
      else firstLine |> fst |> Array.map (fun columnName -> columnName.Trim()) |> Some

    // If there are no headers, use the number of columns of the first line
    let numberOfColumns =
      match headers, firstLine with
      | Some headers, _ -> headers.Length
      | _, (columns, _) -> columns.Length
      
    { FirstLine = firstLine
      SecondLine = None
      Headers = headers
      LineIterator = linesIterator
      ColumnCount = numberOfColumns
      HasHeaders = hasHeaders
      Separators = separators
      Quote = quote }

  // Always ignore empty rows
  let inline ignoreRow untypedRow =
    Array.forall String.IsNullOrWhiteSpace untypedRow

  let parseIntoTypedRows newReader 
                         ignoreErrors 
                         stringArrayToRow
                         { FirstLine = firstLine
                           SecondLine = secondLine
                           LineIterator = linesIterator
                           ColumnCount = numberOfColumns 
                           HasHeaders = hasHeaders
                           Separators = separators
                           Quote = quote } = 

    // On the first read, finish reading the opened reader
    // On future reads, get a new reader (and skip headers)
    let firstSeq = seq {
      use linesIterator = linesIterator
      if not hasHeaders then yield firstLine
      match secondLine with
      | Some line -> yield line
      | None -> ()
      while linesIterator.MoveNext() do yield linesIterator.Current }

    let nextSeq() = 
      let reader : TextReader = newReader()
      let csv = CsvReader.readCsvFile reader separators quote
      if hasHeaders then Seq.skip 1 csv else csv

    let untypedRows = ReentrantEnumerable<_>(firstSeq, nextSeq)
    // Return data with parsed columns
    seq {
      for untypedRow, lineNumber in untypedRows do
        if untypedRow.Length <> numberOfColumns then
          // Ignore rows with different number of columns when ignoreErrors is set to true
          if not ignoreErrors then
            let lineNumber = if hasHeaders then lineNumber else lineNumber + 1
            failwithf "Couldn't parse row %d according to schema: Expected %d columns, got %d" lineNumber numberOfColumns untypedRow.Length
        else
          if not (ignoreRow untypedRow) then
            // Try to convert the untyped rows to 'RowType      
            let convertedRow = 
              try 
                Choice1Of2 (stringArrayToRow untypedRow)
              with exn -> 
                Choice2Of2 exn
            match convertedRow, ignoreErrors with
            | Choice1Of2 convertedRow, _ -> yield convertedRow
            | Choice2Of2 _, true -> ()
            | Choice2Of2 exn, false -> 
                let lineNumber = if hasHeaders then lineNumber else lineNumber + 1
                failwithf "Couldn't parse row %d according to schema: %s" lineNumber exn.Message
    }

// --------------------------------------------------------------------------------------

/// [omit]
type CsvFile<'RowType> private (rowToStringArray:Func<'RowType,string[]>, disposer:IDisposable, rows:seq<'RowType>, headers, numberOfColumns, separators, quote) =

  /// The rows with data
  member __.Rows = rows
  /// The names of the columns
  member __.Headers = headers
  /// The number of columns
  member __.NumberOfColumns = numberOfColumns
  /// The character(s) used as column separator(s)
  member __.Separators = separators
  /// The quotation mark use for surrounding values containing separator chars
  member __.Quote = quote
  
  interface IDisposable with
    member __.Dispose() = disposer.Dispose()

  /// [omit]
  [<EditorBrowsableAttribute(EditorBrowsableState.Never)>]
  [<CompilerMessageAttribute("This method is intended for use in generated code only.", 10001, IsHidden=true, IsError=false)>]
  static member Create (stringArrayToRow, rowToStringArray, reader:TextReader, separators, quote, hasHeaders, ignoreErrors, skipRows, cacheRows) =    
    let uncachedCsv = new CsvFile<'RowType>(stringArrayToRow, rowToStringArray, Func<_>(fun _ -> reader), separators, quote, hasHeaders, ignoreErrors, skipRows)
    if cacheRows then uncachedCsv.Cache() else uncachedCsv

  /// [omit]
  new (stringArrayToRow:Func<obj,string[],'RowType>, rowToStringArray, readerFunc:Func<TextReader>, separators, quote, hasHeaders, ignoreErrors, skipRows) as this =

    // Track created Readers so that we can dispose of all of them
    let disposeFuncs = new ResizeArray<_>()
    let disposed = ref false
    let disposer = 
      { new IDisposable with
          member x.Dispose() = 
            if not !disposed then
                Seq.iter (fun f -> f()) disposeFuncs
                disposed := true }

    let newReader() =
        if !disposed then
            raise <| ObjectDisposedException(this.GetType().Name)
        let reader = readerFunc.Invoke()
        disposeFuncs.Add reader.Dispose
        reader

    let noSeparatorsSpecified = String.IsNullOrEmpty separators
    let separators = if noSeparatorsSpecified then "," else separators

    let parsedCsvLines = parseIntoLines newReader separators quote hasHeaders skipRows

    // Auto-Detect tab separated files that may not have .TSV extension when no explicit separators defined
    let probablyTabSeparated =
      parsedCsvLines.ColumnCount < 2 && noSeparatorsSpecified &&
      fst parsedCsvLines.FirstLine |> Array.exists (fun c -> c.Contains("\t"))

    let parsedCsvLines =
      if probablyTabSeparated
      then parseIntoLines newReader "\t" quote hasHeaders skipRows
      else parsedCsvLines

    // Detect header that has empty trailing column name that doesn't correspond to a column in
    // the following data lines.  This is checked if headers exist and the last column in the header
    // is empty.  The secondLine field of the parsedCsvLines record is used to store the second line
    // that is read when testing the length of the first data row following the header.
    let parsedCsvLines =
      match parsedCsvLines.Headers with
      | None -> parsedCsvLines
      | Some headers -> let columnCount = parsedCsvLines.ColumnCount
                        if String.IsNullOrWhiteSpace headers.[columnCount-1]
                        then let secondline = if parsedCsvLines.LineIterator.MoveNext() then
                                                Some (parsedCsvLines.LineIterator.Current)
                                              else
                                                None
                             match secondline with
                             | Some line -> let linecontents = fst line
                                            if linecontents.Length = columnCount - 1
                                            then { parsedCsvLines with SecondLine = secondline;
                                                                       ColumnCount = columnCount - 1;
                                                                       Headers = Some headers.[..columnCount-2] }
                                            else { parsedCsvLines with SecondLine = secondline }
                             | None -> parsedCsvLines
                        else parsedCsvLines

    let rows = 
      parsedCsvLines
      |> parseIntoTypedRows newReader ignoreErrors (fun untypedRow -> stringArrayToRow.Invoke(this, untypedRow)) 

    new CsvFile<'RowType>(rowToStringArray,
                          disposer, 
                          rows, 
                          parsedCsvLines.Headers, 
                          parsedCsvLines.ColumnCount, 
                          parsedCsvLines.Separators, 
                          parsedCsvLines.Quote)

  /// Saves CSV to the specified writer
  member x.Save(writer:TextWriter, [<Optional>] ?separator, [<Optional>] ?quote) =

    let separator = (defaultArg separator x.Separators.[0]).ToString()
    let quote = (defaultArg quote x.Quote).ToString()
    let doubleQuote = quote + quote

    use writer = writer

    let writeLine writeItem (items:string[]) =
      for i = 0 to items.Length-2 do
        writeItem items.[i]
        writer.Write separator
      writeItem items.[items.Length-1]
      writer.WriteLine()

    match x.Headers with
    | Some headers -> headers |> writeLine writer.Write
    | None -> ()

    for row in x.Rows do
      row |> rowToStringArray.Invoke |> writeLine (fun item -> 
        if item.Contains separator then
          writer.Write quote
          writer.Write (item.Replace(quote, doubleQuote))
          writer.Write quote
        else
          writer.Write item)

  /// Saves CSV to the specified stream
  member x.Save(stream:Stream, [<Optional>] ?separator, [<Optional>] ?quote) = 
    let writer = new StreamWriter(stream)
    x.Save(writer, ?separator=separator, ?quote=quote)

#if FX_NO_LOCAL_FILESYSTEM
#else
  /// Saves CSV to the specified file
  member x.Save(path:string, [<Optional>] ?separator, [<Optional>] ?quote) = 
    let writer = new StreamWriter(File.OpenWrite(path))
    x.Save(writer, ?separator=separator, ?quote=quote)
#endif

  /// Saves CSV to a string
  member x.SaveToString([<Optional>] ?separator, [<Optional>] ?quote) = 
     let writer = new StringWriter()
     x.Save(writer, ?separator=separator, ?quote=quote)
     writer.ToString()

  member inline private x.map f =
    new CsvFile<'RowType>(rowToStringArray, disposer, f x.Rows,  x.Headers, x.NumberOfColumns, x.Separators, x.Quote)

  /// Returns a new csv with the same rows as the original but which guarantees
  /// that each row will be only be read and parsed from the input at most once.
  member x.Cache() =   
    Seq.cache |> x.map

  /// Returns a new csv containing only the rows for which the given predicate returns "true".
  member x.Filter (predicate:Func<_,_>) = 
    Seq.filter predicate.Invoke |> x.map
  
  /// Returns a new csv with only the first N rows of the underlying csv.
  member x.Take count = 
    Seq.take count |> x.map
  
  /// Returns a csv that, when iterated, yields rowswhile the given predicate
  /// returns <c>true</c>, and then returns no further rows.
  member x.TakeWhile (predicate:Func<_,_>) = 
    Seq.takeWhile predicate.Invoke |> x.map
  
  /// Returns a csv that skips N rows and then yields the remaining rows.
  member x.Skip count = 
    Seq.skip count |> x.map
  
  /// Returns a csv that, when iterated, skips rows while the given predicate returns
  /// <c>true</c>, and then yields the remaining rows.
  member x.SkipWhile (predicate:Func<_,_>) = 
    Seq.skipWhile predicate.Invoke |> x.map
  
  /// Returns a csv that when enumerated returns at most N rows.
  member x.Truncate count = 
    Seq.truncate count |> x.map
