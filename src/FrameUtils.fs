namespace FSharp.DataFrame

module internal Reflection = 
  open System.Linq
  open System.Linq.Expressions
  open Microsoft.FSharp.Reflection
  open Microsoft.FSharp

  let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance

  let enumerableSelect =
    match <@@ Enumerable.Select([0], fun v -> v) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() 
    | _ -> failwith "Could not find Enumerable.Select"
  let enumerableToArray =
    match <@@ Enumerable.ToArray([0]) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() 
    | _ -> failwith "Could not find Enumerable.ToArray"
  let createNonOpt = 
    match <@@ vectorBuilder.Create([| |]) @@> with
    | Quotations.Patterns.Call(_, mi, _) -> mi.GetGenericMethodDefinition() 
    | _ -> failwith "Could not find vectorBuilder.Create"

  let getRecordConvertors<'T>() = 
    let recdTy = typeof<'T>
    let fields = recdTy.GetProperties() |> Seq.filter (fun p -> p.CanRead) 
    let colIndex = indexBuilder.Create<string>([ for f in fields -> f.Name ], Some false)
    let fieldConvertors = 
      [| for f in fields ->
          // Information about the types involved...
          let fldTy = f.PropertyType
          // Build: fun recd -> recd.<Field>
          let recd = Expression.Parameter(recdTy)
          let func = Expression.Lambda(Expression.Call(recd, f.GetGetMethod()), [recd])
          // Build: Enumerable.ToArray(Enumerable.Select(<input>, fun recd -> recd.<Field>))
          let input = Expression.Parameter(typeof<seq<'T>>)
          let selected = Expression.Call(enumerableSelect.MakeGenericMethod [| recdTy; fldTy |], input, func)
          let body = Expression.Call(enumerableToArray.MakeGenericMethod [| fldTy |], selected)
          // Build: vectorBuilder.Create( ... body ... )
          let conv = Expression.Call(Expression.Constant(vectorBuilder), createNonOpt.MakeGenericMethod [| fldTy |], body)
          // Compile & run
          Expression.Lambda(conv, [input]).Compile() |]
    colIndex, fieldConvertors

  let convertRecordSequence<'T>(data:seq<'T>) =
    let colIndex, convertors = getRecordConvertors<'T>()
    let frameData = 
      [| for convFunc in convertors ->
          convFunc.DynamicInvoke( [| box data |] ) :?> IVector |]
      |> vectorBuilder.Create
    Frame<int, string>(Index.ofKeys [0 .. (Seq.length data) - 1], colIndex, frameData)

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FrameUtils = 
  open FSharp.Data
  open System
  open System.IO
  open System.Collections.Generic
  open System.Globalization
  open ProviderImplementation
  open FSharp.Data.RuntimeImplementation
  open FSharp.Data.RuntimeImplementation.StructuralTypes

  let internal formatter (f:'T -> string) = 
    typeof<'T>, (fun (o:obj) -> f (unbox o))

  let writeCsv (writer:TextWriter) fileNameOpt separatorOpt cultureOpt includeRowKeys (rowKeyNames:seq<_> option) (frame:Frame<_, _>) = 
    let ci = defaultArg cultureOpt CultureInfo.InvariantCulture
    let includeRowKeys = defaultArg includeRowKeys false
    let separator = 
      // Automatically use \t if the file name ends with .tsv
      match separatorOpt, fileNameOpt with 
      | Some s, _ -> s 
      | _, Some (file:string) when file.EndsWith(".tsv", StringComparison.InvariantCultureIgnoreCase) -> '\t'
      | _ -> ','

    // If there is a special character in the string, then we wrap it with quotes
    let specials = 
      ['"'; ','; ';'; '\t'; ' '; '\r'; '\n'; separator] 
      |> Seq.map (fun v -> v, true) |> dict
    // Formatters for printing strings/IFormattable/DateTimes
    let formatQuotedString (s:string) _ = 
      "\"" + s.Replace("\"", "\"\"") + "\""
    let formatPlainString (s:string) = 
      if not (Seq.exists specials.ContainsKey s) then s
      else formatQuotedString s ci
    let formatFormattable (o:IFormattable) =
      formatPlainString <| o.ToString(null, ci)
    let formatters = 
      [ formatter (fun (dt:System.DateTime) ->
          if dt.TimeOfDay = TimeSpan.Zero then dt.ToShortDateString() 
          else dt.ToString(ci))
        formatter (fun (dt:System.DateTimeOffset) ->
          if dt.TimeOfDay = TimeSpan.Zero then dt.Date.ToShortDateString() 
          else dt.DateTime.ToString(ci)) ] |> dict
    
    // Format optional value, using 
    let formatOptional (typ, opt:obj option) = 
      match opt, formatters.TryGetValue(typ) with
      | Some null, _ | None, _ -> "" 
      | Some value, (true, formatter) -> formatter value
      | Some value, _ when typeof<IFormattable>.IsAssignableFrom(typ) ->
            formatFormattable (unbox value)
      | Some value, _ -> formatPlainString <| value.ToString()
        

    // Create flat headers (if there is a hierarchical column index)
    let flattenKeys keys =
      keys |> Seq.map (fun objs -> 
        objs |> Seq.map (fun o -> if o = null then "" else o.ToString()) 
             |> String.concat " - ")

    // Get the data from the data frame
    let colKeys, rowKeys, columns = frame.GetFrameData()

    // Get number of row keys (or 1 if there are no values)
    let rowKeyCount = defaultArg (rowKeys |> Seq.map List.length |> Seq.tryPick Some) 1
    
    // Generate or get names for row keys & format header row
    let rowKeyNames = 
      match rowKeyNames with 
      | Some keys when keys |> Seq.length <> rowKeyCount -> invalidArg "rowKeys" "Mismatching numbe of row keys"
      | Some keys -> keys
      | None when rowKeyCount = 1 -> Seq.singleton "Key" 
      | None -> seq { for i in 1 .. rowKeyCount -> sprintf "Key #%d" i }
    let headers = 
      [ if includeRowKeys then yield! rowKeyNames
        yield! flattenKeys colKeys ]

    // Concatenate correctly formatted columns & write
    let sepStr = separator.ToString()
    let writeLine seq = writer.WriteLine(String.concat sepStr seq)

    // Generate CSV - write headers
    headers |> Seq.map (fun s -> formatPlainString s) |> writeLine
    // Generate CSV - start reading columns in parallel
    let columnEns = [| for t, c in columns -> t, c.GetEnumerator() |]
    let rowKeyEn = rowKeys.GetEnumerator()
    let tryRead (en:IEnumerator<_>) = if en.MoveNext() then Some en.Current else None
    // Generate CSV - iterate over columns and write them all
    let rec writeColumns () =
      let values = [| for t, en in columnEns -> t, tryRead en |]
      let anyNonEmpty = Seq.exists (snd >> Option.isSome) values
      if anyNonEmpty then
        let keys = tryRead rowKeyEn |> Option.get |> Seq.map (fun k -> k.GetType(), Some k)
        let flatVals = values |> Seq.map (fun (t, v) -> t, match v with Some(OptionalValue.Present v) -> Some v | _ -> None) 
        let data = if includeRowKeys then Seq.append keys flatVals else flatVals
        data |> Seq.map formatOptional |> writeLine
        writeColumns ()
    writeColumns ()

(*
    let transformed = 
      headers
      |> Seq.map (Seq.map (Array.create 1))
      |> Seq.reduce (Seq.map2 Array.append)
    let flatHeaders = 
      transformed |> Seq.map (fun sub -> 
        sub |> Seq.map (fun o -> if o = null then "" else o.ToString()) |> String.concat " - ")
            
    let writeLine seq = writer.WriteLine(String.concat (separator.ToString()) seq)
    
    writeLine flatHeaders
    data |> Seq.iter (Seq.map formatOptional >> writeLine)
*)

  /// Load data from a CSV file using F# Data API
  let readCsv (reader:TextReader) inferTypes inferRows schema (missingValues:string) separators culture =
    let schema = defaultArg schema ""
    let schema = if schema = null then "" else schema
    let missingValuesArr = missingValues.Split(',')
    let inferRows = defaultArg inferRows 0
    let safeMode = false // Irrelevant - all DF values can be missing
    let preferOptionals = true // Ignored
    let culture = defaultArg culture ""
    let culture = if culture = null then "" else culture
    let cultureInfo = System.Globalization.CultureInfo.GetCultureInfo(culture)

    let createVector typ (data:string[]) = 
      if typ = typeof<bool> then Vector.ofOptionalValues (Array.map (fun s -> Operations.ConvertBoolean(culture, Some(s))) data) :> IVector
      elif typ = typeof<decimal> then Vector.ofOptionalValues (Array.map (fun s -> Operations.ConvertDecimal(culture, Some(s))) data) :> IVector
      elif typ = typeof<float> then Vector.ofOptionalValues (Array.map (fun s -> Operations.ConvertFloat(culture, missingValues, Some(s))) data) :> IVector
      elif typ = typeof<int> then Vector.ofOptionalValues (Array.map (fun s -> Operations.ConvertInteger(culture, Some(s))) data) :> IVector
      elif typ = typeof<int64> then Vector.ofOptionalValues (Array.map (fun s -> Operations.ConvertInteger64(culture, Some(s))) data) :> IVector
      else Vector.ofValues data :> IVector

    // If 'inferTypes' is specified (or by default), use the CSV type inference
    // to load information about types in the CSV file. By default, use the entire
    // content (but inferRows can be set to smaller number). Otherwise we just
    // "infer" all columns as string.
    let data = Csv.CsvFile.Load(reader, ?separators=separators)
    let inferedProperties = 
      if not (inferTypes = Some false) then
        CsvInference.inferType 
          data inferRows (missingValuesArr, cultureInfo) schema safeMode preferOptionals
        ||> CsvInference.getFields preferOptionals
      else 
        if data.Headers.IsNone then failwith "CSV file is missing headers!"
        [ for c in data.Headers.Value -> 
            PrimitiveInferedProperty.Create(c, typeof<string>, true) ]

    // Load the data and convert the values to the appropriate type
    let data = data.Cache()
    let columnIndex = Index.ofKeys data.Headers.Value
    let columns = 
      [| for name, prop in Seq.zip data.Headers.Value inferedProperties  ->
            [| for row in data.Data -> row.GetColumn(name) |]
            |> createVector prop.RuntimeType |]
    let rowIndex = Index.ofKeys [ 0 .. (Seq.length data.Data) - 1 ]
    Frame(rowIndex, columnIndex, Vector.ofValues columns)
