namespace Deedle

// ------------------------------------------------------------------------------------------------
// Utilities that use reflection (flattening records etc.)
// ------------------------------------------------------------------------------------------------

module internal Reflection = 
  open System
  open System.Linq
  open System.Reflection
  open System.Linq.Expressions
  open Microsoft.FSharp.Reflection
  open Microsoft.FSharp
  open Microsoft.FSharp.Quotations
  open System.Collections.Generic

  let indexBuilder = Indices.Linear.LinearIndexBuilder.Instance
  let vectorBuilder = Vectors.ArrayVector.ArrayVectorBuilder.Instance

  /// Helper function that creates IVector<'T> and casts it to IVector
  let createTypedVectorHelper<'T> (input:seq<OptionalValue<obj>>) =
    input |> Seq.map (OptionalValue.map unbox<'T>) |> Vector.ofOptionalValues :> IVector

  /// Helper function that returns contents of IDictionary<'K, 'V>
  let getDictionaryValues<'K, 'V> (input:IDictionary<'K, 'V>) =
    seq { for (KeyValue(k, v)) in input -> k.ToString(), (v.GetType(), box v) }

  // Functions called from emitted code
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
  let createTypedVectorMi =
    match <@@ createTypedVectorHelper @@> with
    | Patterns.Lambda(_, Patterns.Call(_, mi, _)) -> mi.GetGenericMethodDefinition()
    | _ -> failwith "Failed to get method info for 'createTypedVector'"
  let getDictionaryValuesMi =
    match <@@ getDictionaryValues @@> with
    | Patterns.Lambda(_, Patterns.Call(_, mi, _)) -> mi.GetGenericMethodDefinition()
    | _ -> failwith "Failed to get method info for 'getDictionaryValues'"

  /// Helper function used when building frames from data tables
  let createTypedVector : _ -> seq<OptionalValue<obj>> -> _ =
    let cache = Dictionary<_, _>()
    (fun typ -> 
      match cache.TryGetValue(typ) with
      | true, res -> res 
      | false, _ -> 
          let par = Expression.Parameter(typeof<seq<OptionalValue<obj>>>)
          let body = Expression.Call(createTypedVectorMi.MakeGenericMethod([| typ |]), par)
          let f = Expression.Lambda<Func<seq<OptionalValue<obj>>, IVector>>(body, par).Compile()
          cache.Add(typ, f.Invoke)
          f.Invoke )

  /// Given System.Type for some .NET object, get a sequence of projections
  /// that return the values of all readonly properties (together with their name & type)
  let getMemberProjections (recdTy:System.Type) =
    [| let fields = 
         recdTy.GetProperties(BindingFlags.Instance ||| BindingFlags.Public) 
         |> Seq.filter (fun p -> p.CanRead && p.GetIndexParameters().Length = 0) 
       for f in fields ->
         let fldTy = f.PropertyType
         // Build: fun recd -> recd.<Field>
         let recd = Expression.Parameter(recdTy)
         let call = Expression.Call(recd, f.GetGetMethod())
         f.Name, fldTy, Expression.Lambda(call, [recd]) |]

  /// Given value, return names, types and values of all its IDictionary contents (or None)
  let expandDictionary (value:obj) =
    if value = null then None else
    match value with 
    | :? ISeries<string> as sstr ->
        seq { for key in sstr.Index.Keys do
                let obj = sstr.TryGetObject(key)
                if obj.HasValue then
                  yield key, (obj.Value.GetType(), obj.Value) } |> Some
    | _ -> 
    /// Get type arguments of the IDictionary<T1, T2> implementation or None
    let optTyArgs =
      value.GetType().GetInterfaces() |> Seq.tryPick (fun ityp ->
        if ityp.IsGenericType && ityp.GetGenericTypeDefinition() = typedefof<IDictionary<_, _>> 
        then Some(ityp.GetGenericArguments()) else None)
    match optTyArgs with
    | Some typs -> 
        let res = getDictionaryValuesMi.MakeGenericMethod(typs).Invoke(null, [| value |]) 
        Some(unbox<seq<string * (Type * obj)>> res)
    | _ -> None

  /// Given a single vector, expand its values into multiple vectors. This may be:
  /// - `IDictionary` is expanded based on keys/values
  /// - `ISeries<string>` is expanded 
  /// - .NET types with readable properties are expanded
  let expandVector (vector:IVector<'T>) =
    // Compile all projections from the type, so that we can run them fast
    let compiled = 
      [| for name, fldTy, proj in getMemberProjections (typeof<'T>) -> 
           name, fldTy, proj.Compile() |]    
    // For each vector element, build a list of all expanded columns
    // The list includes all dictionary values, or all fields
    let expanded =
      vector.Select(fun input ->
        [ match expandDictionary input  with
          | Some dict -> yield! dict
          | _ -> for name, fldTy, f in compiled do 
                   yield name, (fldTy, f.DynamicInvoke(input)) ] |> dict)
    // Get a dictionary of all fields that we want to get from the vector    
    let fields = Dictionary<_, _>()
    expanded.DataSequence |> Seq.iter (function
      | OptionalValue.Present(list) -> 
          for KeyValue(fld, (typ, _)) in list do 
            match fields.TryGetValue(fld) with
            | true, typ2 when typ = typ2 || typ2 = typeof<obj> -> ()
            | true, typ2 -> fields.[fld] <- typeof<obj>
            | false, _ -> fields.[fld] <- typ 
      | _ -> () )

    // Iterate over all the fields and turn them into vectors
    [ for (KeyValue(fieldName, fieldTyp)) in fields ->
        let it = expanded.SelectMissing(OptionalValue.bind (fun lookup -> 
          match lookup.TryGetValue(fieldName) with
          | true, (_, v) -> OptionalValue(v)
          | _ -> OptionalValue.Missing)).DataSequence
        fieldName, createTypedVector fieldTyp it ]

  let expandUntypedVector =
    { new VectorHelpers.VectorCallSite1<_> with
        member x.Invoke(vect) = expandVector vect }
    |> VectorHelpers.createVectorDispatcher    

  /// Given type 'T that represents some .NET object, generate an array of 
  /// functions that take seq<'T> and generate IVector with each column:
  ///
  ///     vectorBuilder.Create(<input>.Select(fun (recd:'T) -> 
  ///       recd.<field>).ToArray()) : IVector<'F>
  ///
  let getRecordConvertorExprs (recdTy:System.Type) = 
    getMemberProjections recdTy |> Array.map (fun (name, fldTy, funcExpr) ->
      // Build: Enumerable.ToArray(Enumerable.Select(<input>, fun recd -> recd.<Field>))
      let input = Expression.Parameter(typedefof<seq<_>>.MakeGenericType(recdTy))
      let selected = Expression.Call(enumerableSelect.MakeGenericMethod [| recdTy; fldTy |], input, funcExpr)
      let body = Expression.Call(enumerableToArray.MakeGenericMethod [| fldTy |], selected)
      // Build: vectorBuilder.Create( ... body ... )
      let conv = Expression.Call(Expression.Constant(vectorBuilder), createNonOpt.MakeGenericMethod [| fldTy |], body)
      // Compile & return
      name, (input, conv))

  /// Convert a sequence of records to a data frame - automatically 
  /// get the columns based on information available using reflection
  let convertRecordSequence<'T>(data:seq<'T>) =
    let convertors = getRecordConvertorExprs (typeof<'T>)
    let colIndex = indexBuilder.Create<string>(Seq.map fst convertors, Some false)
    let convertors = 
      [| for _, (input, body) in convertors -> 
           let cast = Expression.Convert(body, typeof<IVector>)
           Expression.Lambda<Func<seq<'T>, IVector>>(cast, input).Compile() |]
    let frameData = 
      [| for convFunc in convertors -> convFunc.Invoke(data) |]
      |> vectorBuilder.Create
    Frame<int, string>(Index.ofKeys [0 .. (Seq.length data) - 1], colIndex, frameData)

// ------------------------------------------------------------------------------------------------
// Utilities, mostly dealing with construction & serialization of frames
// ------------------------------------------------------------------------------------------------

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FrameUtils = 
  open FSharp.Data
  open System
  open System.Data
  open System.IO
  open System.Collections.Generic
  open System.Globalization
  open ProviderImplementation
  open FSharp.Data.RuntimeImplementation
  open FSharp.Data.RuntimeImplementation.StructuralTypes

  let internal formatter (f:'T -> string) = 
    typeof<'T>, (fun (o:obj) -> f (unbox o))

  let writeFrameRows includeRowKeys (rowKeys:seq<_>) (columns:seq<_ * IVector<_>>) writeLine =
    // Generate CSV - start reading columns in parallel
    let columnEns = [| for t, c in columns -> t, c.DataSequence.GetEnumerator() |]
    let rowKeyEn = rowKeys.GetEnumerator()
    let tryRead (en:IEnumerator<_>) = if en.MoveNext() then Some en.Current else None
    // Generate CSV - iterate over columns and write them all
    let rec loop () =
      let values = [| for t, en in columnEns -> t, tryRead en |]
      let anyNonEmpty = Seq.exists (snd >> Option.isSome) values
      if anyNonEmpty then
        let keys = tryRead rowKeyEn |> Option.get |> Seq.map (fun k -> k.GetType(), Some k)
        let flatVals = values |> Seq.map (fun (t, v) -> t, match v with Some(OptionalValue.Present v) -> Some v | _ -> None) 
        let data = if includeRowKeys then Seq.append keys flatVals else flatVals
        writeLine data 
        loop ()
    loop ()

  // Create flat headers (if there is a hierarchical column index)
  let flattenKeys keys =
    keys |> Seq.map (fun objs -> 
      objs |> Seq.map (fun o -> if o = null then "" else o.ToString()) 
            |> String.concat " - ")

  /// Store data frame to a CSV file using the specified information
  /// (use TSV format if file ends with .tsv, when including row keys, the
  /// caller needs to provide headers)
  let writeCsv (writer:TextWriter) fileNameOpt separatorOpt cultureOpt includeRowKeys (rowKeyNames:seq<_> option) (frame:Frame<_, _>) = 
    let ci = defaultArg cultureOpt CultureInfo.InvariantCulture
    let includeRowKeys = defaultArg includeRowKeys (rowKeyNames.IsSome)
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
        

    // Get the data from the data frame
    let { ColumnKeys = colKeys; RowKeys = rowKeys; Columns = columns } = frame.GetFrameData()
    // Get number of row keys (or 1 if there are no values)
    let rowKeyCount = defaultArg (rowKeys |> Seq.map Array.length |> Seq.tryPick Some) 1
    
    // Generate or get names for row keys & format header row
    let rowKeyNames = 
      match rowKeyNames with 
      | Some keys when keys |> Seq.length <> rowKeyCount -> invalidArg "rowKeyNames" "Mismatching numbe of row keys"
      | Some keys -> keys
      | None when rowKeyCount = 1 -> Seq.singleton "Key" 
      | None -> seq { for i in 1 .. rowKeyCount -> sprintf "Key #%d" i }
    let headers = 
      [ if includeRowKeys then yield! rowKeyNames
        yield! flattenKeys colKeys ]

    // Concatenate correctly formatted columns & write
    let sepStr = separator.ToString()
    let writeLine seq = writer.WriteLine(String.concat sepStr seq)

    // Generate CSV - write headers & data 
    headers |> Seq.map (fun s -> formatPlainString s) |> writeLine
    writeFrameRows includeRowKeys rowKeys columns (fun data ->
      data |> Seq.map formatOptional |> writeLine)

      
  /// Export the specified frame to 'DataTable'. Caller needs to specify
  /// keys (headers) for the row keys (there may be more of them for multi-level index)
  let toDataTable rowKeyNames (frame:Frame<_, _>) =
    // Get the data from the data frame
    let { ColumnKeys = colKeys; RowKeys = rowKeys; Columns = columns } = frame.GetFrameData()
    // Get number of row keys (or 1 if there are no values)
    let rowKeyCount = defaultArg (rowKeys |> Seq.map Array.length |> Seq.tryPick Some) 1
    let rowKeyTypes = defaultArg (rowKeys |> Seq.map (Array.map (fun v -> v.GetType())) |> Seq.tryPick Some) [| typeof<obj> |]
    if rowKeyNames |> Seq.length <> rowKeyCount then invalidArg "rowKeyNames" "Mismatching numbe of row keys"
    
    // Create data table & add index columns with their types, then add data columns with types
    let dt = new DataTable()
    (rowKeyNames, rowKeyTypes) ||> Seq.map2 (fun rk rt -> new DataColumn(rk, rt)) |> Seq.iter dt.Columns.Add
    (flattenKeys colKeys, columns) ||> Seq.map2 (fun ck (ct, _) -> new DataColumn(ck, ct)) |> Seq.iter dt.Columns.Add

    // Write all data to the data table
    writeFrameRows true rowKeys columns (fun data ->
      dt.Rows.Add([| for _, v in data -> match v with Some o -> o | _ -> box System.DBNull.Value |])
      |> ignore
    )
    dt

  /// Load data frame from a data reader (and cast the values of columns
  /// to their actual types to create IVector<T> with the right T)
  let readReader (reader:System.Data.IDataReader) =
    let fields = reader.FieldCount
    let lists = Array.init fields (fun _ -> ResizeArray<_>(1000))
    let mutable count = 0
    while reader.Read() do
      count <- count + 1
      for i in 0 .. fields - 1 do
        let value = 
          if reader.IsDBNull(i) then OptionalValue.Missing
          else OptionalValue(reader.GetValue(i))
        lists.[i].Add(value)
    
    let frameData =
      lists 
      |> Array.mapi (reader.GetFieldType >> Reflection.createTypedVector)
      |> Vector.ofValues
    let rowIndex = Index.ofKeys [ 0 .. count - 1 ]
    let colIndex = Index.ofKeys [ for i in 0 .. fields - 1 -> reader.GetName(i) ]
    Frame<int, string>(rowIndex, colIndex, frameData)


  /// Load data from a CSV file using F# Data API
  let readCsv (reader:TextReader) hasHeaders inferTypes inferRows schema (missingValues:string) separators culture =
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
    let data = Csv.CsvFile.Load(reader, ?separators=separators, ?hasHeaders=hasHeaders)
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
    let headers = 
      match data.Headers with
      | Some headers -> headers
      | None -> [| for i in 1 .. data.NumberOfColumns -> sprintf "Column %d" i |]
      
    let columnIndex = Index.ofKeys headers
    let columns = 
      Seq.zip headers inferedProperties |> Seq.mapi (fun i (name, prop) ->
            [| for row in data.Data -> row.Columns.[i] |]
            |> createVector prop.RuntimeType )
    let rowIndex = Index.ofKeys [ 0 .. (Seq.length data.Data) - 1 ]
    Frame(rowIndex, columnIndex, Vector.ofValues columns)


  /// Create data frame from a sequence of values using
  /// projections that return column/row keys and the value
  let fromValues values colSel rowSel valSel =
    values 
    |> Seq.groupBy colSel
    |> Seq.map (fun (col, items) -> 
        let items = Array.ofSeq items
        // TODO: "infer" type for the column
        col, Series(Array.map rowSel items, Array.map valSel items) )
    |> Series.ofObservations
    |> FrameUtils.fromColumns

  /// Expand properties of vectors recursively. Nothing is done when `nesting = 0`.
  let expandVectors nesting (frame:Frame<'R, string>) =
    let rec loop nesting cols =      
      if nesting = 0 then cols else
      seq {
        for name, col in cols do
          match Reflection.expandUntypedVector col with
          | [] -> yield name, col
          | cols -> for newName, newCol in cols do yield name + "." + newName, newCol }
      |> loop (nesting - 1)

    let cols = Seq.zip frame.ColumnKeys (frame.Data.DataSequence |> Seq.map OptionalValue.get)
    let newCols = loop nesting cols |> Array.ofSeq
    let newColIndex = Index.ofKeys (Seq.map fst newCols)
    let newData = Vector.ofValues (Seq.map snd newCols)
    Frame<_, _>(frame.RowIndex, newColIndex, newData)

  /// Expand properties of vectors recursively. Nothing is done when `nesting = 0`.
  let expandColumns expandNames (frame:Frame<'R, string>) =
    let cols = Seq.zip frame.ColumnKeys (frame.Data.DataSequence |> Seq.map OptionalValue.get)
    let expandName = set expandNames
    let newCols = cols |> Seq.collect (fun (name, vector) ->
      if Set.contains name expandNames then 
        [ for n, v in Reflection.expandUntypedVector vector -> name + "." + n, v ]
      else [name, vector]) |> Array.ofSeq
    let newColIndex = Index.ofKeys (Seq.map fst newCols)
    let newData = Vector.ofValues (Seq.map snd newCols)
    Frame<_, _>(frame.RowIndex, newColIndex, newData)
