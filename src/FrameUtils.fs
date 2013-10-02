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
  open System.IO
  open ProviderImplementation
  open FSharp.Data.RuntimeImplementation
  open FSharp.Data.RuntimeImplementation.StructuralTypes

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
