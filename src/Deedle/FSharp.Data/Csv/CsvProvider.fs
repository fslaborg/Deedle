// --------------------------------------------------------------------------------------
// CSV type provider
// --------------------------------------------------------------------------------------
namespace ProviderImplementation

open System
open System.IO
open FSharp.Core.CompilerServices
open FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open ProviderImplementation.ProviderHelpers
open FSharp.Data
open FSharp.Data.Runtime
open FSharp.Data.Runtime.CsvInference
open ProviderImplementation
open ProviderImplementation.QuotationBuilder

// --------------------------------------------------------------------------------------

[<TypeProvider>]
type public CsvProvider(cfg:TypeProviderConfig) as this =
    inherit DisposableTypeProviderForNamespaces(cfg, assemblyReplacementMap=[ "FSharp.Data.DesignTime", "FSharp.Data" ])
  
    // Generate namespace and type 'FSharp.Data.CsvProvider'
    let asm = AssemblyResolver.init cfg (this :> TypeProviderForNamespaces)
    let ns = "FSharp.Data"
    let csvProvTy = ProvidedTypeDefinition(asm, ns, "CsvProvider", None, hideObjectMethods=true, nonNullable = true)
  
    let buildTypes (typeName:string) (args:obj[]) =
  
        let sample = args.[0] :?> string
        let separators = args.[1] :?> string
        let inferRows = args.[2] :?> int
        let schema = args.[3] :?> string
        let hasHeaders = args.[4] :?> bool
        let ignoreErrors = args.[5] :?> bool
        let skipRows = args.[6] :?> int
        let assumeMissingValues = args.[7] :?> bool
        let preferOptionals = args.[8] :?> bool
        let quote = args.[9] :?> char
        let missingValuesStr = args.[10] :?> string
        let cacheRows = args.[11] :?> bool
        let cultureStr = args.[12] :?> string
        let encodingStr = args.[13] :?> string
        let resolutionFolder = args.[14] :?> string
        let resource = args.[15] :?> string
        
        if sample = "" then
            if schema = "" then
                failwith "When the Sample parameter is not specified, the Schema parameter must be provided"
            if hasHeaders then
                failwith "When the Sample parameter is not specified, the HasHeaders parameter must be set to false"
        
        let getSpec (extension:string) value = 
          
            use sampleCsv = using (IO.logTime "Parsing" sample) <| fun _ ->
                let separators = 
                    if String.IsNullOrEmpty separators && extension.ToLowerInvariant() = ".tsv"
                    then "\t" else separators
                let value = 
                    if sample = "" then 
                        // synthetize sample from the schema
                        use reader = new StringReader(value)
                        let schemaStr = CsvReader.readCsvFile reader "," '"' |> Seq.exactlyOne |> fst
                        Array.zeroCreate schemaStr.Length
                        |> String.concat (if String.IsNullOrEmpty separators then "," else separators.[0].ToString())
                    else
                        value
                
                CsvFile.Parse(value, separators, quote, hasHeaders, ignoreErrors, skipRows)
            
            let separators = sampleCsv.Separators
            
            let inferredFields = using (IO.logTime "Inference" sample) <| fun _ ->
                sampleCsv.InferColumnTypes(inferRows, TextRuntime.GetMissingValues missingValuesStr, TextRuntime.GetCulture cultureStr, schema,
                                           assumeMissingValues, preferOptionals, ProviderHelpers.unitsOfMeasureProvider)
            
            using (IO.logTime "TypeGeneration" sample) <| fun _ ->
            
            let csvType, csvErasedType, rowType, stringArrayToRow, rowToStringArray = 
                inferredFields 
                |> CsvTypeBuilder.generateTypes asm ns typeName (missingValuesStr, cultureStr) 
            
            let stringArrayToRowVar = Var("stringArrayToRow", stringArrayToRow.Type)
            let rowToStringArrayVar = Var("rowToStringArray", rowToStringArray.Type)
            
            let paramType = typedefof<seq<_>>.MakeGenericType(rowType)
            let headers = 
                match sampleCsv.Headers with 
                | None -> <@@ None: string[] option @@> 
                | Some headers -> Expr.NewArray(typeof<string>, headers |> Array.map (fun h -> Expr.Value(h)) |> List.ofArray) |> (fun x-> <@@ Some (%%x : string[]) @@>)
            
            let ctor = 
                ProvidedConstructor(
                    [ ProvidedParameter("rows", paramType) ], 
                    invokeCode = (fun (Singleton paramValue) ->
                      let body = csvErasedType?CreateEmpty () (Expr.Var rowToStringArrayVar, paramValue, headers,  sampleCsv.NumberOfColumns, separators, quote)
                      Expr.Let(rowToStringArrayVar, rowToStringArray, body)))
            csvType.AddMember(ctor) 
            
            let parseRows = 
                ProvidedMethod("ParseRows", 
                    [ProvidedParameter("text", typeof<string>)], 
                    rowType.MakeArrayType(), 
                    isStatic = true,
                    invokeCode = fun (Singleton text) ->         
                      let body = csvErasedType?ParseRows () (text, Expr.Var stringArrayToRowVar, separators, quote, ignoreErrors)
                      Expr.Let(stringArrayToRowVar, stringArrayToRow, body))
            csvType.AddMember parseRows
            
            { GeneratedType = csvType
              RepresentationType = csvType
              CreateFromTextReader = fun reader ->
                  let body = 
                      csvErasedType?Create () (Expr.Var stringArrayToRowVar, Expr.Var rowToStringArrayVar, reader, 
                                               separators, quote, hasHeaders, ignoreErrors, skipRows, cacheRows)
                  Expr.Let(stringArrayToRowVar, stringArrayToRow, Expr.Let(rowToStringArrayVar, rowToStringArray, body))
              CreateFromTextReaderForSampleList = fun _ -> failwith "Not Applicable" }
        
        let maxNumberOfRows = if inferRows > 0 then Some inferRows else None
        
        // On the CsvProvider the schema might be partial and we will still infer from the sample
        // So we handle it in a custom way
        generateType "CSV" (if sample <> "" then Sample sample else Schema schema) getSpec this cfg  encodingStr resolutionFolder resource typeName maxNumberOfRows
  
    // Add static parameter that specifies the API we want to get (compile-time) 
    let parameters = 
        [ ProvidedStaticParameter("Sample", typeof<string>, parameterDefaultValue = "") 
          ProvidedStaticParameter("Separators", typeof<string>, parameterDefaultValue = "") 
          ProvidedStaticParameter("InferRows", typeof<int>, parameterDefaultValue = 1000)
          ProvidedStaticParameter("Schema", typeof<string>, parameterDefaultValue = "")
          ProvidedStaticParameter("HasHeaders", typeof<bool>, parameterDefaultValue = true)
          ProvidedStaticParameter("IgnoreErrors", typeof<bool>, parameterDefaultValue = false)
          ProvidedStaticParameter("SkipRows", typeof<int>, parameterDefaultValue = 0)
          ProvidedStaticParameter("AssumeMissingValues", typeof<bool>, parameterDefaultValue = false)
          ProvidedStaticParameter("PreferOptionals", typeof<bool>, parameterDefaultValue = false)
          ProvidedStaticParameter("Quote", typeof<char>, parameterDefaultValue = '"')
          ProvidedStaticParameter("MissingValues", typeof<string>, parameterDefaultValue = "")
          ProvidedStaticParameter("CacheRows", typeof<bool>, parameterDefaultValue = true)
          ProvidedStaticParameter("Culture", typeof<string>, parameterDefaultValue = "")
          ProvidedStaticParameter("Encoding", typeof<string>, parameterDefaultValue = "") 
          ProvidedStaticParameter("ResolutionFolder", typeof<string>, parameterDefaultValue = "")
          ProvidedStaticParameter("EmbeddedResource", typeof<string>, parameterDefaultValue = "") ]
  
    let helpText = 
        """<summary>Typed representation of a CSV file.</summary>
           <param name='Sample'>Location of a CSV sample file or a string containing a sample CSV document.</param>
           <param name='Separators'>Column delimiter(s). Defaults to `,`.</param>
           <param name='InferRows'>Number of rows to use for inference. Defaults to `1000`. If this is zero, all rows are used.</param>
           <param name='Schema'>Optional column types, in a comma separated list. Valid types are `int`, `int64`, `bool`, `float`, `decimal`, `date`, `guid`, `string`, `int?`, `int64?`, `bool?`, `float?`, `decimal?`, `date?`, `guid?`, `int option`, `int64 option`, `bool option`, `float option`, `decimal option`, `date option`, `guid option` and `string option`.
           You can also specify a unit and the name of the column like this: `Name (type&lt;unit&gt;)`, or you can override only the name. If you don't want to specify all the columns, you can reference the columns by name like this: `ColumnName=type`.</param>
           <param name='HasHeaders'>Whether the sample contains the names of the columns as its first line.</param>
           <param name='IgnoreErrors'>Whether to ignore rows that have the wrong number of columns or which can't be parsed using the inferred or specified schema. Otherwise an exception is thrown when these rows are encountered.</param>
           <param name='SkipRows'>Skips the first n rows of the CSV file.</param>
           <param name='AssumeMissingValues'>When set to true, the type provider will assume all columns can have missing values, even if in the provided sample all values are present. Defaults to false.</param>
           <param name='PreferOptionals'>When set to true, inference will prefer to use the option type instead of nullable types, `double.NaN` or `""` for missing values. Defaults to false.</param>
           <param name='Quote'>The quotation mark (for surrounding values containing the delimiter). Defaults to `"`.</param>
           <param name='MissingValues'>The set of strings recogized as missing values. Defaults to `""" + String.Join(",", TextConversions.DefaultMissingValues) + """`.</param>
           <param name='CacheRows'>Whether the rows should be caches so they can be iterated multiple times. Defaults to true. Disable for large datasets.</param>
           <param name='Culture'>The culture used for parsing numbers and dates. Defaults to the invariant culture.</param>
           <param name='Encoding'>The encoding used to read the sample. You can specify either the character set name or the codepage number. Defaults to UTF8 for files, and to ISO-8859-1 the for HTTP requests, unless `charset` is specified in the `Content-Type` response header.</param>
           <param name='ResolutionFolder'>A directory that is used when resolving relative file references (at design time and in hosted execution).</param>
           <param name='EmbeddedResource'>When specified, the type provider first attempts to load the sample from the specified resource 
              (e.g. 'MyCompany.MyAssembly, resource_name.csv'). This is useful when exposing types generated by the type provider.</param>"""
       
    do csvProvTy.AddXmlDoc helpText
    do csvProvTy.DefineStaticParameters(parameters, buildTypes)
  
    // Register the main type with F# compiler
    do this.AddNamespace(ns, [ csvProvTy ])
  