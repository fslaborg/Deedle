// --------------------------------------------------------------------------------------
// CSV type provider - generate code for accessing inferred elements
// --------------------------------------------------------------------------------------
namespace ProviderImplementation

open System
open System.Reflection
open FSharp.Quotations
open FSharp.Reflection
open FSharp.Data
open FSharp.Data.Runtime
open ProviderImplementation
open ProviderImplementation.ProvidedTypes
open ProviderImplementation.QuotationBuilder

module internal CsvTypeBuilder =

  type private FieldInfo = 
    { /// The representation type that is part of the tuple we extract the field from
      TypeForTuple : Type
      /// The provided property corresponding to the field
      ProvidedProperty : ProvidedProperty
      Convert: Expr -> Expr
      ConvertBack: Expr -> Expr
      /// The provided parameter corresponding to the field
      ProvidedParameter : ProvidedParameter }

  let generateTypes asm ns typeName (missingValuesStr, cultureStr) inferredFields =
    
    let fields = inferredFields |> List.mapi (fun index field ->
      let typ, typWithoutMeasure, conv, convBack = ConversionsGenerator.convertStringValue missingValuesStr cultureStr field
      let propertyName = NameUtils.capitalizeFirstLetter field.Name
      { TypeForTuple = typWithoutMeasure
        ProvidedProperty = ProvidedProperty(propertyName, typ, getterCode = fun (Singleton row) -> 
            match inferredFields with 
            | [ _ ] -> row
            | _ -> Expr.TupleGet(row, index))
        Convert = fun rowVarExpr -> conv <@ TextConversions.AsString((%%rowVarExpr:string[]).[index]) @>
        ConvertBack = fun rowVarExpr -> convBack (match inferredFields with [ _ ] -> rowVarExpr | _ -> Expr.TupleGet(rowVarExpr, index))
        ProvidedParameter = ProvidedParameter(NameUtils.niceCamelName propertyName, typ) } )

    // The erased row type will be a tuple of all the field types (without the units of measure).  If there is a single column then it is just the column type.
    let rowErasedType = 
        match fields with 
        | [ field ] -> field.TypeForTuple
        | _ -> FSharpType.MakeTupleType([| for field in fields -> field.TypeForTuple |])
    
    let rowType = ProvidedTypeDefinition("Row", Some rowErasedType, hideObjectMethods = true, nonNullable = true)

    let ctor = 
        ProvidedConstructor([ for field in fields -> field.ProvidedParameter ], invokeCode = fun args -> 
            match args with 
            | [ arg ] -> arg
            | _ -> Expr.NewTuple(args))
    rowType.AddMember ctor
    
    // Each property of the generated row type will simply be a tuple get
    for field in fields do
      rowType.AddMember field.ProvidedProperty

    // The erased csv type will be parameterised by the tuple type
    let csvErasedTypeWithRowErasedType = typedefof<CsvFile<_>>.MakeGenericType(rowErasedType) 
    let csvErasedTypeWithGeneratedRowType = typedefof<CsvFile<_>>.MakeGenericType(rowType) 

    let csvType = ProvidedTypeDefinition(asm, ns, typeName, Some csvErasedTypeWithGeneratedRowType, hideObjectMethods = true, nonNullable = true)
    csvType.AddMember rowType
    
    // Based on the set of fields, create a function that converts a string[] to the tuple type
    let stringArrayToRow = 
      let parentVar = Var("parent", typeof<obj>)
      let rowVar = Var("row", typeof<string[]>)
      let rowVarExpr = Expr.Var rowVar

      // Convert each element of the row using the appropriate conversion
      let body = 
          match [ for field in fields -> field.Convert rowVarExpr ] with 
          | [ col ] ->  col
          | cols -> Expr.NewTuple cols

      let delegateType = typedefof<Func<_,_,_>>.MakeGenericType(typeof<obj>, typeof<string[]>, rowErasedType)

      Expr.NewDelegate(delegateType, [parentVar; rowVar], body)

    // Create a function that converts the tuple type to a string[]
    let rowToStringArray =
      let rowVar = Var("row", rowErasedType)
      let rowVarExpr = Expr.Var rowVar
      let body = Expr.NewArray(typeof<string>, [ for field in fields -> field.ConvertBack rowVarExpr ])
      let delegateType = typedefof<Func<_,_>>.MakeGenericType(rowErasedType, typeof<string[]>)

      Expr.NewDelegate(delegateType, [rowVar], body)

    csvType, csvErasedTypeWithRowErasedType, rowType, stringArrayToRow, rowToStringArray
