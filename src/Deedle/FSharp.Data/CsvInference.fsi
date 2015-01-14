// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
module internal FSharp.Data.Runtime.CsvInference

open FSharp.Data
open System.Globalization
open FSharp.Data.Runtime.StructuralTypes
open FSharp.Data.Runtime.StructuralInference

type CsvFile with
  member InferColumnTypes : inferRows:int * missingValues:string [] * cultureInfo:CultureInfo * schema:string * assumeMissingValues:bool * preferOptionals:bool * ?unitsOfMeasureProvider:IUnitsOfMeasureProvider -> PrimitiveInferedProperty list