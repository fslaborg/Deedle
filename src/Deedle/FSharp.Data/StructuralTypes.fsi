// This is an interface file for F# Data component referenced via Paket. We use this 
// to mark all F# Data types & modules as internal, so that they are private to Deedle.
//
// When updating to a new version of F# Data, this may need to be updated. The easiest way
// is to go through the *.fs files, Alt+Enter them into F# Interactive & copy the output.
namespace FSharp.Data.Runtime.StructuralTypes

open System

type internal InferedProperty =
  { Name: string 
    mutable Type: InferedType }
  override ToString : unit -> string

and internal InferedMultiplicity =
  | Single
  | OptionalSingle
  | Multiple

and [<RequireQualifiedAccess>] internal InferedTypeTag =
  | Null
  | Number
  | Boolean
  | String
  | Json
  | DateTime
  | Guid
  | Collection
  | Heterogeneous
  | Record of string option
  member Code : string
  member NiceName : string
  static member ParseCode : str:string -> InferedTypeTag

and [<CustomEquality; NoComparison; RequireQualifiedAccess>] internal InferedType =
  | Primitive of typ: Type * unit: Type option * optional: bool
  | Record of name: string option * fields: InferedProperty list * optional: bool
  | Json of typ: InferedType * optional: bool
  | Collection of order: InferedTypeTag list * types: Map<InferedTypeTag,(InferedMultiplicity * InferedType)>
  | Heterogeneous of types: Map<InferedTypeTag,InferedType>
  | Null
  | Top
  member DropOptionality : unit -> InferedType
  member EnsuresHandlesMissingValues : allowEmptyValues:bool -> InferedType
  member IsOptional : bool
  static member CanHaveEmptyValues : typ:Type -> bool

type internal Bit0 = Bit0
type internal Bit1 = Bit1
type internal Bit = Bit

type internal PrimitiveInferedProperty =
  { Name: string
    InferedType: Type
    RuntimeType: Type
    UnitOfMeasure: Type option
    TypeWrapper: TypeWrapper }
  static member Create : name:string * typ:Type * typWrapper:TypeWrapper * unit:Type option -> PrimitiveInferedProperty
  static member Create : name:string * typ:Type * optional:bool * unit:Type option -> PrimitiveInferedProperty

and [<RequireQualifiedAccess>] internal TypeWrapper =
  | None
  | Option
  | Nullable