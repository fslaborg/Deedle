namespace Deedle.RProvider.Plugin

open Deedle
open Deedle.Indices
open RProvider
open System.ComponentModel.Composition

//[<Export(typedefof<IConvertToR<Frame<_,_>>>)>]
[<Export(typeof<IDefaultConvertFromR>)>]
type Class1() = 
  interface IDefaultConvertFromR with
    member x.Convert(symExpr) =
      match symExpr with
      | RDotNet.ActivePatterns.DataFrame df ->
          let rowIndex = Index.ofKeys df.RowNames 
          let colIndex = Index.ofKeys df.ColumnNames
          let rows = df.GetRows() |> Array.ofSeq
          let data = Array.init df.ColumnCount (fun colIndex ->
            let colData = rows |> Array.map (fun r -> r.[colIndex])
            (Vector.ofValues colData) :> IVector)
          Some(box (Frame<_,_>(rowIndex, colIndex, Vector.ofValues data)))
      | _ -> None

//        abstract member Convert : REngine * 'inType -> SymbolicExpression
