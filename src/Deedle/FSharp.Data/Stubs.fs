namespace FSharp.Data.Runtime

module NameUtils =
  let nicePascalName name = name
  let uniqueGenerator _ =
    let set = new System.Collections.Generic.HashSet<_>()
    fun name ->
      let names = seq { 
        yield name
        for i in 0 .. System.Int32.MaxValue do yield sprintf "%s%d" name i }
      let newName = names |> Seq.filter (set.Contains >> not) |> Seq.head
      set.Add(newName) |> ignore
      newName

module IO =
  let asyncReadTextAtRuntime a b c d e f : Async<System.IO.TextReader> = 
    failwith "asyncReadTextAtRuntime: F# Data stub - not implemented"
