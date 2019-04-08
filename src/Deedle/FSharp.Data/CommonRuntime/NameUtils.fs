/// Tools for generating nice member names that follow F# & .NET naming conventions
module FSharp.Data.Runtime.NameUtils

open System
open System.Globalization
open System.Collections.Generic
open FSharp.Data.Runtime

// --------------------------------------------------------------------------------------
// Active patterns & operators for parsing strings

let private tryAt (s:string) i = if i >= s.Length then None else Some s.[i]
let private sat f (c:option<char>) = match c with Some c when f c -> Some c | _ -> None
let private (|EOF|_|) c = match c with Some _ -> None | _ -> Some ()
let private (|LetterDigit|_|) = sat Char.IsLetterOrDigit
let private (|Upper|_|) = sat (fun c -> Char.IsUpper c || Char.IsDigit c)
let private (|Lower|_|) = sat (fun c -> Char.IsLower c || Char.IsDigit c)

// --------------------------------------------------------------------------------------

/// Turns a given non-empty string into a nice 'PascalCase' identifier
let nicePascalName (s:string) = 
  if s.Length = 1 then s.ToUpperInvariant() else
  // Starting to parse a new segment 
  let rec restart i = seq {
    match tryAt s i with 
    | EOF -> ()
    | LetterDigit _ & Upper _ -> yield! upperStart i (i + 1)
    | LetterDigit _ -> yield! consume i false (i + 1)
    | _ -> yield! restart (i + 1) }
  // Parsed first upper case letter, continue either all lower or all upper
  and upperStart from i = seq {
    match tryAt s i with 
    | Upper _ -> yield! consume from true (i + 1) 
    | Lower _ -> yield! consume from false (i + 1) 
    | _ ->
        yield from, i
        yield! restart (i + 1) }
  // Consume are letters of the same kind (either all lower or all upper)
  and consume from takeUpper i = seq {
    match tryAt s i with
    | Lower _ when not takeUpper -> yield! consume from takeUpper (i + 1)
    | Upper _ when takeUpper -> yield! consume from takeUpper (i + 1)
    | Lower _ when takeUpper ->
        yield from, (i - 1)
        yield! restart (i - 1)
    | _ -> 
        yield from, i
        yield! restart i }
    
  // Split string into segments and turn them to PascalCase
  seq { for i1, i2 in restart 0 do 
          let sub = s.Substring(i1, i2 - i1) 
          if Array.forall Char.IsLetterOrDigit (sub.ToCharArray()) then
            yield sub.[0].ToString().ToUpperInvariant() + sub.ToLowerInvariant().Substring(1) }
  |> String.Concat

/// Turns a given non-empty string into a nice 'camelCase' identifier
let niceCamelName (s:string) = 
  let name = nicePascalName s
  if name.Length > 0 then
    name.[0].ToString().ToLowerInvariant() + name.Substring(1)
  else name

/// Given a function to format names (such as `niceCamelName` or `nicePascalName`)
/// returns a name generator that never returns duplicate name (by appending an
/// index to already used names)
/// 
/// This function is curried and should be used with partial function application:
///
///     let makeUnique = uniqueGenerator nicePascalName
///     let n1 = makeUnique "sample-name"
///     let n2 = makeUnique "sample-name"
///
let uniqueGenerator (niceName:string->string) =
  let set = new HashSet<_>()
  fun name ->
    let mutable name = niceName name
    if name.Length = 0 then name <- "Unnamed"
    while set.Contains name do 
      let mutable lastLetterPos = String.length name - 1
      while Char.IsDigit name.[lastLetterPos] && lastLetterPos > 0 do
        lastLetterPos <- lastLetterPos - 1
      if lastLetterPos = name.Length - 1 then
        if name.Contains " " then
            name <- name + " 2"
        else
            name <- name + "2"
      elif lastLetterPos = 0 && name.Length = 1 then
        name <- (UInt64.Parse name + 1UL).ToString()
      else
        let number = name.Substring(lastLetterPos + 1)
        name <- name.Substring(0, lastLetterPos + 1) + (UInt64.Parse number + 1UL).ToString()
    set.Add name |> ignore
    name

let capitalizeFirstLetter (s:string) =    
    match s.Length with
        | 0 -> ""
        | 1 -> (Char.ToUpperInvariant s.[0]).ToString()                
        | _ -> (Char.ToUpperInvariant s.[0]).ToString() + s.Substring(1)

/// Trim HTML tags from a given string and replace all of them with spaces
/// Multiple tags are replaced with just a single space. (This is a recursive 
/// implementation that is somewhat faster than regular expression.)
let trimHtml (s:string) = 
  let chars = s.ToCharArray()
  let res = new Text.StringBuilder()

  // Loop and keep track of whether we're inside a tag or not
  let rec loop i emitSpace inside = 
    if i >= chars.Length then () else
    let c = chars.[i] 
    match inside, c with
    | true, '>' -> loop (i + 1) false false
    | false, '<' -> 
        if emitSpace then res.Append(' ') |> ignore
        loop (i + 1) false true
    | _ -> 
        if not inside then res.Append(c) |> ignore
        loop (i + 1) true inside

  loop 0 false false      
  res.ToString().TrimEnd()

/// Return the plural of an English word
let pluralize s =
  Pluralizer.toPlural s

/// Return the singular of an English word
let singularize s =
  Pluralizer.toSingular s
