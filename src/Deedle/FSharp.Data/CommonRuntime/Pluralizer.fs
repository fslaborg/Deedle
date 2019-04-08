// --------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// This source code is subject to terms and conditions of the Microsoft Permissive
// License (MS-PL). A copy of the license can be found in the license.htm file
// included in this distribution.
//
// You must not remove this notice, or any other, from this software.
//
// --------------------------------------------------------------------------------------

module internal FSharp.Data.Runtime.Pluralizer

open System
open System.Collections.Generic

// Pluralization service for nice 'NameUtils.fs' based on C# code from 
// http://blogs.msdn.com/b/dmitryr/archive/2007/01/11/simple-english-noun-pluralizer-in-c.aspx 
// (with a couple of more rules were added)

type private SuffixRule = 
    { SingularSuffix : string
      PluralSuffix : string }

let private tables = lazy(

    let suffixRules =
        ["ch",       "ches"
         "sh",       "shes"
         "ss",       "sses"
         
         "ay",       "ays"
         "ey",       "eys"
         "iy",       "iys"
         "oy",       "oys"
         "uy",       "uys"
         "y",        "ies"
         
         "ao",       "aos"
         "eo",       "eos"
         "io",       "ios"
         "oo",       "oos"
         "uo",       "uos"
         "o",        "oes"

         "house",    "houses"
         "course",   "courses"
         
         "cis",      "ces"
         "us",       "uses"
         "sis",      "ses"
         "xis",      "xes"
         
         "louse",    "lice"
         "mouse",    "mice"
         
         "zoon",     "zoa"
         
         "man",      "men"
         
         "deer",     "deer"
         "fish",     "fish"
         "sheep",    "sheep"
         "itis",     "itis"
         "ois",      "ois"
         "pox",      "pox"
         "ox",       "oxes"

         "foot",     "feet"
         "goose",    "geese"
         "tooth",    "teeth"

         "alf",      "alves"
         "elf",      "elves"
         "olf",      "olves"
         "arf",      "arves"
         "leaf",     "leaves"
         "nife",     "nives"
         "life",     "lives"
         "wife",     "wives"]

    let specialWords = 
        ["agendum",          "agenda",           ""
         "aircraft",         "",                 ""
         "albino",           "albinos",          ""
         "alga",             "algae",            ""
         "alumna",           "alumnae",          ""
         "alumnus",          "alumni",          ""
         "apex",             "apices",           "apexes"
         "archipelago",      "archipelagos",     ""
         "bacterium",        "bacteria",         ""
         "beef",             "beefs",            "beeves"
         "bison",            "",                 ""
         "brother",          "brothers",         "brethren"
         "candelabrum",      "candelabra",       ""
         "carp",             "",                 ""
         "casino",           "casinos",          ""
         "child",            "children",         ""
         "chassis",          "",                 ""
         "chinese",          "",                 ""
         "clippers",         "",                 ""
         "cod",              "",                 ""
         "codex",            "codices",          ""
         "commando",         "commandos",        ""
         "corps",            "",                 ""
         "cortex",           "cortices",         "cortexes"
         "cow",              "cows",             "kine"
         "criterion",        "criteria",         ""
         "datum",            "data",             ""
         "debris",           "",                 ""
         "diabetes",         "",                 ""
         "ditto",            "dittos",           ""
         "djinn",            "",                 ""
         "dynamo",           "",                 ""
         "elk",              "",                 ""
         "embryo",           "embryos",          ""
         "ephemeris",        "ephemeris",        "ephemerides"
         "erratum",          "errata",           ""
         "extremum",         "extrema",          ""
         "fiasco",           "fiascos",          ""
         "fish",             "fishes",           "fish"
         "flounder",         "",                 ""
         "focus",            "focuses",          "foci"
         "fungus",           "fungi",            "funguses"
         "gallows",          "",                 ""
         "genie",            "genies",           "genii"
         "ghetto",           "ghettos",          ""
         "graffiti",         "",                 ""
         "headquarters",     "",                 ""
         "herpes",           "",                 ""
         "homework",         "",                 ""
         "index",            "indices",          "indexes"
         "inferno",          "infernos",         ""
         "japanese",         "",                 ""
         "jumbo",            "jumbos",           ""
         "latex",            "latices",          "latexes"
         "lingo",            "lingos",           ""
         "mackerel",         "",                 ""
         "macro",            "macros",           ""
         "manifesto",        "manifestos",       ""
         "measles",          "",                 ""
         "money",            "moneys",           "monies"
         "mongoose",         "mongooses",        "mongoose"
         "mumps",            "",                 ""
         "murex",            "murecis",          ""
         "mythos",           "mythos",           "mythoi"
         "news",             "",                 ""
         "octopus",          "octopuses",        "octopodes"
         "ovum",             "ova",              ""
         "ox",               "ox",               "oxen"
         "photo",            "photos",           ""
         "pincers",          "",                 ""
         "pliers",           "",                 ""
         "pro",              "pros",             ""
         "rabies",           "",                 ""
         "radius",           "radiuses",         "radii"
         "rhino",            "rhinos",           ""
         "salmon",           "",                 ""
         "scissors",         "",                 ""
         "series",           "",                 ""
         "shears",           "",                 ""
         "silex",            "silices",          ""
         "simplex",          "simplices",        "simplexes"
         "soliloquy",        "soliloquies",      "soliloquy"
         "species",          "",                 ""
         "stratum",          "strata",           ""
         "swine",            "",                 ""
         "trout",            "",                 ""
         "tuna",             "",                 ""
         "vertebra",         "vertebrae",        ""
         "vertex",           "vertices",         "vertexes"
         "vortex",           "vortices",         "vortexes"]

    let suffixRules =
        suffixRules
        |> List.map (fun (singular, plural) -> { SingularSuffix = singular; PluralSuffix = plural })

    let specialSingulars = new Dictionary<_, _>(StringComparer.OrdinalIgnoreCase)
    let specialPlurals = new Dictionary<_, _>(StringComparer.OrdinalIgnoreCase)
    
    for singular, plural, plural2 in specialWords do
        let plural = if plural = "" then singular else plural
        specialPlurals.Add(singular, plural)
        specialSingulars.Add(plural, singular)
        if plural2 <> "" then
            specialSingulars.Add(plural2, singular)

    suffixRules, specialSingulars, specialPlurals)

let private adjustCase (s: string) (template: string) =
    if String.IsNullOrEmpty s then 
        s
    else
        // determine the type of casing of the template string
        let mutable foundUpperOrLower = false
        let mutable allLower = true
        let mutable allUpper = true
        let mutable firstUpper = false

        for i = 0 to template.Length - 1 do
            if Char.IsUpper template.[i] then
                if i = 0 then
                    firstUpper <- true
                allLower <- false
                foundUpperOrLower <- true
            else if Char.IsLower template.[i] then
                allUpper <- false
                foundUpperOrLower <- true

        // change the case according to template
        if not foundUpperOrLower then
            s
        else if allLower then
            s.ToLowerInvariant()
        else if allUpper then
            s.ToUpperInvariant()
        else if firstUpper && not <| Char.IsUpper s.[0] then
            s.Substring(0, 1).ToUpperInvariant() + s.Substring(1)
        else
            s

let private tryToPlural (word: string) suffixRule =
    if word.EndsWith(suffixRule.SingularSuffix, StringComparison.OrdinalIgnoreCase) then
        Some <| word.Substring(0, word.Length - suffixRule.SingularSuffix.Length) + suffixRule.PluralSuffix
    else
        None

let private tryToSingular (word: string) suffixRule = 
    if word.EndsWith(suffixRule.PluralSuffix, StringComparison.OrdinalIgnoreCase) then
        Some <| word.Substring(0, word.Length - suffixRule.PluralSuffix.Length) + suffixRule.SingularSuffix
    else
        None

let toPlural noun =
    if String.IsNullOrEmpty noun then noun
    else 
        let suffixRules, _, specialPlurals = tables.Value
        let plural = 
            match specialPlurals.TryGetValue noun with
            | true, plural -> plural
            | false, _ -> 
                match suffixRules |> Seq.tryPick (tryToPlural noun) with
                | Some plural -> plural
                | None -> 
                    if noun.EndsWith("s", StringComparison.OrdinalIgnoreCase) then
                        noun
                    else
                        noun + "s"
                        
        (plural, noun) ||> adjustCase

let toSingular noun =
    if String.IsNullOrEmpty noun then noun
    else 
        let suffixRules, specialSingulars, _ = tables.Value
        let singular = 
            match specialSingulars.TryGetValue noun with
            | true, singular -> singular
            | false, _ -> 
                match suffixRules |> Seq.tryPick (tryToSingular noun) with
                | Some singular -> singular
                | None ->
                    if noun.EndsWith("s", StringComparison.OrdinalIgnoreCase) && not <| noun.EndsWith("us", StringComparison.OrdinalIgnoreCase) then
                        noun.Substring(0, noun.Length - 1)
                    else
                        noun
        (singular, noun) ||> adjustCase
