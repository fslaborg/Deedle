


(*



(** 
Hiearrchical indexing
---------------------

*)


// F# 3.0 needs this
world.Columns.[Lookup1Of2 "Euro area"]
world.Columns.[Lookup1Of2 "Euro area"]
world.Columns.[Lookup1Of2 "Euro area"].Columns.[Lookup2Of2 "Austria"]
world.Columns.[Lookup2Of2 "Mexico"]
world.Columns.[Lookup2Of2 "Belgium"]

// F# 3.1 makes this way nicer
world.Columns.["Euro area", *]
world.Columns.[*, "Belgium"]
world.Columns.[("Euro area", "Belgium")]

let euro = 
  world.Columns.[Lookup1Of2 "Euro area"]
  |> Frame.mapColKeys snd

let grouped = 
  euro
  |> Frame.groupColsUsing (fun k _ -> k.Substring(0, 1))
  |> Frame.orderCols
  |> Frame.groupRowsUsing (fun k _ -> sprintf "%d0s" (k / 10))

grouped.Rows.["1990s", *].Columns.["F", *]

grouped.Columns.[("A", "Austria")].As<float>() / 1.0e9
|> Series.meanBy fst

grouped.Columns.[("C", "Cyprus")].As<float>() / 1.0e9
|> Series.meanBy fst

grouped / 1.0e9
|> Frame.meanBy fst

grouped / 1.0e9
|> Frame.transpose
|> Frame.meanBy fst

grouped / 1.0e9
|> Frame.meanBy fst
|> Frame.transpose
|> Frame.meanBy fst

let oddEvenGroups =
  euro / 1.0e9
  |> Frame.mapRowKeys (fun rk -> (sprintf "%ds" (rk/10*10), ((if rk%2 = 0 then "even" else "odd"), rk)))
  |> Frame.orderRows

oddEvenGroups 
|> Frame.meanBy fst




let ageByClassAndPort = byClassAndPort.Columns.["Age"].As<float>()

Frame.ofColumns
  [ "AgeMeans", ageByClassAndPort |> Series.meanBy Tuple.get1And2Of3
    "AgeCounts", float $ (ageByClassAndPort |> Series.countBy Tuple.get1And2Of3) ]

byClassAndPort
|> Frame.meanBy Tuple.get1And2Of3

byClassAndPort
|> Frame.sumBy Tuple.get1And2Of3

let survivedByClassAndPort = byClassAndPort.Columns.["Survived"].As<bool>()

// survivedByClassAndPort
// |> Series.meanBy By1Of3

survivedByClassAndPort 
|> Series.foldBy Tuple.get1And2Of3 (fun sr -> sprintf "%A" (sr.Values |> Seq.countBy id |> List.ofSeq))

byClassAndPort
|> Frame.foldBy Tuple.get1And2Of3 (fun sr -> sr.CountValues())



let sample = Frame.ofColumns [ "Test" => Series.ofValues [ 1.0 .. 4.0 ] ]
let groupedSample = sample.ReplaceRowIndexKeys [ ("Small", 0); ("Small", 1); ("Big", 0); ("Big", 1) ] |> Frame.orderRows

(** 
Overlaoded slicing
------------------
*)

let wb2 = WorldBankData.GetDataContext()

let arab = 
  Frame.ofColumns
    [ for country in wb2.Regions.``Arab World``.Countries -> 
        country.Name => Series.ofObservations country.Indicators.``GDP (current US$)`` ]

arab.Columns.[ ["Algeria"; "Bahrain"] ]
|> Frame.mean

arab.Rows.[ 2000 .. 2012 ]



(**
Joining series
--------------
*)

// sortBy : seq<'T> -> orderdSeq<'T>
// thenBy : orderdSeq<'T> -> orderdSeq<'T>


(*
query { for n in [ 1 .. 10 ] do
        let m = n + 1 
        sortBy n
        thenBy m
        select (string n) into g
        take 10
        where (n > 4)
        select n }
// 
frame { for r in frame do
        withIndex (r.GetAs<string>("Name"))
        shift 1
        window 5 into win 
        ... } 
*)

let sa = Series.ofObservations [ 1 => "a"; 2 => "b" ]
let sb = Series.ofObservations [ 3 => "c"; 2 => "b" ]

sa.Join(sb, JoinKind.Inner, Lookup.Exact)
sa.Join(sb, JoinKind.Left, Lookup.Exact)
sa.Join(sb, JoinKind.Outer, Lookup.Exact)

(** 
Grouping 
--------
*)

// Load the data from the Titanic data set
let titanic = Frame.readCsv(__SOURCE_DIRECTORY__ + "/data/Titanic.csv")

// Group the data frame by sex 
let grouped = titanic |> Frame.groupRowsBy "Sex"

// For each group, calculate the total number of survived & died
let survivalBySex =
  grouped 
  |> Series.map (fun sex df -> 
      // Group each sex by the Survived column & count elements in each group
      df.GetSeries<bool>("Survived") |> Series.groupBy (fun k v -> v) 
      |> Frame.ofColumns |> Frame.countValues )
  |> Frame.ofRows
  |> Frame.withColumnKeys ["Died"; "Survived"]

// Add column with Total number of males/females on Titanic
survivalBySex?Total <- grouped |> Series.map (fun _ -> Frame.countKeys)

// Build a data frame with nice summary of rates in percents
let results =
  [ "Died (%)" => survivalBySex?Died / survivalBySex?Total * 100.0
    "Survived (%)" => survivalBySex?Survived / survivalBySex?Total * 100.0 ]
  |> Frame.ofColumns



(**
Grouping
--------

*)
let peopleCountries = 
  df.GetSeries<string list>("Countries")
  |> Series.map (fun k countries -> 
      [ for c, k in Seq.countBy id countries -> c, k ] |> Series.ofObservations )
  |> Frame.ofRows
  |> Frame.withMissingVal 0

let sums = peopleCountries |> Frame.sum 

sums.Observations
|> Seq.sortBy (fun (c, v) -> -v)
|> Chart.Column


(* More options when working with records *)
let recdSeries = 
  Series.ofValues [ "Tomas"; null; "Joe"; "Eve" ]
  |> Series.map (fun _ v -> { Name = v; Age = -1; Countries = [] })

// Missing values are preserved at the right index..
Frame.ofRecords recdSeries



let dfPeople = Frame.ofRecords people 
dfPeople?CountryCount <- List.length $ dfPeople.GetSeries<string list>("Countries")
dfPeople |> Frame.groupRowsBy






(* TEST *)

let f1 = Frame.ofRows [ 1 => Series.ofObservations [ "A" => 1; "B" => 2 ] ]
let f2 = Frame.ofRows [ 2 => Series.ofObservations [ "C" => 3 ] 
                        3 => Series.ofObservations [ "C" => 4 ]  ]
f1.Append(f2)


*)