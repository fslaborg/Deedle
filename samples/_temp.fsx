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