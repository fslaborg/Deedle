module TallyHo

[<AutoOpen>]
module internal Array =
    /// Tally up items that match specified criteria
    let tally criteria items = 
        items |> Array.filter criteria |> Seq.length
    /// Percentage of items that match specified criteria
    let percentage criteria items =
        let total = items |> Seq.length
        let count = items |> tally criteria
        float count * 100.0 / float total
    /// Where = filter
    let where f xs = Array.filter f xs
    /// F# interactive friendly groupBy
    let groupBy f xs =
        xs 
        |> Seq.groupBy f |> Seq.toArray 
        |> Array.map (fun (k,vs) -> k, vs |> Seq.toArray)