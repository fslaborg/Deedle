module DecisionTree

open System.Collections.Generic

module internal Tuple =
    open Microsoft.FSharp.Reflection
    let toArray = FSharpValue.GetTupleFields

module internal Array =
    let removeAt i (xs:'a[]) = [|yield! xs.[..i-1];yield! xs.[i+1..]|]

let splitDataSet(dataSet:obj[][], axis, value) = [|
    for featVec in dataSet do
        if featVec.[axis] = value then 
            yield featVec |> Array.removeAt axis
    |]

let calcShannonEnt(dataSet:obj[][]) =
    let numEntries = dataSet.Length
    dataSet 
    |> Seq.countBy (fun featVec -> featVec.[featVec.Length-1])
    |> Seq.sumBy (fun (key,count) -> 
        let prob = float count / float numEntries
        -prob * log(prob)/log(2.0)
    )

let chooseBestFeatureToSplit(dataSet:obj[][]) =
    let numFeatures = dataSet.[0].Length - 1
    let baseEntropy = calcShannonEnt(dataSet)
    [0..numFeatures-1] |> List.map (fun i ->
        let featList = [for example in dataSet -> example.[i]]
        let newEntropy =
            let uniqueValues = Seq.distinct featList
            uniqueValues |> Seq.sumBy (fun value ->
                let subDataSet = splitDataSet(dataSet, i, value)
                let prob = float subDataSet.Length / float dataSet.Length
                prob * calcShannonEnt(subDataSet)
            )
        let infoGain = baseEntropy - newEntropy
        i, infoGain
    ) 
    |> List.maxBy snd |> fst

let majorityCnt(classList:obj[]) =
    let classCount = Dictionary()
    for vote in classList do
        if not <| classCount.ContainsKey(vote) then 
            classCount.Add(vote,0)
        classCount.[vote] <- classCount.[vote] + 1
    [for kvp in classCount -> kvp.Key, kvp.Value]
    |> List.sortBy (snd >> (~-))
    |> List.head 
    |> fst

type Label = string
type Value = obj
type Tree = Leaf of Value | Branch of Label * (Value * Tree)[]

let rec createTree(dataSet:obj[][], labels:string[]) =
    let classList = [|for example in dataSet -> example.[example.Length-1]|]
    if classList |> Seq.forall((=) classList.[0])
    then Leaf(classList.[0])
    elif dataSet.[0].Length = 1 
    then Leaf(majorityCnt(classList))
    else
    let bestFeat = chooseBestFeatureToSplit(dataSet)
    let bestFeatLabel = labels.[bestFeat]
    let labels = labels |> Array.removeAt bestFeat
    let featValues = [|for example in dataSet -> example.[bestFeat]|]
    let uniqueVals = featValues |> Seq.distinct |> Seq.toArray
    let subTrees =
        [|for value in uniqueVals ->
            let subLabels = labels.[*]
            let split = splitDataSet(dataSet, bestFeat, value)
            value, createTree(split, subLabels)|]
    Branch(bestFeatLabel, subTrees)

let mode (dataSet:obj[][]) =
    dataSet |> Seq.countBy (fun example -> example.[example.Length-1])
    |> Seq.maxBy snd |> fst

let rec classify(inputTree, featLabels:string[], testVec:obj[]) =
    match inputTree with
    | Leaf(x) -> Some x
    | Branch(s,xs) ->
        let featIndex = featLabels |> Array.findIndex ((=) s)
        xs |> Array.tryPick (fun (value,tree) ->
            if testVec.[featIndex] = value 
            then classify(tree, featLabels, testVec)
            else None
        )
