//
// Titanic: Machine Learning from Disaster 
//

#I "../../bin"
#load "Deedle.fsx"

#load "TallyHo.fs"
#load "DecisionTree.fs"
open TallyHo
open DecisionTree
open Deedle

let df = Frame.readCsv(__SOURCE_DIRECTORY__ + "/../data/Titanic.csv")
let passengers = df.Rows.[0 .. 600] |> Frame.ofRows

type Passenger = ObjectSeries<string>

let female key (passenger:Passenger) = passenger.GetAs<string>("Sex") = "female"
let survived key (passenger:Passenger) = passenger?Survived = 1.0

// Female passengers
let females = passengers |> Frame.filterRows female
let femaleSurvivors = females |> Frame.filterRows survived |> Frame.countKeys
let femaleBySurvived = females |> Frame.groupRowsUsing survived |> Series.mapValues Frame.countKeys
let femaleSurvivorsPc = (float $ femaleBySurvived) / (females |> Frame.countKeys |> float)

// a) Children under 10
let children key (passenger:Passenger) = passenger?Age < 10.0

// b) Passesngers over 50
let over30 key (passenger:Passenger) = passenger?Age > 30.0

// c) Upper class passengers
let classy key (passenger:Passenger) = passenger?PClass = 1.0

// 2. Discover statistics - groups  

/// Survival rate of a criterias group
let survivalRate (criteria:_ -> _ -> 'T) = 
  let grouped = passengers |> Frame.groupRowsUsing criteria
  let counts = grouped |> Series.mapValues (Frame.groupRowsUsing survived >> Series.mapValues Frame.countKeys) 
  let totals = grouped |> Series.mapValues (Frame.countKeys >> float)
  (counts |> Frame.ofRows) / totals * 100.0

let embarked = survivalRate (fun _ p -> p.GetAs<string>("Embarked"))
embarked.["S"]

// a) By passenger class
let passengerClass key (passenger:Passenger) = passenger?PClass
let embarked key (passenger:Passenger) = passenger.GetAs<string>("Embarked")
// Your code here <---

// b) By age group (under 10, adult, over 50)
// Your code here <---


// 3. Scoring

let testPassengers : Passenger[] =
    Train.Load(path).Skip(600).Data 
    |> Seq.toArray

let score f = testPassengers |> Array.percentage (fun p -> f p = survived p)

let notSurvived (p:Passenger) = false

let notSurvivedRate = score notSurvived

// a) Score by embarked point
// Your code here <---

// b) Construct function to score over 80%
// Your code here <---


// 4. Decision trees

let labels = 
    [|"sex"; "class"|]

let features (p:Passenger) : obj[] = 
    [|p.Sex; p.Pclass|]

let dataSet : obj[][] =
    [|for p in passengers ->
        [|yield! features p; 
          yield box (p.Survived = 1)|] |]

let tree = createTree(dataSet, labels)

// Classify

let test (p:Passenger) = 
    match classify(tree, labels, features p) with
    | Some(x) -> x
    | None -> mode dataSet
    :?> bool

let treeRate = score test

// a) Optimize features