#I "../bin"
#load "FSharp.DataFrame.fsx"

// --------------------------------------------------------------------------------------
// Some basic examples of using the data frame
// --------------------------------------------------------------------------------------

open System
open FSharp.DataFrame

/// Generate date range from 'first' with 'count' days
let dateRange (first:System.DateTime) count = (*[omit:(...)]*)
  seq { for i in 0 .. (count - 1) -> first.AddDays(float i) }(*[/omit]*)
/// Generate 'count' number of random doubles
let rand count = (*[omit:(...)]*)
  let rnd = System.Random()
  seq { for i in 0 .. (count - 1) -> rnd.NextDouble() }(*[/omit]*)

(*
f.Rows.Select(fun (KeyValue(_, s)) -> s?a + s?b)
f.Rows.Select(fun (KeyValue(_, s)) -> s?a + s?b)

(f.Columns.["a", "b"] |> Frame.FromColumns).RowsDense.Select(fun (KeyValue(_, s)) -> s?a + s?b)

f.Rows.Get(1)

f?a + f?b

f?sum <- f.Rows.Where(fun kvp -> kvp.Value.TryGet("a").IsSome && kvp.Value.TryGet("b").IsSome).Select(fun kv -> kv.Value?a + kv.Value?a)

f.Rows.Select(fun kvp -> if kvp.Value.HasAll ... then kvp.Value else missing)
f.Rows.Mask(fun kvp -> kvp.Value.HasAll ["a"; "b"]).Select(fun v -> v?a + v?a)

f.Rows.SelectOptional(fun kvp -> vp.Value.HasAll ["a"; "b" ] with
    match kvp.Value.Value.TryGetAs<int>("a"), kvp.Value.Value.TryGetAs<int>("b") with
    | Some a, Some b -> OptionalValue(a + b)
    | _ -> OptionalValue.Missing)
*)

let tsOld = Series(dateRange (DateTime(2013,1,1)) 10, rand 10)
let tsNew = Series([DateTime(2013,1,1); DateTime(2013,1,2); DateTime(2013,1,3)], [ 10.0; 20.0; 30.0 ])

let df = Frame(["sierrats"; "olympus"], [tsOld; tsNew])

let df1 = Frame.ofValues [ (1, "Tomas", "happy"); (2, "Tomas", "unhappy"); (1, "Adam", "happy") ]
let df2 = Frame.ofRows ["sierrats" => tsOld; "olympus" => tsNew] 
let df3 = Frame.ofColumns ["sierrats" => tsOld; "olympus" => tsNew] 
                

// S1 and S2 are ordered series, S3 is not ordered
let s1 = Series.Create(["a"; "b"; "c"], [1 .. 3])
s1.ObservationsOptional |> printfn "%A"

let s2 = Series.Create(["b"; "c"; "d"], [6; 4; 5])

let s3 = Series.Create(["d"; "c"; "b"], [6; 4; 5])

// Snull is ordered with ordinal index (but has missing values)
let snull = Series.Create [1.0; 2.0; Double.NaN ]
snull.ObservationsOptional |> printfn "%A"


s3
s3 + 10

let d3 = Series.Create(["d"; "c"; "b"], [6.0; 4.0; 5.0])
d3 + 10.0

s3 + s3
d3 + d3

// Create data frames, get the series
let f1 = Frame.Create("S1", s1)
f1.GetSeries<int>("S1")

let f2 = Frame.Create("S2", s2)
let f3 = Frame.Create("S3", s3)

f1
f2
f3

f1.Join(f2, JoinKind.Outer)
f1.Join(f3, JoinKind.Outer)
f3.Join(f1, JoinKind.Outer)

f1.Join(f2, JoinKind.Inner) 
f1.Join(f2, JoinKind.Left) 
f1.Join(f2, JoinKind.Right) 

f1.Join(f3, JoinKind.Inner) 
f1.Join(f3, JoinKind.Left) 
f1.Join(f3, JoinKind.Right) 

let a1 = Frame.Create("C1", Series.Create([1;2], ["one"; "two"]))
a1?C2 <- ["three"]

let a2 = Frame.Create("C2", Series.Create([2;3], ["one"; "two"]))

a1
a2

// This is fine
a1.Append(a2)

// but this fails
try a1.Append(a1) |> ignore with _ -> printfn "ok"


// let f1 = Frame.Create("S1", s1)
f1?Another <- f2.GetSeries<int>("S2")

f1?Test0 <- [ "a"; "b" ]
f1?Test <- [ "a"; "b"; "!" ]
f1?Test2 <- [ "a"; "b"; "!"; "?" ]

f1
f2

let joined = f1.Join(f2, JoinKind.Outer)
let joinedR = f1.Join(f2, JoinKind.Right)
let joinedI = f1.Join(f2, JoinKind.Inner)

// All values are missing
let zerosQ = joined.Rows.SelectOptional(fun (KeyValue(key, row)) -> 
  if key = "a" then OptionalValue.Missing else OptionalValue(Double.NaN))

let zerosE = joined.Rows.Select(fun (KeyValue(key, row)) -> 
  if key = "a" then None else Some Double.NaN)

let zeros = joined.Rows.SelectOptional(fun (KeyValue(key, row)) -> 
  if key = "a" then OptionalValue.Missing else OptionalValue(0.0))

joined?Zeros <- zeros
joined 


// Matching two frames in append
let c1 = Frame.CreateRow(1, joined.Rows.["a"])
let c2 = Frame.CreateRow(2, joined.Rows.["b"])
c1.Append(c2)

// Appending things to an empty frame
//let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
//initial.Append(a1)

joined 

joined.Rows.["a"]
joined.Rows.["c"]

joined.Rows?a
joined.Rows?c

joined.Rows?c?S1
joined.Rows?c?Another

try joined.Rows?c?Test |> ignore with _ -> printfn "assuming double.."

joined.Rows?c.GetAs<string>("Test")
let cs = joined.Rows?c

cs.Get("Test")
cs.GetAs<string>("Test")
cs.["S1" .. "S2"]
cs.["S1", "Zeros", "S2"]


joined.Rows.["a", "c"] |> Frame.FromColumns
joined.Rows.["a", "c"] |> Frame.FromRows

joined.Rows.["a", "c", "d"] |> Frame.FromRows
joined.Rows.["a" .. "c"] |> Frame.FromRows
joined.Rows.["d", "c", "b", "a"] |> Frame.FromRows

// preserve ordering of selectors etc.
let reversed = joined.Rows.["d", "c", "b", "a"] |> Frame.FromRows
reversed
reversed.Rows.["d" .. "d"] |> Frame.FromRows
reversed.Rows.["d" .. "c"] |> Frame.FromRows
reversed.Rows.["a" .. "c"]

let tf = Frame.Create("First", joined.Rows?a) 
tf

tf?Second <- joined.Rows.["b"]
tf

let a = joined.Rows.["a"] 
a

a?S1
a.["S1"]
a.GetAs<int>("S1")
a.GetAs<float>("S1")
a.GetAs<byte>("S1")


// This fails as expected
try joined?Sum <- joined.Rows.Select(fun (KeyValue(key, row)) -> row?S1 + row?S2) with _ -> printfn "missing"

// This works, but it is not very useful as we only need S1 and S2 (and not all columns)
joined?Sum <- joined.RowsDense.SelectOptional(fun (KeyValue(key, row)) -> 
  match row with
  | OptionalValue.Present v -> OptionalValue(v?S1 + v?S2)
  | OptionalValue.Missing -> OptionalValue.Missing)

// Get frame with just S1 and S2
let sub1 = joined.Columns.["S1", "S2"] |> Frame.FromColumns
sub1

let sub2 = joined.Columns.["S1", "S2"] |> Frame.FromColumns
sub2
joined?SumS1_S2 <- sub2.RowsDense.Select(fun row -> row.Value?S1 + row.Value?S2)

joined

//
joined.Rows?c.GetAs<string>("Test")

    
joined.GetSeries<int>("SumS1_S2")

joined

// Conversions
joined.GetSeries<int>("SumS1_S2")
joined.GetSeries<float>("SumS1_S2")

joined?SumS1_S2 |> Series.sum


// --------------------------------------------------------------------------------------
// Digits
// --------------------------------------------------------------------------------------
(*
/// Given a float array, display it in a form
let showDigit (data:RowReader) = 
  let bmp = new Bitmap(28, 28)
  for i in 0 .. 783 do
    let value = int (data.GetColumnAt<string>(i))
    let color = Color.FromArgb(value, value, value)
    bmp.SetPixel(i % 28, i / 28, color)
  let frm = new Form(Visible = true, ClientSize = Size(280, 280))
  let img = new PictureBox(Image = bmp, Dock = DockStyle.Fill, SizeMode = PictureBoxSizeMode.StretchImage)
  frm.Controls.Add(img)

let data = Frame.ReadCsv(__SOURCE_DIRECTORY__ + "\\data\\digitssample.csv")

data

let labels = data.GetSeries<string>("label")
labels.Values

let digitData = data.[0 .., "pixel0" ..]
let digitData = data.[0 .. 20, "pixel0" .. "pixel5"]

joined // .["a" .. "c", "Column" .. ]
|> prettyPrintFrame

digitData 
|> prettyPrintFrame

digitData.[0].Columns.Length
digitData.[0].GetColumn<string>("pixel0")
digitData.[0].GetColumnAt<string>(0)

// Examples
labels.[0]
digitData.[0] |> showDigit

labels.[1]
digitData.[1] |> showDigit

labels.[3]
digitData.[3] |> showDigit

// --------------------------------------------------------
// TODO: Calculate distance between two arrays
// --------------------------------------------------------

let aggreageDistance (sample1:float[]) (sample2:float[]) =
  let sum =
    Array.map2 (fun a b -> sqrt ((a - b) * (a - b))) sample1 sample2
    |> Seq.sum
  sum / (float sample1.Length)

let zero1 = data.Data |> Seq.nth 1 |> getDataArray
let zero2 = data.Data |> Seq.nth 5 |> getDataArray
let one1 = data.Data |> Seq.nth 0 |> getDataArray
let one2 = data.Data |> Seq.nth 2 |> getDataArray

// The first two should be smaller
aggreageDistance zero1 zero2
aggreageDistance one1 one2

// But this one should be larger
aggreageDistance one1 zero1

// --------------------------------------------------------
// TODO: Calculate "average" samples
// --------------------------------------------------------

let samples = 
  data.Data 
  |> Seq.groupBy (fun r -> getLabel r)
  |> Array.ofSeq
  |> Array.Parallel.map (fun (k, v) -> 
      let samples = Array.map getDataArray (Array.ofSeq v)
      let average = 
        Array.init 784 (fun i ->
          let sum = samples  |> Seq.sumBy (fun a -> Array.get a i) 
          sum / float (Seq.length samples) )
      k, average)

// Show the average samples
// (WARNING: Opens lots of new windows!)
for i in 0 .. 9 do
  samples.[i] |> snd |> showDigit

// --------------------------------------------------------
// TODO: Pick the closest average sample
// --------------------------------------------------------

let classify row =
  let data = getDataArray row
  samples 
  |> Seq.map (fun (key, image) -> 
      key, aggreageDistance image data)
  |> Seq.minBy snd
  |> fst

// --------------------------------------------------------
// Test the classifier using the samples
// --------------------------------------------------------

let test = CsvFile.Load(__SOURCE_DIRECTORY__ + "\\data\\digitscheck.csv").Cache()

for input in test.Data |> Seq.take 10 do
  let got = classify input
  let actual = getLabel input
  printfn "Actual %d, got %A" actual got

test.Data |> Seq.countBy (fun input ->
  classify input = getLabel input)


#load @"..\NuGet\test.fsx"


// IReadOnlyList<'T> ???
// 


(*

    let newData = 
      [| for colKey, _ in newColumnIndex.Elements ->
           let column = 
             let thisCol = columnIndex.Lookup(colKey)
             if thisCol.HasValue then data.GetValue(thisCol.Value).Value
             else 
               let otherCol = otherFrame.ColumnIndex.Lookup(colKey)
               otherFrame.Data.GetValue(otherCol.Value).Value

           reindex rowIndex newRowIndex column |]

      |> Vector.Create
    Frame(newRowIndex, newColumnIndex, newData)

    let getColumns (index:Index<_>) (columns:seq<string * IVec>) usedNames = 
      seq { for name, column in columns ->
              let name = 
                if Set.contains name usedNames |> not then name
                else
                  name 
                  |> Seq.unfold (fun name -> Some(name + "'", name + "'"))
                  |> Seq.find (fun el -> Set.contains el usedNames |> not)
              name, column.Reorder(allKeys.Length, fun i -> 
                match index.Lookup.TryGetValue(allKeys.[i]) with
                | true, index -> Some index 
                | _ -> None ) }
    let newColumns = 
      [ getColumns index columns Set.Missing; 
        getColumns frame.Index frame.Columns (Seq.map fst columns |> set) ]
      |> Seq.concat               
    Frame(newIndex, newColumns)
*)  
*)
