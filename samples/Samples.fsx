#r "../bin/FSharp.DataFrame.dll"

// --------------------------------------------------------------------------------------
// Some basic examples of using the data frame
// --------------------------------------------------------------------------------------

open System
open FSharp.DataFrame
open FSharp.DataFrame.PrettyPrint

// S1 and S2 are ordered series, S3 is not ordered
let s1 = Series.Create(["a"; "b"; "c"], [1 .. 3])
s1.Observations |> printfn "%A"

let s2 = Series.Create(["b"; "c"; "d"], [6; 4; 5])
s2.Observations |> printfn "%A"

let s3 = Series.Create(["d"; "c"; "b"], [6; 4; 5])
s3.Observations |> printfn "%A"

// Snull is ordered with ordinal index (but has missing values)
let snull = Series.Create [1.0; 2.0; Double.NaN ]
snull |> prettyPrintSeries
snull.Observations |> printfn "%A"

// Create data frames, get the series
let f1 = Frame.Create("S1", s1)
f1.GetSeries<int>("S1") |> prettyPrintSeries

let f2 = Frame.Create("S2", s2)
let f3 = Frame.Create("S3", s3)

f1 |> prettyPrintFrame
f2 |> prettyPrintFrame
f3 |> prettyPrintFrame

f1.Join(f2, JoinKind.Outer) |> prettyPrintFrame
f1.Join(f3, JoinKind.Outer) |> prettyPrintFrame
f3.Join(f1, JoinKind.Outer) |> prettyPrintFrame

f1.Join(f2, JoinKind.Inner) |> prettyPrintFrame 
f1.Join(f2, JoinKind.Left) |> prettyPrintFrame 
f1.Join(f2, JoinKind.Right) |> prettyPrintFrame 

f1.Join(f3, JoinKind.Inner) |> prettyPrintFrame 
f1.Join(f3, JoinKind.Left) |> prettyPrintFrame 
f1.Join(f3, JoinKind.Right) |> prettyPrintFrame 


let f1 = Frame.Create("S1", s1)
f1?Another <- f2.GetSeries<int>("S2")

f1?Test0 <- [ "a"; "b" ] // TODO: bug - fixed
f1?Test <- [ "a"; "b"; "!" ]
f1?Test2 <- [ "a"; "b"; "!"; "?" ]

f1 |> prettyPrintFrame
f2 |> prettyPrintFrame

let joined = f1.Join(f2, JoinKind.Outer)
joined |> prettyPrintFrame 

let joinedR = f1.Join(f2, JoinKind.Right)
joinedR |> prettyPrintFrame 

// let joinedI = f1.Join(f2, JoinKind.Inner) // TODO!
// joinedI |> prettyPrintFrame 

// TODO: Think what this should do...
// (special case construction OR sum/mean/...)
let zerosQ = joined.Rows.MapRaw(fun key reader -> if key = "a" then None else Some Double.NaN)
zerosQ |> prettyPrintSeries

let zerosE = joined.Rows.Map(fun key reader -> if key = "a" then None else Some Double.NaN)
zerosE |> prettyPrintSeries

let zeros = joined.Rows.MapRaw(fun key reader -> if key = "a" then None else Some 0.0)
zeros |> prettyPrintSeries

joined?Zeros <- zeros
joined |> prettyPrintFrame 


let a1 = Frame.CreateRow(1, joined.Rows.["a"])
let a2 = Frame.CreateRow(2, joined.Rows.["b"])
a1 |> prettyPrintFrame
a2 |> prettyPrintFrame
a1.Append(a2) |> prettyPrintFrame

let initial = Frame(Index.Create [], Index.Create [], Vector.Create [| |])
initial.Append(a1) |> prettyPrintFrame

joined |> prettyPrintFrame 

joined.Rows.["a"] |> prettyPrintSeries
joined.Rows.["c"] |> prettyPrintSeries

joined.Rows.["a", "c"] |> Frame.FromColumns |> prettyPrintFrame
joined.Rows.["a", "c"] |> Frame.FromRows |> prettyPrintFrame

(*
joined.Rows.["a", "c", "d"] |> Frame.Create |> prettyPrintFrame
joined.Rows.["a" .. "c"] |> Frame.Create |> prettyPrintFrame

let reversed = joined.Rows.["d", "c", "b", "a"] |> Frame.Create
reversed |> prettyPrintFrame
reversed.Rows.["d" .. "d"] |> prettyPrintFrame
reversed.Rows.["d" .. "c"] |> prettyPrintFrame
reversed.Rows.["a" .. "c"] |> prettyPrintFrame // Empty data frame - as expected
*)
let tf = Frame.Create("First", joined.Rows.["a"]) 
tf |> prettyPrintFrame

tf?Second <- joined.Rows.["b"]
tf |> prettyPrintFrame

let a = joined.Rows.["a"] 
a |> prettyPrintSeries

a?S1
a.["S1"]
a.TryGetAny<int>("S1")
a.GetAny<int>("S1")

joined?Sum <- joined.Rows.Map(fun key row -> 
  match row.TryGetAny<int>("S1"), row.TryGetAny<int>("S2") with
  | Some n1, Some n2 -> Some(n1 + n2)
  | _ -> Some -2)

prettyPrintFrame joined

//
// joined?Sum <- joined.Columns.["S1", "S2"].Rows.Map(fun key reader -> 
//   reader.GetColumn<int>("S1") + reader.GetColumn<int>("S2") )
//

joined.Rows.["c"].GetAny<string>("Test")

    
joined.GetSeries<int>("Sum")
|> prettyPrintSeries

prettyPrintFrame joined

// Conversions
joined.GetSeries<int>("Sum") |> prettyPrintSeries
joined.GetSeries<float>("Sum") |> prettyPrintSeries

joined?Sum |> Series.sum


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

data |> prettyPrintFrame

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
      [ getColumns index columns Set.empty; 
        getColumns frame.Index frame.Columns (Seq.map fst columns |> set) ]
      |> Seq.concat               
    Frame(newIndex, newColumns)
*)  
*)
