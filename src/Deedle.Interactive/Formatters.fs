namespace Deedle.Interactive

open Deedle
open Deedle.Internal
open Microsoft.DotNet.Interactive.Formatting
open Deedle.Keys

module Formatters =

  let frameToHtmlTable (f: IFrameFormattable) =

    let colLevels = f.GetColLevels()

    let rowLevels = f.GetRowLevels()

    let dims = f.GetDimensions()

    let formattedStrings = f.InteractiveFormat(InteractiveConfig.Frame.MaxRows,InteractiveConfig.Frame.MaxCols,InteractiveConfig.Frame.ShowColTypes)

    let colHeaders =
      formattedStrings
      |> Seq.take (colLevels + 1)
      |> Seq.map (fun row ->
        row
        |> Seq.map (fun v ->
          sprintf"<th>%s</th>" v
        )
        |> String.concat ""
      )
      |> Seq.map (fun row -> sprintf "<thead>%s</thead>" row)
      |> String.concat ""

    let values =
      formattedStrings
      |> Seq.skip (colLevels + 1)
      |> Seq.map (fun row ->
        row
        |> Seq.mapi (fun i v ->
          if i < rowLevels then
            sprintf "<td><b>%s</b></td>" v
          elif i = rowLevels then
            sprintf """<td class="no-wrap">%s</td>""" v
          else
            sprintf "<td>%s</td>" v
        )
        |> String.concat ""
      )
      |> Seq.map (fun row -> sprintf "<tr>%s</tr>" row)
      |> String.concat ""

    let frameInfo =
      match (InteractiveConfig.Frame.ShowInfo, InteractiveConfig.Frame.ShowDimensions, InteractiveConfig.Frame.ShowMissingValueCount) with
      | (true, true, true) -> sprintf "<p><b>%i</b> rows x <b>%i</b> columns</p><p><b>%i</b> missing values</p>" (fst dims) (snd dims) (f.GetMissingValueCount())
      | (true, true, false) -> sprintf "<p><b>%i</b> rows x <b>%i</b> columns</p>" (fst dims) (snd dims)
      | (true, false, true) -> sprintf "<p><b>%i</b> missing values</p>" (f.GetMissingValueCount())
      | _  -> ""

    sprintf "<div>
<style scoped>,
  .dataframe tbody tr th:only-of-type {
    vertical-align: middle;
  }
  .dataframe tbody tr th {,
    vertical-align: top
  }
  .dataframe thead th {
    text-align: right;
  }
  .no-wrap {
    white-space: nowrap;
  }
</style>
<table border='1' class='dataframe'>
%s
%s
</table>
%s
</div>
"
      colHeaders values frameInfo 

  let seriesToHtmlTable (s: ISeriesFormattable) =

    let keyLevels = s.GetKeyLevels()

    let keyCount = s.GetKeyCount()

    let valueCount = s.GetValueCount()

    let formattedStrings = s.InteractiveFormat(InteractiveConfig.Series.MaxItems)

    let values = 
      formattedStrings
      |> Seq.map (fun row ->
        row
        |> Seq.mapi (fun i v ->
          if i < keyLevels then
            sprintf "<td><b>%s</b></td>" v
          elif i = keyLevels then
            sprintf """<td class="no-wrap">%s</td>""" v
          else
            sprintf "<td>%s</td>" v
        )
        |> String.concat ""
      )
      |> Seq.map (fun row -> sprintf "<tr>%s</tr>" row)
      |> String.concat ""

    let seriesInfo =
      match (InteractiveConfig.Series.ShowInfo, InteractiveConfig.Series.ShowItemCount, InteractiveConfig.Series.ShowMissingValueCount) with
      | (true, true, true) -> sprintf "<p>Series of <b>%i</b> items<p><b>%i</b> missing values</p>" (keyCount) (keyCount - valueCount)
      | (true, true, false) -> sprintf "<p>Series of <b>%i</b> items" (keyCount)
      | (true, false, true) -> sprintf "<p><b>%i</b> missing values</p>" (keyCount - valueCount)
      | _  -> ""

    sprintf "<div>
<style scoped>,
  .series tbody tr th:only-of-type {
vertical-align: middle;
  }
  .series tbody tr {,
vertical-align: top
  }
  .no-wrap {
white-space: nowrap;
  }
</style>
<table border='1' class='series'>
%s
</table>
%s
</div>
"
      values seriesInfo
