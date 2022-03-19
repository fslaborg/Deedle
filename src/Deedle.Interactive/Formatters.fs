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

    let formattedStrings = f.InteractiveFormat(InteractiveConfig.MaxRows,InteractiveConfig.MaxCols,InteractiveConfig.ShowColTypes)

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
    sprintf """<div>
<style scoped>\n",
  .dataframe tbody tr th:only-of-type {
    vertical-align: middle;
  }
  .dataframe tbody tr th {\n",
    vertical-align: top
  }

  .dataframe thead th {
    text-align: right;
  }
  .no-wrap {
    white-space: nowrap;
  }
</style>
<table border="1" class="dataframe">
%s
%s
</table>
<br>
<p>%i rows x %i columns</p>
"""
      colHeaders values (fst dims) (snd dims)
