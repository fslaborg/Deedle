// Script to generate sample .arrow and .parquet test data files
// Run from repo root:  dotnet fsi tests/generate-sample-data.fsx

#r "../bin/net10.0/Deedle.dll"
#r "../bin/net10.0/Deedle.Arrow.dll"
#r "../bin/net10.0/Deedle.Parquet.dll"
#r "../packages/Apache.Arrow/lib/net8.0/Apache.Arrow.dll"
#r "../packages/Parquet.Net/lib/net10.0/Parquet.dll"
#r "../packages/Microsoft.IO.RecyclableMemoryStream/lib/net6.0/Microsoft.IO.RecyclableMemoryStream.dll"
#r "../packages/CommunityToolkit.HighPerformance/lib/net8.0/CommunityToolkit.HighPerformance.dll"
#r "../packages/K4os.Compression.LZ4/lib/netstandard2.1/K4os.Compression.LZ4.dll"
#r "../packages/Snappier/lib/net8.0/Snappier.dll"
#r "../packages/ZstdSharp.Port/lib/net8.0/ZstdSharp.dll"

open System
open System.IO
open Deedle
open Deedle.Arrow
open Deedle.Parquet

// ---- Sample "stocks" data ----
// 5 rows, mixed types: string, float, int, DateTime
let stocksDf =
    Frame.ofColumns [
        "Ticker", (Series.ofValues [ "MSFT"; "AAPL"; "GOOG"; "AMZN"; "META" ] :> ISeries<_>)
        "Open",   (Series.ofValues [ 420.5; 185.3; 175.8; 200.1; 510.0 ]      :> ISeries<_>)
        "Close",  (Series.ofValues [ 425.0; 183.0; 178.2; 205.5; 515.3 ]      :> ISeries<_>)
        "Volume", (Series.ofValues [ 28000000; 55000000; 20000000; 45000000; 32000000 ] :> ISeries<_>)
        "Date",   (Series.ofValues [
                        DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc)
                        DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc)
                        DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc)
                        DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc)
                        DateTime(2024, 6, 3, 0, 0, 0, DateTimeKind.Utc) ] :> ISeries<_>)
    ]

// ---- Sample "missing" data ----
// 4 rows, float column with NaN gaps
let missingDf =
    Frame.ofColumns [
        "A", (Series.ofValues [ 1.0; nan; 3.0; nan ]  :> ISeries<_>)
        "B", (Series.ofValues [ 10; 20; 30; 40 ]      :> ISeries<_>)
        "C", (Series.ofValues [ "x"; "y"; "z"; "w" ]  :> ISeries<_>)
    ]

// ---- Sample "indexed" data ----
// Frame with string row keys
let indexedDf =
    let keys = [| "Jan"; "Feb"; "Mar"; "Apr" |]
    Frame.ofColumns [
        "Revenue", Series(keys, [| 1200.0; 1350.0; 1100.0; 1500.0 |]) :> ISeries<_>
        "Cost",    Series(keys, [| 800.0;  900.0;  750.0;  1000.0 |]) :> ISeries<_>
        "Profit",  Series(keys, [| 400.0;  450.0;  350.0;  500.0 |])  :> ISeries<_>
    ]

// ---- Write Arrow files ----
let arrowDataDir = Path.Combine(__SOURCE_DIRECTORY__, "Deedle.Arrow.Tests", "data")
Directory.CreateDirectory(arrowDataDir) |> ignore

Frame.writeArrow (Path.Combine(arrowDataDir, "stocks.arrow")) stocksDf
Frame.writeArrow (Path.Combine(arrowDataDir, "missing.arrow")) missingDf
Frame.writeArrowWithIndex (Path.Combine(arrowDataDir, "indexed.arrow")) indexedDf

printfn "Arrow sample files written to %s" arrowDataDir

// ---- Write Parquet files ----
let parquetDataDir = Path.Combine(__SOURCE_DIRECTORY__, "Deedle.Parquet.Tests", "data")
Directory.CreateDirectory(parquetDataDir) |> ignore

Frame.writeParquet (Path.Combine(parquetDataDir, "stocks.parquet")) stocksDf
Frame.writeParquet (Path.Combine(parquetDataDir, "missing.parquet")) missingDf
Frame.writeParquetWithIndex (Path.Combine(parquetDataDir, "indexed.parquet")) indexedDf

printfn "Parquet sample files written to %s" parquetDataDir
printfn "Done."
