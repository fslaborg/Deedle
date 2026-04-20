/// <summary>
/// Provides cross-platform functions for writing Deedle data frames to Excel files
/// (.xlsx) using the MiniExcel library.
/// </summary>
/// <category>Excel Writer integration</category>
namespace Deedle.ExcelWriter

open System
open System.Collections.Generic
open System.IO
open MiniExcelLibs
open Deedle

[<AutoOpen>]
module private Implementation =

    /// Convert a frame to a list of row dictionaries suitable for MiniExcel.
    let frameToRows (frame: Frame<'R, string>) : obj =
        let colKeys = frame.ColumnKeys |> Seq.toArray
        frame.Rows.Observations
        |> Seq.map (fun kv ->
            let row = kv.Value
            let dict = Dictionary<string, obj>(colKeys.Length)
            for colKey in colKeys do
                match row.TryGet(colKey) with
                | OptionalValue.Present v -> dict.[colKey] <- v
                | OptionalValue.Missing   -> dict.[colKey] <- null
            dict :> IDictionary<string, obj>)
        |> Seq.toList
        :> obj

    /// <summary>
    /// Write a Deedle frame to an Excel (.xlsx) file.
    /// The frame's column keys become column headers in the first row.
    /// Any existing file at <paramref name="path"/> is overwritten.
    /// </summary>
    let writeExcel (path: string) (frame: Frame<'R, string>) : unit =
        MiniExcel.SaveAs(path, frameToRows frame, true, "Sheet1", ExcelType.XLSX, null, true) |> ignore

    /// <summary>
    /// Write a Deedle frame to a named sheet within an Excel (.xlsx) file.
    /// Any existing file at <paramref name="path"/> is overwritten.
    /// </summary>
    let writeExcelSheet (path: string) (sheetName: string) (frame: Frame<'R, string>) : unit =
        MiniExcel.SaveAs(path, frameToRows frame, true, sheetName, ExcelType.XLSX, null, true) |> ignore

    /// <summary>
    /// Write multiple Deedle frames to separate sheets within a single Excel (.xlsx) file.
    /// The sequence provides (sheetName, frame) pairs; sheet order is preserved.
    /// Any existing file at <paramref name="path"/> is overwritten.
    /// </summary>
    let writeExcelSheets (path: string) (sheets: seq<string * Frame<'R, string>>) : unit =
        let dict = Dictionary<string, obj>()
        for (name, frame) in sheets do
            dict.[name] <- frameToRows frame
        MiniExcel.SaveAs(path, dict :> obj, true, "Sheet1", ExcelType.XLSX, null, true) |> ignore

    /// <summary>
    /// Write a Deedle frame to an Excel (.xlsx) stream.
    /// The frame's column keys become column headers in the first row.
    /// </summary>
    let writeExcelStream (stream: Stream) (frame: Frame<'R, string>) : unit =
        MiniExcel.SaveAs(stream, frameToRows frame, false, "Sheet1", ExcelType.XLSX, null) |> ignore

    /// <summary>
    /// Write a Deedle frame to a named sheet within an Excel (.xlsx) stream.
    /// </summary>
    let writeExcelSheetStream (stream: Stream) (sheetName: string) (frame: Frame<'R, string>) : unit =
        MiniExcel.SaveAs(stream, frameToRows frame, false, sheetName, ExcelType.XLSX, null) |> ignore

// ------------------------------------------------------------------------------------------------
// F# Frame module extensions
// ------------------------------------------------------------------------------------------------

/// <summary>
/// F# module extensions for writing Excel files.
/// Open <c>Deedle.ExcelWriter</c> and then call these as <c>Frame.writeExcel</c>,
/// <c>Frame.writeExcelSheet</c>, etc.
/// </summary>
module Frame =

    /// <summary>
    /// Write a frame to the first sheet of an Excel (.xlsx) file.
    /// Column keys become the header row. Any existing file is overwritten.
    /// </summary>
    let writeExcel (path: string) (frame: Frame<'R, string>) : unit =
        Implementation.writeExcel path frame

    /// <summary>
    /// Write a frame to a named sheet of an Excel (.xlsx) file.
    /// Any existing file is overwritten.
    /// </summary>
    let writeExcelSheet (path: string) (sheetName: string) (frame: Frame<'R, string>) : unit =
        Implementation.writeExcelSheet path sheetName frame

    /// <summary>
    /// Write multiple frames to separate sheets of a single Excel (.xlsx) file.
    /// Provide a sequence of (sheetName, frame) pairs. Any existing file is overwritten.
    /// </summary>
    let writeExcelSheets (path: string) (sheets: seq<string * Frame<'R, string>>) : unit =
        Implementation.writeExcelSheets path sheets

    /// <summary>
    /// Write a frame to an Excel (.xlsx) stream (first sheet, default name).
    /// </summary>
    let writeExcelStream (stream: Stream) (frame: Frame<'R, string>) : unit =
        Implementation.writeExcelStream stream frame

    /// <summary>
    /// Write a frame to a named sheet of an Excel (.xlsx) stream.
    /// </summary>
    let writeExcelSheetStream (stream: Stream) (sheetName: string) (frame: Frame<'R, string>) : unit =
        Implementation.writeExcelSheetStream stream sheetName frame

// ------------------------------------------------------------------------------------------------
// C#-friendly API: static methods
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Static methods for writing Deedle frames to Excel files, accessible from C# as
/// <c>ExcelFrameWriter.WriteExcel(path, frame)</c> etc.
/// </summary>
type ExcelFrameWriter =

    /// <summary>
    /// Write a frame to the first sheet of an Excel (.xlsx) file.
    /// Column keys become the header row. Any existing file is overwritten.
    /// </summary>
    static member WriteExcel(path: string, frame: Frame<int, string>) : unit =
        writeExcel path frame

    /// <summary>
    /// Write a frame to a named sheet of an Excel (.xlsx) file.
    /// Any existing file is overwritten.
    /// </summary>
    static member WriteExcelSheet(path: string, sheetName: string, frame: Frame<int, string>) : unit =
        writeExcelSheet path sheetName frame

    /// <summary>
    /// Write multiple frames to separate sheets of a single Excel (.xlsx) file.
    /// Any existing file is overwritten.
    /// </summary>
    static member WriteExcelSheets(path: string, sheets: IEnumerable<string * Frame<int, string>>) : unit =
        writeExcelSheets path sheets

    /// <summary>
    /// Write a frame to an Excel (.xlsx) stream (first sheet).
    /// </summary>
    static member WriteExcelStream(stream: Stream, frame: Frame<int, string>) : unit =
        writeExcelStream stream frame

    /// <summary>
    /// Write a frame to a named sheet of an Excel (.xlsx) stream.
    /// </summary>
    static member WriteExcelSheetStream(stream: Stream, sheetName: string, frame: Frame<int, string>) : unit =
        writeExcelSheetStream stream sheetName frame

