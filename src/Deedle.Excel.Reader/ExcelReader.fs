/// <summary>
/// Provides cross-platform functions for reading Deedle data frames from Excel files
/// (.xls and .xlsx) using the ExcelDataReader library.
/// </summary>
/// <category>Excel Reader integration</category>
module Deedle.ExcelReader

open System
open System.IO
open ExcelDataReader
open Deedle

/// <summary>
/// Register the CodePages encoding provider required by ExcelDataReader on .NET Core / .NET 5+.
/// Called automatically by all read functions in this module.
/// </summary>
let private registerEncodings () =
    Text.Encoding.RegisterProvider(Text.CodePagesEncodingProvider.Instance)

/// Read a single <see cref="System.Data.DataTable"/> as a Deedle frame.
/// The DataTable's column names become the frame's column keys.
let private frameOfDataTable (table: System.Data.DataTable) : Frame<int, string> =
    use dr = table.CreateDataReader()
    Frame.ReadReader(dr)

/// <summary>
/// Read the first worksheet of an Excel file (.xls or .xlsx) as a Deedle data frame.
/// The first row is treated as column headers.
/// </summary>
/// <param name="path">Path to the Excel file.</param>
/// <returns>A frame whose row keys are integers (0-based) and column keys are strings.</returns>
let readExcel (path: string) : Frame<int, string> =
    registerEncodings ()
    use stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
    use reader = ExcelReaderFactory.CreateReader(stream)
    let conf =
        ExcelDataSetConfiguration(
            ConfigureDataTable = fun _ -> ExcelDataTableConfiguration(UseHeaderRow = true))
    let ds = reader.AsDataSet(conf)
    if ds.Tables.Count = 0 then
        Frame.ofColumns (seq [])
    else
        frameOfDataTable ds.Tables.[0]

/// <summary>
/// Read a specific worksheet by name from an Excel file (.xls or .xlsx) as a Deedle data frame.
/// The first row is treated as column headers.
/// </summary>
/// <param name="path">Path to the Excel file.</param>
/// <param name="sheetName">Name of the worksheet to read (case-sensitive).</param>
/// <returns>A frame whose row keys are integers (0-based) and column keys are strings.</returns>
let readExcelSheet (path: string) (sheetName: string) : Frame<int, string> =
    registerEncodings ()
    use stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
    use reader = ExcelReaderFactory.CreateReader(stream)
    let conf =
        ExcelDataSetConfiguration(
            ConfigureDataTable = fun _ -> ExcelDataTableConfiguration(UseHeaderRow = true))
    let ds = reader.AsDataSet(conf)
    match ds.Tables.[sheetName] with
    | null -> failwithf "Sheet '%s' not found in workbook. Available sheets: %s"
                sheetName
                (String.concat ", " [ for t in ds.Tables -> t.TableName ])
    | table -> frameOfDataTable table

/// <summary>
/// Read a specific worksheet by zero-based index from an Excel file (.xls or .xlsx) as a Deedle data frame.
/// The first row is treated as column headers.
/// </summary>
/// <param name="path">Path to the Excel file.</param>
/// <param name="sheetIndex">Zero-based index of the worksheet to read.</param>
/// <returns>A frame whose row keys are integers (0-based) and column keys are strings.</returns>
let readExcelSheetByIndex (path: string) (sheetIndex: int) : Frame<int, string> =
    registerEncodings ()
    use stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
    use reader = ExcelReaderFactory.CreateReader(stream)
    let conf =
        ExcelDataSetConfiguration(
            ConfigureDataTable = fun _ -> ExcelDataTableConfiguration(UseHeaderRow = true))
    let ds = reader.AsDataSet(conf)
    if sheetIndex < 0 || sheetIndex >= ds.Tables.Count then
        failwithf "Sheet index %d is out of range. The workbook has %d sheet(s)." sheetIndex ds.Tables.Count
    frameOfDataTable ds.Tables.[sheetIndex]

/// <summary>
/// Return the names of all worksheets in an Excel file.
/// </summary>
/// <param name="path">Path to the Excel file.</param>
let sheetNames (path: string) : string list =
    registerEncodings ()
    use stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
    use reader = ExcelReaderFactory.CreateReader(stream)
    [ yield reader.Name
      while reader.NextResult() do
          yield reader.Name ]

// ------------------------------------------------------------------------------------------------
// F# Frame module extensions
// ------------------------------------------------------------------------------------------------

/// <summary>
/// F# module extensions for reading Excel files.
/// Open <c>Deedle.ExcelReader</c> and then call these as <c>Frame.readExcel</c>,
/// <c>Frame.readExcelSheet</c>, etc.
/// </summary>
module Frame =

    /// <summary>
    /// Read the first worksheet of an Excel file as a <c>Frame&lt;int, string&gt;</c>.
    /// </summary>
    let readExcel (path: string) : Frame<int, string> =
        readExcel path

    /// <summary>
    /// Read a specific worksheet by name from an Excel file.
    /// </summary>
    let readExcelSheet (path: string) (sheetName: string) : Frame<int, string> =
        readExcelSheet path sheetName

    /// <summary>
    /// Read a specific worksheet by zero-based index from an Excel file.
    /// </summary>
    let readExcelSheetByIndex (path: string) (sheetIndex: int) : Frame<int, string> =
        readExcelSheetByIndex path sheetIndex

    /// <summary>
    /// Return the names of all worksheets in an Excel file.
    /// </summary>
    let sheetNames (path: string) : string list =
        sheetNames path

// ------------------------------------------------------------------------------------------------
// C#-friendly API: static factory methods
// ------------------------------------------------------------------------------------------------

/// <summary>
/// Static factory methods for reading Excel files, accessible from C# as
/// <c>ExcelFrame.ReadExcel(path)</c> etc.
/// </summary>
type ExcelFrame =

    /// <summary>
    /// Read the first worksheet of an Excel file (.xls or .xlsx) as a
    /// <c>Frame&lt;int, string&gt;</c>. The first row is treated as column headers.
    /// </summary>
    static member ReadExcel(path: string) : Frame<int, string> =
        readExcel path

    /// <summary>
    /// Read a specific worksheet by name from an Excel file (.xls or .xlsx).
    /// The first row is treated as column headers.
    /// </summary>
    static member ReadExcelSheet(path: string, sheetName: string) : Frame<int, string> =
        readExcelSheet path sheetName

    /// <summary>
    /// Read a specific worksheet by zero-based index from an Excel file (.xls or .xlsx).
    /// The first row is treated as column headers.
    /// </summary>
    static member ReadExcelSheetByIndex(path: string, sheetIndex: int) : Frame<int, string> =
        readExcelSheetByIndex path sheetIndex

    /// <summary>
    /// Return the names of all worksheets in an Excel file.
    /// </summary>
    static member SheetNames(path: string) : string list =
        sheetNames path
