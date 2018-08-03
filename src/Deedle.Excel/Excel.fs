module Deedle.Excel

open Deedle

open NetOffice.ExcelApi

open Microsoft.FSharp.Collections
open System
open System.Collections.Generic
open System.Collections.Specialized
open System.Diagnostics
open System.Reflection

type IExportExcelTable =
    abstract ColumnHeaders : obj[,]
    abstract RowHeaders : obj[,]
    abstract IndexLabel : string
    abstract ShowIndex : bool
    abstract Count : int
    abstract DataArray : obj[,]
    abstract ColumnHeaderDescriptions : string[]

type ExcelRange =
    | Empty
    | RealExcel of Range
    override x.ToString() =
        match x with
        | Empty -> "()"
        | RealExcel r -> r.get_Address(0,0)

type ExcelState = {
    LastRange : ExcelRange
    NextRange : ExcelRange
    History : ExcelRange list
    }

type ColumnFormat = {
    Columns : string list
    Format : (ExcelState -> ExcelState)
    }

module ExcelStyles =
    let ApplyTableStyleRange(range:Range, tableStyle, showFilter:bool, showRowHeaders:bool, unlist:bool) =
        let workbook = range.Application.ActiveWorkbook
        let style = workbook.TableStyles.Item("TableStyleMedium6")
        let listObject = range.Worksheet.ListObjects.Add(Enums.XlListObjectSourceType.xlSrcRange,range,null,Enums.XlYesNoGuess.xlYes)
        listObject.ShowAutoFilter <- showFilter
        listObject.TableStyle <- style
        listObject.ShowTableStyleFirstColumn <- showRowHeaders
        let tableRange = listObject.Range;
        if (unlist && not showFilter) then
              //Unlist because otherwise throws exceptions when adding multiple tables
              listObject.Unlist()
        tableRange;

    let ApplyTableStyle(range:obj, tableStyle, showFilter, showRowHeaders) =
        ApplyTableStyleRange((range :?> Range), tableStyle, showFilter, showRowHeaders, true)


let mutable attach = false

[<System.Serializable>]
type ColumnDescription = {
    Column : string
    Description : string
    }

let mutable excelApp : Application = Application.GetActiveInstance()

let setApplication (app : obj) =
    excelApp <- new Application(null, app)

let mutable private activeWb : Workbook = null

let setExcelVisibility visible =
    try
        excelApp.Visible <- visible
    with
    | _ -> printfn "Unable to set excel visibility. Try calling setExcelVisibility true if Excel doesn't show up"

let openNewExcelApplication () =
    //NetOffice.Factory.Initialize()
    excelApp <- new Application()
    excelApp.DisplayAlerts <- false
    excelApp.Workbooks.Add(Enums.XlWBATemplate.xlWBATWorksheet) |> ignore
    setExcelVisibility true
    excelApp

let private assertInstance () =
    if excelApp <> null then
        let wbs = excelApp.Workbooks
        if wbs.Count = 0 then
            wbs.Add(Enums.XlWBATemplate.xlWBATWorksheet) |> ignore
            
        if excelApp = null then
            openNewExcelApplication () |> ignore
        elif activeWb <> null then
            activeWb.Activate()

let private openWorkbook (readonly : bool) filename =
    excelApp.get_Workbooks().Open(filename, null, readonly)

let getActiveWorkbook () =
    if activeWb = null then
        excelApp.ActiveWorkbook
    else
        activeWb

let changeActiveWorkbook (wbIdx : int) =
    activeWb <- excelApp.get_Workbooks().[wbIdx]
    activeWb.Activate()

let openExcelReadOnly = openWorkbook true
let openExcel = openWorkbook false

let renameSheet (sheetName : string) =
    let sheet = getActiveWorkbook().ActiveSheet :?> Worksheet
    sheet.Name <- sheetName

let deleteSheet (sheetName : string) =
    let orig = excelApp.DisplayAlerts
    excelApp.DisplayAlerts <- false
    getActiveWorkbook().Worksheets
    |> Seq.cast<Worksheet>
    |> Seq.tryFind (fun s -> s.Name = sheetName)
    |> Option.iter (fun s -> s.Delete())
    excelApp.DisplayAlerts <- orig

let switchSheet (sheetName : string) =
    let worksheet =
        getActiveWorkbook().Worksheets
        |> Seq.cast<Worksheet>
        |> Seq.tryFind
            (fun w -> w.Name.Equals(sheetName, System.StringComparison.InvariantCultureIgnoreCase))

    match worksheet with
    | Some w -> w.Activate()
    | None ->
        let active = getActiveWorkbook()
        let currentSheet = active.ActiveSheet
        let w = active.Worksheets.Add(null, currentSheet) :?> Worksheet
        w.Name <- sheetName
        w.Activate()

let getRealRange (range : obj) =
    assertInstance ()
    match range with
    | :? string as str ->
        let idx = str.IndexOf("!")
        if idx > 0 then switchSheet (str.Substring(0, idx))
    |  _ -> ()
    excelApp.Range(range)

let resize (r,c) (range : Range) =
    range.get_Resize(r,c)

let offset (r,c) (range : Range) =
    range.get_Offset(r,c)

let convertRange startRange =
    match startRange with
    | Empty -> getRealRange "A1"
    | RealExcel target -> target

let private arraySize a =
    (Array2D.length1 a, Array2D.length2 a)

let private unionRanges r1 r2 =
    let real1 = convertRange r1
    let real2 = convertRange r2
    RealExcel (excelApp.Union(real1, real2))

let private putArray isH (arr : obj [,]) startRange =
    if arr = null || Array2D.length1 arr = 0 || Array2D.length2 arr = 0 then
        startRange
    else
        let (height, width) = arr |> arraySize
        let range = startRange.NextRange |> convertRange |> resize(height, width)
        range.Value2 <- arr
        
        let next = if isH then range |> offset (0, width) else range |> offset (height, 0)
        let current = RealExcel range
        {
            LastRange = current
            NextRange = RealExcel (next |> resize (1, 1))
            History = current :: startRange.History
        }

let private putSingle (single : obj) startRange =
    let range = startRange.NextRange |> convertRange
    range.Value2 <- single
    let next = range |> offset (1,0)
    let current = RealExcel range
    {
        LastRange = current
        NextRange = RealExcel next
        History = current :: startRange.History
    }

let moveOffset delta state =
    {
        LastRange = state.NextRange
        NextRange = RealExcel(state.NextRange |> convertRange |> offset delta)
        History = state.NextRange :: state.History
    }

let moveLeftCols n = moveOffset (0, -n)
let moveRightcols n = moveOffset (0, n)
let moveDownRows n = moveOffset (n, 0)
let moveUpRows n = moveOffset (-n, 0)

let startRange (range : string) =
    let actual = getRealRange range
    {
        LastRange = Empty
        NextRange = RealExcel actual
        History = []
    }

let saveWorkbook (filename : string) =
    getActiveWorkbook().SaveCopyAs(filename)

let saveWorksheetAsHtml (filename : string) (sheet : string) =
    getActiveWorkbook()
        .PublishObjects
        .Add(
            Enums.XlSourceType.xlSourceSheet,
            filename,
            sheet,
            "",
            Enums.XlHtmlType.xlHtmlStatic,
            "id")
        .Publish(true)

let overwriteSave (filename : string ) =
    if System.IO.File.Exists(filename) then
        System.IO.File.Delete(filename)
    saveWorkbook filename

let closeWorkbook () =
    excelApp.DisplayAlerts <- false
    getActiveWorkbook().Close()

let quit () =
    excelApp.Quit()
    excelApp.Dispose(true)
    excelApp <- null

let freezePanes rows cols =
    let window = excelApp.ActiveWindow
    if rows = 0 && cols = 0 then
        window.FreezePanes <- false
    else
        window.SplitColumn <- cols
        window.SplitRow <- rows
        window.FreezePanes <- true

let clearRange range =
    let r = getRealRange range
    r.ClearContents() |> ignore

module internal XlHelper =
    let ap opt f state = 
        match opt with 
        | Some a -> f a state
        | None -> state
        
    let aptest opt f state = 
        if opt then 
            f state
        else 
            state

    let unionRange = Seq.reduce (fun r1 r2 -> excelApp.Union(r1,r2))

    let debugRange (r:Range) =
        Debug.WriteLine(r.Address)
        r

    let debugState msg (state:ExcelState) = 
        let printRange (msg:string) = function
            | Empty -> Debug.Write(sprintf "%s -> %s" msg "Empty")
            | RealExcel r -> Debug.Write(sprintf "%s -> %s" msg (r.Address))
        Debug.WriteLine("-------------")
        Debug.WriteLine(msg)
        printRange "LastRange" state.LastRange
        Debug.WriteLine("");
        printRange "NextRange" state.NextRange
        Debug.WriteLine("");
        state.History |> List.iter (fun x->printRange "\tH" x ; Debug.WriteLine("");)
        Debug.WriteLine("-------------")
        state
        

    let skipHistory n state = 
        let rec loop count l = 
            if count = n then 
                l
            else 
                match l with 
                | h::t -> loop (count+1) t
                | [] -> l

        { state with History = state.History |> loop 0 |> Seq.toList }

    let applyToHistory f state =
        let res = state.History |> List.choose (function | Empty -> None | RealExcel r -> Some r) 
                                |> unionRange
                                |> (fun r -> if r.Areas.Count > 0 then r.Areas.[1] else r)
                                |> f
        {state with LastRange = RealExcel res}

    let (|?) (str:string) defaultText = 
        if System.String.IsNullOrEmpty(str) then 
            defaultText
        else 
            str

    let asTable (t:IExportExcelTable) start showRowHeaders showColumnHeaders tableTitle tableStyle showFilter= 
        // Remove the check that bailed if t.Count = 0, because if we don't generate row and column
        // headers, various other downstream things fail.
        let showRows = defaultArg showRowHeaders t.ShowIndex
        let showCols = defaultArg showColumnHeaders true
        let showFilter = defaultArg showFilter false
        let tableStyle = defaultArg tableStyle ("TableStyleLight9" :> obj)

        start |> ap tableTitle putSingle 
                |> skipHistory 1
                |> aptest showRows (putSingle (t.IndexLabel |? "Index") >> (putArray true t.RowHeaders) >> moveUpRows 1 )
                |> aptest showCols (putArray false t.ColumnHeaders)
                |> putArray false t.DataArray     
                |> aptest showRows (moveLeftCols 1 >> skipHistory 1)
                |> aptest (tableStyle <> null) (applyToHistory (fun r->
                    ExcelStyles.ApplyTableStyle(r,tableStyle, showFilter, showRows)))

    // ----------------------------------------------------------------------------------------
    // Converting Deedle frame to IExportExcelTable
    // ----------------------------------------------------------------------------------------

    open System

    let transpose (array:'T[,]) =
      Array2D.init (array.GetLength(1)) (array.GetLength(0)) (fun i j ->
        array.[j, i])
    
    let formatMap = 
      let register (f:'T -> 'R) = typeof<'T>, (fun (o:obj) ->
          let res = f (unbox<'T> o)
          box res)
      [ register (fun (dateTime:DateTime) ->
          if dateTime = DateTime.MinValue then ""
          elif dateTime.TimeOfDay = TimeSpan.Zero then (dateTime.ToShortDateString())
          else (dateTime.ToString()) )
        register (fun (d:double) -> 
          if Double.IsNaN(d) then box "" else box d) ] |> dict

    let getExcelValue typ obj =
      match obj, formatMap.TryGetValue(typ) with
      | Deedle.OptionalValue.Present obj, (true, f) -> f obj
      | Deedle.OptionalValue.Present obj, _ -> box (obj.ToString()) 
      | _ -> box ""

    let formatExcelHeader (value:obj) = 
      if value = null then box "" else
      match formatMap.TryGetValue(value.GetType()) with
      | true, f -> f value
      | _ -> box (value.ToString())

    let deedleDataToExcel (data:Deedle.FrameData) =
      { new IExportExcelTable with
          member x.ColumnHeaders = 
            data.ColumnKeys |> array2D |> transpose |> Array2D.map formatExcelHeader
          member x.RowHeaders = 
            data.RowKeys |> array2D |> Array2D.map formatExcelHeader
          member x.IndexLabel = "Index"
          member x.ShowIndex = true
          member x.Count = 0
          member x.DataArray = 
            data.Columns 
            |> Seq.map (fun (ty, vec) -> 
                  vec.ObjectSequence |> Seq.map (fun o -> getExcelValue ty o) |> Array.ofSeq ) 
            |> array2D |> transpose
          member x.ColumnHeaderDescriptions =
            data.ColumnKeys 
            |> Seq.map (fun objs ->
                objs |> Seq.map (fun o -> o.ToString()) |> String.concat " - ")
            |> Array.ofSeq }

    let deedleFrameToExcel (frame:Deedle.Frame<_, _>) =
      frame.GetFrameData() |> deedleDataToExcel

type Xl = 
    static member WithSheetFormat(?FontSize, ?AutoFit) =
        fun state -> 
            let range = state.LastRange |> convertRange
            if FontSize.IsSome then range.Worksheet.Cells.Font.Size <- FontSize.Value
            if AutoFit.IsSome then range.Worksheet.Cells.AutoFit() |> ignore
            state

    static member WithColumnFormats (fs:seq<ColumnFormat>) state =
         let lastRange = state.LastRange
         let rows = (lastRange |> convertRange).Rows.Count 
         let range = (lastRange |> convertRange) 
         for cf in fs do
            for c in cf.Columns do
                 let colRange = range.Find(c,null,null,Enums.XlLookAt.xlWhole,null)
                 if colRange <> null then
                     {state with LastRange = (RealExcel  (colRange |> resize(rows,1)))} |> cf.Format |> ignore
         state


    static member WithEntireColumnFormats (fs:seq<ColumnFormat>) state =
         let lastRange = state.LastRange
         let range = (lastRange |> convertRange) 
         for cf in fs do
            for c in cf.Columns do
                 let colRange = range.Find(c,null,null,Enums.XlLookAt.xlWhole,null)
                 if colRange <> null then
                     {state with LastRange = (RealExcel  colRange.EntireColumn)} |> cf.Format |> ignore
         state    

    static member WithColumnFormatsWithOffset (fs:seq<ColumnFormat>) offSet numOfRows useEndOfData state =
        if numOfRows > 0 && numOfRows > offSet then
            let magicNumber = 9999
            let lastRange = state.LastRange
            let range = (lastRange |> convertRange)
            for cf in fs do
               for c in cf.Columns do
                     let headerCell = range.Find(c,null,null,null,null)
                     let colRange = if useEndOfData then
                                        range.Range(headerCell |> offset (offSet,0), headerCell.End(Enums.XlDirection.xlDown))
                                    else
                                        range.Range(headerCell,headerCell |> offset (magicNumber,0))
                     {state with LastRange = (RealExcel  colRange)} |> cf.Format |> ignore
        state

    static member WithColumn(col:string) = //Format (?Background2, ?Foreground, ?FontSize, ?NumberFormat, ?AutoFit) = 
        fun state -> 
            let lastRange = state.LastRange
            let range = lastRange |> convertRange
            let colRange = (range |> offset (-1,0)).Find(col,null,null,null,null).EntireColumn
            {state with LastRange = (RealExcel  colRange)} //|> Xl.WithFormat(Background =Background2) |> ignore
//            states

    static member WithColumnDescriptions (ds:seq<ColumnDescription>) state =
         let lastRange = state.LastRange
         let range = (lastRange |> convertRange)
         for cd in ds do
            let colRange = range.Find(cd.Column,null,null,Enums.XlLookAt.xlWhole,null)
            if colRange <> null then
                let c = colRange.AddComment(cd.Description)
                c.Visible <- false
                c.Shape.TextFrame.AutoSize <- true
                c.Shape.TextFrame.Characters().Font.Bold <- false
         state

    static member GroupColumns cStart cEnd state =
         let lastRange = state.LastRange
         let range = (lastRange |> convertRange) 
         let startRange = range.Find(cStart,null,null,Enums.XlLookAt.xlWhole,null)
         let endRange = range.Find(cEnd,null,null,Enums.XlLookAt.xlWhole,null)
         if startRange <> null && endRange <> null then
             let startAddress = startRange.Address
             let endAddress = endRange.Address
             let r = getRealRange (startAddress + ":" + endAddress)
             r.EntireColumn.Group() |> ignore
         state

    static member AsCell(range:string) = 
        fun v -> 
            let start = {LastRange = RealExcel (getRealRange range); NextRange = RealExcel (getRealRange range); History = []}
            (getRealRange range).Value2 <- v
            start

    static member ToCell(data) (start:ExcelState) = 
        (getRealRange ((start.NextRange |> convertRange).get_Address(0,0))).Value2 <- data
        start

let (|DeedleFrameAsExcelTable|_|) (v:obj) =
    let typ = v.GetType()
    if typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Deedle.Frame<_, _>> then
        let data = typ.GetMethod("GetFrameData").Invoke(v, [| |]) :?> Deedle.FrameData
        let excel = XlHelper.deedleDataToExcel data
        Some excel
    else None

let toExcel (rangeStr:string) (v:obj) = 
    assertInstance()
    let range = getRealRange rangeStr
    match v with 
        | DeedleFrameAsExcelTable df
        | (:? IExportExcelTable as df) -> 
            let start = {LastRange = Empty; NextRange = RealExcel range; History = []}
            XlHelper.asTable df start None None None None None 
            |> ignore
        | _ -> range.Value2 <- v

let ( *=) (range:string) (v:obj) = 
    toExcel range v


let saveToImage filename =
    fun state ->
        let range = state.LastRange |> convertRange
        let sheet = range.Worksheet.Name
        let activeWb = getActiveWorkbook()
        let newSheet = activeWb.Sheets.Add() :?> Worksheet
        let chart = activeWb.Charts.Add() :?> Chart
        newSheet.Delete()
        range.CopyPicture(Enums.XlPictureAppearance.xlScreen,Enums.XlCopyPictureFormat.xlBitmap) |> ignore
        chart.Paste()
        chart.Export(filename) |> ignore
        excelApp.DisplayAlerts <- false
        chart.Delete()

type DynamicExcel(app, ?keepInSync) =
    let mutable localExcelApp : Application=app
    let mutable keepInSync = defaultArg keepInSync false
    let syncLock = obj()

    let fsiAssembly =
        lazy
            System.AppDomain.CurrentDomain.GetAssemblies()
            |> Array.tryFind (fun assm -> assm.GetName().Name = "FSI-ASSEMBLY`")

    member this.KeepInSync
        with get () = keepInSync
        and set (choice) = keepInSync <- choice

    member private this.createInstance() = 
        if localExcelApp = null then 
            localExcelApp <- openNewExcelApplication()
        else
            ()

    member this.Reset() = excelApp <- null
    static member (?) (excel : DynamicExcel, r : string) = (getRealRange r).Value2

    member this.syncToExcel (r : string, value : 'a, ?sheet : string) =
        lock syncLock <| fun () ->
        this.createInstance()
        match sheet with
        | Some sheetName -> this.SwitchSheet sheetName
        | None -> ()
        match box value with
        | :? INotifyCollectionChanged as c -> toExcel r <| c
        | _ -> toExcel r <| value

    member this.asyncToExcel (r : string, value : 'a, ?sheet : string, ?force : bool) =
        if (defaultArg force this.KeepInSync) then
            Async.StartAsTask <|
                async {
                    match sheet with
                    | Some s -> this.syncToExcel(r, value, s)
                    | None -> this.syncToExcel(r, value)
                    }
            |> ignore

    member this.registerSyncToExcel (r : string, value : 'a, ?sheet : string) =
        match box value with
        | :? INotifyCollectionChanged as c ->
            c.CollectionChanged.Add <| fun arg ->
                this.asyncToExcel(r, value, ?sheet = sheet)
        | _ -> ()
        this.syncToExcel(r, value, ?sheet = sheet)
    
    static member (?<-)(excel : DynamicExcel, r : string, value : 'a) : unit = 
        excel.registerSyncToExcel(r, value)

    member this.SwitchSheet(name : string) = 
        this.createInstance()
        switchSheet name

    member this.RenameSheet(name : string) = 
        this.createInstance()
        renameSheet name

    member this.DeleteSheet(name : string ) = 
        this.createInstance()
        deleteSheet name

    member this.fromSeq (seq, ?cell) = 
            (?<-) this (defaultArg cell "A1") (seq
                                               |> Seq.map (fun (n, xs) -> (n, Series.ofValues xs))
                                               |> Frame.ofColumns)

let xl = DynamicExcel(excelApp)

type Xl with
    static member AsTable<'R, 'C, 'T when 'T :> Deedle.Frame<'R, 'C>> (range:string, ?ShowRowHeaders, ?ShowColumnHeaders, ?TableStyle,?ShowFilter, ?Style, ?RowSpan, ?ColSpan, ?TableTitle, ?TableTitleStyle) =
        fun (t:'T) -> 
            let start = {LastRange = Empty; NextRange = RealExcel (getRealRange range); History = []}
            let te = XlHelper.deedleFrameToExcel t
            XlHelper.asTable te start ShowRowHeaders ShowColumnHeaders TableTitle TableStyle ShowFilter

    static member AsTable<'R, 'C, 'T when 'T :> Deedle.Frame<'R, 'C>> (t:'T, ?ShowRowHeaders, ?ShowColumnHeaders, ?TableStyle,?ShowFilter, ?Style, ?RowSpan, ?ColSpan, ?TableTitle, ?TableTitleStyle) =
        fun start -> 
            let te = XlHelper.deedleFrameToExcel t
            XlHelper.asTable te start ShowRowHeaders ShowColumnHeaders TableTitle TableStyle ShowFilter

let (|GenericSeries|_|) s =
    let stype = typeof<Series<_,_>>.GetGenericTypeDefinition()
    let atype = s.GetType()
    if atype.IsGenericType && atype.GetGenericTypeDefinition() = stype then
        Some(atype.GetGenericArguments())
    else
        None

let (|GenericFrame|_|) f =
    let ftype = typeof<Frame<_,_>>.GetGenericTypeDefinition()
    let atype = f.GetType()
    if atype.IsGenericType && atype.GetGenericTypeDefinition() = ftype then
        Some(atype.GetGenericArguments())
    else
        None

let internal FsiAssembly =
    lazy
        System.AppDomain.CurrentDomain.GetAssemblies()
        |> Array.tryFind (fun assm -> assm.GetName().Name = "FSI-ASSEMBLY")

let GetCurrentFsiTypes () =
    match FsiAssembly.Force() with
    | Some fsia -> fsia.GetTypes()
    | None -> Array.empty

type FsiVariable = {
    name : string
    type_ : Type
    value : obj
    }

/// Inspect the FSharp interactive session and find all Series and Frames
let GetFsiSeriesAndFrames (knownTypes : ICollection<Type>) =
    dict [|
        let types = GetCurrentFsiTypes()
        for t in types do
            if t.Name.StartsWith("FSI") && not(knownTypes.Contains(t)) then
                yield t, [|
                    let flags = BindingFlags.Static ||| BindingFlags.NonPublic ||| BindingFlags.Public
                    for pi in t.GetProperties(flags) do
                        if not(pi.Name.Contains("@"))
                                && pi.GetIndexParameters().Length = 0
                                && pi.PropertyType <> typeof<Unit>
                                then
                            let pv = pi.GetValue(null, Array.empty)
                            if pv <> null then
                                match pv with
                                | GenericSeries argTypes | GenericFrame argTypes ->
                                    yield { name = pi.Name; type_ = pi.PropertyType; value = pi.GetValue(null, Array.empty); }
                                | _ -> ()
                |]
        |]

let internal makeGenericType (baseType : Type) (types : Type list) =
    if (not baseType.IsGenericTypeDefinition) then
        invalidArg "baseType" "The base type must be a generic type definition"
    baseType.MakeGenericType (types |> List.toArray)

let internal makeISeriesTypeOf itemType =
    makeGenericType typedefof<ISeries<_>> [ itemType; ]

type internal DynamicFrame =
    abstract Invoke<'t> : string * 't -> obj

let asFrame name aSeries =
    let TSeries = aSeries.GetType()
    let TSeriesArgs = TSeries.GetGenericArguments();
    let TSeriesKey, TSeriesValue = TSeriesArgs.[0], TSeriesArgs.[1]
    let TFrameColumnKeys = typeof<string>
    let assembly = System.Reflection.Assembly.GetAssembly(TSeries)
    let module_ = assembly.GetType("Deedle.FSharpFrameExtensions")
    let func_ = module_.GetMethod("frame")
    let TISeriesOfTSeriesValue = makeISeriesTypeOf TSeriesKey
    let funcOfTSeries = func_.MakeGenericMethod( [| TFrameColumnKeys; TISeriesOfTSeriesValue; TSeriesKey |] )
    let castTo = { new DynamicFrame with member __.Invoke(name : string, arg : 't) = funcOfTSeries.Invoke(null, [| [ name => arg ]; |]) }
    let castToMethod = typeof<DynamicFrame>.GetMethod("Invoke")
    castToMethod.MakeGenericMethod([|TISeriesOfTSeriesValue|]).Invoke(castTo, [|name; aSeries|])

/// Copy all Series and Frames in the FSharp interactive session to
/// Excel. A sheet is created for each Series and each Frame.
/// As series and frames are added to the session they will be added to
/// the current Excel workbook as new sheets.
let DeedleToExcel (addPrintTransformer : ('t -> obj) -> unit) =
    let variableToExcel (v : FsiVariable) =
        match v.value with
        | GenericSeries typeargs ->
            // series are immutable, no need to register and listen
            xl.syncToExcel("A1", (asFrame v.name v.value), v.name)
        | GenericFrame typeargs ->
            // frames can have rows and columns added, register to listen for changes
            xl.registerSyncToExcel("A1", v.value, v.name)
        | _ -> ()
    let allToExcel (valueCollections : System.Collections.Generic.ICollection<FsiVariable[]>) =
        Async.StartAsTask <|
            async {
                for values in valueCollections do
                    for v in values do
                        variableToExcel v
            }
        |> ignore
    let seriesAndFrames = GetFsiSeriesAndFrames Array.empty
    let knownFsiTypes = new System.Collections.Generic.List<_>(seriesAndFrames.Keys)
    xl.KeepInSync <- true
    allToExcel seriesAndFrames.Values
    addPrintTransformer <| fun (_ : 't) ->
        let newSeriesAndFrames = GetFsiSeriesAndFrames knownFsiTypes
        knownFsiTypes.AddRange(newSeriesAndFrames.Keys)
        allToExcel newSeriesAndFrames.Values
        null


