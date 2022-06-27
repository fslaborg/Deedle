namespace Deedle.Interactive

module InteractiveConfig =

  module Frame =

    let mutable MaxRows = 10
    let mutable MaxCols = 20
    let mutable ShowColTypes = true
    let mutable ShowInfo = true
    let mutable ShowDimensions = true
    let mutable ShowMissingValueCount = true

  module Series = 

    let mutable MaxItems = 10
    let mutable ShowInfo = true
    let mutable ShowItemCount = true
    let mutable ShowMissingValueCount = true

  let Reset() =

    Frame.MaxRows <- 10
    Frame.MaxCols <- 20
    Frame.ShowColTypes <- true
    Frame.ShowInfo <- true
    Frame.ShowDimensions <- true
    Frame.ShowMissingValueCount <- true

    Series.MaxItems <- 10
    Series.ShowInfo <- true
    Series.ShowItemCount <- true
    Series.ShowMissingValueCount <- true
