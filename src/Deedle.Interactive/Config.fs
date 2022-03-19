namespace Deedle.Interactive

module InteractiveConfig =

  let mutable MaxRows = 10

  let mutable MaxCols = 20

  let mutable ShowColTypes = true

  let Reset() =
    MaxRows <- 10
    MaxCols <- 20
    ShowColTypes <- true
