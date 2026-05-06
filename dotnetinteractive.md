# Deedle.DotNetInteractive — .NET Interactive Notebook Formatting

The `Deedle.DotNetInteractive` package registers HTML formatters for Deedle frames and
series in [.NET Interactive](https://github.com/dotnet/interactive) notebooks —
including **Polyglot Notebooks** (VS Code) and **Jupyter** via the .NET kernel.

Install it alongside Deedle in a notebook cell:

```fsharp
#r "nuget: Deedle"
#r "nuget: Deedle.DotNetInteractive"

```

Once loaded, any `Frame<_,_>` or `Series<_,_>` displayed in a notebook cell is
automatically rendered as a styled HTML table instead of plain text.

<a name="example"></a>
## Example output

Below is a frame as it would appear in a notebook. When using `Deedle.DotNetInteractive`,
display output includes column types, dimensions, and missing-value counts.

```fsharp
let prices =
    frame [ "Open"  => Series.ofValues [ 100.0; 102.5; 101.0; 103.0 ]
            "Close" => Series.ofValues [ 101.5; 101.0; 103.5; 104.0 ]
            "Vol"   => Series.ofValues [ 12000; 15000; 11000; 14000 ] ]
```

```
val prices: Frame<int,string> =
  
     Open  Close Vol   
0 -> 100   101.5 12000 
1 -> 102.5 101   15000 
2 -> 101   103.5 11000 
3 -> 103   104   14000
```

```fsharp
let vols = Series.ofValues [ 12000; 15000; 11000; 14000 ]
```

```
val vols: Series<int,int> = 
0 -> 12000 
1 -> 15000 
2 -> 11000 
3 -> 14000
```

<a name="config"></a>
## Configuring display options

The `InteractiveConfig` module lets you control how frames and series are rendered.
All settings are mutable and take effect immediately for subsequent cell evaluations.

### Frame display settings

Setting | Default | Description
--- | --- | ---
`InteractiveConfig.Frame.MaxRows` | 10 | Maximum rows to display before truncating
`InteractiveConfig.Frame.MaxCols` | 20 | Maximum columns to display before truncating
`InteractiveConfig.Frame.ShowColTypes` | true | Show column type names in the header
`InteractiveConfig.Frame.ShowInfo` | true | Show the info footer (dimensions and/or missing counts)
`InteractiveConfig.Frame.ShowDimensions` | true | Show "N rows x M columns" in the footer
`InteractiveConfig.Frame.ShowMissingValueCount` | true | Show the count of missing values in the footer


### Series display settings

Setting | Default | Description
--- | --- | ---
`InteractiveConfig.Series.MaxItems` | 10 | Maximum items to display before truncating
`InteractiveConfig.Series.ShowInfo` | true | Show the info footer
`InteractiveConfig.Series.ShowItemCount` | true | Show total item count in the footer
`InteractiveConfig.Series.ShowMissingValueCount` | true | Show the count of missing values in the footer


### Example: compact display

In a notebook cell you might write:

```fsharp
open Deedle.DotNetInteractive

InteractiveConfig.Frame.MaxRows <- 5
InteractiveConfig.Frame.MaxCols <- 4
InteractiveConfig.Frame.ShowColTypes <- false
InteractiveConfig.Frame.ShowMissingValueCount <- false

```

### Resetting to defaults

Call `InteractiveConfig.Reset()` to restore all settings to their defaults:

```fsharp
InteractiveConfig.Reset()

```

<a name="how-it-works"></a>
## How it works

`Deedle.DotNetInteractive` ships an `extension.dib` file that is loaded automatically
by the .NET Interactive kernel. It registers two `Formatter` instances:

* One for any type implementing `IFrameFormattable` (all `Frame<_,_>` types)

* One for any type implementing `ISeriesFormattable` (all `Series<_,_>` types)

Both produce `text/html` output. The formatters call the `InteractiveFormat` method
on the frame or series, which returns a string grid that is then wrapped in an HTML
`<table>` with basic CSS styling.

Because registration happens via the standard .NET Interactive extension mechanism,
no manual setup code is needed — just reference the NuGet package.

<a name="nuget"></a>
## NuGet package information

Package | Description
--- | ---
`Deedle` | Core library
`Deedle.DotNetInteractive` | .NET Interactive HTML formatters for frames and series

