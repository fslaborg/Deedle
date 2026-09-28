# Delay-loaded series

The `DelayedSeries` type provides an efficient way to create series whose data is loaded
on-demand. For example, you may have a large time series stored in a CSV file or in a
database and you do not want to load all the data in memory if the user only needs a
small part of it.

When you create a delayed series, you specify the overall range of the series (i.e. the
minimum and maximum key value) and you provide a function that loads a specified sub-range
of the series. When the user accesses a continuous range of the series, the loading function
is called to retrieve the data.

<a name="create"></a>
## Creating a delayed series

To create a delayed series, we need a function that generates data for a given range.
The following function generates a series with random data for a given date range with
a day frequency:

```fsharp
let generate (low:DateTime) (high:DateTime) : seq<KeyValuePair<DateTime,float>> = 
    let rnd = Random()
    let days = int (high - low).TotalDays
    seq [ for d in 0 .. days -> KeyValuePair(low.AddDays(float d), rnd.NextDouble()) ]
```

Now we use `DelayedSeries.FromValueLoader` to create a delayed series. It takes the overall
minimum and maximum key of the series and a function that loads data for a sub-range. The
loading function gets the lower and upper bound as a tuple of `(key, BoundaryBehavior)`
values where `BoundaryBehavior` is either `Inclusive` or `Exclusive`:

```fsharp
let min = DateTime(2010, 1, 1)
let max = DateTime(2013, 1, 1)

let ls = DelayedSeries.FromValueLoader(min, max, fun (lo, lob) (hi, hib) -> async {
    printfn "Query: %A - %A" lo hi
    let lo = if lob = BoundaryBehavior.Inclusive then lo else lo.AddDays(1.0)
    let hi = if hib = BoundaryBehavior.Inclusive then hi else hi.AddDays(-1.0)
    return generate lo hi })
```

The key thing about the above is that, so far, no data has been loaded. The loading function
is called only when we access part of the series.

<a name="slicing"></a>
## Slicing and using delayed series

We can now use the series as usual - for example, to get data for the entire year 2012:

```fsharp
let slice = ls.[DateTime(2012, 1, 1) .. DateTime(2012, 12, 31)]
slice
```

```
val slice: Series<DateTime,float> =
  
(Delayed series [01/01/2012 .. 12/31/2012]) 

val it: Series<DateTime,float> =
  
(Delayed series [01/01/2012 .. 12/31/2012])
```

Similarly, we can add the delayed series to a data frame. When doing this, Deedle will
only load the data that is needed. In the following example, we add the series to a frame
and then access only a slice:

```fsharp
let df = frame ["Values" => ls]
let slicedDf = df.Rows.[DateTime(2012,6,1) .. DateTime(2012,6,30)]
slicedDf
```

```
Query: 01/01/2010 00:00:00 - 01/01/2013 00:00:00
Query: 06/01/2012 00:00:00 - 06/30/2012 00:00:00
val df: Frame<DateTime,string> =
  
              Values              
01/01/2010 -> 0.8662872777311922  
01/02/2010 -> 0.6234150445463271  
01/03/2010 -> 0.5497826415936199  
01/04/2010 -> 0.15640575767547626 
01/05/2010 -> 0.10640607060256424 
01/06/2010 -> 0.49394324462778905 
01/07/2010 -> 0.11631294673137316 
01/08/2010 -> 0.37815398528165967 
01/09/2010 -> 0.42630510835979996 
01/10/2010 -> 0.19796730736640433 
01/11/2010 -> 0.32934561944548923 
01/12/2010 -> 0.7785930542022867  
01/13/2010 -> 0.4278432850331474  
01/14/2010 -> 0.15122497198691787 
01/15/2010 -> 0.4213414712117083  
:             ...                 
12/18/2012 -> 0.5705171634518074  
12/19/2012 -> 0.9279470497342733  
12/20/2012 -> 0.977058666448017   
12/21/2012 -> 0.45803249811383007 
12/22/2012 -> 0.520015301878488   
12/23/2012 -> 0.5457042951668624  
12/24/2012 -> 0.9256861281917287  
12/25/2012 -> 0.7159646385748839  
12/26/2012 -> 0.39232906951105573 
12/27/2012 -> 0.25293391909569585 
12/28/2012 -> 0.924970757714352   
12/29/2012 -> 0.6687185515702719  
12/30/2012 -> 0.17648153752351103 
12/31/2012 -> 0.7549485852988316  
01/01/2013 -> 0.34223919912633805 

val slicedDf: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.9295250933974335   
06/02/2012 -> 0.32561604163183844  
06/03/2012 -> 0.5302365122737046   
06/04/2012 -> 0.5482780775173793   
06/05/2012 -> 0.30557783828179075  
06/06/2012 -> 0.6125461731070699   
06/07/2012 -> 0.345478986362997    
06/08/2012 -> 0.47890052347324474  
06/09/2012 -> 0.7063576476380934   
06/10/2012 -> 0.05048029821450861  
06/11/2012 -> 0.5603696387134182   
06/12/2012 -> 0.67350444828088     
06/13/2012 -> 0.9550333223350478   
06/14/2012 -> 0.17190153473152414  
06/15/2012 -> 0.9204022830933345   
06/16/2012 -> 0.9219821708309768   
06/17/2012 -> 0.6486387520727832   
06/18/2012 -> 0.17746660758872845  
06/19/2012 -> 0.2311842671995452   
06/20/2012 -> 0.9030403050337079   
06/21/2012 -> 0.027121810685743375 
06/22/2012 -> 0.6142019841156807   
06/23/2012 -> 0.47913637052186797  
06/24/2012 -> 0.2944246639627345   
06/25/2012 -> 0.1321507671359069   
06/26/2012 -> 0.7703469797624082   
06/27/2012 -> 0.4577373453400764   
06/28/2012 -> 0.4905410019026353   
06/29/2012 -> 0.26445957910020146  
06/30/2012 -> 0.2310027660452939   

val it: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.9295250933974335   
06/02/2012 -> 0.32561604163183844  
06/03/2012 -> 0.5302365122737046   
06/04/2012 -> 0.5482780775173793   
06/05/2012 -> 0.30557783828179075  
06/06/2012 -> 0.6125461731070699   
06/07/2012 -> 0.345478986362997    
06/08/2012 -> 0.47890052347324474  
06/09/2012 -> 0.7063576476380934   
06/10/2012 -> 0.05048029821450861  
06/11/2012 -> 0.5603696387134182   
06/12/2012 -> 0.67350444828088     
06/13/2012 -> 0.9550333223350478   
06/14/2012 -> 0.17190153473152414  
06/15/2012 -> 0.9204022830933345   
06/16/2012 -> 0.9219821708309768   
06/17/2012 -> 0.6486387520727832   
06/18/2012 -> 0.17746660758872845  
06/19/2012 -> 0.2311842671995452   
06/20/2012 -> 0.9030403050337079   
06/21/2012 -> 0.027121810685743375 
06/22/2012 -> 0.6142019841156807   
06/23/2012 -> 0.47913637052186797  
06/24/2012 -> 0.2944246639627345   
06/25/2012 -> 0.1321507671359069   
06/26/2012 -> 0.7703469797624082   
06/27/2012 -> 0.4577373453400764   
06/28/2012 -> 0.4905410019026353   
06/29/2012 -> 0.26445957910020146  
06/30/2012 -> 0.2310027660452939
```
