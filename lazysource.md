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
01/01/2010 -> 0.9412400131836955   
01/02/2010 -> 0.844632319521777    
01/03/2010 -> 0.4155819718164717   
01/04/2010 -> 0.6857329869113181   
01/05/2010 -> 0.030397195827914447 
01/06/2010 -> 0.8715988787047938   
01/07/2010 -> 0.9902934396051305   
01/08/2010 -> 0.7873826677122377   
01/09/2010 -> 0.6169469571634757   
01/10/2010 -> 0.3909329352430908   
01/11/2010 -> 0.6591993700788839   
01/12/2010 -> 0.8025872823774322   
01/13/2010 -> 0.31817787235122275  
01/14/2010 -> 0.6701452413462291   
01/15/2010 -> 0.16622290978403664  
:             ...                  
12/18/2012 -> 0.27740070226240365  
12/19/2012 -> 0.06534036088280304  
12/20/2012 -> 0.8459276927913242   
12/21/2012 -> 0.04854440982986541  
12/22/2012 -> 0.6961614022549054   
12/23/2012 -> 0.8287507493251526   
12/24/2012 -> 0.2548888387850663   
12/25/2012 -> 0.049390096956643315 
12/26/2012 -> 0.7801225620021958   
12/27/2012 -> 0.362292858840111    
12/28/2012 -> 0.7075645954931757   
12/29/2012 -> 0.20994484849138606  
12/30/2012 -> 0.7336650684438506   
12/31/2012 -> 0.9108107647283797   
01/01/2013 -> 0.2613430267452863   

val slicedDf: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.5386354450081648   
06/02/2012 -> 0.46431583384935027  
06/03/2012 -> 0.851185150910351    
06/04/2012 -> 0.0969550485599402   
06/05/2012 -> 0.7280521578882846   
06/06/2012 -> 0.39854079713779855  
06/07/2012 -> 0.3504113407714152   
06/08/2012 -> 0.34662087870836444  
06/09/2012 -> 0.590717969640712    
06/10/2012 -> 0.4692924049878916   
06/11/2012 -> 0.4906123541734648   
06/12/2012 -> 0.30622500956678744  
06/13/2012 -> 0.020852472899706598 
06/14/2012 -> 0.09900932910269977  
06/15/2012 -> 0.9174857735319353   
06/16/2012 -> 0.42314309258478855  
06/17/2012 -> 0.4147211358800744   
06/18/2012 -> 0.7188703213503336   
06/19/2012 -> 0.21374131013284337  
06/20/2012 -> 0.6454661445289075   
06/21/2012 -> 0.4621434507304393   
06/22/2012 -> 0.872263911906171    
06/23/2012 -> 0.6123262948564022   
06/24/2012 -> 0.30506139913497576  
06/25/2012 -> 0.9478043970336817   
06/26/2012 -> 0.5521648879221501   
06/27/2012 -> 0.4196368159281546   
06/28/2012 -> 0.8487635728708679   
06/29/2012 -> 0.9425476657452861   
06/30/2012 -> 0.2996262152178336   

val it: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.5386354450081648   
06/02/2012 -> 0.46431583384935027  
06/03/2012 -> 0.851185150910351    
06/04/2012 -> 0.0969550485599402   
06/05/2012 -> 0.7280521578882846   
06/06/2012 -> 0.39854079713779855  
06/07/2012 -> 0.3504113407714152   
06/08/2012 -> 0.34662087870836444  
06/09/2012 -> 0.590717969640712    
06/10/2012 -> 0.4692924049878916   
06/11/2012 -> 0.4906123541734648   
06/12/2012 -> 0.30622500956678744  
06/13/2012 -> 0.020852472899706598 
06/14/2012 -> 0.09900932910269977  
06/15/2012 -> 0.9174857735319353   
06/16/2012 -> 0.42314309258478855  
06/17/2012 -> 0.4147211358800744   
06/18/2012 -> 0.7188703213503336   
06/19/2012 -> 0.21374131013284337  
06/20/2012 -> 0.6454661445289075   
06/21/2012 -> 0.4621434507304393   
06/22/2012 -> 0.872263911906171    
06/23/2012 -> 0.6123262948564022   
06/24/2012 -> 0.30506139913497576  
06/25/2012 -> 0.9478043970336817   
06/26/2012 -> 0.5521648879221501   
06/27/2012 -> 0.4196368159281546   
06/28/2012 -> 0.8487635728708679   
06/29/2012 -> 0.9425476657452861   
06/30/2012 -> 0.2996262152178336
```
