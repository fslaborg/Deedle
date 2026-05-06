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
01/01/2010 -> 0.27848604340773975  
01/02/2010 -> 0.11084867502645157  
01/03/2010 -> 0.4810974745535421   
01/04/2010 -> 0.9769243347629257   
01/05/2010 -> 0.11735623115974225  
01/06/2010 -> 0.5554570889713729   
01/07/2010 -> 0.008336441044109577 
01/08/2010 -> 0.07240137420478698  
01/09/2010 -> 0.458524880576505    
01/10/2010 -> 0.7734077868815874   
01/11/2010 -> 0.3999992005668843   
01/12/2010 -> 0.3940850146698325   
01/13/2010 -> 0.31589393131393695  
01/14/2010 -> 0.7138886470495805   
01/15/2010 -> 0.20761176668501713  
:             ...                  
12/18/2012 -> 0.7141011508056943   
12/19/2012 -> 0.1410003679989179   
12/20/2012 -> 0.9102184644435817   
12/21/2012 -> 0.44790786118375714  
12/22/2012 -> 0.4921673924126436   
12/23/2012 -> 0.7935141396683856   
12/24/2012 -> 0.6241737931702118   
12/25/2012 -> 0.5730135559044243   
12/26/2012 -> 0.26083288939389826  
12/27/2012 -> 0.07157847376751736  
12/28/2012 -> 0.02366833396639534  
12/29/2012 -> 0.6309440229481756   
12/30/2012 -> 0.4555091988265385   
12/31/2012 -> 0.3811005358729387   
01/01/2013 -> 0.18167036610562404  

val slicedDf: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.3717282698688884   
06/02/2012 -> 0.9146220222382779   
06/03/2012 -> 0.8983916638277767   
06/04/2012 -> 0.9525181765082196   
06/05/2012 -> 0.4044901313898137   
06/06/2012 -> 0.8371364634495783   
06/07/2012 -> 0.7017170683164694   
06/08/2012 -> 0.52865997260727     
06/09/2012 -> 0.7225938219935071   
06/10/2012 -> 0.9182212264158754   
06/11/2012 -> 0.9744514225435132   
06/12/2012 -> 0.7734061595470098   
06/13/2012 -> 0.3065094080047026   
06/14/2012 -> 0.8658807750588954   
06/15/2012 -> 0.6847211387621502   
06/16/2012 -> 0.42003571670974094  
06/17/2012 -> 0.4856710896941955   
06/18/2012 -> 0.35585497848648595  
06/19/2012 -> 0.28941001002312394  
06/20/2012 -> 0.15952482141994007  
06/21/2012 -> 0.14239299966445662  
06/22/2012 -> 0.010726476142192354 
06/23/2012 -> 0.5890994336286979   
06/24/2012 -> 0.6935275702511072   
06/25/2012 -> 0.561267052639851    
06/26/2012 -> 0.4024395892130246   
06/27/2012 -> 0.447731053282723    
06/28/2012 -> 0.7014345167829735   
06/29/2012 -> 0.6696098058755123   
06/30/2012 -> 0.00845270403042575  

val it: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.3717282698688884   
06/02/2012 -> 0.9146220222382779   
06/03/2012 -> 0.8983916638277767   
06/04/2012 -> 0.9525181765082196   
06/05/2012 -> 0.4044901313898137   
06/06/2012 -> 0.8371364634495783   
06/07/2012 -> 0.7017170683164694   
06/08/2012 -> 0.52865997260727     
06/09/2012 -> 0.7225938219935071   
06/10/2012 -> 0.9182212264158754   
06/11/2012 -> 0.9744514225435132   
06/12/2012 -> 0.7734061595470098   
06/13/2012 -> 0.3065094080047026   
06/14/2012 -> 0.8658807750588954   
06/15/2012 -> 0.6847211387621502   
06/16/2012 -> 0.42003571670974094  
06/17/2012 -> 0.4856710896941955   
06/18/2012 -> 0.35585497848648595  
06/19/2012 -> 0.28941001002312394  
06/20/2012 -> 0.15952482141994007  
06/21/2012 -> 0.14239299966445662  
06/22/2012 -> 0.010726476142192354 
06/23/2012 -> 0.5890994336286979   
06/24/2012 -> 0.6935275702511072   
06/25/2012 -> 0.561267052639851    
06/26/2012 -> 0.4024395892130246   
06/27/2012 -> 0.447731053282723    
06/28/2012 -> 0.7014345167829735   
06/29/2012 -> 0.6696098058755123   
06/30/2012 -> 0.00845270403042575
```
