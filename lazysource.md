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
01/01/2010 -> 0.9378227481223016   
01/02/2010 -> 0.020973756360873508 
01/03/2010 -> 0.23830184141515343  
01/04/2010 -> 0.712144889119251    
01/05/2010 -> 0.702501394676738    
01/06/2010 -> 0.2684437435714223   
01/07/2010 -> 0.9590289993835162   
01/08/2010 -> 0.1880480595723194   
01/09/2010 -> 0.8355318781485446   
01/10/2010 -> 0.0742196952877795   
01/11/2010 -> 0.5898488196757402   
01/12/2010 -> 0.5553673261514295   
01/13/2010 -> 0.6338493191028526   
01/14/2010 -> 0.8327770312879557   
01/15/2010 -> 0.13217803127619998  
:             ...                  
12/18/2012 -> 0.9061316012398075   
12/19/2012 -> 0.6689349049826107   
12/20/2012 -> 0.07510399158111425  
12/21/2012 -> 0.9218195168276107   
12/22/2012 -> 0.9093934179455248   
12/23/2012 -> 0.035858780102666854 
12/24/2012 -> 0.2878158540890263   
12/25/2012 -> 0.2156601944135138   
12/26/2012 -> 0.1137985532754241   
12/27/2012 -> 0.22187001653400784  
12/28/2012 -> 0.27647737258901817  
12/29/2012 -> 0.08330342264723634  
12/30/2012 -> 0.9331881583259012   
12/31/2012 -> 0.7061652401107418   
01/01/2013 -> 0.11841210142029635  

val slicedDf: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.5559900019132942  
06/02/2012 -> 0.986026371607082   
06/03/2012 -> 0.16082074978656813 
06/04/2012 -> 0.644971932587615   
06/05/2012 -> 0.4201863615729997  
06/06/2012 -> 0.5603942861720103  
06/07/2012 -> 0.9681623526483214  
06/08/2012 -> 0.4695923918180692  
06/09/2012 -> 0.33721282692709964 
06/10/2012 -> 0.27009673164222625 
06/11/2012 -> 0.6652712336963145  
06/12/2012 -> 0.8194068231508703  
06/13/2012 -> 0.06142324522994824 
06/14/2012 -> 0.10917328656138514 
06/15/2012 -> 0.3465143881841717  
06/16/2012 -> 0.5333570186354901  
06/17/2012 -> 0.2504023387120978  
06/18/2012 -> 0.08674602865583447 
06/19/2012 -> 0.6413246581958247  
06/20/2012 -> 0.1693251963228536  
06/21/2012 -> 0.14063886548686555 
06/22/2012 -> 0.8159055351887834  
06/23/2012 -> 0.55435343478245    
06/24/2012 -> 0.6206024485311397  
06/25/2012 -> 0.1855239060155114  
06/26/2012 -> 0.8390843293696948  
06/27/2012 -> 0.7594439365699006  
06/28/2012 -> 0.9288297133126787  
06/29/2012 -> 0.8212921484526158  
06/30/2012 -> 0.608519852691538   

val it: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.5559900019132942  
06/02/2012 -> 0.986026371607082   
06/03/2012 -> 0.16082074978656813 
06/04/2012 -> 0.644971932587615   
06/05/2012 -> 0.4201863615729997  
06/06/2012 -> 0.5603942861720103  
06/07/2012 -> 0.9681623526483214  
06/08/2012 -> 0.4695923918180692  
06/09/2012 -> 0.33721282692709964 
06/10/2012 -> 0.27009673164222625 
06/11/2012 -> 0.6652712336963145  
06/12/2012 -> 0.8194068231508703  
06/13/2012 -> 0.06142324522994824 
06/14/2012 -> 0.10917328656138514 
06/15/2012 -> 0.3465143881841717  
06/16/2012 -> 0.5333570186354901  
06/17/2012 -> 0.2504023387120978  
06/18/2012 -> 0.08674602865583447 
06/19/2012 -> 0.6413246581958247  
06/20/2012 -> 0.1693251963228536  
06/21/2012 -> 0.14063886548686555 
06/22/2012 -> 0.8159055351887834  
06/23/2012 -> 0.55435343478245    
06/24/2012 -> 0.6206024485311397  
06/25/2012 -> 0.1855239060155114  
06/26/2012 -> 0.8390843293696948  
06/27/2012 -> 0.7594439365699006  
06/28/2012 -> 0.9288297133126787  
06/29/2012 -> 0.8212921484526158  
06/30/2012 -> 0.608519852691538
```
