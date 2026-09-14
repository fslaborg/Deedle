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
01/01/2010 -> 0.38621714237564797  
01/02/2010 -> 0.044604134993612043 
01/03/2010 -> 0.506861715572953    
01/04/2010 -> 0.22055263558535532  
01/05/2010 -> 0.35759022076952485  
01/06/2010 -> 0.33935743567402443  
01/07/2010 -> 0.3762756005941431   
01/08/2010 -> 0.36406002174987917  
01/09/2010 -> 0.3176110639282822   
01/10/2010 -> 0.13598279404014546  
01/11/2010 -> 0.5623290125704966   
01/12/2010 -> 0.06562935651236435  
01/13/2010 -> 0.5195427321418316   
01/14/2010 -> 0.5353617445005387   
01/15/2010 -> 0.25573491588983677  
:             ...                  
12/18/2012 -> 0.19964599531781024  
12/19/2012 -> 0.5953472775093719   
12/20/2012 -> 0.13568056911463877  
12/21/2012 -> 0.8897516436787168   
12/22/2012 -> 0.5190630979816514   
12/23/2012 -> 0.1810390529876158   
12/24/2012 -> 0.08885849024097003  
12/25/2012 -> 0.18783837534786352  
12/26/2012 -> 0.2515772952631744   
12/27/2012 -> 0.3836023208773024   
12/28/2012 -> 0.6610804301738546   
12/29/2012 -> 0.880181846289666    
12/30/2012 -> 0.9981266312775856   
12/31/2012 -> 0.4505831542428623   
01/01/2013 -> 0.26784240340618026  

val slicedDf: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.21537288374499342 
06/02/2012 -> 0.37520981213382143 
06/03/2012 -> 0.6493533043331662  
06/04/2012 -> 0.7841089687142915  
06/05/2012 -> 0.8339536198390441  
06/06/2012 -> 0.4775522990608978  
06/07/2012 -> 0.1865608523540464  
06/08/2012 -> 0.6449700417535196  
06/09/2012 -> 0.1815598522694608  
06/10/2012 -> 0.4420208012750748  
06/11/2012 -> 0.14876551455547515 
06/12/2012 -> 0.2977135318883114  
06/13/2012 -> 0.6811377776483144  
06/14/2012 -> 0.7189907518627207  
06/15/2012 -> 0.5815367892307581  
06/16/2012 -> 0.8322504367174408  
06/17/2012 -> 0.9403059157692267  
06/18/2012 -> 0.2552868946453929  
06/19/2012 -> 0.9168702997575422  
06/20/2012 -> 0.5603545551999249  
06/21/2012 -> 0.9649642811648094  
06/22/2012 -> 0.23798489716465943 
06/23/2012 -> 0.7755713721637412  
06/24/2012 -> 0.7672173556303834  
06/25/2012 -> 0.16722212278690518 
06/26/2012 -> 0.832465866775724   
06/27/2012 -> 0.8661032090630479  
06/28/2012 -> 0.36862250446805245 
06/29/2012 -> 0.7578287888689638  
06/30/2012 -> 0.27960300490832635 

val it: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.21537288374499342 
06/02/2012 -> 0.37520981213382143 
06/03/2012 -> 0.6493533043331662  
06/04/2012 -> 0.7841089687142915  
06/05/2012 -> 0.8339536198390441  
06/06/2012 -> 0.4775522990608978  
06/07/2012 -> 0.1865608523540464  
06/08/2012 -> 0.6449700417535196  
06/09/2012 -> 0.1815598522694608  
06/10/2012 -> 0.4420208012750748  
06/11/2012 -> 0.14876551455547515 
06/12/2012 -> 0.2977135318883114  
06/13/2012 -> 0.6811377776483144  
06/14/2012 -> 0.7189907518627207  
06/15/2012 -> 0.5815367892307581  
06/16/2012 -> 0.8322504367174408  
06/17/2012 -> 0.9403059157692267  
06/18/2012 -> 0.2552868946453929  
06/19/2012 -> 0.9168702997575422  
06/20/2012 -> 0.5603545551999249  
06/21/2012 -> 0.9649642811648094  
06/22/2012 -> 0.23798489716465943 
06/23/2012 -> 0.7755713721637412  
06/24/2012 -> 0.7672173556303834  
06/25/2012 -> 0.16722212278690518 
06/26/2012 -> 0.832465866775724   
06/27/2012 -> 0.8661032090630479  
06/28/2012 -> 0.36862250446805245 
06/29/2012 -> 0.7578287888689638  
06/30/2012 -> 0.27960300490832635
```
