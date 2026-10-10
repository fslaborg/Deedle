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
01/01/2010 -> 0.4378846601997247  
01/02/2010 -> 0.9904816133299714  
01/03/2010 -> 0.14087358910307912 
01/04/2010 -> 0.3266625842321581  
01/05/2010 -> 0.7972901586791217  
01/06/2010 -> 0.6555123911363107  
01/07/2010 -> 0.8978731872228382  
01/08/2010 -> 0.6904253866096856  
01/09/2010 -> 0.7147286178468     
01/10/2010 -> 0.614153341091547   
01/11/2010 -> 0.45985000688144584 
01/12/2010 -> 0.2065949313349239  
01/13/2010 -> 0.2884080708913688  
01/14/2010 -> 0.9112545919169682  
01/15/2010 -> 0.6731748679433656  
:             ...                 
12/18/2012 -> 0.32310051574246035 
12/19/2012 -> 0.410432244216708   
12/20/2012 -> 0.3043873683643902  
12/21/2012 -> 0.9870388348929867  
12/22/2012 -> 0.9013071069407401  
12/23/2012 -> 0.21325272501725    
12/24/2012 -> 0.6044963483269387  
12/25/2012 -> 0.9993689377297713  
12/26/2012 -> 0.7107421973153552  
12/27/2012 -> 0.4707345332207953  
12/28/2012 -> 0.8206287550268666  
12/29/2012 -> 0.2759179849462928  
12/30/2012 -> 0.04013935209082775 
12/31/2012 -> 0.8756775089485073  
01/01/2013 -> 0.5365166632414373  

val slicedDf: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.45876795428811357 
06/02/2012 -> 0.42236057751748357 
06/03/2012 -> 0.6892082697900636  
06/04/2012 -> 0.2387411882568592  
06/05/2012 -> 0.942065108602365   
06/06/2012 -> 0.9588006412316861  
06/07/2012 -> 0.4251126828249415  
06/08/2012 -> 0.559240347536318   
06/09/2012 -> 0.8270962895050584  
06/10/2012 -> 0.5419924529602878  
06/11/2012 -> 0.6001812294972932  
06/12/2012 -> 0.5810728637352972  
06/13/2012 -> 0.9710207712961116  
06/14/2012 -> 0.24326729291250493 
06/15/2012 -> 0.5223230211423509  
06/16/2012 -> 0.4616327554523697  
06/17/2012 -> 0.5550339415612568  
06/18/2012 -> 0.2649036572236857  
06/19/2012 -> 0.7099515416355812  
06/20/2012 -> 0.989261030390905   
06/21/2012 -> 0.5310300415778396  
06/22/2012 -> 0.24210881303426846 
06/23/2012 -> 0.828550596408595   
06/24/2012 -> 0.1556756319139566  
06/25/2012 -> 0.5591956605935858  
06/26/2012 -> 0.9854394754908217  
06/27/2012 -> 0.8515908449040239  
06/28/2012 -> 0.5318258749071569  
06/29/2012 -> 0.8914431430821499  
06/30/2012 -> 0.9428714379727777  

val it: Frame<DateTime,string> =
  
              Values              
06/01/2012 -> 0.45876795428811357 
06/02/2012 -> 0.42236057751748357 
06/03/2012 -> 0.6892082697900636  
06/04/2012 -> 0.2387411882568592  
06/05/2012 -> 0.942065108602365   
06/06/2012 -> 0.9588006412316861  
06/07/2012 -> 0.4251126828249415  
06/08/2012 -> 0.559240347536318   
06/09/2012 -> 0.8270962895050584  
06/10/2012 -> 0.5419924529602878  
06/11/2012 -> 0.6001812294972932  
06/12/2012 -> 0.5810728637352972  
06/13/2012 -> 0.9710207712961116  
06/14/2012 -> 0.24326729291250493 
06/15/2012 -> 0.5223230211423509  
06/16/2012 -> 0.4616327554523697  
06/17/2012 -> 0.5550339415612568  
06/18/2012 -> 0.2649036572236857  
06/19/2012 -> 0.7099515416355812  
06/20/2012 -> 0.989261030390905   
06/21/2012 -> 0.5310300415778396  
06/22/2012 -> 0.24210881303426846 
06/23/2012 -> 0.828550596408595   
06/24/2012 -> 0.1556756319139566  
06/25/2012 -> 0.5591956605935858  
06/26/2012 -> 0.9854394754908217  
06/27/2012 -> 0.8515908449040239  
06/28/2012 -> 0.5318258749071569  
06/29/2012 -> 0.8914431430821499  
06/30/2012 -> 0.9428714379727777
```
