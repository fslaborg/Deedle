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
01/01/2010 -> 0.4625310431576888  
01/02/2010 -> 0.03849696933277669 
01/03/2010 -> 0.08514743844795702 
01/04/2010 -> 0.2776129798921282  
01/05/2010 -> 0.1501239590739848  
01/06/2010 -> 0.5374072958364162  
01/07/2010 -> 0.3186404677495822  
01/08/2010 -> 0.01574154481246126 
01/09/2010 -> 0.2864340968648662  
01/10/2010 -> 0.26255247306603513 
01/11/2010 -> 0.3679325755089221  
01/12/2010 -> 0.17396916174933985 
01/13/2010 -> 0.5191758770082541  
01/14/2010 -> 0.7175259299992067  
01/15/2010 -> 0.65268418290344    
:             ...                 
12/18/2012 -> 0.06902201617824866 
12/19/2012 -> 0.3982324995844353  
12/20/2012 -> 0.7151728495678826  
12/21/2012 -> 0.9069879435182261  
12/22/2012 -> 0.9000127276065644  
12/23/2012 -> 0.1736879468917143  
12/24/2012 -> 0.8778356589942753  
12/25/2012 -> 0.5529737528004741  
12/26/2012 -> 0.5844095831992748  
12/27/2012 -> 0.01214482914410997 
12/28/2012 -> 0.1603223839179566  
12/29/2012 -> 0.9570597757486118  
12/30/2012 -> 0.7544919514078035  
12/31/2012 -> 0.5836341372921696  
01/01/2013 -> 0.09080074207053501 

val slicedDf: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.7750321668633428   
06/02/2012 -> 0.021921199894606058 
06/03/2012 -> 0.27126027981995615  
06/04/2012 -> 0.40088162834975005  
06/05/2012 -> 0.5734079977924067   
06/06/2012 -> 0.42436882022257183  
06/07/2012 -> 0.5035427031375076   
06/08/2012 -> 0.7407395643475243   
06/09/2012 -> 0.4230543593346482   
06/10/2012 -> 0.2859428806620432   
06/11/2012 -> 0.4450495862821442   
06/12/2012 -> 0.24628050067860752  
06/13/2012 -> 0.09179691289512626  
06/14/2012 -> 0.1817955036657456   
06/15/2012 -> 0.603296735707329    
06/16/2012 -> 0.9643939276988227   
06/17/2012 -> 0.3130360925369504   
06/18/2012 -> 0.04734334378620153  
06/19/2012 -> 0.1317854078230326   
06/20/2012 -> 0.9683122856510614   
06/21/2012 -> 0.4533290701062196   
06/22/2012 -> 0.40128653437613915  
06/23/2012 -> 0.47150231374428286  
06/24/2012 -> 0.050169316753532645 
06/25/2012 -> 0.5070869476588131   
06/26/2012 -> 0.7035485913619396   
06/27/2012 -> 0.9749216888300862   
06/28/2012 -> 0.5036393201962283   
06/29/2012 -> 0.6340304503927038   
06/30/2012 -> 0.9195732853461546   

val it: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.7750321668633428   
06/02/2012 -> 0.021921199894606058 
06/03/2012 -> 0.27126027981995615  
06/04/2012 -> 0.40088162834975005  
06/05/2012 -> 0.5734079977924067   
06/06/2012 -> 0.42436882022257183  
06/07/2012 -> 0.5035427031375076   
06/08/2012 -> 0.7407395643475243   
06/09/2012 -> 0.4230543593346482   
06/10/2012 -> 0.2859428806620432   
06/11/2012 -> 0.4450495862821442   
06/12/2012 -> 0.24628050067860752  
06/13/2012 -> 0.09179691289512626  
06/14/2012 -> 0.1817955036657456   
06/15/2012 -> 0.603296735707329    
06/16/2012 -> 0.9643939276988227   
06/17/2012 -> 0.3130360925369504   
06/18/2012 -> 0.04734334378620153  
06/19/2012 -> 0.1317854078230326   
06/20/2012 -> 0.9683122856510614   
06/21/2012 -> 0.4533290701062196   
06/22/2012 -> 0.40128653437613915  
06/23/2012 -> 0.47150231374428286  
06/24/2012 -> 0.050169316753532645 
06/25/2012 -> 0.5070869476588131   
06/26/2012 -> 0.7035485913619396   
06/27/2012 -> 0.9749216888300862   
06/28/2012 -> 0.5036393201962283   
06/29/2012 -> 0.6340304503927038   
06/30/2012 -> 0.9195732853461546
```
