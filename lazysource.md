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
01/01/2010 -> 0.22784560031468237 
01/02/2010 -> 0.9874313679385174  
01/03/2010 -> 0.3908313137955711  
01/04/2010 -> 0.6066390403999894  
01/05/2010 -> 0.8544123608106885  
01/06/2010 -> 0.5488816040682833  
01/07/2010 -> 0.8223532770420217  
01/08/2010 -> 0.3988896659532495  
01/09/2010 -> 0.9695685117181507  
01/10/2010 -> 0.43430836337627954 
01/11/2010 -> 0.3725799630851786  
01/12/2010 -> 0.875744113397311   
01/13/2010 -> 0.7035856958975981  
01/14/2010 -> 0.2664543396532134  
01/15/2010 -> 0.9327115290878256  
:             ...                 
12/18/2012 -> 0.5778447008280899  
12/19/2012 -> 0.741691379628421   
12/20/2012 -> 0.6127764359744915  
12/21/2012 -> 0.8825653947533421  
12/22/2012 -> 0.0541334526941889  
12/23/2012 -> 0.4817707174544871  
12/24/2012 -> 0.6742782441373346  
12/25/2012 -> 0.3527099004442995  
12/26/2012 -> 0.03771783381075533 
12/27/2012 -> 0.7592866535975478  
12/28/2012 -> 0.95143082175503    
12/29/2012 -> 0.25406719209819506 
12/30/2012 -> 0.5556196487734064  
12/31/2012 -> 0.35456055784404406 
01/01/2013 -> 0.186598738734429   

val slicedDf: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.5385542115189207   
06/02/2012 -> 0.3802249529154491   
06/03/2012 -> 0.2129793691214844   
06/04/2012 -> 0.6103354905363877   
06/05/2012 -> 0.2659810182106521   
06/06/2012 -> 0.002404050324344764 
06/07/2012 -> 0.7028604286070843   
06/08/2012 -> 0.7007195005030815   
06/09/2012 -> 0.06988861635484966  
06/10/2012 -> 0.6925564284734762   
06/11/2012 -> 0.6762692344237756   
06/12/2012 -> 0.14712415282881997  
06/13/2012 -> 0.6789616676744826   
06/14/2012 -> 0.6369375253552491   
06/15/2012 -> 0.8332431959730481   
06/16/2012 -> 0.6801016195005768   
06/17/2012 -> 0.7602910053798646   
06/18/2012 -> 0.5624037559154258   
06/19/2012 -> 0.5761670220969838   
06/20/2012 -> 0.877019893705412    
06/21/2012 -> 0.24363790594169787  
06/22/2012 -> 0.06507358752733705  
06/23/2012 -> 0.6908677234215124   
06/24/2012 -> 0.4922583741358667   
06/25/2012 -> 0.4547833422827222   
06/26/2012 -> 0.422857804275068    
06/27/2012 -> 0.9254311368408266   
06/28/2012 -> 0.6840761291338365   
06/29/2012 -> 0.5697611211597947   
06/30/2012 -> 0.28291758507145315  

val it: Frame<DateTime,string> =
  
              Values               
06/01/2012 -> 0.5385542115189207   
06/02/2012 -> 0.3802249529154491   
06/03/2012 -> 0.2129793691214844   
06/04/2012 -> 0.6103354905363877   
06/05/2012 -> 0.2659810182106521   
06/06/2012 -> 0.002404050324344764 
06/07/2012 -> 0.7028604286070843   
06/08/2012 -> 0.7007195005030815   
06/09/2012 -> 0.06988861635484966  
06/10/2012 -> 0.6925564284734762   
06/11/2012 -> 0.6762692344237756   
06/12/2012 -> 0.14712415282881997  
06/13/2012 -> 0.6789616676744826   
06/14/2012 -> 0.6369375253552491   
06/15/2012 -> 0.8332431959730481   
06/16/2012 -> 0.6801016195005768   
06/17/2012 -> 0.7602910053798646   
06/18/2012 -> 0.5624037559154258   
06/19/2012 -> 0.5761670220969838   
06/20/2012 -> 0.877019893705412    
06/21/2012 -> 0.24363790594169787  
06/22/2012 -> 0.06507358752733705  
06/23/2012 -> 0.6908677234215124   
06/24/2012 -> 0.4922583741358667   
06/25/2012 -> 0.4547833422827222   
06/26/2012 -> 0.422857804275068    
06/27/2012 -> 0.9254311368408266   
06/28/2012 -> 0.6840761291338365   
06/29/2012 -> 0.5697611211597947   
06/30/2012 -> 0.28291758507145315
```
