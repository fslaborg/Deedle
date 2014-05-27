using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Deedle;

namespace CSharp
{
  static class SeriesSamples
  {
    public static void Samples([CallerFilePath] string file = "")
    {
      var root = Path.GetDirectoryName(file);

      // ------------------------------------------------------------
      // Creating time series
      // ------------------------------------------------------------

      // [create-builder]
      var numNames = new SeriesBuilder<int, string>() {
        { 1, "one" }, { 2, "two" }, { 3, "three" } }.Series;
      numNames.Print();
      // [/create-builder]

      // [create-heterogen]
      // Create series builder and use it via 'dynamic'
      var nameNumsBuild = new SeriesBuilder<string, int>();
      dynamic nameNumsDyn = nameNumsBuild;
      nameNumsDyn.One = 1;
      nameNumsDyn.Two = 2;
      nameNumsDyn.Three = 3;

      // Build series and print it
      var nameNums = nameNumsBuild.Series;
      nameNums.Print();
      // [/create-heterogen]

      // [create-ordinal]
      var rnd = new Random();
      var randNums = Enumerable.Range(0, 100)
        .Select(_ => rnd.NextDouble()).ToOrdinalSeries();
      randNums.Print();
      // [/create-ordinal]

      // [create-kvp]
      var sin = 
        ( from i in Enumerable.Range(0, 1000)
          let x = i / 100.0
          select KeyValue.Create(x, Math.Sin(x)) ).ToSeries();
      sin.Print();
      // [/create-kvp]

      // [create-sparse]
      var opts =
        (from i in Enumerable.Range(0, 10)
         let v = OptionalValue.OfNullable(LookupEven(i))
         select KeyValue.Create(i, v)).ToSparseSeries();
      opts.Print();
      // [/create-sparse]

      // [create-csv]
      var frame = Frame.ReadCsv(Path.Combine(root, "../data/stocks/msft.csv"));
      var frameDate = frame.IndexRows<DateTime>("Date").SortRowsByKey();
      var msftOpen = frameDate.GetColumn<double>("Open");
      msftOpen.Print();
      // [/create-csv]

      // ------------------------------------------------------------
      // Lookup and slicing
      // ------------------------------------------------------------

      // [lookup-key]
      // Get value for a specified int and string key
      var tenth = randNums[10];
      var one = nameNums["One"];

      // Get first and last value using index
      var fst = nameNums.GetAt(0);
      var lst = nameNums.GetAt(nameNums.KeyCount - 1);
      // [/lookup-key]

      // [lookup-opt]
      // Get value as OptionalValue<T> and use it
      var opt = opts.TryGet(5);
      if (opt.HasValue) Console.Write(opt.Value);
      
      // For value types, we can convert to nullable type
      int? value1 = opts.TryGet(5).AsNullable();
      int? value2 = opts.TryGetAt(0).AsNullable();
      // [/lookup-opt]

      // [lookup-ord]
      // Get value exactly at the specified key
      var jan3 = msftOpen
        .Get(new DateTime(2012, 1, 3));

      // Get value at a key or for the nearest previous date
      var beforeJan1 = msftOpen
        .Get(new DateTime(2012, 1, 1), Lookup.ExactOrSmaller);

      // Get value at a key or for the nearest later date
      var afterJan1 = msftOpen
        .Get(new DateTime(2012, 1, 1), Lookup.ExactOrGreater);
      // [/lookup-ord]

      // [lookup-slice]
      // Get a series starting/ending at 
      // the specified date (inclusive)
      var msftStartIncl = msftOpen.StartAt(new DateTime(2012, 1, 1));
      var msftEndIncl = msftOpen.EndAt(new DateTime(2012, 12, 31));
      
      // Get a series starting/ending after/before 
      // the specified date (exclusive)
      var msftStartExcl = msftOpen.After(new DateTime(2012, 1, 1));
      var msftEndExcl = msftOpen.Before(new DateTime(2012, 12, 31));

      // Get prices for 2012 (both keys are inclusive)
      var msft2012 = msftOpen.Between
        (new DateTime(2012, 1, 1), new DateTime(2012, 12, 31));
      // [/lookup-slice]

      // ------------------------------------------------------------
      // Statistics and calculations
      // ------------------------------------------------------------

      // [calc-stat]
      // Calculate median & mean price
      var msftMed = msft2012.Median();
      var msftAvg = msft2012.Mean();
      
      // Calculate sum of square differences
      var msftDiff = msft2012 - msftAvg;
      var msftSq = (msftDiff * msftDiff).Sum();
      // [/calc-stat]

      // [calc-diff]
      // Subtract previous day value from current day
      var msftChange = msft2012 - msft2012.Shift(1);

			// Use built-in Diff method to do the same
			var msftChangeAlt = msft2012.Diff(1);
      
      // Get biggest loss and biggest gain
      var minMsChange = msftChange.Min();
      var maxMsChange = msftChange.Max();
      // [/calc-diff]

      // [calc-custom]
      var wackyStat = msft2012.Observations.Select(kvp =>
        kvp.Value / (kvp.Key - msft2012.FirstKey()).TotalDays).Sum();
      // [/calc-custom]
      
      // ------------------------------------------------------------
      // Missing data
      // ------------------------------------------------------------

      // [fill-const-drop]
      // Fill missing data with constant
      var fillConst = opts.FillMissing(-1);
      fillConst.Print();

      // Drop keys with no value from the series
      var drop = opts.DropMissing();
      drop.Print();
      // [/fill-const-drop]

      // [fill-dir]
      // Fill with previous available value
      var fillFwd = opts.FillMissing(Direction.Forward);
      fillFwd.Print();

      // Fill with the next available value
      var fillBwd = opts.FillMissing(Direction.Backward);
      fillBwd.Print();
      // [/fill-dir]

      // ------------------------------------------------------------
      // Windows and chunks, grouping
      // ------------------------------------------------------------

			// [aggreg-group]
			// Group random numbers by the first digit & get distribution
			var buckets = randNums
                .GroupBy(kvp => (int)(kvp.Value * 10))
                .Select(kvp => OptionalValue.Create(kvp.Value.KeyCount));
			buckets.Print();
			// [/aggreg-group]

			// [aggreg-win]
			// Average over 25 element floating window
			var monthlyWinMean = msft2012.WindowInto(25, win => win.Mean());

			// Get floating window over 5 elements as series of series
			// and then apply average on each series individually
			var weeklyWinMean = msft2012.Window(5).Select(kvp =>
				kvp.Value.Mean());
			// [/aggreg-win]

			// [aggreg-chunk]
			// Get chunks of size 25 and mean each (disjoint) chunk
			var monthlyChunkMean = msft2012.ChunkInto(25, win => win.Mean());

			// Get series containing individual chunks (as series)
			var weeklyChunkMean = msft2012.Chunk(5).Select(kvp =>
				kvp.Value.Mean());
			// [/aggreg-chunk]

			// [aggreg-pair]
			// For each key, get the previous value and average them
			var twoDayAvgs = msft2012.Pairwise().Select(kvp => 
				(kvp.Value.Item1 + kvp.Value.Item2) / 2.0);
			// [/aggreg-pair]

			// [aggreg-any]
			msft2012.Aggregate
				( // Get chunks while the month & year of the keys are the same
				  Aggregation.ChunkWhile<DateTime>((k1, k2) => 
						k1.Month == k2.Month && k2.Year == k1.Year ),
				  // For each chunk, return the first key as the key and
					// either average value or missing value if it was empty
					chunk => KeyValue.Create
					 ( chunk.Data.FirstKey(),
						 chunk.Data.ValueCount > 0 ?
							OptionalValue.Create(chunk.Data.Mean()) :
							OptionalValue.Empty<double>() ) );
			// [/aggreg-any]

			
      // ------------------------------------------------------------
      // Operations (Select, where)
      // ------------------------------------------------------------

      // [linq-methods]
      var overMean = msft2012
        .Select(kvp => kvp.Value - msftAvg)
        .Where(kvp => kvp.Value > 0.0).KeyCount;
      // [/linq-methods]

      // [linq-query]
      var underMean = 
        ( from kvp in msft2012
          where kvp.Value - msftAvg < 0.0
          select kvp ).KeyCount;
      // [/linq-query]

      Console.WriteLine(overMean);
      Console.WriteLine(underMean);

      // ------------------------------------------------------------
      // Indexing and sampling & resampling
      // ------------------------------------------------------------

      // [index-keys]
      // Turn DateTime keys into DateTimeOffset keys
      var byOffs = msft2012.SelectKeys(kvp => 
        new DateTimeOffset(kvp.Key));

      // Replace keys with ordinal numbers 0 .. KeyCount-1
      var byInt = msft2012.IndexOrdinally();
      // [/index-keys]

      // [index-with]
      // Replace keys with explictly specified new keys
      var byDays = numNames.IndexWith(new[] { 
        DateTime.Today,
        DateTime.Today.AddDays(1.0),
        DateTime.Today.AddDays(2.0) });
      // [/index-with]
    }

    private static int? LookupEven(int i)
    {
      return i % 2 == 1 ? null : (int?)i;
    }
  }
}
