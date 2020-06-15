using System;
using System.Linq;

using NUnit.Framework;
using Deedle;

namespace Deedle.CSharp.Tests
{
	/* ----------------------------------------------------------------------------------
	 * Test series builder
	 * --------------------------------------------------------------------------------*/
	class SeriesBuilderTests
	{
		[Test]
		public static void SeriesBuilderSupportsDynamic()
		{
			for (int i = 1; i <= 2; i++) // Run this twice, to check that instance references are right
			{
				dynamic sb = new SeriesBuilder<string>();
				sb.ID = 1;
				sb.Value = 3.4;
				sb.Date = DateTime.Today;

				Assert.AreEqual(sb.ID, 1);
				Assert.AreEqual(sb.Value, 3.4);
				Assert.AreEqual(sb.Date, DateTime.Today);

				SeriesBuilder<string> staticTypedSb = sb;
				var actual = staticTypedSb.Series;
				var expected =
					new SeriesBuilder<string, object> { { "ID", 1 }, { "Value", 3.4 }, { "Date", DateTime.Today } }.Series;
				Assert.AreEqual(expected, actual);
			}
		}
	}

	/* ----------------------------------------------------------------------------------
	 * Test aggregation
	 * --------------------------------------------------------------------------------*/
	class SeriesAggregationTests
	{
		[Test]
		public static void CanAggregateLettersUsingFloatingWindow()
		{
			var nums =
				(from n in Enumerable.Range(0, 10)
				 select KeyValue.Create(n, (char)('a' + n))).ToSeries();

			var actual =
				nums.Aggregate(Aggregation.WindowSize<int>(5, Boundary.Skip),
					(segment => segment.Data.Keys.First()),
					(segment => new OptionalValue<string>(new string(segment.Data.Values.ToArray()))));

			var expected =
				new SeriesBuilder<int, string> {
					{ 0, "abcde" },
					{ 1, "bcdef" },
					{ 2, "cdefg" },
					{ 3, "defgh" },
					{ 4, "efghi" },
					{ 5, "fghij" }}.Series;

			Assert.AreEqual(expected, actual);
		}

		[Test]
		public static void CanAggregateLettersUsingChunking()
		{
			var nums =
				(from n in Enumerable.Range(0, 10)
				 select KeyValue.Create(n, (char)('a' + n))).ToSeries();

			var actual =
				nums.Aggregate(Aggregation.ChunkSize<int>(5, Boundary.Skip),
					segment => segment.Data.Keys.First(),
					segment => new OptionalValue<string>(new string(segment.Data.Values.ToArray())));

			var expected =
				new SeriesBuilder<int, string> {
					{ 0, "abcde" },
					{ 5, "fghij" }}.Series;

			Assert.AreEqual(expected, actual);
		}

		[Test]
		public static void CanAggregateLettersUsingChunkWhile()
		{
			var nums =
				new SeriesBuilder<int, char>
					{ {0, 'a'}, {10, 'b'}, {11, 'c'} }.Series;

			var actual =
				nums.Aggregate(Aggregation.ChunkWhile<int>((k1, k2) => k2 - k1 < 10),
					segment => segment.Data.Keys.First(),
					segment => new OptionalValue<string>(new string(segment.Data.Values.ToArray())));

			var expected =
				new SeriesBuilder<int, string> {
					{ 0,  "a" },
					{ 10, "bc" }}.Series;

			Assert.AreEqual(expected, actual);
		}

        [Test]
        public static void FunctionsFromModuleStatsWorkFineInCSharp()
        {
            var s1 = new[] { 0.0, -1.0, 2.0, 3.0, -5.0, 4.0, 8.0 }.ToOrdinalSeries();

            var delta = 1e-9;

            var e1 = 20.0;
            var r1 = Stats.expandingMax(s1).Sum();

            Assert.AreEqual(e1, r1, delta);

            var e2 = -18.0;
            var r2 = Stats.expandingMin(s1).Sum();

            Assert.AreEqual(e2, r2, delta);

            var emin = -5.0;
            var emax = 8.0;

            var min = s1.Min();
            var max = s1.Max();

            Assert.AreEqual(emin, min);
            Assert.AreEqual(emax, max);

            var rsmx = Stats.movingMax(3, s1).Sum();
            var rsmn = Stats.movingMin(3, s1).Sum();

            Assert.AreEqual(e1, rsmx, delta);
            Assert.AreEqual(e2, rsmn, delta);

            var s2 = Enumerable.Range(1, 3).ToOrdinalSeries();

            Assert.AreEqual(6, s2.Sum());
            Assert.AreEqual(2, s2.Mean());
        }
	}
/*
  class Program
  {
    static void Main(string[] args)
    {
		var msft = Frame.ReadCsv(@"..\..\..\..\samples\data\msft.csv");

		var s = msft.GetColumn<double>("Open");

    IEnumerable<KeyValuePair<int, double>> kvps =
        Enumerable.Range(0, 10).Select(k => new KeyValuePair<int, double>(k, k * k));

		var series = kvps.ToSeries();
		foreach (var kvp in series.Observations)
			Console.WriteLine("{0} -> {1}", kvp.Key, kvp.Value);

		var s2 = series + series;

		Console.WriteLine(s2.Sum());

    var df = Frame.FromColumns(new[] { 1, 2, 3 }, new[] { new KeyValuePair<string, Series<int, double>>("Test", s2) });
    Console.WriteLine(((Deedle.Internal.IFsiFormattable)df).Format());


		// Aggregation.WindowSize(0, Boundary.AtBeginning)
    }
  }*/
}
