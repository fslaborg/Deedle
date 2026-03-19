#pragma warning disable 1591

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

				Assert.That(1, Is.EqualTo(sb.ID));
				Assert.That(3.4, Is.EqualTo(sb.Value));
				Assert.That(DateTime.Today, Is.EqualTo(sb.Date));

				SeriesBuilder<string> staticTypedSb = sb;
				var actual = staticTypedSb.Series;
				var expected =
					new SeriesBuilder<string, object> { { "ID", 1 }, { "Value", 3.4 }, { "Date", DateTime.Today } }.Series;
				Assert.That(actual, Is.EqualTo(expected));
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

			Assert.That(actual, Is.EqualTo(expected));
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

			Assert.That(actual, Is.EqualTo(expected));
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

			Assert.That(actual, Is.EqualTo(expected));
		}

        [Test]
        public static void FunctionsFromModuleStatsWorkFineInCSharp()
        {
            var s1 = new[] { 0.0, -1.0, 2.0, 3.0, -5.0, 4.0, 8.0 }.ToOrdinalSeries();

            var delta = 1e-9;

            var e1 = 20.0;
            var r1 = Stats.expandingMax(s1).Sum();

            Assert.That(r1, Is.EqualTo(e1).Within(delta));

            var e2 = -18.0;
            var r2 = Stats.expandingMin(s1).Sum();

            Assert.That(r2, Is.EqualTo(e2).Within(delta));

            var emin = -5.0;
            var emax = 8.0;

            var min = s1.Min();
            var max = s1.Max();

            Assert.That(min, Is.EqualTo(emin));
            Assert.That(max, Is.EqualTo(emax));

            var rsmx = Stats.movingMax(3, s1).Sum();
            var rsmn = Stats.movingMin(3, s1).Sum();

            Assert.That(rsmx, Is.EqualTo(e1).Within(delta));
            Assert.That(rsmn, Is.EqualTo(e2).Within(delta));

            var s2 = Enumerable.Range(1, 3).ToOrdinalSeries();

            Assert.That(s2.Sum(), Is.EqualTo(6));
            Assert.That(s2.Mean(), Is.EqualTo(2));
        }
	}

	/* ----------------------------------------------------------------------------------
	 * Test new windowing/chunking/pairwise extensions
	 * --------------------------------------------------------------------------------*/
	class SeriesWindowChunkPairwiseTests
	{
		[Test]
		public static void WindowWhileReturnsNestedSeries()
		{
			// WindowWhile creates a sliding window starting at each key
			var nums = new SeriesBuilder<int, char>
				{ { 0, 'a' }, { 1, 'b' }, { 5, 'c' }, { 6, 'd' } }.Series;

			var windows = nums.WindowWhile((k1, k2) => k2 - k1 < 3);

			// One window per input key (sliding)
			Assert.That(windows.KeyCount, Is.EqualTo(4));
			// Window at key 0 spans keys 0,1 (diff=1 < 3); key 5 is out (diff=5 >= 3)
			Assert.That(new string(windows[0].Values.ToArray()), Is.EqualTo("ab"));
			// Window at key 5 spans keys 5,6 (diff=1 < 3)
			Assert.That(new string(windows[5].Values.ToArray()), Is.EqualTo("cd"));
		}

		[Test]
		public static void WindowWhileIntoAggregatesWindows()
		{
			var nums = new SeriesBuilder<int, char>
				{ { 0, 'a' }, { 1, 'b' }, { 5, 'c' }, { 6, 'd' } }.Series;

			var result = nums.WindowWhileInto(
				(k1, k2) => k2 - k1 < 3,
				s => new string(s.Values.ToArray()));

			Assert.That(result.KeyCount, Is.EqualTo(4));
			Assert.That(result[0], Is.EqualTo("ab"));
			Assert.That(result[5], Is.EqualTo("cd"));
		}

		[Test]
		public static void ChunkWhileReturnsNestedSeries()
		{
			var nums = new SeriesBuilder<int, char>
				{ { 0, 'a' }, { 10, 'b' }, { 11, 'c' } }.Series;

			var chunks = nums.ChunkWhile((k1, k2) => k2 - k1 < 10);

			Assert.That(chunks.KeyCount, Is.EqualTo(2));
			Assert.That(new string(chunks[0].Values.ToArray()), Is.EqualTo("a"));
			Assert.That(new string(chunks[10].Values.ToArray()), Is.EqualTo("bc"));
		}

		[Test]
		public static void ChunkWhileIntoAggregatesChunks()
		{
			var nums = new SeriesBuilder<int, char>
				{ { 0, 'a' }, { 10, 'b' }, { 11, 'c' } }.Series;

			var result = nums.ChunkWhileInto(
				(k1, k2) => k2 - k1 < 10,
				s => new string(s.Values.ToArray()));

			Assert.That(result[0], Is.EqualTo("a"));
			Assert.That(result[10], Is.EqualTo("bc"));
		}

		[Test]
		public static void PairwiseWithCombinesConsecutiveValues()
		{
			var nums = new SeriesBuilder<int, int>
				{ { 0, 10 }, { 1, 20 }, { 2, 30 } }.Series;

			var result = nums.PairwiseWith((k, v1, v2) => v2 - v1);

			Assert.That(result.KeyCount, Is.EqualTo(2));
			Assert.That(result[1], Is.EqualTo(10));
			Assert.That(result[2], Is.EqualTo(10));
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
