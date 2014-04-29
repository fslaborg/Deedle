using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
