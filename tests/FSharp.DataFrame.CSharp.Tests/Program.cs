using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using FSharp.DataFrame;

namespace FSharp.DataFrame.CSharp.Tests
{
	class Foo
	{
		//[Test]
		public static void Test2()
		{
			//var s = Series

			// TODO: can call Aggregate
		}
	}

  class Program
  {
    static void Main(string[] args)
    {
		var msft = Frame.ReadCsv(@"..\..\..\..\samples\data\msft.csv");

		var s = msft.GetSeries<double>("Open");

    IEnumerable<KeyValuePair<int, double>> kvps =
        Enumerable.Range(0, 10).Select(k => new KeyValuePair<int, double>(k, k * k));

		var series = kvps.ToSeries();
		foreach (var kvp in series.Observations)
			Console.WriteLine("{0} -> {1}", kvp.Key, kvp.Value);

		var s2 = series + series;

		Console.WriteLine(s2.Sum());

    var df = Frame.FromColumns(new[] { 1, 2, 3 }, new[] { new KeyValuePair<string, Series<int, double>>("Test", s2) });
    Console.WriteLine(((FSharp.DataFrame.Internal.IFsiFormattable)df).Format());


		// Aggregation.WindowSize(0, Boundary.AtBeginning)
    }
  }
}
