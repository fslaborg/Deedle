using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using Deedle;
using System.Runtime.CompilerServices;
using System.IO;

namespace Deedle.CSharp.Tests
{
	/* ----------------------------------------------------------------------------------
	 * Test calling Frame.Zip from C#
	 * --------------------------------------------------------------------------------*/
	public class FrameZipTests
	{
		public static Frame<int, string> LoadMSFT([CallerFilePath] string source = "")
		{
			var file = Path.Combine(Path.GetDirectoryName(source), ".." + Path.DirectorySeparatorChar + "Deedle.Tests" + Path.DirectorySeparatorChar + "data" + Path.DirectorySeparatorChar + "MSFT.csv");
			return Frame.ReadCsv(file, inferRows:10);
		}

		[Test]
		public static void CanSubtractNumericalValues()
		{
			var df = LoadMSFT();
			var actual = df.Zip<float, float, float>(df, (n1, n2) => n1 - n2).GetAllValues<float>().Sum();
			Assert.AreEqual(0.0, actual);
		}
	}

	/* ----------------------------------------------------------------------------------
	 * Test data frame dynamic 
	 * --------------------------------------------------------------------------------*/
	public class DynamicFrameTests
	{
		[Test]
		public static void CanAddSeriesDynamically()
		{
			for (int i = 1; i <= 2; i++) // Run this twice, to check that instance references are right
			{
				var df =
					new FrameBuilder.Columns<int, string> {
					{ "Test", new SeriesBuilder<int> { {1, 11.1}, {2,22.4} }.Series }
				}.Frame;
				dynamic dfd = df;
				dfd.Test1 = new[] { 1, 2 };
				dfd.Test2 = df.GetSeries<double>("Test");
				dfd.Test3 = new Dictionary<int, string> { { 1, "A" }, { 2, "B" } };
				var row = df.Rows[2];

				Assert.AreEqual(22.4, row["Test"]);
				Assert.AreEqual(2, row["Test1"]);
				Assert.AreEqual(22.4, row["Test2"]);
				Assert.AreEqual("B", ((KeyValuePair<int, string>)row["Test3"]).Value);
			}
		}

		[Test]
		public static void CanGetSeriesDynamically()
		{
			for (int i = 1; i <= 2; i++) // Run this twice, to check that instance references are right
			{
				var df =
					new FrameBuilder.Columns<int, string> {
					{ "Test", new SeriesBuilder<int> { {1, 11.1}, {2,22.2} }.Series }
				}.Frame;
				dynamic dfd = df;

				Series<int, double> s0 = dfd.Test;
				dynamic s1 = dfd.Test;

				Assert.AreEqual(11.1, s0[1]);
				Assert.AreEqual(11.1, s1[1]);
			}
		}
	}
}
