using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using FSharp.DataFrame;
using System.Runtime.CompilerServices;
using System.IO;

namespace FSharp.DataFrame.CSharp.Tests
{
	/* ----------------------------------------------------------------------------------
	 * Test calling Frame.Zip from C#
	 * --------------------------------------------------------------------------------*/
	class FrameZipTests
	{
		public static Frame<int, string> LoadMSFT([CallerFilePath] string source = "")
		{
			var file = Path.Combine(Path.GetDirectoryName(source), @"..\FSharp.DataFrame.Tests\data\MSFT.csv");
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
	class DynamicFrameTests
	{
		[Test, Ignore]
		public static void CanAddSeriesDynamically()
		{
			var df =
				new FrameBuilder.Columns<int, string> {
					{ "Test", new SeriesBuilder<int> { {1, 11.1}, {2,22.2} }.Series }
				}.Frame;
			dynamic dfd = df;
			dfd.Test2 = df.GetSeries<float>("Test");
		}

		[Test, Ignore]
		public static void CanGetSeriesDynamically()
		{
			var df =
				new FrameBuilder.Columns<int, string> {
					{ "Test", new SeriesBuilder<int> { {1, 11.1}, {2,22.2} }.Series }
				}.Frame;
			dynamic dfd = df;
			Series<int, double> s = dfd.Test;
		}
	}
}
