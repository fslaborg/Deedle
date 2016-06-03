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
			var file = Path.Combine(Path.GetDirectoryName(source), "..", "Deedle.Tests", "data", "MSFT.csv");
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
   * Creating frames and getting frame data
   * --------------------------------------------------------------------------------*/

  public class PublicFields
  {
    public readonly int A;
    public readonly string B;
    public PublicFields(int a, string b)
    {
      this.A = a; 
      this.B = b;
    }
  }

  public class FrameCreateAccessTests
  {
    [Test]
    public static void CanCreateFrameFromRecords()
    {
      var df = Frame.FromRecords(new[] {
        new { A = 1, B = "Test" },
        new { A = 2, B = "Another"}
      });
      var firstRow = df.Rows[0].Values.ToArray();
      Assert.AreEqual(new object[] { 1, "Test" }, firstRow);
      Assert.AreEqual(new[] { "A", "B" }, df.ColumnKeys.ToArray());
    }

    [Test]
    public static void CanCreateFrameFromFields()
    {
      var df = Frame.FromRecords(new[] {
        new PublicFields(1, "Test"),
        new PublicFields(2, "Another")
      });
      var firstRow = df.Rows[0].Values.ToArray();
      Assert.AreEqual(new object[] { 1, "Test" }, firstRow);
      Assert.AreEqual(new[] { "A", "B" }, df.ColumnKeys.ToArray());
    }

    [Test]
    public static void CanRoundtripWithArray2D()
    {
      var arr = new double[200, 500];
      for (var r = 0; r < 200; r++)
        for (var c = 0; c < 500; c++)
          arr[r, c] = (r + c == 10) ? Double.NaN : (r + c);

      var arr2 = Frame.FromArray2D(arr).ToArray2D<double>();
      Assert.AreEqual(arr, arr2);
    }

    [Test]
    public static void CannotGetDefaultValueOfBoolean()
    {
      var df = Frame.FromColumns(new[] {
        KeyValue.Create(1, (new SeriesBuilder<string>() { { "A", true } }).Series),
        KeyValue.Create(2, (new SeriesBuilder<string>() { { "B", true } }).Series)
      });
      Assert.Throws<InvalidOperationException>(() =>
        df.ToArray2D<bool>());
    }

    [Test]
    public static void CanGetDataAsBooleanWithDefault()
    {
      var df = Frame.FromColumns(new[] {
        KeyValue.Create(1, (new SeriesBuilder<string>() { { "A", true } }).Series),
        KeyValue.Create(2, (new SeriesBuilder<string>() { { "B", true } }).Series)
      });
      var data = df.ToArray2D<bool>(true);
      var bools = data.OfType<bool>().ToArray();
      Assert.AreEqual(new[] { true, true, true, true }, bools);
    }

    [Test]
    public static void FramesOrderedAfterBuild()
    {
      var builder = new FrameBuilder.Columns<int, string>();
      builder.Add("column1", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
      builder.Add("column2", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
      builder.Add("column3", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
      builder.Add("column4", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
      var frame = builder.Frame;

      Assert.AreEqual(
        new[] { "column1", "column2", "column3", "column4" },
        frame.ColumnKeys.ToArray()
        );
    }

    [Test]
    public static void DropSparseRowsFromRecords()
    {
      var f = Deedle.Frame.FromRecords(new[] {
          new { a = "x", b = 1.0 },
          new { a = "y", b = Double.NaN },
          new { a = "z", b = 3.0 }
      });
      var b = Deedle.FrameModule.DropSparseRows(f).GetColumn<float>("b").GetAllValues().ToArray();
      Assert.AreEqual(1.0, b[0].Value);
      Assert.AreEqual(3.0, b[1].Value);
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
				dfd.Test2 = df.GetColumn<double>("Test");
				dfd.Test3 = new Dictionary<int, string> { { 1, "A" }, { 2, "B" } };
				var row = df.Rows[2];

				Assert.AreEqual(22.4, row["Test"]);
				Assert.AreEqual(2, row["Test1"]);
				Assert.AreEqual(22.4, row["Test2"]);
				Assert.AreEqual("B", ((KeyValuePair<int, string>)row["Test3"]).Value);
			}
		}

		[Test]
		public static void CanGetColumnDynamically()
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
