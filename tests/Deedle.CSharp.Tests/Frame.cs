#pragma warning disable 1591

using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Deedle;
using System.Runtime.CompilerServices;
using System.IO;

namespace Deedle.CSharp.Tests
{
    public class Common
    {
        public static Frame<int, string> LoadMSFT([CallerFilePath] string source = "")
        {
            var file = Path.Combine(Path.GetDirectoryName(source), "..", "Deedle.Tests", "data", "MSFT.csv");
            return Frame.ReadCsv(file, inferRows: 10);
        }

        public static Frame<int, string> LoadMSFTStream([CallerFilePath] string source = "")
        {
            var file = Path.Combine(Path.GetDirectoryName(source), "..", "Deedle.Tests", "data", "MSFT.csv");
            var sr = new StreamReader(file);
            var csv = Frame.ReadCsv(sr.BaseStream);
            sr.Close();
            return csv;
        }
    }
	/* ----------------------------------------------------------------------------------
	 * Test calling Frame.Zip from C#
	 * --------------------------------------------------------------------------------*/
	public class FrameZipTests
	{
        [Test]
		public static void CanSubtractNumericalValues()
		{
			var df = Common.LoadMSFT();
			var actual = df.Zip<float, float, float>(df, (n1, n2) => n1 - n2).GetAllValues<float>().Sum();
			Assert.That(actual, Is.EqualTo(0.0));
		}

        [Test]
        public static void CanSubtractNumericalValuesStream()
        {
            var df = Common.LoadMSFTStream();
            var actual = df.Zip<float, float, float>(df, (n1, n2) => n1 - n2).GetAllValues<float>().Sum();
            Assert.That(actual, Is.EqualTo(0.0));
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
            Assert.That(firstRow, Is.EqualTo(new object[] { 1, "Test" }));
            Assert.That(df.ColumnKeys.ToArray(), Is.EqualTo(new[] { "A", "B" }));
        }

        [Test]
        public static void CanCreateFrameFromFields()
        {
            var df = Frame.FromRecords(new[] {
            new PublicFields(1, "Test"),
            new PublicFields(2, "Another")
            });
            var firstRow = df.Rows[0].Values.ToArray();
            Assert.That(firstRow, Is.EqualTo(new object[] { 1, "Test" }));
            Assert.That(df.ColumnKeys.ToArray(), Is.EqualTo(new[] { "A", "B" }));
        }

        [Test]
        public static void CanRoundtripWithArray2D()
        {
            var arr = new double[200, 500];
            for (var r = 0; r < 200; r++)
            for (var c = 0; c < 500; c++)
                arr[r, c] = (r + c == 10) ? Double.NaN : (r + c);

            var arr2 = Frame.FromArray2D(arr).ToArray2D<double>();
            Assert.That(arr2, Is.EqualTo(arr));
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
            Assert.That(bools, Is.EqualTo(new[] { true, true, true, true }));
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

            Assert.That(frame.ColumnKeys.ToArray(), Is.EqualTo(new[] { "column1", "column2", "column3", "column4" }));
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
            Assert.That(b[0].Value, Is.EqualTo(1.0));
            Assert.That(b[1].Value, Is.EqualTo(3.0));
        }

        [Test]
        public static void CanSliceRows()
        {
            var df = Common.LoadMSFT();
            var actual = df.Rows[new int[] { 0, 1, 2, 3, 4 }];
            Assert.That(actual.RowCount, Is.EqualTo(5));
        }

        [Test]
        public static void CanSliceCols()
        {
            var df = Common.LoadMSFT();
            var actual = df.Columns[new string[] { "Open", "High" }];
            Assert.That(actual.ColumnCount, Is.EqualTo(2));
        }
    }

    /* ----------------------------------------------------------------------------------
    * Stats functions on frame
    * --------------------------------------------------------------------------------*/
    public class FrameStatsTests
    {
        [Test]
        public static void SumOfFrame()
        {
            var builder = new FrameBuilder.Columns<int, string>();
            builder.Add("column1", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
            builder.Add("column2", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
            builder.Add("column3", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
            builder.Add("column4", new[] { 1, 2, 3, 4 }.ToOrdinalSeries());
            var df = builder.Frame;
            var sum = df.Sum().GetAllValues().ToArray();
            Assert.That(sum[0].Value, Is.EqualTo(10.0));
            Assert.That(sum[1].Value, Is.EqualTo(10.0));
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

				Assert.That(row["Test"], Is.EqualTo(22.4));
				Assert.That(row["Test1"], Is.EqualTo(2));
				Assert.That(row["Test2"], Is.EqualTo(22.4));
				Assert.That(((KeyValuePair<int, string>)row["Test3"]).Value, Is.EqualTo("B"));
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

				Assert.That(s0[1], Is.EqualTo(11.1));
				Assert.That(s1[1], Is.EqualTo(11.1));
			}
		}
    }

    /* ----------------------------------------------------------------------------------
    * Tests for GetRowsAs with C# interfaces (issue #503)
    * --------------------------------------------------------------------------------*/

    public interface INamedRow
    {
        string Name { get; }
        double Value { get; }
    }

    public interface IRowWithSetter
    {
        string Name { get; set; }
        double Value { get; }
    }

    public class GetRowsAsTests
    {
        [Test]
        public static void CanGetRowsAsReadOnlyCSharpInterface()
        {
            var df = Frame.FromRecords(new[] {
                new { Name = "Alice", Value = 1.0 },
                new { Name = "Bob",   Value = 2.0 }
            });
            var rows = df.GetRowsAs<INamedRow>();
            Assert.That(rows[0].Name, Is.EqualTo("Alice"));
            Assert.That(rows[1].Value, Is.EqualTo(2.0));
        }

        [Test]
        public static void CanGetRowsAsCSharpInterfaceWithSetter()
        {
            // Only the getter properties should be used as columns;
            // the setter stub should not be accessible via this path.
            var df = Frame.FromRecords(new[] {
                new { Name = "Alice", Value = 1.0 },
                new { Name = "Bob",   Value = 2.0 }
            });
            var rows = df.GetRowsAs<IRowWithSetter>();
            Assert.That(rows[0].Name, Is.EqualTo("Alice"));
            Assert.That(rows[0].Value, Is.EqualTo(1.0));
        }

        [Test]
        public static void GetRowsAsThrowsForNonInterfaceType()
        {
            var df = Frame.FromRecords(new[] { new { Name = "Alice", Value = 1.0 } });
            Assert.Throws<InvalidOperationException>(() => df.GetRowsAs<string>());
        }
    }

    /* ----------------------------------------------------------------------------------
     * Test MapRows and MapCols extensions
     * --------------------------------------------------------------------------------*/
    public class FrameMapTests
    {
        [Test]
        public static void MapRowsExtractsValuePerRow()
        {
            var df = Frame.FromRecords(new[] {
                new { Name = "Alice", Value = 1.0 },
                new { Name = "Bob",   Value = 2.0 }
            });

            var names = df.MapRows((k, row) => row.GetAs<string>("Name"));

            Assert.That(names[0], Is.EqualTo("Alice"));
            Assert.That(names[1], Is.EqualTo("Bob"));
        }

        [Test]
        public static void MapColsTransformsEachColumn()
        {
            var df = Frame.FromColumns(new Dictionary<string, Series<int, double>> {
                { "A", new SeriesBuilder<int, double> { { 0, 1.0 }, { 1, 2.0 } }.Series },
                { "B", new SeriesBuilder<int, double> { { 0, 3.0 }, { 1, 4.0 } }.Series }
            });

            // Return the same column unchanged - verifies MapCols maps all columns
            var sameFrame = df.MapCols((colKey, col) => col);

            Assert.That(sameFrame.ColumnKeys.ToArray(), Is.EqualTo(new[] { "A", "B" }));
            Assert.That(sameFrame["A"][0], Is.EqualTo(1.0));
            Assert.That(sameFrame["B"][1], Is.EqualTo(4.0));
        }
    }
}
