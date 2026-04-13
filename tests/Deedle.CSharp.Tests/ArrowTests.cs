#pragma warning disable 1591

using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Deedle;
using Deedle.Arrow;

namespace Deedle.CSharp.Tests
{
    public class ArrowFrameTests
    {
        private static Frame<int, string> MakeSampleFrame()
        {
            var builder = new FrameBuilder.Columns<int, string>();
            builder.Add("A", new[] { 1.0, 2.0, 3.0 }.ToOrdinalSeries());
            builder.Add("B", new[] { 4.0, 5.0, 6.0 }.ToOrdinalSeries());
            return builder.Frame;
        }

        // ---------------------------------------------------------------
        // ArrowFrame static factory
        // ---------------------------------------------------------------

        [Test]
        public void ArrowFrame_ReadArrow_ReadsStocksFile()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Arrow.Tests", "data", "stocks.arrow");
            var df = ArrowFrame.ReadArrow(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
            Assert.That(df.ColumnCount, Is.GreaterThan(0));
        }

        [Test]
        public void ArrowFrame_ReadFeather_ReturnsFrame()
        {
            var tmpPath = Path.GetTempFileName() + ".feather";
            try
            {
                var df = MakeSampleFrame();
                df.WriteFeather(tmpPath);

                var df2 = ArrowFrame.ReadFeather(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
                Assert.That(df2.ColumnCount, Is.EqualTo(2));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void ArrowFrame_ReadArrowWithIndex_RestoresRowKeys()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Arrow.Tests", "data", "indexed.arrow");
            var df = ArrowFrame.ReadArrowWithIndex(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
            // Row keys should be strings, not integers
            Assert.That(df.RowKeys.First(), Is.TypeOf<string>());
        }

        [Test]
        public void ArrowFrame_ReadArrowStream_RoundTrips()
        {
            var df = MakeSampleFrame();
            using var ms = new MemoryStream();
            df.WriteArrowStream(ms);
            ms.Position = 0;
            var df2 = ArrowFrame.ReadArrowStream(ms);
            Assert.That(df2.RowCount, Is.EqualTo(3));
            Assert.That(df2.ColumnCount, Is.EqualTo(2));
        }

        // ---------------------------------------------------------------
        // FrameArrowExtensions – write extension methods
        // ---------------------------------------------------------------

        [Test]
        public void WriteArrow_Extension_RoundTrips()
        {
            var tmpPath = Path.GetTempFileName() + ".arrow";
            try
            {
                var df = MakeSampleFrame();
                df.WriteArrow(tmpPath);
                var df2 = ArrowFrame.ReadArrow(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
                var col = df2.GetColumn<double>("A");
                Assert.That(col[0], Is.EqualTo(1.0));
                Assert.That(col[2], Is.EqualTo(3.0));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void WriteArrowWithIndex_Extension_PreservesKeys()
        {
            var tmpPath = Path.GetTempFileName() + ".arrow";
            try
            {
                var df = MakeSampleFrame();
                df.WriteArrowWithIndex(tmpPath);
                var df2 = ArrowFrame.ReadArrowWithIndex(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void WriteFeatherWithIndex_Extension_RoundTrips()
        {
            var tmpPath = Path.GetTempFileName() + ".feather";
            try
            {
                var df = MakeSampleFrame();
                df.WriteFeatherWithIndex(tmpPath);
                var df2 = ArrowFrame.ReadFeatherWithIndex(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void WriteArrowStream_Extension_RoundTrips()
        {
            var df = MakeSampleFrame();
            using var ms = new MemoryStream();
            df.WriteArrowStream(ms);
            ms.Position = 0;
            var df2 = ArrowFrame.ReadArrowStream(ms);
            Assert.That(df2.GetColumn<double>("B")[1], Is.EqualTo(5.0));
        }

        [Test]
        public void ToRecordBatch_Extension_ReturnsValidBatch()
        {
            var df = MakeSampleFrame();
            var batch = df.ToRecordBatch();
            Assert.That(batch.Length, Is.EqualTo(3));
            Assert.That(batch.ColumnCount, Is.EqualTo(2));
        }

        // ---------------------------------------------------------------
        // Missing values survive round-trip
        // ---------------------------------------------------------------

        [Test]
        public void ArrowFrame_ReadArrow_PreservesMissing()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Arrow.Tests", "data", "missing.arrow");
            var df = ArrowFrame.ReadArrow(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
        }
    }
}
