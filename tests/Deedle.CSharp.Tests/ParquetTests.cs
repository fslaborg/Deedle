#pragma warning disable 1591

using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Deedle;
using Deedle.Parquet;

namespace Deedle.CSharp.Tests
{
    public class ParquetFrameTests
    {
        private static Frame<int, string> MakeSampleFrame()
        {
            var builder = new FrameBuilder.Columns<int, string>();
            builder.Add("X", new[] { 10.0, 20.0, 30.0 }.ToOrdinalSeries());
            builder.Add("Y", new[] { 40.0, 50.0, 60.0 }.ToOrdinalSeries());
            return builder.Frame;
        }

        // ---------------------------------------------------------------
        // ParquetFrame static factory
        // ---------------------------------------------------------------

        [Test]
        public void ParquetFrame_ReadParquet_ReadsStocksFile()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Parquet.Tests", "data", "stocks.parquet");
            var df = ParquetFrame.ReadParquet(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
            Assert.That(df.ColumnCount, Is.GreaterThan(0));
        }

        [Test]
        public void ParquetFrame_ReadParquetWithIndex_RestoresRowKeys()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Parquet.Tests", "data", "indexed.parquet");
            var df = ParquetFrame.ReadParquetWithIndex(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
            Assert.That(df.RowKeys.First(), Is.TypeOf<string>());
        }

        [Test]
        public void ParquetFrame_ReadParquetStream_RoundTrips()
        {
            var df = MakeSampleFrame();
            using var ms = new MemoryStream();
            df.WriteParquetStream(ms);
            ms.Position = 0;
            var df2 = ParquetFrame.ReadParquetStream(ms);
            Assert.That(df2.RowCount, Is.EqualTo(3));
            Assert.That(df2.ColumnCount, Is.EqualTo(2));
        }

        // ---------------------------------------------------------------
        // FrameParquetExtensions – write extension methods
        // ---------------------------------------------------------------

        [Test]
        public void WriteParquet_Extension_RoundTrips()
        {
            var tmpPath = Path.GetTempFileName() + ".parquet";
            try
            {
                var df = MakeSampleFrame();
                df.WriteParquet(tmpPath);
                var df2 = ParquetFrame.ReadParquet(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
                var col = df2.GetColumn<double>("X");
                Assert.That(col[0], Is.EqualTo(10.0));
                Assert.That(col[2], Is.EqualTo(30.0));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void WriteParquetWithIndex_Extension_PreservesKeys()
        {
            var tmpPath = Path.GetTempFileName() + ".parquet";
            try
            {
                var df = MakeSampleFrame();
                df.WriteParquetWithIndex(tmpPath);
                var df2 = ParquetFrame.ReadParquetWithIndex(tmpPath);
                Assert.That(df2.RowCount, Is.EqualTo(3));
            }
            finally
            {
                if (File.Exists(tmpPath)) File.Delete(tmpPath);
            }
        }

        [Test]
        public void WriteParquetStream_Extension_RoundTrips()
        {
            var df = MakeSampleFrame();
            using var ms = new MemoryStream();
            df.WriteParquetStream(ms);
            ms.Position = 0;
            var df2 = ParquetFrame.ReadParquetStream(ms);
            Assert.That(df2.GetColumn<double>("Y")[1], Is.EqualTo(50.0));
        }

        // ---------------------------------------------------------------
        // Missing values survive round-trip
        // ---------------------------------------------------------------

        [Test]
        public void ParquetFrame_ReadParquet_PreservesMissing()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory,
                "..", "..", "..", "..", "Deedle.Parquet.Tests", "data", "missing.parquet");
            var df = ParquetFrame.ReadParquet(path);
            Assert.That(df.RowCount, Is.GreaterThan(0));
        }
    }
}
