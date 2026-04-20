#pragma warning disable 1591

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Deedle;
using Deedle.ExcelReader;

namespace Deedle.CSharp.Tests
{
    public class ExcelReaderTests
    {
        private static string TestFile([CallerFilePath] string source = "")
        {
            return Path.Combine(Path.GetDirectoryName(source),
                "..", "Deedle.ExcelReader.Tests", "data", "test.xlsx");
        }

        private static string MissingFile([CallerFilePath] string source = "")
        {
            return Path.Combine(Path.GetDirectoryName(source),
                "..", "Deedle.ExcelReader.Tests", "data", "missing.xlsx");
        }

        // ---------------------------------------------------------------
        // ExcelFrame static factory
        // ---------------------------------------------------------------

        [Test]
        public void ExcelFrame_ReadExcel_ReadsFirstSheet()
        {
            var df = ExcelFrame.ReadExcel(TestFile());
            Assert.That(df.RowCount, Is.GreaterThan(0));
            Assert.That(df.ColumnCount, Is.GreaterThan(0));
        }

        [Test]
        public void ExcelFrame_ReadExcel_ColumnKeysAreStrings()
        {
            var df = ExcelFrame.ReadExcel(TestFile());
            Assert.That(df.ColumnKeys.First(), Is.TypeOf<string>());
        }

        [Test]
        public void ExcelFrame_ReadExcelSheet_ReadsByName()
        {
            var names = ExcelFrame.SheetNames(TestFile());
            Assert.That(names.Length, Is.GreaterThanOrEqualTo(2));

            var df = ExcelFrame.ReadExcelSheet(TestFile(), names.Tail.Head);
            Assert.That(df.RowCount, Is.GreaterThan(0));
        }

        [Test]
        public void ExcelFrame_ReadExcelSheetByIndex_ReadsByIndex()
        {
            var df = ExcelFrame.ReadExcelSheetByIndex(TestFile(), 0);
            Assert.That(df.RowCount, Is.GreaterThan(0));
        }

        [Test]
        public void ExcelFrame_ReadExcelSheetByIndex_ThrowsForBadIndex()
        {
            Assert.Throws<Exception>(() => ExcelFrame.ReadExcelSheetByIndex(TestFile(), 99));
        }

        [Test]
        public void ExcelFrame_ReadExcelSheet_ThrowsForBadName()
        {
            Assert.Throws<Exception>(() => ExcelFrame.ReadExcelSheet(TestFile(), "DoesNotExist"));
        }

        [Test]
        public void ExcelFrame_SheetNames_ReturnsList()
        {
            var names = ExcelFrame.SheetNames(TestFile());
            Assert.That(names.Length, Is.GreaterThanOrEqualTo(1));
        }

        [Test]
        public void ExcelFrame_ReadExcel_HandlesFilesWithMissing()
        {
            var df = ExcelFrame.ReadExcel(MissingFile());
            Assert.That(df.RowCount, Is.GreaterThan(0));
        }
    }
}
