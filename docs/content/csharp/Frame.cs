using Deedle;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace CSharp
{
	class FrameSamples
	{
		public static void Samples([CallerFilePath] string file = "")
		{
			var root = Path.GetDirectoryName(file);

			// ------------------------------------------------------------
			// Creating and loading data frames
			// ------------------------------------------------------------

			// [create-records]
			// Create a collection of anonymous types
			var objects = Enumerable.Range(0, 100).Select(i =>
				new { Group = "g" + (i/10).ToString(), Number = i });

			// Create data frame with properties as column names
			var dfObjects = Frame.FromRecords(objects);
			dfObjects.Print();
			// [/create-records]

			// [create-rows]
			// Generate collection of rows
			var rows = Enumerable.Range(0, 100).Select(i => {
				// Build each row using series builder & return 
				// KeyValue representing row key with row data
				var sb = new SeriesBuilder<string>();
				sb.Add("Index", i);
				sb.Add("Sin", Math.Sin(i / 100.0));
				sb.Add("Cos", Math.Cos(i / 100.0));
				return KeyValue.Create(i, sb.Series); 
			});

			// Turn sequence of row information into data frame
			var df = Frame.FromRows(rows);
			// [/create-rows]

			// [create-csv]
			// Read MSFT & FB stock prices from a CSV file
			var msftRaw = Frame.ReadCsv(Path.Combine(root, "../data/stocks/msft.csv"));
			var fbRaw = Frame.ReadCsv(Path.Combine(root, "../data/stocks/fb.csv"));
			// [/create-csv]

			// ------------------------------------------------------------
			// Working with row and column indices
			// ------------------------------------------------------------

			// [index-date]
			// Get MSFT & FB stock prices indexed by data
			var msft = msftRaw.IndexRows<DateTime>("Date").OrderRows();
			var fb = fbRaw.IndexRows<DateTime>("Date").OrderRows();
			
			// And rename columns to avoid overlap
			msft.RenameSeries(s => "Msft" + s);
			fb.RenameSeries(s => "Fb" + s);
			// [/index-date]

			// [index-cols]
			// Read US debt data from a CSV file
			var debt = Frame.ReadCsv(Path.Combine(root, "../data/us-debt.csv"));
			// Index by Year column and
			var debtByYear = debt
				.IndexRows<int>("Year")
				.IndexColumnsWith(new[] { "Year", "GDP", "Population", "Debt", "?" });
			// [/index-cols]

			// ------------------------------------------------------------
			// Joining and aligning data frames
			// ------------------------------------------------------------

			// [join-inout]
			// Inner join (take intersection of dates)
			var joinIn = msft.Join(fb, JoinKind.Inner);
			// Outer join (take union & fill with missing)
			var joinOut = msft.Join(fb, JoinKind.Outer);
			// [/join-inout]

			// [join-lookup]
			// Shift MSFT observations by +1 hour for testing
			var msftShift = msft.SelectRowKeys(k => k.Key.AddHours(1.0));

			// MSFT data are missing because keys do not match
			var joinLeftWrong = fb.Join(msftShift, JoinKind.Left);

			// This works! Find the value for the nearest smaller key
			// (that is, for the nearest earlier time with value)
			var joinLeft = fb.Join(msftShift, JoinKind.Left, Lookup.NearestSmaller);
			joinLeft.Print();
			// [/join-lookup]

			// ------------------------------------------------------------
			// Accessing data and series operations
			// ------------------------------------------------------------

			// [series-get]
			// Get MSFT and FB opening prices and calculate the difference
			var msOpen = joinIn.GetSeries<double>("MsftOpen");
			var msClose = joinIn.GetSeries<double>("MsftClose");
			var msDiff = msClose - msOpen;
			// [/series-get]

			// [series-dropadd]
			// Drop series from a data frame
			joinIn.DropSeries("MsftAdj Close");
			joinIn.DropSeries("FbAdj Close");

			// Add new series to a frame
			joinIn.AddSeries("MsftDiff", msDiff);
			joinIn.Print();
			// [/series-dropadd]

			// [series-rows]
			// Get row and then look at row properties
			var row = joinIn.Rows[new DateTime(2013, 1, 4)];
			var msLo = row.GetAs<double>("MsftLow");
			var msHi = row.GetAs<double>("MsftHigh");

			// Get value for a specified column & row keys
			var diff = joinIn["MsftDiff", new DateTime(2013, 1, 4)];
			// [/series-rows]

			// ------------------------------------------------------------
			// LINQ to data frame
			// ------------------------------------------------------------

			// [linq-select]
			// Project rows into a new series using the Select method
			var diffs = joinIn.Rows.Select(kvp =>
				kvp.Value.GetAs<double>("MsftOpen") - 
					kvp.Value.GetAs<double>("FbOpen"));
			// [/linq-select]

			// [linq-where]
			// Filter rows using a specified condition 
			var msftGreaterRows = joinIn.Rows.Where(kvp =>
				kvp.Value.GetAs<double>("MsftOpen") > 
					kvp.Value.GetAs<double>("FbOpen"));

			// Transform row collection into a new data frame
			var msftGreaterDf = Frame.FromRows(msftGreaterRows);
			// [/linq-where]
		}
	}
}
