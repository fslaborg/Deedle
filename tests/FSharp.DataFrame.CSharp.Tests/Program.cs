using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FSharp.DataFrame;

namespace FSharp.DataFrame.CSharp.Tests
{
  class Program
  {
    static void Main(string[] args)
    {
		var msft = Frame.ReadCsv(@"..\..\..\..\samples\data\msft.csv");

		var s = msft.GetSeries<double>("Open");
		

		// Aggregation.WindowSize(0, Boundary.AtBeginning)
    }
  }
}
