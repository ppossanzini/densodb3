using DeNSo;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace test2
{
  class Program
  {
    private const string dbName = "TestDB.denso";
    static void Main(string[] args)
    {
      
      Session.DefaultDataBase = dbName;
      Session.Start();

      var collection = Session.New.Collections["test"];

      Stopwatch s = new Stopwatch();
      s.Start();
      var data = File.ReadAllBytes(@"c:\downloads\script.sql");
      s.Stop();

      Console.WriteLine("Reading file: " + s.Elapsed.TotalMilliseconds);

      s.Reset();
      s.Start();
      collection.RawData.Set("t1", data);
      s.Stop();

      Console.WriteLine("Writing to DB : " + s.Elapsed.TotalMilliseconds);

      s.Reset();
      s.Start();
      data = collection.RawData.Get("t1");
      s.Stop();

      Console.WriteLine("Read from DB : " + s.Elapsed.TotalMilliseconds);

      Session.ShutDown();
      Session.Start();

    }
  }
}
