using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DeNSo;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace UnitTest
{
  [TestClass]
  public class BaseTests
  {
    private const string dbName = "TestDB.denso";


    [TestCleanup]
    public void Clean()
    {
      ManualResetEvent wait = new ManualResetEvent(false);

      Session.ShutDown();

      ThreadPool.QueueUserWorkItem(w => { Thread.Sleep(5000); wait.Set(); });

      wait.WaitOne();
    }

    [TestInitialize]
    public void Init()
    {
      Session.DefaultDataBase = dbName;
      Session.Start();
    }

    [TestMethod]
    public void SimpleWrite()
    {
      Session.New.Collections["test"].Strings.Append("t1", "test");
    }

    [TestMethod]
    public void WriteAndRead()
    {
      var s = Session.New;
      s.Collections["test"].Strings.Append("t1", "test2");

      Assert.AreEqual("test2", s.Collections["test"].Strings.Get("t1"));
    }

    [TestMethod]
    public void WriteRawBigData()
    {
      var collection = Session.New.Collections["test"];

      Stopwatch s = new Stopwatch();
      s.Start();
      var data = File.ReadAllBytes(@"c:\downloads\WhatsApp.apk");
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

    }

    [TestMethod]
    public void OnlyRead()
    {
      var collection = Session.New.Collections["test"];
      Stopwatch s = new Stopwatch();
      s.Start();
      var data = collection.RawData.Get("t1");
      s.Stop();

      Console.WriteLine("Read from DB : " + s.Elapsed.TotalMilliseconds);
      Console.WriteLine(data.Length.ToString());

    }
  }
}
