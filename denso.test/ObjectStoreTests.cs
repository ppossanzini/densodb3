using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DeNSo.test
{
  [TestClass]
  public class ObjectStoreTests
  {
    [TestInitialize]
    public void Prepare()
    {
      DeNSo.Configuration.BasePath = new string[] { @"G:\DensoDB\test" };
      DeNSo.Configuration.EnableJournaling = true;

      DeNSo.Configuration.EnableDataCompression = false;

      DeNSo.Configuration.DBCheckTimeSpan = new TimeSpan(0, 2, 0);
      DeNSo.Configuration.SaveInterval = new TimeSpan(0, 2, 0);

      DeNSo.Session.DefaultDataBase = "test";
      DeNSo.Session.Start();
    }

    [TestMethod]
    public void Clear()
    {
      DeNSo.Session.New.Clear();
    }

    [TestMethod]
    public void CalcBlock()
    {
      var os = new ObjectStore();

      //os.

    }
  }
}
