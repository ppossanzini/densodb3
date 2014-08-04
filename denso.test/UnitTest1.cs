using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DeNSo.test.TestModel;

namespace DeNSo.test
{
  [TestClass]
  public class UnitTest1
  {

    [TestInitialize]
    public void Prepare()
    {
      DeNSo.Configuration.BasePath = new string[] { @"H:\DensoDB\test" };
      DeNSo.Configuration.EnableJournaling = true;

      DeNSo.Configuration.EnableDataCompression = false;

      DeNSo.Configuration.DBCheckTimeSpan = new TimeSpan(0, 2, 0);
      DeNSo.Configuration.SaveInterval = new TimeSpan(0, 2, 0);

      DeNSo.Session.DefaultDataBase = "test";
      DeNSo.Session.Start();
    }

    [TestMethod]
    public void Serialize()
    {
      DeNSo.Session.New.Set(new Model1());
    }

    [TestMethod]
    public void Serialize2()
    {
      DeNSo.Session.New.Set(new Model2());
    }

    [TestMethod]
    public void Serialize3()
    {
      DeNSo.Session.New.Set(new Model3());
    }


    [TestMethod]
    public void SerializeAndDeserialize()
    {
      var id = Guid.NewGuid().ToString();
      var db = DeNSo.Session.New;
      db.Set(new Model3() { _Id = id, MyProperty4 = new Model2[] { new Model2() { MyProperty = "2" } } });

      var item = db.GetById<Model3>(id);

      Assert.AreEqual(item._Id, id);
      Assert.IsNotNull(item.MyProperty4);
      Assert.AreEqual(item.MyProperty4[0].MyProperty, "2");
    }
  }
}
