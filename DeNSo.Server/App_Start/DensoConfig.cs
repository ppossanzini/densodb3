using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

[assembly: PreApplicationStartMethod(typeof(DeNSo.Server.App_Start.DensoConfig), "Start")]

namespace DeNSo.Server.App_Start
{
  public static class DensoConfig
  {
    public static void Start()
    {
      var path = System.Web.Hosting.HostingEnvironment.ApplicationHost.GetPhysicalPath();
      Start(path);
    }

    public static void Start(string rootpath)
    {
      DeNSo.Configuration.BasePath = new string[] { System.IO.Path.Combine(rootpath, "App_Data") };
      DeNSo.Configuration.EnableJournaling = true;

      StoreManager.Start();

      
    }

    public static void ShutDown()
    {
      StoreManager.ShutDown();
    }
  }
}