using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Windows;
using DeNSo.Core;
using DeNSo.P2P;
using System.Net.PeerToPeer;
using DeNSo;

namespace denso.twitterlike
{
  /// <summary>
  /// Interaction logic for App.xaml
  /// </summary>
  public partial class App : Application
  {
    protected override void OnStartup(StartupEventArgs e)
    {
      base.OnStartup(e);

      //Session.DefaultDataBase = "twitterlike";
      //Configuration.BasePath = new string[] { Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "densosamples/" + System.Diagnostics.Process.GetCurrentProcess().Id) };

      //foreach (var p in Configuration.BasePath)
      //  if (!Directory.Exists(p))
      //    Directory.CreateDirectory(p);

      //EventP2PDispatcher.EnableP2PEventMesh();
      //EventP2PDispatcher.MakeNodeAvaiableToPNRP(Cloud.Available);
    }

    protected override void OnExit(ExitEventArgs e)
    {
      EventP2PDispatcher.StopP2PEventMesh();
      Session.ShutDown();
      base.OnExit(e);

    }

  }
}
