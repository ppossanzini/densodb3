using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DeNSo;
using System.Threading;
using System.IO;

#if NETFX_CORE
using System.Threading.Tasks;
#endif

#if WINDOWS
using System.Runtime.Serialization.Formatters.Binary;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Runtime.CompilerServices;
#endif

namespace DeNSo
{
  public static class StoreManager
  {
    private static DensoExtensions _extensions = new DensoExtensions();

    private static bool _started = false;

    internal static ManualResetEvent ShutDownEvent = new ManualResetEvent(false);
    internal static bool ShuttingDown { get; private set; }

    private volatile static Dictionary<string, Dictionary<string, IObjectStore>> _stores = new Dictionary<string, Dictionary<string, IObjectStore>>();

    private volatile static Dictionary<string, EventStore> _eventStore = new Dictionary<string, EventStore>();

    static StoreManager()
    {

    }

    public static string[] GetCollections(string databasename)
    {
      if (_stores.ContainsKey(databasename))
        return _stores[databasename].Keys.ToArray();
      return null;
    }

    public static EventStore GetEventStore(string databasename)
    {
      Monitor.Enter(_eventStore);

      if (!_eventStore.ContainsKey(databasename))
        _eventStore.Add(databasename, new EventStore(databasename, 0));
      Monitor.Exit(_eventStore);
      return _eventStore[databasename];
    }

    public static ObjectStore GetObjectStore(string databasename, string collection)
    {
      lock (_stores)
        if (!_stores.ContainsKey(databasename))
          _stores.Add(databasename, new Dictionary<string, IObjectStore>());

      lock (_stores[databasename])
        if (!_stores[databasename].ContainsKey(collection))
        {
          var newstore = new ObjectStore();
          newstore.OpenCollection(databasename, collection, Configuration.GetBasePath());
          _stores[databasename].Add(collection, newstore);
        }

      return _stores[databasename][collection] as ObjectStore;
    }

    public static void Start()
    {
      if (!_started)
      {
        LogWriter.LogMessage("Starting StoreManager", EventLogEntryType.Warning);
        ShutDownEvent.Reset();

        LogWriter.LogMessage("Initializing Extensions", EventLogEntryType.Warning);
        // Init all the extensions. 
        _extensions.Init();
        foreach (var db in (GetDatabases()))
        {
          LogWriter.LogMessage(string.Format("Opening Database {0}", db), EventLogEntryType.Warning);
          OpenDataBase(db);
        }

        LogWriter.LogMessage("Store Manager initialization completed", EventLogEntryType.SuccessAudit);
        _started = true;
      }
    }

    public static void ShutDown()
    {
      ShutDownEvent.Set();
      ShuttingDown = true;

      foreach (var db in _stores.Keys)
      {
        var collections = GetCollections(db);
        foreach (var coll in collections)
          GetObjectStore(db, coll).CloseCollection();
      }

      Monitor.Enter(_eventStore);
      _eventStore.Clear();

      Monitor.Exit(_eventStore);
      _stores.Clear();
      _started = false;
    }

    public static string[] Databases
    {
      get { return _stores.Keys.ToArray(); }
    }

    private static string[] GetDatabases()
    {
      List<string> result = new List<string>();
      if (Directory.Exists(Configuration.GetBasePath()))
      {
        foreach (var dir in Directory.GetDirectories(Configuration.GetBasePath()))
        {
          var dinfo = new DirectoryInfo(dir);
          if (dinfo.Exists)
            result.Add(dinfo.Name);
        }
      }
      return result.ToArray();
    }

    private static void OpenDataBase(string databasename)
    {
      try
      {
        var filename = Path.Combine(Path.Combine(Configuration.GetBasePath(), databasename), "denso.trn");

        Monitor.Enter(_eventStore);
        if (!_eventStore.ContainsKey(databasename))
        {
          long eventcommandsn = 0;
          if (File.Exists(filename))
            using (var fs = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.Read))
              if (fs.Length > 0)
                using (var br = new BinaryReader(fs))
                  eventcommandsn = br.ReadInt64();

          _eventStore.Add(databasename, new EventStore(databasename, eventcommandsn));
        }
        Monitor.Exit(_eventStore);
      }
      catch (Exception e)
      {
        LogWriter.LogException(e);
      }
    }

    internal static void SaveDataBase(string databasename)
    {
      var es = GetEventStore(databasename);
      foreach (var path in Configuration.BasePath)
        using (var fs = File.Create(Path.Combine(Path.Combine(path, databasename), "denso.trn")))
        using (var bw = new BinaryWriter(fs))
          bw.Write(es.LastExecutedCommandSN);

      //foreach (var c in StoreManager.GetCollections(databasename).ToArray())
      //  StoreManager.GetObjectStore(databasename, c).ShrinkCollection();
    }
  }
}
