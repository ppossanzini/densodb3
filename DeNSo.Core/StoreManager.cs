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
    private static Thread _saveDBThread = null;
    private static Thread _indexerThread = null;

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
      {
        if (!_eventStore.ContainsKey(databasename))
          _eventStore.Add(databasename, new EventStore(databasename, 0));
        Monitor.Exit(_eventStore);
        return _eventStore[databasename];
      }
      return null;
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
          newstore.LoadCollection(databasename, collection);
          _stores[databasename].Add(collection, newstore);
        }

      return _stores[databasename][collection] as ObjectStore;
    }

    public static void Start()
    {
      if (!_started)
      {
        LogWriter.LogInformation("Starting StoreManager", LogEntryType.Warning);
        ShutDownEvent.Reset();

        LogWriter.LogInformation("Initializing Extensions", LogEntryType.Warning);
        // Init all the extensions. 
        _extensions.Init();
        foreach (var db in (GetDatabases()))
        {
          LogWriter.LogInformation(string.Format("Opening Database {0}", db), LogEntryType.Warning);
          OpenDataBase(db);
        }

        _saveDBThread = new Thread(new ThreadStart(SaveDBThreadMethod));
        _saveDBThread.Start();

        _indexerThread = new Thread(new ThreadStart(CheckBloomIndexes));
        _indexerThread.IsBackground = true;
        _indexerThread.Start();

        LogWriter.LogInformation("Store Manager initialization completed", LogEntryType.SuccessAudit);
        _started = true;
      }
    }

    public static void ShutDown()
    {
      ShutDownEvent.Set();
      ShuttingDown = true;
      if (_saveDBThread != null)
        _saveDBThread.Join((int)new TimeSpan(0, 5, 0).TotalMilliseconds);


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
      catch (Exception e){
        LogWriter.LogException(e);
      }
    }

    private static void SaveDBThreadMethod()
    {
      while (!ShutDownEvent.WaitOne(2))
      {
        ShutDownEvent.WaitOne(Configuration.SaveInterval);
        //lock (_stores)
        foreach (var db in _stores.Keys)
        {
          SaveDataBase(db);
          ShutDownEvent.WaitOne(Configuration.DBCheckTimeSpan);
        }
      }
      ShutDownEvent.Reset();
    }

    private static void CheckBloomIndexes()
    {
      while (!ShutDownEvent.WaitOne(Configuration.ReindexCheck))
      {
        var dbkeys = _stores.Keys.ToArray();
        foreach (var d in dbkeys)
        {
          var collkeys = _stores[d].Keys.ToArray();
          foreach (var c in collkeys)
          {
            var errorratio = _stores[d][c].IncoerenceIndexRatio();
            if (errorratio > 1)
            {
              LogWriter.LogInformation(string.Format("Reindexing {0} - {1} - IndexRatio: {2}", d, c, errorratio), LogEntryType.Warning);
              _stores[d][c].Reindex();
              LogWriter.LogInformation(string.Format("Completed {0} - {1}", d, c), LogEntryType.Warning);
            }
          }
        }
      }
    }

    internal static void SaveDataBase(string databasename)
    {
      var collections = GetCollections(databasename);
      foreach (var coll in collections)
        GetObjectStore(databasename, coll).SaveCollection();

      var es = GetEventStore(databasename);
      foreach (var path in Configuration.BasePath)
        using (var fs = File.Create(Path.Combine(Path.Combine(path, databasename), "denso.trn")))
        using (var bw = new BinaryWriter(fs))
          bw.Write(es.LastExecutedCommandSN);
    }
  }
}
