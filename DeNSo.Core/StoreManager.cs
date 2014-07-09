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

#if NETFX_CORE
    private static Task _saveDBThread = null;
    private static Task _indexerThread = null;
#else
    private static Thread _saveDBThread = null;
    private static Thread _indexerThread = null;
#endif

    internal static bool ShuttingDown = false;
    internal static ManualResetEvent ShutDownEvent = new ManualResetEvent(false);

#if WINDOWS_PHONE 
    internal static System.IO.IsolatedStorage.IsolatedStorageFile iss = System.IO.IsolatedStorage.IsolatedStorageFile.GetUserStoreForApplication();
#endif
#if NETFX_CORE
    internal static Windows.Storage.StorageFolder iss = Windows.Storage.ApplicationData.Current.LocalFolder;
#endif

    private volatile static Dictionary<string, Dictionary<string, IObjectStore>> _stores =
              new Dictionary<string, Dictionary<string, IObjectStore>>();

    private volatile static Dictionary<string, EventStore> _eventStore =
              new Dictionary<string, EventStore>();

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
      if (Monitor.TryEnter(_eventStore, 1000))
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

#if NETFX_CORE
    public async static void Start()
#else
    public static void Start()
#endif
    {
      if (!_started)
      {
        LogWriter.LogInformation("Starting StoreManager", LogEntryType.Warning);
        ShuttingDown = false;
        ShutDownEvent.Reset();

        LogWriter.LogInformation("Initializing Extensions", LogEntryType.Warning);
        // Init all the extensions. 
        _extensions.Init();
#if NETFX_CORE
        foreach (var db in (await GetDatabases()))
#else
        foreach (var db in (GetDatabases()))
#endif
        {
          LogWriter.LogInformation(string.Format("Opening Database {0}", db), LogEntryType.Warning);
          OpenDataBase(db);
        }


#if NETFX_CORE
        _saveDBThread = Task.Factory.StartNew(SaveDBThreadMethod);
        _indexerThread = Task.Factory.StartNew(CheckBloomIndexes);
#else
        _saveDBThread = new Thread(new ThreadStart(SaveDBThreadMethod));
        _saveDBThread.Start();

        _indexerThread = new Thread(new ThreadStart(CheckBloomIndexes));
        _indexerThread.IsBackground = true;
        _indexerThread.Start();
#endif


        LogWriter.LogInformation("Store Manager initialization completed", LogEntryType.SuccessAudit);
        _started = true;
      }
    }

    public static void ShutDown()
    {
      JournalWriter.RaiseCloseEvent();
      ShuttingDown = true;
      ShutDownEvent.Set();
      if (_saveDBThread != null)
#if NETFX_CORE
        _saveDBThread.Wait((int)new TimeSpan(0, 5, 0).TotalMilliseconds);
#else
        _saveDBThread.Join((int)new TimeSpan(0, 5, 0).TotalMilliseconds);
#endif


      // remove all Event Store
      Monitor.Enter(_eventStore);
      _eventStore.Clear();

      Monitor.Exit(_eventStore);
      //lock (_stores)
      _stores.Clear();

      _started = false;
    }

    public static string[] Databases
    {
      get { return _stores.Keys.ToArray(); }
    }

#if NETFX_CORE
    private async static Task<string[]> GetDatabases()
#else
    private static string[] GetDatabases()
#endif
    {
      List<string> result = new List<string>();
#if NETFX_CORE
      try
      {
        var folder = await iss.GetFolderAsync(Configuration.BasePath);
        if (folder != null)
        {
          foreach (var dir in (await folder.GetFoldersAsync()))
          {
            result.Add(dir.Name);
          }
        }
      }
      catch { }

#else
      if (Directory.Exists(Configuration.GetBasePath()))
      {
        foreach (var dir in Directory.GetDirectories(Configuration.GetBasePath()))
        {
          var dinfo = new DirectoryInfo(dir);
          if (dinfo.Exists)
            result.Add(dinfo.Name);
        }
      }
#endif
      return result.ToArray();
    }

#if NETFX_CORE
    private async static void OpenDataBase(string databasename)
#else
    private static void OpenDataBase(string databasename)
#endif
    {
      try
      {
        var filename = Path.Combine(Path.Combine(Configuration.GetBasePath(), databasename), "denso.trn");

#if NETFX_CORE
        var file = await iss.GetFileAsync(filename);
        var fs = await file.OpenStreamForReadAsync();
#endif

        Monitor.Enter(_eventStore);
        if (!_eventStore.ContainsKey(databasename))
        {
          long eventcommandsn = 0;
#if WINDOWS_PHONE
        if (iss.FileExists(filename))
          using (var fs = iss.OpenFile(filename, FileMode.Open, FileAccess.Read))
#else
#if NETFX_CORE
            using (fs)
#else
          if (File.Exists(filename))
            using (var fs = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.Read))
#endif
#endif
              if (fs.Length > 0)
                using (var br = new BinaryReader(fs))
                  eventcommandsn = br.ReadInt64();

          _eventStore.Add(databasename, new EventStore(databasename, eventcommandsn));
        }
        Monitor.Exit(_eventStore);
      }
      catch { }
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

#if NETFX_CORE
    internal async static void SaveDataBase(string databasename)
#else
    internal static void SaveDataBase(string databasename)
#endif
    {
      var collections = GetCollections(databasename);
      foreach (var coll in collections)
        GetObjectStore(databasename, coll).SaveCollection();
      //SaveCollection(databasename, coll);

      var es = GetEventStore(databasename);
#if WINDOWS_PHONE 
      using (var fs = iss.CreateFile(Path.Combine(Path.Combine(Configuration.BasePath, databasename), "denso.trn")))
#else
#if NETFX_CORE
      var file = await iss.CreateFileAsync(Path.Combine(Path.Combine(Configuration.BasePath, databasename), "denso.trn"), Windows.Storage.CreationCollisionOption.ReplaceExisting);
      using (var fs = await file.OpenStreamForWriteAsync())

#else
      foreach (var path in Configuration.BasePath)
        using (var fs = File.Create(Path.Combine(Path.Combine(path, databasename), "denso.trn")))
#endif
#endif
        using (var bw = new BinaryWriter(fs))
          bw.Write(es.LastExecutedCommandSN);

      es.ShrinkEventStore();
    }

  }
}
