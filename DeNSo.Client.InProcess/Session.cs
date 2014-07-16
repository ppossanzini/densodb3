using DeNSo;
using DeNSo.Exceptions;
using DeNSo.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DeNSo
{
  public delegate void StoreUpdatedHandler(long executedcommandsn);

  public class Session : DeNSo.ISession, IDisposable
  {
    private Command _command = new Command();
    private Query _query = new Query();

    private ManualResetEvent _waiting = new ManualResetEvent(false);
    private long _waitingfor = 0;
    private long _lastexecutedcommand = 0;

    public static string DefaultDataBase { get; set; }
    public static Session New { get { return new Session() { DataBase = DefaultDataBase ?? string.Empty }; } }

    public string DataBase { get; set; }
    public static event StoreUpdatedHandler StoreUpdatedHandler;

    internal static void RaiseStoreUpdated(long commandnumber)
    {
      if (StoreUpdatedHandler != null)
        StoreUpdatedHandler(commandnumber);
    }

    private Session()
    {
      StoreManager.Start();
      //RegisterWaitEventAsync();
    }

    public void Dispose()
    {
    }

    private void RegisterWaitEventAsync()
    {
      Thread waitingThread = new Thread((ThreadStart)delegate
      {
        Session.StoreUpdatedHandler += (sn) =>
        {
          _lastexecutedcommand = sn;
          //if (_waitingfor <= sn)
          _waiting.Set();
        };
      });
      waitingThread.IsBackground = true;
      waitingThread.Start();
    }

    #region Wait Methods
    public void WaitForNonStaleDataAt(long eventcommandnumber)
    {
      //if (_lastexecutedcommand >= eventcommandnumber) return;
      _waitingfor = eventcommandnumber;
      while (_lastexecutedcommand < eventcommandnumber)
      {
        _waiting.WaitOne(200);
        _waiting.Reset();
      }
      _waitingfor = 0;
    }
    public bool WaitForNonStaleDataAt(long eventcommandnumber, TimeSpan timeout)
    {
      //if (_lastexecutedcommand >= eventcommandnumber) return;
      _waitingfor = eventcommandnumber;
      _waiting.WaitOne(timeout);
      if (_lastexecutedcommand < eventcommandnumber) return false;
      _waiting.Reset();
      _waitingfor = 0;
      return true;
    }
    public bool WaitForNonStaleDataAt(long eventcommandnumber, int timeout)
    {
      _waitingfor = eventcommandnumber;

      _waiting.WaitOne(timeout);
      if (_lastexecutedcommand < eventcommandnumber) return false;
      _waiting.Reset();
      _waitingfor = 0;
      return true;
    }
    #endregion

    #region Set Methods
    public EventCommandStatus Set<T>(T entity) where T : class
    {
      //var t = Task.Factory.StartNew<EventCommandStatus>(() =>
      //{
      var enttype = typeof(T);
      var pi = enttype.GetProperty(DocumentMetadata.IdPropertyName);
      if (pi != null)
      {
        var idval = (string)pi.GetValue(entity, null);
        if (string.IsNullOrEmpty(idval))
        {
          idval = Guid.NewGuid().ToString();
          pi.SetValue(entity, idval);
        }
        var cmd = new { _action = DensoBuiltinCommands.Set, _collection = typeof(T).Name, _id = idval };
        return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), JsonConvert.SerializeObject(entity)), this);
      }
      return EventCommandStatus.InvalidStatus;
      //});
      //t.Wait();
      //return t.Result;
    }

    public EventCommandStatus Set<T>(string collection, T entity) where T : class
    {
      //var t = Task.Factory.StartNew(() =>
      //{
      var enttype = typeof(T);
      var pi = enttype.GetProperty(DocumentMetadata.IdPropertyName);
      if (pi != null)
      {
        var idval = (string)pi.GetValue(entity, null);
        if (string.IsNullOrEmpty(idval))
        {
          idval = Guid.NewGuid().ToString();
          pi.SetValue(entity, idval);
        }

        var cmd = new { _action = DensoBuiltinCommands.Set, _collection = collection };
        return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), JsonConvert.SerializeObject(entity)), this);
      }
      return EventCommandStatus.InvalidStatus;
      //});
      //t.Wait();
      //return t.Result;
    }

    public EventCommandStatus SetAll<T>(IEnumerable<T> entity) where T : class
    {
      //List<Task<EventCommandStatus>> tasks = new List<Task<EventCommandStatus>>();
      foreach (var item in entity)
      {
        Set<T>(item);
        //tasks.Add(Task.Factory.StartNew(() => Set<T>(item)));
      }

      //Task.WaitAll(tasks.ToArray());
      //return new EventCommandStatus() { Value = tasks.Select(t => t.Result.Value).Max() };
      return EventCommandStatus.InvalidStatus;
    }

    public EventCommandStatus SetAll<T>(string collection, IEnumerable<T> entity) where T : class
    {
      List<Task<EventCommandStatus>> tasks = new List<Task<EventCommandStatus>>();
      foreach (var item in entity)
      {
        tasks.Add(Task.Factory.StartNew(() => Set<T>(collection, item)));
      }
      Task.WaitAll(tasks.ToArray());
      return new EventCommandStatus() { Value = tasks.Select(t => t.Result.Value).Max() };
    }

    #endregion

    public EventCommandStatus Execute<T>(T command) where T : class
    {
      return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(command), (byte[])null), this);
    }

    #region Delete methods
    public EventCommandStatus Delete<T>(T entity) where T : class
    {
      return Delete<T>(typeof(T).Name, entity);
    }
    public EventCommandStatus Delete<T>(string collection, T entity)
    {
      var enttype = typeof(T);
      var pi = enttype.GetProperty(DocumentMetadata.IdPropertyName);
      if (pi != null)
      {
        var cmd = new { _action = DensoBuiltinCommands.Delete, _id = pi.GetValue(entity, null), _collection = collection };
        return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), (byte[])null), this);
      }
      throw new DocumentWithoutIdException();
    }
    public EventCommandStatus DeleteAll<T>(IEnumerable<T> entities) where T : class
    {
      return DeleteAll<T>(typeof(T).Name, entities);
    }
    public EventCommandStatus DeleteAll<T>(string collection, IEnumerable<T> entities) where T : class
    {
      EventCommandStatus cs = new EventCommandStatus();
      foreach (var item in entities)
      {
        cs = Delete(collection, item);
      }
      return cs;
    }
    #endregion

    #region Flush methods
    public EventCommandStatus Flush<T>() where T : class
    {
      var cmd = new { _action = DensoBuiltinCommands.CollectionFlush, _collection = typeof(T).Name };
      return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), (byte[])null), this);
    }
    public EventCommandStatus Flush(string collection)
    {
      var cmd = new { _action = DensoBuiltinCommands.CollectionFlush, _collection = collection };
      return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), (byte[])null), this);
    }
    #endregion

    #region Get Methods
    //public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter = null) where T : class, new()
    //{
    //  return this.Get(typeof(T).Name, filter);
    //}
    //public IEnumerable<T> Get<T>(string collection, Expression<Func<T, bool>> filter = null) where T : class, new()
    //{
    //  Generic2BsonLambdaConverter visitor = new Generic2BsonLambdaConverter();
    //  var expr = visitor.Visit(filter) as Expression<Func<BSonDoc, bool>>;
    //  return Get(collection, expr != null ? expr.Compile() : null).Select(doc => doc.FromBSon<T>()).AsEnumerable();
    //}

    public IEnumerable<T> Get<T>() where T : class, new()
    {
      return GetJSon(typeof(T).Name).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc)).AsEnumerable();
    }

    public IEnumerable<T> Get<T>(params JsonConverter[] converters) where T : class, new()
    {
      return GetJSon(typeof(T).Name).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public async Task<IEnumerable<T>> GetAsync<T>(params JsonConverter[] converters) where T : class, new()
    {
      return (await GetJSonAsync(typeof(T).Name)).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter = null) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return GetJSon(typeof(T).Name, tfilter).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc)).AsEnumerable();
    }

    public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter = null, params JsonConverter[] converters) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return GetJSon(typeof(T).Name, tfilter).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public async Task<IEnumerable<T>> GetAsync<T>(Expression<Func<T, bool>> filter = null, params JsonConverter[] converters) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return (await GetJSonAsync(typeof(T).Name, tfilter)).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public IEnumerable<T> Get<T>(string collection, Expression<Func<T, bool>> filter = null) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return GetJSon(collection, tfilter).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc)).AsEnumerable();
    }

    public IEnumerable<T> Get<T>(string collection, Expression<Func<T, bool>> filter = null, params JsonConverter[] converters) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return GetJSon(collection, tfilter).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public async Task<IEnumerable<T>> GetAsync<T>(string collection, Expression<Func<T, bool>> filter = null, params JsonConverter[] converters) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return (await GetJSonAsync(collection, tfilter)).AsParallel().Select(doc => JsonConvert.DeserializeObject<T>(doc, converters)).AsEnumerable();
    }

    public IEnumerable<string> GetJSon<T>(Expression<Func<JObject, bool>> filter = null) where T : class, new()
    {
      return GetJSon(typeof(T).Name, filter).AsEnumerable();
    }

    public async Task<IEnumerable<string>> GetJSonAsync<T>(Expression<Func<JObject, bool>> filter = null) where T : class, new()
    {
      return await GetJSonAsync(typeof(T).Name, filter);
    }

    public IEnumerable<string> GetJSon(string collection, Expression<Func<JObject, bool>> filter = null)
    {

      return _query.GetAsStrings(DataBase, collection, filter != null ? filter.Compile() : (Func<JObject, bool>)null);
    }

    public async Task<IEnumerable<string>> GetJSonAsync(string collection, Expression<Func<JObject, bool>> filter = null)
    {
      return await Task.Factory.StartNew(() => _query.GetAsStrings(DataBase, collection, filter != null ? filter.Compile() : (Func<JObject, bool>)null));
    }

    public T GetById<T>(string id, params JsonConverter[] converters) where T : class, new()
    {
      var result = _query.GetAsString(DataBase, typeof(T).Name, id);
      if (result != null)
        return JsonConvert.DeserializeObject<T>(result, converters);
      return default(T);
    }

    public T GetById<T>(string id) where T : class, new()
    {
      var result = _query.GetAsString(DataBase, typeof(T).Name, id);
      if (result != null)
        return JsonConvert.DeserializeObject<T>(result);
      return default(T);
    }

    public T GetById<T>(string collection, string id, params JsonConverter[] converters) where T : class, new()
    {
      var result = _query.GetAsString(DataBase, collection, id);
      if (result != null)
        return JsonConvert.DeserializeObject<T>(result, converters);
      return default(T);
    }

    public string GetById(string collection, string id)
    {
      return _query.GetAsString(DataBase, collection, id);
    }
    #endregion

    #region Count Methods
    public int Count<T>() where T : class, new()
    {
      return Count(typeof(T).Name);
    }
    public int Count<T>(Expression<Func<T, bool>> filter) where T : class, new()
    {
      var qt = new QueryTranslator<T>();
      var tfilter = qt.Translate(filter) as Expression<Func<JObject, bool>>;
      return Count(typeof(T).Name, tfilter);
    }

    public int Count(string collection)
    {
      return _query.Count(DataBase, collection);
    }
    public int Count(string collection, Expression<Func<JObject, bool>> filter)
    {
      return _query.Count(DataBase, collection, filter.Compile());
    }
    //public int Count<T>(Expression<Func<T, bool>> filter) where T : class, new()
    //{
    //  Generic2BsonLambdaConverter visitor = new Generic2BsonLambdaConverter();
    //  var expr = visitor.Visit(filter) as Expression<Func<JObject, bool>>;
    //  return Count(typeof(T).Name, expr);
    //}
    #endregion

    public static void ShutDown()
    {
      StoreManager.ShutDown();
    }
    public static void Start()
    {
      StoreManager.Start();
    }
  }
}
