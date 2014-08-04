using DeNSo;
using DeNSo.Exceptions;
using DeNSo.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
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
    public static JsonSerializer _serializer = new JsonSerializer();

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

    public Task<EventCommandStatus> SetAsync<T>(T entity) where T : class
    {
      return Task.Factory.StartNew<EventCommandStatus>(() => Set<T>(typeof(T).Name, entity));
    }

    public EventCommandStatus Set<T>(T entity) where T : class
    {
      return Set<T>(typeof(T).Name, entity);
    }

    public Task<EventCommandStatus> SetAsync<T>(string collection, T entity) where T : class
    {
      return Task.Factory.StartNew(() => Set<T>(collection, entity));
    }

    public EventCommandStatus Set<T>(string collection, T entity) where T : class
    {
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

        MemoryStream ms = new MemoryStream();
        ms = new MemoryStream();
        using (var sw = new StreamWriter(ms))
        using (var jtw = new JsonTextWriter(sw))
          _serializer.Serialize(jtw, entity);
        //PrepareForSerialization<T>();
        //ProtoBuf.Serializer.Serialize(ms, entity);

        var cmd = new { _action = DensoBuiltinCommands.Set, _collection = collection, _id = idval };
        return EventCommandStatus.Create(_command.Execute(DataBase, JsonConvert.SerializeObject(cmd), ms.ToArray()), this);
      }
      return EventCommandStatus.InvalidStatus;
    }

    public Task<EventCommandStatus> SetAllAsync<T>(IEnumerable<T> entity) where T : class
    {
      return Task.Factory.StartNew(() => SetAll(entity));
    }

    public EventCommandStatus SetAll<T>(IEnumerable<T> entity) where T : class
    {
      var max = 0L;
      foreach (var item in entity)
      {
        max = Math.Max(Set<T>(item).Value, max);
      }
      return new EventCommandStatus() { Value = max };
    }

    public Task<EventCommandStatus> SetAllAsync<T>(string collection, IEnumerable<T> entity) where T : class
    {
      return Task.Factory.StartNew(() => SetAll(collection, entity));
    }

    public EventCommandStatus SetAll<T>(string collection, IEnumerable<T> entity) where T : class
    {
      var max = 0L;
      foreach (var item in entity)
      {
        max = Math.Max(Set<T>(collection, item), max);
      }
      return new EventCommandStatus() { Value = max };
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

    public async Task<IEnumerable<T>> GetAsync<T>() where T : class, new()
    {
      return await Task.Factory.StartNew(() => Get<T>());
    }

    public IEnumerable<T> Get<T>() where T : class, new()
    {
      //PrepareForSerialization<T>();
      foreach (var item in GetStream(typeof(T).Name))
        //yield return ProtoBuf.Serializer.Deserialize<T>(item);
        yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
    }

    //public IEnumerable<T> Get<T>(params JsonConverter[] converters) where T : class, new()
    //{
    //  foreach (var item in GetStream(typeof(T).Name))
    //    yield return ProtoBuf.Serializer.Deserialize<T>(item);
    //    //yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
    //}

    public async Task<IEnumerable<T>> GetAsync<T>(Func<T, bool> filter = null) where T : class, new()
    {
      return await Task.Factory.StartNew(() => Get<T>(filter));
    }

    public IEnumerable<T> Get<T>(Func<T, bool> filter = null) where T : class, new()
    {
      //PrepareForSerialization<T>();
      foreach (var item in GetStream(typeof(T).Name))
      {
        if (item == null) continue;
        //var result = ProtoBuf.Serializer.Deserialize<T>(item);
        var result = _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
        if (filter == null || filter(result))
          yield return result;
      }
    }

    //public IEnumerable<T> Get<T>(Func<T, bool> filter = null, params JsonConverter[] converters) where T : class, new()
    //{
    //  //var ser = new JsonSerializer();
    //  //if (converters != null)
    //  //  foreach (var c in converters)
    //  //    ser.Converters.Add(c);

    //  foreach (var item in GetStream(typeof(T).Name))
    //  {
    //    var result = ProtoBuf.Serializer.Deserialize<T>(item);
    //    //var result = ser.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
    //    if (filter == null || filter(result))
    //      yield return result;
    //  }
    //}

    public IEnumerable<T> Get<T>(string collection, Func<T, bool> filter = null) where T : class, new()
    {
      //PrepareForSerialization<T>();
      foreach (var item in GetStream(collection))
      {
        //var result = ProtoBuf.Serializer.Deserialize<T>(item);
        var result = _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
        if (filter == null || filter(result))
          yield return result;
      }
    }

    //public IEnumerable<T> Get<T>(string collection, Func<T, bool> filter = null, params JsonConverter[] converters) where T : class, new()
    //{

    //  //var s = new JsonSerializer();
    //  //if (converters != null)
    //  //  foreach (var c in converters)
    //  //    s.Converters.Add(c);

    //  foreach (var item in GetStream(collection))
    //  {
    //    var result = ProtoBuf.Serializer.Deserialize<T>(item);
    //    //var result = s.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
    //    if (filter == null || filter(result))
    //      yield return result;
    //  }
    //}

    public IEnumerable<Stream> GetJSonStream<T>(Func<JObject, bool> filter = null) where T : class, new()
    {
      return GetStream(typeof(T).Name, filter).AsEnumerable();
    }

    public IEnumerable<Stream> GetStream(string collection, Func<JObject, bool> filter = null)
    {
      IEnumerable<string> result;
      //if (filter == null)
      result = _query.GetAllKeys(DataBase, collection);

      foreach (var k in result)
        yield return _query.GetAsStream(DataBase, collection, k);
    }

    public IEnumerable<string> GetJSon<T>(Func<JObject, bool> filter = null) where T : class, new()
    {
      return GetJSon(typeof(T).Name, filter).AsEnumerable();
    }

    public IEnumerable<string> GetJSon(string collection, Func<JObject, bool> filter = null)
    {
      IEnumerable<string> result;
      //if (filter == null)
      result = _query.GetAllKeys(DataBase, collection);

      foreach (var k in result)
        yield return _query.GetAsString(DataBase, collection, k);
    }

    //public T GetById<T>(string id, params JsonConverter[] converters) where T : class, new()
    //{
    //  var result = _query.GetAsStream(DataBase, typeof(T).Name, id);
    //  if (result != null)
    //  {
    //    var ser = new JsonSerializer();
    //    if (converters != null)
    //      foreach (var p in converters)
    //        ser.Converters.Add(p);
    //    return ser.Deserialize<T>(new JsonTextReader(new StreamReader(result)));
    //  }
    //  return default(T);
    //}

    public T GetById<T>(string id) where T : class, new()
    {
      //PrepareForSerialization<T>();
      var result = _query.GetAsStream(DataBase, typeof(T).Name, id);
      if (result != null)
        //return ProtoBuf.Serializer.Deserialize<T>(result);
        return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(result)));
      return default(T);
    }

    public T GetById<T>(string collection, string id) where T : class, new()
    {
      //PrepareForSerialization<T>();
      var result = _query.GetAsStream(DataBase, collection, id);
      if (result != null)
        //return ProtoBuf.Serializer.Deserialize<T>(result);
        return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(result)));
      return default(T);
    }

    //public T GetById<T>(string collection, string id, params JsonConverter[] converters) where T : class, new()
    //{
    //  var result = _query.GetAsStream(DataBase, collection, id);
    //  if (result != null)
    //    return ProtoBuf.Serializer.Deserialize<T>(result);
    //    //return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(result)));
    //  return default(T);
    //}

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
    //public int Count<T>(Func<T, bool> filter) where T : class, new()
    //{
    //  //return Count(typeof(T).Name, filter);
    //}

    public int Count(string collection)
    {
      return _query.Count(DataBase, collection);
    }
    //public int Count(string collection, Func<JObject, bool> filter)
    //{
    //  return _query.Count(DataBase, collection, filter.Compile());
    //}
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

    //private bool IsKnownType<T>()
    //{
    //  return ProtoBuf.Meta.RuntimeTypeModel.Default.CanSerialize(typeof(T));
    //}

    //private void PrepareForSerialization<T>()
    //{
    //  PrepareForSerialization(typeof(T));
    //}

    //private void PrepareForSerialization(Type ttype)
    //{
    //  var otype = ttype;
    //  if (ttype.IsArray)
    //  {
    //    ttype = ttype.GetElementType();
    //  }

    //  if (!ProtoBuf.Meta.RuntimeTypeModel.Default.CanSerialize(ttype))
    //  {
    //    var mtype = ProtoBuf.Meta.RuntimeTypeModel.Default.Add(ttype, false);

    //    int x = 1;
    //    foreach (var p in ttype.GetProperties(System.Reflection.BindingFlags.GetProperty | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
    //    {
    //      if (p.CanRead && p.CanWrite)
    //      {
    //        PrepareForSerialization(p.PropertyType);
    //        var f = mtype.AddField(x++, p.Name);
    //        if (otype.IsArray)
    //        {
    //          f.AsReference = true;
    //          //f.IsPacked = true;
    //        }

    //      }
    //    }

    //    mtype.CompileInPlace();
    //  }
    //}
  }
}
