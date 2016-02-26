using DeNSo;
using DeNSo.Commands;
using DeNSo.Exceptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DeNSo
{
  public class Session : DeNSo.ISession//, IDisposable
  {
    public static JsonSerializer _serializer = new JsonSerializer();
    public static string DefaultDataBase { get; set; }

    public static Session New
    {
      get { return new Session() { DataBase = DefaultDataBase ?? string.Empty }; }
    }

    private string _database;
    public string DataBase
    {
      get { return _database; }
      private set
      {
        _database = value;
        Collections = new Collections() { _database = value };
      }
    }

    public Collections Collections { get; private set; }

    private Session()
    {
      StoreManager.Start();      
    }

    #region Set Methods

    public Task<T> SetAsync<T>(T entity) where T : class
    {
      return Task.Factory.StartNew<T>(() => Set<T>(typeof(T).Name, entity));
    }

    public T Set<T>(T entity) where T : class
    {
      return Set<T>(typeof(T).Name, entity);
    }

    public Task<T> SetAsync<T>(string collection, T entity) where T : class
    {
      return Task.Factory.StartNew(() => Set<T>(collection, entity));
    }

    public T Set<T>(string collection, T entity) where T : class
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
        StoreManager.GetObjectStore(DataBase, collection).Set(idval, ms.ToArray());
      }
      return entity;
    }

    public Task<IEnumerable<T>> SetAllAsync<T>(IEnumerable<T> entities) where T : class
    {
      return Task.Factory.StartNew(() => SetAll(entities));
    }

    public IEnumerable<T> SetAll<T>(IEnumerable<T> entities) where T : class
    {
      foreach (var item in entities)
        Set<T>(item);
      return entities;
    }

    public Task<IEnumerable<T>> SetAllAsync<T>(string collection, IEnumerable<T> entities) where T : class
    {
      return Task.Factory.StartNew(() => SetAll(collection, entities));
    }

    public IEnumerable<T> SetAll<T>(string collection, IEnumerable<T> entities) where T : class
    {
      foreach (var item in entities)
        Set<T>(collection, item);

      return entities;
    }

    #endregion

    public string[] GetCollections()
    {
      return StoreManager.GetCollections(DataBase);
    }

    public void Clear()
    {
      foreach (var s in StoreManager.GetCollections(DataBase))
        this.Flush(s);
    }

    #region Delete methods

    public void Delete<T>(T entity) where T : class
    {
      Delete<T>(typeof(T).Name, entity);
    }

    public void Delete<T>(string collection, T entity)
    {
      var enttype = typeof(T);
      var pi = enttype.GetProperty(DocumentMetadata.IdPropertyName);
      if (pi != null)
      {
        var k = pi.GetValue(entity, null);
        if (k != null)
          StoreManager.GetObjectStore(DataBase, collection).Remove(k.ToString());

        return;
      }
      throw new DocumentWithoutIdException();
    }

    public void DeleteAll<T>(IEnumerable<T> entities) where T : class
    {
      DeleteAll<T>(typeof(T).Name, entities);
    }

    public void DeleteAll<T>(string collection, IEnumerable<T> entities) where T : class
    {
      EventCommandStatus cs = new EventCommandStatus();
      foreach (var item in entities)
        Delete(collection, item);
    }

    #endregion

    #region Flush methods

    public void Flush<T>() where T : class
    {
      StoreManager.GetObjectStore(DataBase, typeof(T).Name).Flush();
    }

    public void Flush(string collection)
    {
      StoreManager.GetObjectStore(DataBase, collection).Flush();
    }

    #endregion

    #region Get Methods

    public async Task<IEnumerable<T>> GetAsync<T>() where T : class, new()
    {
      return await Task.Factory.StartNew(() => Get<T>());
    }

    public IEnumerable<T> Get<T>() where T : class, new()
    {
      foreach (var item in GetStream(typeof(T).Name))
        yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
    }

    public async Task<IEnumerable<T>> GetAsync<T>(Func<T, bool> filter = null) where T : class, new()
    {
      return await Task.Factory.StartNew(() => Get<T>(filter));
    }

    public IEnumerable<T> Get<T>(Func<T, bool> filter = null) where T : class, new()
    {
      foreach (var item in GetStream(typeof(T).Name))
      {
        if (item == null) continue;
        var result = _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
        if (filter == null || filter(result))
          yield return result;
      }
    }

    public IEnumerable<T> Get<T>(string collection, Func<T, bool> filter = null) where T : class, new()
    {
      foreach (var item in GetStream(collection))
      {
        var result = _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(item)));
        if (filter == null || filter(result))
          yield return result;
      }
    }

    public IEnumerable<Stream> GetJSonStream<T>(Func<JObject, bool> filter = null) where T : class, new()
    {
      return GetStream(typeof(T).Name, filter).AsEnumerable();
    }

    public IEnumerable<Stream> GetStream(string collection, Func<JObject, bool> filter = null)
    {
      var store = StoreManager.GetObjectStore(DataBase, collection);
      var result = store.GetAllKeys();

      foreach (var k in result)
        yield return new MemoryStream(store.GetById(k));
    }

    public IEnumerable<string> GetJSon<T>(Func<JObject, bool> filter = null) where T : class, new()
    {
      return GetJSon(typeof(T).Name, filter);
    }

    public IEnumerable<string> GetJSon(string collection, Func<JObject, bool> filter = null)
    {
      var store = StoreManager.GetObjectStore(DataBase, collection);
      var result = store.GetAllKeys();

      foreach (var k in result)
        yield return store.GetById(k).ToPlainString();
    }

    public T GetById<T>(string id) where T : class, new()
    {
      return GetById<T>(typeof(T).Name, id);
    }

    public T GetById<T>(string collection, string id) where T : class, new()
    {
      var result = StoreManager.GetObjectStore(DataBase, collection).GetById(id);
      if (result != null)
        return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(new MemoryStream(result))));
      return default(T);
    }

    #endregion

    #region Count Methods

    public int Count<T>() where T : class, new()
    {
      return Count(typeof(T).Name);
    }

    public int Count(string collection)
    {
      return StoreManager.GetObjectStore(DataBase, collection).Count();
    }

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
