using System;
using System.Collections.Generic;
using System.Text;
using DeNSo.REST.CQRS;
using DeNSo;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using DeNSo.Exceptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.IO;

namespace DeNSo.REST
{
  public delegate void StoreUpdatedHandler(long executedcommandsn);

  public class RestSession // : DeNSo.ISession, IDisposable
  {
    private Command _command = null;
    private Query _query = null;

    private ManualResetEvent _waiting = new ManualResetEvent(false);
    private string _serveruri;

    public static string DefaultDataBase { get; set; }
    public static string DefaultRestServerUri { get; set; }
    public static JsonSerializer _serializer = new JsonSerializer();

    public static RestSession New
    {
      get
      {
        return new RestSession()
        {
          DataBase = DefaultDataBase ?? string.Empty,
          RestServerUri = DefaultRestServerUri ?? string.Empty
        };
      }
    }

    public string DataBase { get; set; }
    public string RestServerUri
    {
      get { return _serveruri; }
      set
      {
        _serveruri = value;
        _command = new Command(_serveruri);
        _query = new Query(_serveruri);
      }
    }

    private RestSession()
    {
    }

    public void Dispose()
    {
    }

    #region Set Methods
    public EventCommandStatus Set<T>(T entity) where T : class
    {
      return Set(typeof(T).Name, entity);
    }

    public EventCommandStatus Set<T>(string collection, T entity) where T : class
    {
      var t = Task.Factory.StartNew<EventCommandStatus>(() =>
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

          var ms = new MemoryStream();
          using (var sw = new StreamWriter(ms.GetCompressorStream()))
          using (var jtw = new JsonTextWriter(sw))
            _serializer.Serialize(jtw, entity);

          return new EventCommandStatus() { Value = _command.Set(DataBase, collection, idval, ms.ToArray()) };
        }
        return EventCommandStatus.InvalidStatus;
      });
      t.Wait();
      return t.Result;
    }

    public EventCommandStatus SetAll<T>(IEnumerable<T> entity) where T : class
    {
      long rr = 0;
      foreach (var item in entity)
      {
        var r = Set<T>(item);
        rr = Math.Max(r.Value, rr);
      }

      return new EventCommandStatus() { Value = rr };
    }

    public EventCommandStatus SetAll<T>(string collection, IEnumerable<T> entity) where T : class
    {
      long rr = 0;
      foreach (var item in entity)
      {
        var r = Set<T>(collection, item);
        rr = Math.Max(r.Value, rr);
      }
      return new EventCommandStatus() { Value = rr };
    }

    #endregion

    public EventCommandStatus Execute<T>(T command) where T : class
    {
      throw new NotSupportedException("This method is not supported yet");
      //return new EventCommandStatus() { Value = _command.Execute(DataBase, JsonConvert.SerializeObject(command)) };
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
        return new EventCommandStatus() { Value = _command.Delete(DataBase, collection, (pi.GetValue(entity, null) ?? string.Empty).ToString()) };
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
      return new EventCommandStatus() { Value = _command.Flush(DataBase, typeof(T).Name) };
    }
    public EventCommandStatus Flush(string collection)
    {
      var cmd = new { _action = DensoBuiltinCommands.CollectionFlush, _collection = collection };
      return new EventCommandStatus() { Value = _command.Flush(DataBase, collection) };
    }
    #endregion

    #region Get Methods

    public IEnumerable<T> Get<T>() where T : class, new()
    {
      var result = new List<T>();
      var values = GetJSonStream(typeof(T).Name);
      foreach (var val in values)
        result.Add(_serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val))));
      return result;
    }

    public async Task<IEnumerable<T>> GetAsync<T>() where T : class, new()
    {
      var values = await GetJSonStreamAsync(typeof(T).Name);

      var result = new List<T>();
      foreach (var val in values)
        result.Add(await Task.Factory.StartNew(() => _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val)))));

      return result;
    }

    public IEnumerable<T> Get<T>(string field, string id) where T : class, new()
    {
      var values = GetJSonStream(typeof(T).Name, field, id);
      //var result = new List<T>();
      foreach (var val in values)
        yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val)));
    }

    public async Task<IEnumerable<T>> GetAsync<T>(string field, string id) where T : class, new()
    {
      var values = await GetJSonStreamAsync(typeof(T).Name, field, id);
      var result = new List<T>();
      foreach (var val in values)
        result.Add(await Task.Factory.StartNew(() => _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val)))));
      return result;
    }

    public IEnumerable<T> Get<T>(string collection, string field, string id) where T : class, new()
    {
      var values = GetJSonStream(collection, field, id);
      foreach (var val in values)
        yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val)));
    }

    public async Task<IEnumerable<T>> GetAsync<T>(string collection, string field, string id) where T : class, new()
    {
      var values = await GetJSonStreamAsync(collection, field, id);
      var result = new List<T>();
      foreach (var val in values)
        result.Add(await Task.Factory.StartNew(() => _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(val)))));
      return result;
    }

    public IEnumerable<Stream> GetJSon<T>(string field, string id) where T : class, new()
    {
      return GetJSonStream(typeof(T).Name, field, id);
    }

    public async Task<IEnumerable<Stream>> GetJSonAsync<T>(string field, string id) where T : class, new()
    {
      return await GetJSonStreamAsync(typeof(T).Name, field, id);
    }

    public IEnumerable<Stream> GetJSonStream(string collection, string field = null, string id = null)
    {
      return _query.Get(DataBase, collection, field, id);
    }

    public async Task<IEnumerable<Stream>> GetJSonStreamAsync(string collection, string field = null, string id = null)
    {
      return await Task.Factory.StartNew(() => _query.Get(DataBase, collection, field, id));
    }

    public T GetById<T>(string id) where T : class, new()
    {
      var result = _query.GetStream(DataBase, typeof(T).Name, id);
      if (result != null)
        return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(result)));
      return default(T);
    }

    public string GetById(string collection, string id)
    {
      return _query.Get(DataBase, collection, id);
    }

    #endregion

    #region Count Methods
    public int Count<T>() where T : class, new()
    {
      return Count(typeof(T).Name);
    }

    public int Count(string collection)
    {
      return _query.Count(DataBase, collection);
    }

    #endregion

    #region Indexing



    #endregion
  }
}
