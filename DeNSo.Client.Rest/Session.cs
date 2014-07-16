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

namespace DeNSo.REST
{
  public delegate void StoreUpdatedHandler(long executedcommandsn);

  public class RestSession // : DeNSo.ISession, IDisposable
  {
    private Command _command = null;
    private Query _query = null;

    private ManualResetEvent _waiting = new ManualResetEvent(false);
    private long _waitingfor = 0;
    private long _lastexecutedcommand = 0;
    private string _serveruri;

    public static string DefaultDataBase { get; set; }
    public static string DefaultRestServerUri { get; set; }

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
          return new EventCommandStatus() { Value = _command.Set(DataBase, collection, idval, JsonConvert.SerializeObject(entity)) };
        }
        return EventCommandStatus.InvalidStatus;
      });
      t.Wait();
      return t.Result;
    }

    public EventCommandStatus SetAll<T>(IEnumerable<T> entity) where T : class
    {
      List<Task<EventCommandStatus>> tasks = new List<Task<EventCommandStatus>>();
      foreach (var item in entity)
      {
        tasks.Add(Task.Factory.StartNew(() => Set<T>(item)));
      }

      Task.WaitAll(tasks.ToArray());
      return new EventCommandStatus() { Value = tasks.Select(t => t.Result.Value).Max() };
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
      return JsonConvert.DeserializeObject<List<T>>(GetJSon(typeof(T).Name)) as IEnumerable<T>;
    }

    public async Task<IEnumerable<T>> GetAsync<T>() where T : class, new()
    {
      var result = await GetJSonAsync(typeof(T).Name);
      return await JsonConvert.DeserializeObjectAsync<List<T>>(result) as IEnumerable<T>;
    }

    public IEnumerable<T> Get<T>(string field, string id) where T : class, new()
    {
      return JsonConvert.DeserializeObject<List<T>>(GetJSon(typeof(T).Name, field, id)) as IEnumerable<T>;
    }

    public async Task<IEnumerable<T>> GetAsync<T>(string field, string id) where T : class, new()
    {
      var result = await GetJSonAsync(typeof(T).Name, field, id);
      return await JsonConvert.DeserializeObjectAsync<List<T>>(result) as IEnumerable<T>;
    }

    public IEnumerable<T> Get<T>(string collection, string field, string id) where T : class, new()
    {
      return JsonConvert.DeserializeObject<List<T>>(GetJSon(collection, field, id)) as IEnumerable<T>;
    }

    public async Task<IEnumerable<T>> GetAsync<T>(string collection, string field, string id) where T : class, new()
    {
      var result = await GetJSonAsync(collection, field, id);
      return await JsonConvert.DeserializeObjectAsync<List<T>>(result) as IEnumerable<T>;
    }

    public string GetJSon<T>(string field, string id) where T : class, new()
    {
      return GetJSon(typeof(T).Name, field, id);
    }

    public async Task<string> GetJSonAsync<T>(string field, string id) where T : class, new()
    {
      return await GetJSonAsync(typeof(T).Name, field, id);
    }

    public string GetJSon(string collection, string field = null, string id = null)
    {
      return _query.Get(DataBase, collection, field, id);
    }

    public async Task<string> GetJSonAsync(string collection, string field = null, string id = null)
    {
      return await Task.Factory.StartNew(() => _query.Get(DataBase, collection, field, id));
    }

    public T GetById<T>(string id) where T : class, new()
    {
      var result = _query.Get(DataBase, typeof(T).Name, id);
      if (result != null)
        return JsonConvert.DeserializeObject<T>(result);
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

  }
}
