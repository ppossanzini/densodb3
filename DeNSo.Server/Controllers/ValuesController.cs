using DeNSo.DiskIO;
using DeNSo.Server.Helpers;
using DeNSo.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace DeNSo.Server.Controllers
{
  [RoutePrefix("rest")]
  public class ValuesController : ApiController
  {
    [HttpPost]
    [Route("{database}/{collection}/set/{id}")]
    public long Set(string database, string collection, string id)
    {
      var t = Request.Content.ReadAsStringAsync();
      t.Wait();
      var value = t.Result;
      if (value.StartsWith("="))
        value = value.Substring(1);

      if (!string.IsNullOrEmpty(value))
      {
        LogWriter.LogMessage("Received command", EventLogEntryType.Information);
        var es = StoreManager.GetEventStore(database);

        var cmd = new { _action = DensoBuiltinCommands.Set, _collection = collection, _id = id };

        byte[] cresult;
        if (DeNSo.Configuration.EnableDataCompression)
          cresult = value.Compress();
        else
          cresult = Encoding.UTF8.GetBytes(value);

        return es.Enqueue(new EventCommand() { Command = JsonConvert.SerializeObject(cmd), Data = cresult });
      }
      return -1;
    }

    [HttpPost]
    [Route("{database}/{collection}/setbinary/{id}")]
    public long SetBinary(string database, string collection, string id)
    {
      var t = Request.Content.ReadAsByteArrayAsync();
      t.Wait();
      var value = t.Result;
      if (value != null && value.Length > 0)
      {
        LogWriter.LogMessage("Received command", EventLogEntryType.Information);
        var es = StoreManager.GetEventStore(database);
        var cmd = new { _action = DensoBuiltinCommands.Set, _collection = collection, _id = id };
        return es.Enqueue(new EventCommand() { Command = JsonConvert.SerializeObject(cmd), Data = value });
      }
      return -1;
    }

    [HttpPost]
    [Route("{database}/{collection}/delete/{id}")]
    public long Delete(string database, string collection, string id)
    {
      LogWriter.LogMessage("Received command", EventLogEntryType.Information);
      var es = StoreManager.GetEventStore(database);

      var cmd = new { _action = DensoBuiltinCommands.Delete, _collection = collection, _id = id };
      return es.Enqueue(new EventCommand() { Command = JsonConvert.SerializeObject(cmd) });
    }

    [HttpPost]
    [Route("{database}/{collection}/flush")]
    public long Delete(string database, string collection)
    {
      LogWriter.LogMessage("Received command", EventLogEntryType.Information);
      var es = StoreManager.GetEventStore(database);

      var cmd = new { _action = DensoBuiltinCommands.CollectionFlush, _collection = collection };
      return es.Enqueue(new EventCommand() { Command = JsonConvert.SerializeObject(cmd) });
    }

    [HttpGet]
    [Route("{database}/{collection}/get/{id}")]
    public HttpResponseMessage Get(string database, string collection, string id)
    {
      if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(collection) && !string.IsNullOrEmpty(database))
      {
        var store = StoreManager.GetObjectStore(database, collection);
        if (store != null)
        {
          var value = store.GetById(id);

          string result;
          if (DeNSo.Configuration.EnableDataCompression)
            result = value.Decompress();
          else
            result = Encoding.UTF8.GetString(value);

          if (result != null)
            return new HttpResponseMessage
            {
              Content = new StringContent(result, System.Text.Encoding.UTF8, "application/json")
            };
        }
      }
      return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{}", System.Text.Encoding.UTF8, "application/json") };
    }

    [HttpGet]
    [Route("{database}/{collection}/getbinary/{id}")]
    public HttpResponseMessage GetBinary(string database, string collection, string id)
    {
      if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(collection) && !string.IsNullOrEmpty(database))
      {
        var store = StoreManager.GetObjectStore(database, collection);
        if (store != null)
        {
          var value = store.GetById(id);

          if (value != null)
            return new HttpResponseMessage
            {
              Content = new ByteArrayContent(value)
            };
        }
      }
      return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(new byte[0]) };
    }

    [HttpGet]
    [Route("{database}/{collection}/get")]
    public IEnumerable<string> Get(string database, string collection)
    {
      try
      {
        return StoreManager.GetObjectStore(database, collection).GetAllKeys();
      }
      catch { }
      return new string[0];
    }

    [HttpGet]
    [Route("{database}/{collection}/where/{field?}/{id?}")]
    public IEnumerable<string> Search(string database, string collection, string field = null, string id = null)
    {
      try
      {
        List<string> result = new List<string>();
        var values = StoreManager.GetObjectStore(database, collection).GetAllKeys();
        return values;
      }
      catch (Exception ex)
      {
        Debug.WriteLine(ex.Message);
      }
      return new string[0];
    }

    [HttpGet]
    [Route("{database}/{collection}/count")]
    public int Count(string database, string collection)
    {
      return StoreManager.GetObjectStore(database, collection).Count();
    }

    [HttpGet]
    [Route("{database}/collections")]
    public IEnumerable<string> Collections(string database)
    {
      return StoreManager.GetCollections(database);
    }
  }
}
