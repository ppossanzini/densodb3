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
        LogWriter.LogInformation("Received command", LogEntryType.Information);
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
    [Route("{database}/{collection}/delete/{id}")]
    public long Delete(string database, string collection, string id)
    {
      LogWriter.LogInformation("Received command", LogEntryType.Information);
      var es = StoreManager.GetEventStore(database);

      var cmd = new { _action = DensoBuiltinCommands.Delete, _collection = collection, _id = id };
      return es.Enqueue(new EventCommand() { Command = JsonConvert.SerializeObject(cmd) });
    }

    [HttpPost]
    [Route("{database}/{collection}/flush")]
    public long Delete(string database, string collection)
    {
      LogWriter.LogInformation("Received command", LogEntryType.Information);
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
    [Route("{database}/{collection}/get")]
    public IEnumerable<string> Get(string database, string collection)
    {
      var values = StoreManager.GetObjectStore(database, collection).GetAll();

      return values.Select(v =>
      {
        if (DeNSo.Configuration.EnableDataCompression)
          return v.Decompress();
        else
          return Encoding.UTF8.GetString(v);
      });
    }

    //[HttpGet]
    //[Route("{database}/{collection}/where/{field?}/{id?}")]
    //public HttpResponseMessage Search(string database, string collection, string field = null, string id = null)
    //{
    //  try
    //  {
    //    StringBuilder sb = new StringBuilder();
    //    IEnumerable<string> result = null;
    //    sb.Append("[");
    //    if (!string.IsNullOrEmpty(field) && !string.IsNullOrEmpty(id))
    //      result = StoreManager.GetObjectStore(database, collection).Where(j => j[field].ToString() == id).AsEnumerable();
    //    else
    //      result = StoreManager.GetObjectStore(database, collection).GetAll();

    //    var index = 0;
    //    foreach (var r in result)
    //    {
    //      if (index > 0) sb.Append(",");
    //      index++;
    //      sb.Append(r);

    //    }
    //    sb.Append("]");
    //    return new HttpResponseMessage
    //    {
    //      Content = new StringContent(sb.ToString(), System.Text.Encoding.UTF8, "application/json")
    //    };
    //  }
    //  catch (Exception ex)
    //  {
    //    Debug.WriteLine(ex.Message);
    //  }
    //  return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{}", System.Text.Encoding.UTF8, "application/json") };
    //}

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
