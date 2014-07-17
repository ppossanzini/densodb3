using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.REST.CQRS
{
  internal class Query
  {
    private CompressEnabledWebClient _client;
    public string ServerUri { get; private set; }

    public Query(string serveruri)
    {
      ServerUri = serveruri;

      _client = new CompressEnabledWebClient() { BaseAddress = ServerUri };
      _client.Encoding = Encoding.UTF8;
      _client.Headers.Add("Content-Type", "application/json");
      _client.Headers.Add("Accept", "application/json");
    }

    public string Get(string database, string collection, string objectid)
    {
      var result = _client.DownloadData(string.Format("rest/{0}/{1}/getbinary/{2}", database, collection, objectid));
      if (result != null && result.Length > 0)
        return result.Decompress();
      return null;
    }

    public IEnumerable<string> GetAll(string database, string collection)
    {
      var kvalues = _client.DownloadString(string.Format("rest/{0}/{1}/get", database, collection));
      var keys = JsonConvert.DeserializeObject<List<string>>(kvalues);


      foreach (var k in keys)
      {
        yield return Get(database, collection, k);
      }
    }

    public IEnumerable<string> Get(string database, string collection, string field, string id)
    {
      if (string.IsNullOrEmpty(field) || string.IsNullOrEmpty(id))
        foreach (var val in GetAll(database, collection))
          yield return val;
      else
      {
        var kvalues = _client.DownloadString(string.Format("rest/{0}/{1}/where/{2}/{3}", database, collection, field, id));
        var keys = JsonConvert.DeserializeObject<List<string>>(kvalues);

        foreach (var k in keys)
        {
          yield return Get(database, collection, k);
        }
      }
    }

    public int Count(string database, string collection)
    {
      var result = -1;
      int.TryParse(_client.DownloadString(string.Format("rest/{0}/{1}/count", database, collection)), out result);
      return result; ;
    }

    public IEnumerable<string> Collections(string database)
    {
      var result = _client.DownloadString(string.Format("rest/{0}/collections", database));
      return JsonConvert.DeserializeObject<List<string>>(result);
    }
  }
}
