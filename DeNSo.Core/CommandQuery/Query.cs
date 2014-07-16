using DeNSo.Core;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DeNSo
{
  public class Query
  {
    public byte[] Get(string database, string collection, string objectid)
    {
      if (!string.IsNullOrEmpty(objectid))
        return StoreManager.GetObjectStore(database, collection).GetById(objectid);
      return null;
    }

    public string GetAsString(string database, string collection, string objectid)
    {
      byte[] data = Get(database, collection, objectid);
      string result;
      if (Configuration.EnableDataCompression)
      {
        result = data.Decompress();
      }
      else
      {
        result = Encoding.UTF8.GetString(data);
      }
      return result;
    }

    public IEnumerable<byte[]> Get(string database, string collection, Func<JObject, bool> filter)
    {
      //if (filter != null)
      //  return StoreManager.GetObjectStore(database, collection).Where(filter);
      return StoreManager.GetObjectStore(database, collection).GetAll();
    }

    public IEnumerable<string> GetAsStrings(string database, string collection, Func<JObject, bool> filter)
    {
      //if (filter != null)
      //  return StoreManager.GetObjectStore(database, collection).Where(filter);
      IEnumerable<byte[]> data = Get(database, collection, filter);
      List<string> result = new List<string>();
      foreach (var d in data)
      {
        if (Configuration.EnableDataCompression)
          result.Add(d.Decompress());
        else
          result.Add(Encoding.UTF8.GetString(d));
      }
      return result.AsEnumerable();
    }

    public int Count(string database, string collection)
    {
      return StoreManager.GetObjectStore(database, collection).Count();
    }

    public int Count(string database, string collection, Func<JObject, bool> filter)
    {
      return StoreManager.GetObjectStore(database, collection).Count(filter);
    }

    public IEnumerable<string> Collections(string database)
    {
      return StoreManager.GetCollections(database);
    }
  }
}
