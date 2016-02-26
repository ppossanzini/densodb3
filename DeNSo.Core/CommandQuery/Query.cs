using DeNSo.Core;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DeNSo
{
  public class Query
  {
    public byte[] Get(string database, string collection, string objectid)
    {
      if (!string.IsNullOrEmpty(objectid))
      {
        var data = StoreManager.GetObjectStore(database, collection).GetById(objectid);
        return data;
      }
      return null;
    }

    public string GetAsString(string database, string collection, string objectid)
    {
      return Get(database, collection, objectid).ToPlainString();
    }

    public Stream GetAsStream(string database, string collection, string objectid)
    {
      byte[] data = Get(database, collection, objectid);
      if (data != null) return new MemoryStream(data);
      return null;
    }

    public int Len(string database, string collection, string objectid)
    {
      return StoreManager.GetObjectStore(database, collection).Len(objectid);
    }

    public IEnumerable<string> GetAllKeys(string database, string collection)
    {
      var store = StoreManager.GetObjectStore(database, collection);
      if (store != null)
        foreach (var s in store.GetAllKeys())
          yield return s;
      yield break;
    }

    public int Count(string database, string collection)
    {
      return StoreManager.GetObjectStore(database, collection).Count();
    }

    public IEnumerable<string> Collections(string database)
    {
      return StoreManager.GetCollections(database);
    }
  }
}
