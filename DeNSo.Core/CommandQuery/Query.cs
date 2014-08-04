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

    public Stream GetAsStream(string database, string collection, string objectid)
    {
      byte[] data = Get(database, collection, objectid);
      if (Configuration.EnableDataCompression)
        return data.DecompressToStream();
      if (data != null)
        return new MemoryStream(data);
      return null;
    }

    public IEnumerable<string> GetAllKeys(string database, string collection)
    {
      var store = StoreManager.GetObjectStore(database, collection);
      if (store != null)
        return store.GetAllKeys().ToArray();
      return new string[0];
    }
    //public IEnumerable<byte[]> Get(string database, string collection, Func<JObject, bool> filter)
    //{
    //  var store = StoreManager.GetObjectStore(database, collection);
    //  return store.GetAll();
    //}

    //public IEnumerable<string> GetAsStrings(string database, string collection, Func<JObject, bool> filter)
    //{
    //  //if (filter != null)
    //  //  return StoreManager.GetObjectStore(database, collection).Where(filter);
    //  IEnumerable<byte[]> data = Get(database, collection, filter);
    //  foreach (var d in data)
    //  {
    //    if (Configuration.EnableDataCompression)
    //      yield return d.Decompress();
    //    else
    //      if (d != null)
    //        yield return Encoding.UTF8.GetString(d);
    //  }
    //}

    //public IEnumerable<Stream> GetAsStream(string database, string collection, Func<JObject, bool> filter)
    //{
    //  //if (filter != null)
    //  //  return StoreManager.GetObjectStore(database, collection).Where(filter);
    //  IEnumerable<byte[]> data = Get(database, collection, filter);
    //  foreach (var d in data)
    //  {
    //    if (Configuration.EnableDataCompression)
    //      yield return d.DecompressToStream();
    //    else
    //      if (d != null)
    //        yield return new MemoryStream(d);
    //  }
    //}

    public int Count(string database, string collection)
    {
      return StoreManager.GetObjectStore(database, collection).Count();
    }

    //public int Count(string database, string collection, Func<JObject, bool> filter)
    //{
    //  return StoreManager.GetObjectStore(database, collection).Count(filter);
    //}

    public IEnumerable<string> Collections(string database)
    {
      return StoreManager.GetCollections(database);
    }
  }
}
