using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace DeNSo
{
  public interface IStore
  {
    string DataBaseName { get; }
    IObjectStore GetCollection(string collection);
  }

  public interface IObjectStore
  {
    //IEnumerable<byte[]> Where(Func<JObject, bool> filter);
    void Set(string key, byte[] document);
    void Remove(string key);
    void Flush();
    byte[] GetById(string key);

    float IncoerenceIndexRatio();
    void Reindex();
  }
}
