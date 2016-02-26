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
    void Append(string key, byte[] value);
    int Count();
    void Flush();
    System.Collections.Generic.IEnumerable<string> GetAllKeys();
    byte[] GetById(string key);
    byte[] GetSet(string key, byte[] value);
    int Len(string key);
    System.Collections.Generic.IEnumerable<byte[]> MultipleGet(params string[] keys);
    void Remove(string key);
    void Set(string key, byte[] value);
  }
}
