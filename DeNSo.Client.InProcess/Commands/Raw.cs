using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Raw : BaseCommand
  {
    private ObjectStore _store;
    public Raw(Collection collection) { _store = StoreManager.GetObjectStore(collection.Database, collection.Name); }

    public void Append(string key, byte[] value) { _store.Append(key, value); }
    public byte[] Get(string key) { return _store.GetById(key); }
    public void Set(string key, byte[] value) { _store.Set(key, value); }
    public int Len(string key) { return _store.Len(key); }
    public byte[] GetSet(string key, byte[] value) { return _store.GetSet(key, value); }
    public IEnumerable<byte[]> Get(params string[] keys)
    {
      foreach (var r in _store.MultipleGet(keys))
        yield return r;
      yield break;
    }
  }
}
