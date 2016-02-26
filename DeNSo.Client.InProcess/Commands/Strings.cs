using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Strings : BaseCommand
  {
    private ObjectStore _store;
    public Strings(Collection collection) { _store = StoreManager.GetObjectStore(collection.Database, collection.Name); }

    public void Append(string key, string value) { _store.Append(key, value.ToPlainByteArray()); }
    public string Get(string key) { return _store.GetById(key).ToPlainString(); }
    public void Set(string key, string value) { _store.Set(key, value.ToPlainByteArray()); }
    public int Len(string key) { return _store.Len(key); }
    public string GetSet(string key, string value) { return _store.GetSet(key, value.ToPlainByteArray()).ToPlainString(); }
    public IEnumerable<string> Get(params string[] keys)
    {
      foreach (var r in _store.MultipleGet(keys))
        yield return r.ToPlainString();
      yield break;
    }
  }
}
