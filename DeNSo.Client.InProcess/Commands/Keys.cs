using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Keys : BaseCommand
  {
    private ObjectStore _store;
    public Keys(Collection collection) { _store = StoreManager.GetObjectStore(collection.Database, collection.Name); }

    public IEnumerable<String> All { get { return _store.GetAllKeys(); } }
    public void Remove(string key) { _store.Remove(key); }
    public int Count() { return _store.Count(); }
    public void Flush() { _store.Flush(); }
    public int Len(string key) { return _store.Len(key); }
  }
}
