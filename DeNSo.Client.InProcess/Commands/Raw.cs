using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Raw : BaseCommand
  {
    public Raw(Collection collection) { Collection = collection; }

    public void Append(string key, byte[] value) { GetStore().Append(key, value); }
    public byte[] Get(string key) { return GetStore().GetById(key); }
    public void Set(string key, byte[] value) { GetStore().Set(key, value); }
    public int Len(string key) { return GetStore().Len(key); }
    public byte[] GetSet(string key, byte[] value) { return GetStore().GetSet(key, value); }
    public IEnumerable<byte[]> Get(params string[] keys)
    {
      foreach (var r in GetStore().MultipleGet(keys))
        yield return r;
      yield break;
    }
  }
}
