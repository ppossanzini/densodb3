using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Strings : BaseCommand
  {
    public Strings(Collection collection) { Collection = collection; }

    public void Append(string key, string value) { GetStore().Append(key, value.ToPlainByteArray()); }
    public string Get(string key) { return GetStore().GetById(key).ToPlainString(); }
    public void Set(string key, string value) { GetStore().Set(key, value.ToPlainByteArray()); }
    public int Len(string key) { return GetStore().Len(key); }
    public string GetSet(string key, string value) { return GetStore().GetSet(key, value.ToPlainByteArray()).ToPlainString(); }
    public IEnumerable<string> Get(params string[] keys)
    {
      foreach (var r in GetStore().MultipleGet(keys))
        yield return r.ToPlainString();
      yield break;
    }
  }
}
