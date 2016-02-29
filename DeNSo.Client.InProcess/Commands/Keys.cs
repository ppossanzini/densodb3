using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Keys : BaseCommand
  {
    public Keys(Collection collection) { this.Collection = collection; }

    public IEnumerable<String> All { get { return GetStore().GetAllKeys(); } }
    public void Remove(string key) { GetStore().Remove(key); }
    public int Count() { return GetStore().Count(); }
    public void Flush() { GetStore().Flush(); }
    public int Len(string key) { return GetStore().Len(key); }
  }
}
