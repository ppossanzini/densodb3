using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public abstract class BaseCommand
  {
    internal Collection Collection;

    private ObjectStore _store;
    internal ObjectStore GetStore()
    {
      return StoreManager.GetObjectStore(Collection.Database, Collection.Name);
    }
  }
}
