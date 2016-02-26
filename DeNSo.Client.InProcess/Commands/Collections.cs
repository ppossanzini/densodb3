using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Commands
{
  public class Collection
  {
    internal string Name;
    internal string Database;

    public Strings Strings { get; private set; }
    public Keys Keys { get; private set; }

    internal Collection()
    {
      Strings = new Strings(this);
      Keys = new Keys(this);
    }
  }

  public class Collections
  {
    internal string _database;

    private Dictionary<string, Collection> _helpers = new Dictionary<string, Collection>();
    public Collection this[string index]
    {
      get
      {
        var nn = string.Format("{0}_{1}", _database, index);
        if (!_helpers.ContainsKey(nn))
          _helpers.Add(nn, new Collection() { Database = _database, Name = index });
        return _helpers[nn];
      }
    }
  }
}
