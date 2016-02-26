using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo
{
  public class Indexer
  {
    internal Func<object, object> _action = null;
    internal Type _valueType = null;
  }

  public class Indexer<T> : Indexer, IIndexer<T>
  {
    public string Key { get; private set; }

    public Indexer(string key, Func<object, object> action, Type valueType)
    {
      Key = key;
      _action = action;
      _valueType = valueType;
    }

    public object GetValue(T item)
    {
      if (_action != null && item != null)
        return _action(item);

      return null;
    }

    public Type GetValueType()
    {
      return _valueType;
    }
  }
}
