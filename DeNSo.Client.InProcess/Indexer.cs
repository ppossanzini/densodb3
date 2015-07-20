using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;


[assembly: InternalsVisibleTo("DeNSo.Client.Rest, PublicKey=" +
  "0024000004800000940000000602000000240000525341310004000001000100c98f670ec6f0ac" +
  "f9c8529c014464afba633163628ff0813d34e6bff45a4a35bbc439a8aebe6ef2ee4ccf8467e197" +
  "598fd7fb97b574c38a57ed6d853d8d1a95ad1c45030116bdfe87579cf1d44a4c9fb79a7a0296df" +
  "cebde92e0565f1a72cc0644c8089da4dc5645e64f62a035784b14b18b3f4350f20b3902daef4be" +
  "e701aeee")]
[assembly: InternalsVisibleTo("DeNSo.v3.Client.InProcess, PublicKey=" +
  "0024000004800000940000000602000000240000525341310004000001000100c98f670ec6f0ac" +
  "f9c8529c014464afba633163628ff0813d34e6bff45a4a35bbc439a8aebe6ef2ee4ccf8467e197" +
  "598fd7fb97b574c38a57ed6d853d8d1a95ad1c45030116bdfe87579cf1d44a4c9fb79a7a0296df" +
  "cebde92e0565f1a72cc0644c8089da4dc5645e64f62a035784b14b18b3f4350f20b3902daef4be" +
  "e701aeee")]

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
