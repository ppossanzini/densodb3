using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo
{
  public interface IIndexer
  {
    string Key { get; }
  }

  public interface IIndexer<T> : IIndexer
  {
    object GetValue(T item);
    Type GetValueType();
  }
}
