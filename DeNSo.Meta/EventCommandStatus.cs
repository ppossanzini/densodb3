using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DeNSo
{
  public struct EventCommandStatus
  {
    public long Value;
    internal ISession _eventSession;
    public bool isValid;

    public static EventCommandStatus InvalidStatus
    {
      get
      {
        var result = new EventCommandStatus();
        result.isValid = false;
        return result;
      }
    }

    public static EventCommandStatus Create(long value, ISession session)
    {
      return new EventCommandStatus() { Value = value, _eventSession = session, isValid = true };
    }

    public static implicit operator long(EventCommandStatus value)
    {
      return value.Value;
    }
  }
}
