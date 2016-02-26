using DeNSo;
using DeNSo.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace DeNSo
{
  public class Command
  {
    public long Execute(string databasename, string command, string data)
    {
      return Execute(databasename, command, data.ToPlainByteArray());
    }

    public long Execute(string databasename, string command, byte[] data)
    {
      LogWriter.LogMessage("Received command", EventLogEntryType.Information);
      var es = StoreManager.GetEventStore(databasename);
      return es.Enqueue(command, data);
    }
  }
}
