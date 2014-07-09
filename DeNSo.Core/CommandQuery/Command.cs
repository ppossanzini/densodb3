using DeNSo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DeNSo
{
  public class Command
  {
    public long Execute(string databasename, string command, string data)
    {
      LogWriter.LogInformation("Received command", LogEntryType.Information);
      var es = StoreManager.GetEventStore(databasename);
      return es.Enqueue(command, data);
    }
  }
}
