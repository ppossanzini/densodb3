using DeNSo;
using DeNSo.Core;
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
      byte[] result;
      if (Configuration.EnableDataCompression)
      {
        result = data.Compress();
      }
      else
      {
        result = Encoding.UTF8.GetBytes(data);
      }
      return Execute(databasename, command, result);
    }

    public long Execute(string databasename, string command, byte[] data)
    {
      LogWriter.LogInformation("Received command", LogEntryType.Information);
      var es = StoreManager.GetEventStore(databasename);
      return es.Enqueue(command, data);
    }
  }
}
