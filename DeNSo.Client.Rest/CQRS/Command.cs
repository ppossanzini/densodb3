using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.REST.CQRS
{
  internal class Command
  {
    private CompressEnabledWebClient _client;
    public string ServerUri { get; private set; }

    public Command(string serveruri)
    {
      ServerUri = serveruri;

      _client = new CompressEnabledWebClient() { BaseAddress = ServerUri };
      _client.Encoding = Encoding.UTF8;
      _client.Headers.Add("Content-Type", "application/json;");
    }

    public long Set(string database, string collection, string id, string values)
    {
      long result = -1;
      _client.UploadData(string.Format("rest/{0}/{1}/setbinary/{2}", database, collection, id), "POST", values.Compress());
      return result;
    }

    public long Set(string database, string collection, string id, byte[] values)
    {
      long result = -1;
      _client.UploadData(string.Format("rest/{0}/{1}/setbinary/{2}", database, collection, id), "POST", values);
      return result;
    }

    public long Delete(string database, string collection, string id)
    {
      long result = -1;
      long.TryParse(_client.UploadString(string.Format("rest/{0}/{1}/delete/{2}", database, collection, id), "POST"), out result);
      return result;
    }

    public long Flush(string database, string collection)
    {
      long result = -1;
      long.TryParse(_client.UploadString(string.Format("rest/{0}/{1}/flush", database, collection), "POST"), out result);
      return result;
    }
  }
}
