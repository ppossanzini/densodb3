using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Core.DiskIO
{
  internal static class FileManager
  {
    private volatile static Dictionary<string, MemoryMappedFile> _mappedfiles = new Dictionary<string, MemoryMappedFile>();

    public static MemoryMappedFile GetLogFile(string basepath, string databasename, long capacity, MemoryMappedFileAccess access, bool isoperationlog = false)
    {
      if (!Directory.Exists(Path.Combine(basepath, databasename)))
      {
        LogWriter.SeparationLine();
        LogWriter.LogInformation("Directory for Journaling does not exists. creating it", LogEntryType.Warning);

        try
        {
          Directory.CreateDirectory(Path.Combine(basepath, databasename));
        }
        catch (Exception ex)
        {
          LogWriter.LogException(ex);
          return null;
        }
      }

      var filename = Path.Combine(Path.Combine(basepath, databasename), string.Format("denso.{0}", isoperationlog ? "log" : "jnl"));
      lock (_mappedfiles)
      {
        if (_mappedfiles.ContainsKey(filename))
          return _mappedfiles[filename];

        var nmf = MemoryMappedFile.CreateOrOpen(filename, capacity, access);
        _mappedfiles.Add(filename, nmf);
        return nmf;
      }
    }
  }
}
