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
    private volatile static Dictionary<string, FileStream> _mappedfiles = new Dictionary<string, FileStream>();

    public static FileStream GetLogFile(string basepath, string databasename, long capacity = 10485760, MemoryMappedFileAccess access = MemoryMappedFileAccess.ReadWrite, bool isoperationlog = false)
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
        //if (_mappedfiles.ContainsKey(filename))
        //  return _mappedfiles[filename];

        if (!File.Exists(filename))
          File.WriteAllBytes(filename, new byte[0]);


        var fs = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        //var nmf = MemoryMappedFile.CreateFromFile(fs, new Guid().ToString(), capacity, MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
        //_mappedfiles.Add(filename, fs);
        return fs;
      }
    }
  }
}
