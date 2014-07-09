using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
//using System.Runtime.Serialization.Formatters.Binary;
using DeNSo.DiskIO;
using System.IO.MemoryMappedFiles;


#if NETFX_CORE
using System.Threading.Tasks;
#endif

#if WINDOWS
using System.Runtime.Remoting;
#endif

namespace DeNSo
{
  internal class JournalReader
  {
    #region private fields

    private MemoryMappedViewStream _logfile = null;
    private BinaryReader _reader = null;

    #endregion

    #region public properties

    public long CommandSN { get; internal set; }

    #endregion

    internal JournalReader(MemoryMappedFile mappedfile)
    {
      LogWriter.LogInformation("Initializing Journal Reader", LogEntryType.Information);
      OpenLogFile(mappedfile);
    }

    private void OpenLogFile(MemoryMappedFile mappedfiles)
    {
      try
      {
        _logfile = mappedfiles.CreateViewStream();
        _reader = new BinaryReader(_logfile);
      }
      catch (Exception ex)
      {
        LogWriter.LogException(ex);
      }
    }

    internal bool HasCommandsPending()
    {
      if (_logfile != null)
        return _logfile.Position < _logfile.Length;
      return false;
    }

    internal EventCommand ReadCommand()
    {
      var k = _reader.ReadChar();
      if (k == 'K')
      {
        var csn = _reader.ReadInt64();
        var marker = _reader.ReadString();
        var d = _reader.ReadChar();
        if (d == 'D')
        {
          string command = _reader.ReadString();
          int datalen = _reader.ReadInt32();
          byte[] data = _reader.ReadBytes(datalen);
          return new EventCommand() { Command = command, CommandSN = csn, CommandMarker = marker, Data = data };
        }
      }
      return null;
    }
  }
}
