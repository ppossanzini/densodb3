using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
//using System.Runtime.Serialization.Formatters.Binary;
using DeNSo.DiskIO;
using System.Threading;
using System.IO.MemoryMappedFiles;

#if NETFX_CORE
using System.Threading.Tasks;
#endif

#if WINDOWS
using System.Runtime.Remoting;
using System.Threading;
#endif

namespace DeNSo
{
  internal class JournalWriter
  {
    #region private fields

    private object _filemonitor = new Object();
    private MemoryMappedViewStream _logfile = null;
    private BinaryWriter _writer = null;

    #endregion

    #region public properties

    public long CommandSN { get; internal set; }

    #endregion

    internal JournalWriter(MemoryMappedFile mappedfile)
    {
      LogWriter.SeparationLine();
      LogWriter.LogInformation("Initializing Journal Writer", LogEntryType.Information);
      OpenLogFile(mappedfile);
    }

    public long LogCommand(EventCommand command)
    {
      Monitor.Enter(_filemonitor);
      CommandSN++;

      _writer.Write('K');
      _writer.Write(CommandSN);
      _writer.Write(command.CommandMarker ?? string.Empty);
      _writer.Write('D');
      _writer.Write(command.Command);
      if (command.Data != null && command.Data.Length > 0)
      {
        _writer.Write(command.Data.Length);
        _writer.Write(command.Data);
      }
      else
        _writer.Write((int)0);
      _writer.Flush();

      Monitor.Exit(_filemonitor);
      return CommandSN;
    }

    private void CloseFile()
    {
      Monitor.Enter(_filemonitor);
      if (_logfile != null)
      {
        _writer.Flush();
        _writer.Close();
        _logfile.Close();

        _logfile = null;
        _writer = null;
      }
      Monitor.Exit(_filemonitor);
    }

    private bool OpenLogFile(MemoryMappedFile file)
    {
      try
      {
        if (_logfile == null)
        {

          Monitor.Enter(_filemonitor);

          _logfile = file.CreateViewStream();
          _logfile.Seek(0, SeekOrigin.End);
          _writer = new BinaryWriter(_logfile);

          Monitor.Exit(_filemonitor);
        }
      }
      catch (Exception ex)
      {
        LogWriter.LogException(ex);
        return false;
      }
      return true;
    }


    internal static void WriteCommand(BinaryWriter bw, EventCommand command)
    {
      bw.Write('K');
      bw.Write(command.CommandSN);
      bw.Write(command.CommandMarker ?? string.Empty);
      bw.Write('D');
      bw.Write(command.Command);
      if (command.Data != null && command.Data.Length > 0)
      {
        bw.Write(command.Data.Length);
        bw.Write(command.Data);
      }
      else
        bw.Write((int)0);
    }
  }
}
