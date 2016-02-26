using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using DeNSo.DiskIO;
using System.Threading;
using System.IO.MemoryMappedFiles;

namespace DeNSo
{
  public enum JournalWriterMode
  {
    ShrinkMode,
    Normal
  }

  internal class JournalWriter
  {
    #region private fields

    private object _filemonitor = new Object();
    //private MemoryMappedViewStream _logfile = null;
    private FileStream _logfile = null;
    private BinaryWriter _writer = null;

    private const int MByte = 1024 * 1204;
    private int _increasefileby = 2 * MByte;

    private JournalWriterMode _currentmode = JournalWriterMode.Normal;

    #endregion

    #region public properties

    public long CommandSN { get; internal set; }

    #endregion

    internal JournalWriter(FileStream mappedfile, JournalWriterMode mode = JournalWriterMode.Normal)
    {
      LogWriter.SeparationLine();
      LogWriter.LogMessage("Initializing Journal Writer", EventLogEntryType.Information);
      _currentmode = mode;
      OpenLogFile(mappedfile, mode);
    }

    public long LogCommand(EventCommand command)
    {
      Monitor.Enter(_filemonitor);
      //CommandSN++;

      if (_currentmode == JournalWriterMode.ShrinkMode)
        CommandSN = command.CommandSN;
      else
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

    internal void CloseFile()
    {
      Monitor.Enter(_filemonitor);
      if (_logfile != null)
      {
        _writer.Flush();
        _logfile.Flush();
        _logfile.SetLength(_logfile.Position);
        _writer.Close();
        _logfile.Close();

        _logfile = null;
        _writer = null;
      }
      Monitor.Exit(_filemonitor);
    }

    private bool OpenLogFile(FileStream file, JournalWriterMode position)
    {
      try
      {
        if (_logfile == null)
        {

          Monitor.Enter(_filemonitor);

          //_logfile = file.CreateViewStream();
          _logfile = file;
          if (position == JournalWriterMode.Normal)
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

    private void IncreaseFileSize()
    {
      if (Debugger.IsAttached) return;

      LogWriter.LogMessage("Journalig file is too small, make it bigger", EventLogEntryType.Information);
      var pos = _logfile.Position;
      _logfile.SetLength(pos + _increasefileby);
      _logfile.Flush();
      _logfile.Position = pos;
    }

  }
}
