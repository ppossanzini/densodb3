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
    private FileStream _logfile = null;
    private MemoryStream _logbuffer = null;
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
      if (OpenLogFile())
      {
        CommandSN++;

#if !NETFX_CORE

        if (_ensureatomicwrites)
          _logfile.Flush();
#endif

        _logbuffer.Seek(0, SeekOrigin.Begin);
        _logbuffer.SetLength(0);

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

        // this should be an atomic operation due to filestream flag used during open.
#if NETFX_CORE
            _logwriter.WriteBytes(_logbuffer.ToArray());
#else

        _logfile.Write(_logbuffer.ToArray(), 0, (int)_logbuffer.Length);
        _logfile.Flush(_ensureatomicwrites);

        if (_logfile.Length - _logfile.Position < _logfilefreespace)
        {
          IncreaseFileSize();
        }
#endif
      }
      Monitor.Exit(_filemonitor);
      return CommandSN;
    }

    private void CloseFile()
    {

#if NETFX_CORE
      _writer.Flush();
      await _logfile.CommitAsync();
#else
      Monitor.Enter(_filemonitor);
      if (_logfile != null)
      {
        //_logfile.SetLength(_logfile.Position);

        _logfile.Flush(true);
        _writer.Flush();
        _writer.Close();
        _logfile.Close();

        _logfile = null;
        _writer = null;
      }
      Monitor.Exit(_filemonitor);
#endif


    }

    private bool OpenLogFile()
    {
      try
      {
        if (_logfile == null)
        {
#if !NETFX_CORE
          Monitor.Enter(_filemonitor);
#endif

#if WINDOWS_PHONE
          _logfile = iss.OpenFile(FileName, FileMode.Append, FileAccess.Write, FileShare.Read);
#else

#if NETFX_CORE
          var file = await iss.GetFileAsync(FileName);
          _logfile = await file.OpenTransactedWriteAsync();
          _logwriter = new Windows.Storage.Streams.DataWriter(_logfile.Stream);
#else
          int trycount = 0;
          while (_logfile == null && trycount < 20)
            try
            {
              _logfile = new FileStream(FileName,
                                        FileMode.Append,
                                        System.Security.AccessControl.FileSystemRights.AppendData,
                                        FileShare.ReadWrite, 4096,
                                        FileOptions.None);
            }
            catch (Exception ex)
            {
              trycount++;
              Thread.Sleep(1000);
            }
          //_logfile = File.Open(FileName, FileMode.Append, FileAccess.Write, FileShare.Read);
#endif
#endif
          //IncreaseFileSize();
          _logbuffer = new MemoryStream();
          _writer = new BinaryWriter(_logbuffer);

          if (_logfile != null)
            LogWriter.LogMessage(string.Format("Log File ready: {0}", FileName), LogEntryType.Information);
          else
          {
            LogWriter.LogMessage(string.Format("Unable to open logfile: {0}", FileName), LogEntryType.Error);
            //Server.EmergencyShutdown();
          }

          JournalWriter.Closing += (s, e) => CloseFile();
#if !NETFX_CORE
          Monitor.Exit(_filemonitor);
#endif
        }
      }
      catch (Exception ex)
      {
        LogWriter.LogException(ex);
        //Server.EmergencyShutdown();
        return false;
      }
      return true;
    }

#if NETFX_CORE
    internal async void ShrinkToSN(long commandsn)
#else
    internal void ShrinkToSN(long commandsn)
#endif
    {
      LogWriter.LogInformation("File Shrink requested", LogEntryType.Warning);
      //return;

      try
      {
#if !NETFX_CORE
        if (Monitor.TryEnter(_filemonitor, 100))
        {
#endif
          CloseFile();

#if WINDOWS_PHONE
     
        if (iss.FileExists(FileName))
          using (var readerfs = iss.OpenFile(FileName, FileMode.Open, FileAccess.Read, FileShare.Write))
          using (var writerfs = iss.OpenFile(FileName, FileMode.Open, FileAccess.Write, FileShare.Read))
#else
#if NETFX_CORE
      var file = await iss.GetFileAsync(FileName);

      Stream readerfs, writerfs;

      if (file != null)
        using (readerfs = writerfs = (await file.OpenStreamForWriteAsync()))
#else

          if (File.Exists(FileName))
            using (var readerfs = File.Open(FileName, FileMode.Open, FileAccess.Read, FileShare.Write))
            using (var writerfs = File.Open(FileName, FileMode.Open, FileAccess.Write, FileShare.Read))
#endif
#endif

            {
              using (var br = new BinaryReader(readerfs))
              using (var bw = new BinaryWriter(writerfs))
              {
                LogWriter.LogInformation("Compressing file", LogEntryType.Information);
                while (readerfs.Position < readerfs.Length)
                {
                  var cmd = ReadCommand(br);
                  if (cmd != null)
                    if (cmd.CommandSN > commandsn)
                      WriteCommand(bw, cmd);
                }

                LogWriter.LogInformation("File shrink completed", LogEntryType.SuccessAudit);
#if !NETFX_CORE
                readerfs.Close();
#endif
                writerfs.Flush();
                LogWriter.LogInformation("Free empty space", LogEntryType.SuccessAudit);
                writerfs.SetLength(writerfs.Position);
#if !NETFX_CORE
                writerfs.Close();
#endif

                LogWriter.LogInformation("Shringk completed", LogEntryType.SuccessAudit);
              }
#if ! NETFX_CORE
            }
          Monitor.Exit(_filemonitor);
#endif
        }
      }
      catch { }
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

    private EventCommand ReadCommand(BinaryReader br)
    {
      var k = br.ReadChar();
      if (k == 'K')
      {
        var csn = br.ReadInt64();
        var marker = br.ReadString();
        var d = br.ReadChar();
        if (d == 'D')
        {
          string command = br.ReadString();
          string data = br.ReadString();
          return new EventCommand() { Command = command, CommandSN = csn, CommandMarker = marker, Data = data };
        }
      }
      return null;
    }
  }
}
