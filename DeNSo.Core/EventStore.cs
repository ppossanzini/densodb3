using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using DeNSo.DiskIO;

using System.Diagnostics;
using System.IO;
using DeNSo.Core.DiskIO;

namespace DeNSo
{
  public class EventStore
  {
    internal JournalWriter _jwriter = null;
    internal JournalWriter _operationsLog = null;
    internal JournalReader _jreader = null;

    private Thread _eventHandlerThread = null;

    public long LastExecutedCommandSN { get; private set; }

    internal ManualResetEvent CommandsReady = new ManualResetEvent(false);
    internal ManualResetEvent LoadCompleted = new ManualResetEvent(false);

    public string DatabaseName { get; private set; }

    internal EventStore(string dbname, long lastcommittedcommandsn)
    {
      DatabaseName = dbname;

      LastExecutedCommandSN = lastcommittedcommandsn;

      _jreader = new JournalReader(FileManager.GetLogFile(Configuration.GetBasePath(), dbname));
      long jsn = MoveToUncommittedEventsFromJournal(_jreader);

      CommandsReady.Set();

      _eventHandlerThread = new Thread(new ThreadStart(ExecuteEventCommands));
      _eventHandlerThread.Start();

      LoadCompleted.WaitOne();

      _jwriter = new JournalWriter(FileManager.GetLogFile(Configuration.GetBasePath(), dbname));

      if (Configuration.EnableOperationsLog)
        _operationsLog = new JournalWriter(FileManager.GetLogFile(Configuration.GetBasePath(), dbname, isoperationlog: true));

      _jwriter.CommandSN = Math.Max(jsn, lastcommittedcommandsn);
    }

    internal long MoveToUncommittedEventsFromJournal(JournalReader _jreader)
    {
      long journalsn = _jreader.SeekToSN(LastExecutedCommandSN);
      return journalsn;
    }

    private void ExecuteEventCommands()
    {
      while (!StoreManager.ShuttingDown)
      {
        CommandsReady.WaitOne(5000);
        if (!_jreader.HasCommandsPending())
        {
          CommandsReady.Reset();
          LoadCompleted.Set();
          continue;
        }

        EventCommand we = _jreader.ReadCommand();
        EventHandlerManager.ExecuteCommandEvent(DatabaseName, we);

        LastExecutedCommandSN = we.CommandSN;

        if (Configuration.EnableOperationsLog)
          _operationsLog.LogCommand(we);
      }
    }

    public long Enqueue(string command, byte[] data)
    {
      var cmd = new EventCommand() { Command = command, Data = data };
      return Enqueue(cmd);
    }

    public long Enqueue(EventCommand command)
    {
      command.CommandSN = _jwriter.LogCommand(command);
      CommandsReady.Set();
      return command.CommandSN;
    }
  }
}
