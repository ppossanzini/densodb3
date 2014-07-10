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

    private Thread _eventHandlerThread = null;

    public long LastExecutedCommandSN { get; private set; }

    internal ManualResetEvent CommandsReady = new ManualResetEvent(false);
    internal ManualResetEvent LoadCompleted = new ManualResetEvent(false);

    public string DatabaseName { get; private set; }

    internal EventStore(string dbname, long lastcommittedcommandsn)
    {
      DatabaseName = dbname;

      LastExecutedCommandSN = lastcommittedcommandsn;

      JournalReader _jreader = new JournalReader(FileManager.GetLogFile(Configuration.GetBasePath(), dbname));
      long jsn = LoadUncommittedEventsFromJournal(_jreader);

      CommandsReady.Set();

      _eventHandlerThread = new Thread(new ThreadStart(ExecuteEventCommands));
      _eventHandlerThread.Start();

      LoadCompleted.WaitOne();

      _jwriter = new JournalWriter(FileManager.GetLogFile(Configuration.GetBasePath(), dbname));

      if (Configuration.EnableOperationsLog)
        _operationsLog = Configuration.BasePath.Select(path => new JournalWriter(path, dbname, true)).ToArray();

      // The journal can be empty so i have to evaluate the last committed command serial number 
      // and reset Command Serial number in the journal to ensure command execution coherency.
      foreach (var w in _jwriter)
        w.CommandSN = Math.Max(jsn, lastcommittedcommandsn);
    }

    internal long LoadUncommittedEventsFromJournal(JournalReader _jreader)
    {
      long journalsn = 0;

      while (_jreader.HasCommandsPending())
      {
        var cmd = _jreader.ReadCommand();
        if (cmd != null)
        {
          if (cmd.CommandSN > LastExecutedCommandSN)
          {
            _waitingevents.Enqueue(cmd);
          }
          journalsn = Math.Max(journalsn, cmd.CommandSN);
        }
      }
      return journalsn;
    }

    private void ExecuteEventCommands()
    {
      while (!StoreManager.ShuttingDown)
      {
        CommandsReady.WaitOne(5000);
        if (_waitingevents.Count == 0)
        {
          CommandsReady.Reset();
          LoadCompleted.Set();
          continue;
        }

        EventCommand we;
        lock (_waitingevents)
          we = _waitingevents.Dequeue();

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
