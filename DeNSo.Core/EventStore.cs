using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using DeNSo.DiskIO;

using System.Diagnostics;
using System.IO;

namespace DeNSo
{
  public class EventStore
  {
    internal JournalWriter[] _jwriter = null;
    //internal JournalReader _jreader = null;

    internal JournalWriter[] _operationsLog = null;

#if WINDOWS_PHONE
    internal System.IO.IsolatedStorage.IsolatedStorageFile iss = System.IO.IsolatedStorage.IsolatedStorageFile.GetUserStoreForApplication();
#endif

    private volatile Queue<EventCommand> _waitingevents = new Queue<EventCommand>();

#if NETFX_CORE
    private System.Threading.Tasks.Task _eventHandlerThread = null;
#else
    private Thread _eventHandlerThread = null;
#endif

    public long LastExecutedCommandSN { get; private set; }

    internal ManualResetEvent CommandsReady = new ManualResetEvent(false);
    internal ManualResetEvent LoadCompleted = new ManualResetEvent(false);

    public string DatabaseName { get; private set; }

    internal EventStore(string dbname, long lastcommittedcommandsn)
    {
      DatabaseName = dbname;

      LastExecutedCommandSN = lastcommittedcommandsn;

      JournalReader _jreader = new JournalReader(Configuration.GetBasePath(), dbname);
      long jsn = LoadUncommittedEventsFromJournal(_jreader);
      _jreader.CloseFile();

#if NETFX_CORE
      System.Threading.Tasks.Task.Factory.StartNew(ExecuteEventCommands);
#else
      CommandsReady.Set();

      _eventHandlerThread = new Thread(new ThreadStart(ExecuteEventCommands));
      _eventHandlerThread.Start();

      LoadCompleted.WaitOne();
#endif

      _jwriter = Configuration.BasePath.Select(path => new JournalWriter(path, dbname)).ToArray();

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
        //Debug.Write(string.Format("step1 : {0}", DateTime.Now.ToString("ss:ffff")));
        CommandsReady.WaitOne(5000);
        //Debug.Write(string.Format("step2 : {0}", DateTime.Now.ToString("ss:ffff")));
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

        //if (Debugger.IsAttached)
        //  if (LastExecutedCommandSN % 1000 == 0)
        //    Console.Write(string.Format("LEC: {0} - ", LastExecutedCommandSN));

        if (Configuration.EnableOperationsLog)
#if NETFX_CORE
          _operationsLog.LogCommand(we).Wait();
#else
          foreach (var w in _operationsLog)
            w.LogCommand(we);
#endif

        //if (_waitingevents.Count == 0)
        //  Session.RaiseStoreUpdated(LastExecutedCommandSN);

      }
    }

    public long Enqueue(string command, string data)
    {
      var cmd = new EventCommand() { Command = command, Data = data };
      return Enqueue(cmd);
    }

    public long Enqueue(EventCommand command)
    {
      long csn = 0;
      foreach (var w in _jwriter)
        csn = w.LogCommand(command);
#if NETFX_CORE
      csn.Wait();
      command.CommandSN = csn.Result;
#else
      command.CommandSN = csn;
#endif

      lock (_waitingevents)
        _waitingevents.Enqueue(command);

      CommandsReady.Set();

      return command.CommandSN;
    }

    public void ShrinkEventStore()
    {
      if (_jwriter != null)
        foreach (var w in _jwriter)
          w.ShrinkToSN(LastExecutedCommandSN);
    }
  }
}
