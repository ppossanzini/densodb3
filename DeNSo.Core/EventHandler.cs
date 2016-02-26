using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Linq.Expressions;
using System.Diagnostics;
using DeNSo;
using DeNSo.DiskIO;

using Newtonsoft.Json.Linq;
using System.ComponentModel.Composition;

namespace DeNSo
{
  public static class EventHandlerManager
  {
    private static Dictionary<string, List<ICommandHandler>> _commandHandlers = new Dictionary<string, List<ICommandHandler>>();
    private static List<Action<IStore, EventCommand>> _globaleventhandlers = new List<Action<IStore, EventCommand>>();

    internal static void AnalyzeCommandHandlers(ICommandHandler[] handlers)
    {
      LogWriter.LogMessage("Start analyzing and preparing command handlers", EventLogEntryType.Warning);
      foreach (var hand in handlers)
      {
        LogWriter.LogMessage(string.Format("Registering command handler {0}", hand.GetType().Name), EventLogEntryType.Information);
        var attrs = hand.GetType().GetCustomAttributes(typeof(DeNSo.HandlesCommandAttribute), true);

        foreach (var at in attrs)
        {
          string commandname = ((DeNSo.HandlesCommandAttribute)at).Command;
          if (!_commandHandlers.ContainsKey(commandname))
            _commandHandlers.Add(commandname, new List<ICommandHandler>());
          _commandHandlers[commandname].Add(hand);
          LogWriter.LogMessage(string.Format(" Handler registered for command {0}", commandname), EventLogEntryType.SuccessAudit);
        }
      }
    }

    public static void RegisterGlobalEventHandler(Action<IStore, EventCommand> eventhandler)
    {
      LogWriter.LogMessage("Registering a global event handler", EventLogEntryType.SuccessAudit);
      _globaleventhandlers.Add(eventhandler);
    }

    public static void UnRegisterGlobalEventHandler(Action<IStore, EventCommand> eventhandler)
    {
      LogWriter.LogMessage("Unregistering a global event handler", EventLogEntryType.SuccessAudit);
      _globaleventhandlers.Remove(eventhandler);
    }

    internal static void ExecuteCommandEvent(string database, EventCommand waitingevent)
    {
      var store = new ObjectStoreWrapper(database);

      foreach (var ge in _globaleventhandlers)
      {
        try
        {
          if (ge != null) ge(store, waitingevent);
        }
        catch (Exception ex)
        {
          LogWriter.LogException(ex);
        }
      }

      var command = JObject.Parse(waitingevent.Command);

      var currenthandlers = ChechHandlers(command);
      if (currenthandlers != null)
        foreach (var hh in currenthandlers)
        {
          try
          {
            hh.HandleCommand(store, command, waitingevent.Data);
          }
          catch (Exception ex)
          {
            LogWriter.LogException(ex);
          }
        }
    }

    private static ICommandHandler[] ChechHandlers(JObject command)
    {
      string actionname = string.Empty;
      var r = command.Property(CommandKeyword.Action);
      if (r != null)
        actionname = ((string)r ?? string.Empty).ToString();

      if (_commandHandlers.ContainsKey(actionname))
        return _commandHandlers[actionname].ToArray();

      return null;
    }

  }
}
