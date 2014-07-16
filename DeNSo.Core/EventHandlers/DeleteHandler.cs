using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DeNSo;
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using System.ComponentModel.Composition;

namespace DeNSo.EventHandlers
{
  [HandlesCommand(DensoBuiltinCommands.Delete)]
  [Export(typeof(ICommandHandler))]
  public class DeleteHandler : BaseCommandHandler
  {
    public override void OnHandle(IStore store,
                                  string collection,
                                  JObject command,
                                  byte[] value)
    {
      IObjectStore st = store.GetCollection(collection);

      JToken r = command.Property(CommandKeyword.Id);
      if (r != null)
      {
        st.Remove((string)r);
        return;
      }
    }
  }
}
