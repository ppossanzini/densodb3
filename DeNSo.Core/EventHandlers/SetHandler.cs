using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DeNSo;
using Newtonsoft.Json.Linq;

#if NETFX_CORE
using System.Composition;
#else
using System.ComponentModel.Composition;
#endif


namespace DeNSo.EventHandlers
{
  [HandlesCommand(DensoBuiltinCommands.Set)]
  [Export(typeof(ICommandHandler))]
  public class SetHandler : BaseCommandHandler
  {
    public override void OnHandle(IStore store,
                                  string collection,
                                  JObject command,
                                  byte[] document)
    {
      IObjectStore st = store.GetCollection(collection);
      if (document != null)
      {
        JToken r = command.Property(CommandKeyword.Id);
        st.Set((string)r, document);
        return;
      }
    }
  }
}
