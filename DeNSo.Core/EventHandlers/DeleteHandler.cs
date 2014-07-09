using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DeNSo;
using System.Diagnostics;
using Newtonsoft.Json.Linq;

#if NETFX_CORE
using System.Composition;
#else
using System.ComponentModel.Composition;
#endif


namespace DeNSo.EventHandlers
{
  [HandlesCommand(DensoBuiltinCommands.Delete)]
  [Export(typeof(ICommandHandler))]
  public class DeleteHandler : BaseCommandHandler
  {
    public override void OnHandle(IStore store,
                                  string collection,
                                  JObject command,
                                  string value)
    {
      IObjectStore st = store.GetCollection(collection);

      JToken r = command.Property(CommandKeyword.Id);
      if (r != null)
      {
        st.Remove((string)r);
        return;
      }

      if (!string.IsNullOrEmpty(value))
      {
        JObject document = JObject.Parse(value);
        if (document != null)
        {
          JToken r2 = document.Property(DocumentMetadata.IdPropertyName);
          if (r2 != null)
          {
            st.Remove((string)r2);
          }
        }
      }
    }
  }
}
