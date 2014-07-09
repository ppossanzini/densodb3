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
  [HandlesCommand(DensoBuiltinCommands.SetMany)]
  [Export(typeof(ICommandHandler))]
  public class SetManyHandler : BaseCommandHandler
  {
    public override void OnHandle(IStore store,
                                  string collection,
                                  JObject command,
                                  string value)
    {
      IObjectStore st = store.GetCollection(collection);
      JObject document = JObject.Parse(value);
      if (document.Type == JTokenType.Array)
      {
        var documents = document.Values();
        if (documents != null)
          foreach (JObject d in documents)
          {
            var k = d.Property(DocumentMetadata.IdPropertyName);
            if (k != null)
              st.Set((string)k, d.ToString());
          }

      }
      else
      {
        var k = document.Property(DocumentMetadata.IdPropertyName);
        if (k != null)
          st.Set((string)k, document.ToString());
      }
    }
  }
}
