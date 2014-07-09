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
using Newtonsoft.Json;
#endif

namespace DeNSo.EventHandlers
{
  [HandlesCommand(DensoBuiltinCommands.Update)]
  [Export(typeof(ICommandHandler))]
  public class UpdateHandler : BaseCommandHandler
  {
    public override void OnHandle(IStore store,
                                  string collection,
                                  JObject command,
                                  string document)
    {
      if (document == null || string.IsNullOrEmpty(collection)) return;
      IObjectStore st = store.GetCollection(collection);

      if (((string)command.Property(DocumentMetadata.IdPropertyName)) != null)
      {
        UpdateSingleDocument(document, (string)command.Property(DocumentMetadata.IdPropertyName), st); return;
      }
    }

    private static void UpdateSingleDocument(string value, string id, IObjectStore store)
    {
      var document = JObject.Parse(value);
      var objid = (string)document[DocumentMetadata.IdPropertyName];
      var obj = JObject.Parse(store.GetById(objid));
      foreach (var p in GetRealProperties(document)) // remove properties starting with  
        obj[p] = document[p];

      store.Set(objid, obj.ToString());
    }

    private static void UpdateCollection(JObject document, IObjectStore store)
    {
      // TODO: completely to do. 
    }
  }
}
