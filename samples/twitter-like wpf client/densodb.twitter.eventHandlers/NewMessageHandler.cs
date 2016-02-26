using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using denso.twitterlike.model;
using System.ComponentModel.Composition;
using DeNSo;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.IO;

namespace densodb.twitter.eventHandlers
{
  // In this example we assume the server is server-side or messages are dispatched to ALL nodes in the P2P mesh.

  // The command Attribute, let you intercept event messages BEFORE the Default event handlers. 
  // So you can customize event handlers logic and write your own server-side procedures. 
  [HandlesCommand(DensoBuiltinCommands.Set)]
  [Export(typeof(ICommandHandler))]
  public class MessageHandler : DeNSo.EventHandlers.BaseCommandHandler
  {

    private static JsonSerializer _serializer = new JsonSerializer();

    public override void OnHandle(IStore store, string collection, JObject command, byte[] document)
    {
      // IStore interface gives you a lowlevel access to DB Structure, 
      // Every action you take now will jump directly into DB without any event dispatching

      // The Istore is preloaded from densodb, and you should not have access to densodb internals.

      // Now deserialize message from Bson object. 
      // should be faster using BsonObject directly but this way is more clear. 

      var message = _serializer.Deserialize<Message>(new JsonTextReader(new StreamReader(new MemoryStream(document))));

      // Get the sender UserProfile
      var userprofile = store.GetCollection("users").GetById(message.From).Deserialize<UserProfile>();

      if (userprofile != null)
      {
        // add message to user's messages
        var profilemessages = store.GetCollection(string.Format("messages_{0}", userprofile.UserName));
        profilemessages.Set(message.Id, document);

        // add message to user's wall 
        var profilewall = store.GetCollection(string.Format("wall_{0}", userprofile.UserName));
        profilewall.Set(message.Id, document);

        // Now i have user's follower. 
        foreach (var follower in userprofile.FollowedBy)
        {
          // Get followers's wall 
          var followerwall = store.GetCollection(string.Format("wall_{0}", follower));

          // store the messages in follower's wall. 
          followerwall.Set(message.Id, document);
        }
      }
    }
  }
}
