﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceModel;

namespace DeNSo.P2P.Messages
{
  [MessageContract]
  public class EventMessage
  {
    [MessageBodyMember]
    public Guid NodeIdentity { get; set; }

    [MessageBodyMember]
    public long CommandSN{ get; set; }

    [MessageBodyMember]
    public string Database { get; set; }

    [MessageBodyMember]
    public string Command { get; set; }

    [MessageBodyMember]
    public byte[] Data { get; set; }

    [MessageBodyMember]
    public string CommandMarker { get; set; }

    [PeerHopCount]
    public int MaxHop { get; set; }

  }
}
