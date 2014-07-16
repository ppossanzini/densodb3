﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo
{
  public class CompressEnabledWebClient : WebClient
  {
    protected override WebRequest GetWebRequest(Uri address)
    {
      HttpWebRequest request = base.GetWebRequest(address) as HttpWebRequest;
      request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
      return request;
    }
  }
}