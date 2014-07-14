﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.Core
{
  public static class StringCompressor
  {
    public static byte[] Compress(this string value)
    {
      var memoryStream = new MemoryStream();
      using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
      using (var sw = new StreamWriter(gZipStream))
        sw.Write(value);

      return memoryStream.ToArray();
    }

    public static string Decompress(this byte[] value)
    {
      using (var memoryStream = new MemoryStream(value))
      using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress))
      using (var sr = new StreamReader(gZipStream))
        return sr.ReadToEnd();
    }
  }
}