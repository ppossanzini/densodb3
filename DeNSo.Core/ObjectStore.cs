using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.IO;
using System.Threading;


namespace DeNSo
{
  public class ObjectStore : IObjectStore
  {
    internal volatile CSharpTest.Net.Collections.BPlusTree<string, byte[]> _primarystore;
    private string _fullcollectionpath = string.Empty;

    private ManualResetEvent _StoreReady = new ManualResetEvent(false);

    #region Get Commands

    public int Count()
    {
      return LockForRead(() => _primarystore.Count);
    }

    public IEnumerable<string> GetAllKeys()
    {
      return LockForRead(() => _primarystore.Keys);
    }

    public byte[] GetById(string key)
    {
      //if (BloomFilterCheck(key))
      return LockForRead(() =>
      {
        byte[] result = null;
        _primarystore.TryGetValue(key, out result);
        return result;
      });
      //return null;
    }

    public int Len(string key)
    {
      var result = GetById(key);
      return result != null ? result.Length : 0;
    }

    public IEnumerable<byte[]> MultipleGet(params string[] keys)
    {
      foreach (var k in keys)
        yield return GetById(k);
      yield break;
    }

    #endregion

    #region Set Commands

    public void Append(string key, byte[] value)
    {
      var result = GetById(key) ?? new byte[0];
      var newvalue = new byte[result.Length + value.Length];

      System.Buffer.BlockCopy(result, 0, newvalue, 0, result.Length);
      System.Buffer.BlockCopy(value, 0, newvalue, result.Length, value.Length);

      Set(key, value);
    }

    public byte[] GetSet(string key, byte[] value)
    {
      var result = GetById(key);
      Set(key, value);
      return result;
    }

    public void Remove(string key)
    {
      LockForWrite(() =>
      {
        if (_primarystore.ContainsKey(key))
          _primarystore.Remove(key);
      });
    }

    public void Set(string key, byte[] value)
    {
      if (string.IsNullOrEmpty(key)) return;

      LockForWrite(() =>
      {
        _primarystore[key] = value;
      });
    }

    public void Flush()
    {
      LockForWrite(() =>
      {
        _primarystore.Clear();
      });
    }

    #endregion

    #region Lock Methods
    private T LockForRead<T>(Func<T> method)
    {
      _StoreReady.WaitOne();
      var result = method();
      return result;
    }

    private void LockForRead(Action method)
    {
      _StoreReady.WaitOne();
      method();
    }

    private T LockForWrite<T>(Func<T> method)
    {
      _StoreReady.WaitOne();
      var result = method();
      return result;
    }

    private void LockForWrite(Action method)
    {
      _StoreReady.WaitOne();
      method();
    }
    #endregion

    //internal bool BloomFilterCheck(string key)
    //{
    //  lock (_bloomfilter)
    //    return _bloomfilter.Contains(key);
    //}

    //internal void BloomFilterAdd(string key)
    //{
    //  lock (_bloomfilter)
    //    _bloomfilter.Add(key);
    //}

    //private string CalculateMD5Hash(string input)
    //{
    //  byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
    //  byte[] hash = md5.ComputeHash(inputBytes);

    //  // step 2, convert byte array to hex string
    //  StringBuilder sb = new StringBuilder();
    //  for (int i = 0; i < hash.Length; i++)
    //  {
    //    sb.Append(hash[i].ToString("X2"));
    //  }
    //  return sb.ToString();
    //}

    //private string WriteDocument(string key, byte[] doc)
    //{
    //  var relpath = CalculateMD5Hash(key).Substring(0, 2);
    //  var dir = Path.Combine(_fullcollectionpath, relpath);
    //  if (!Directory.Exists(dir))
    //  {
    //    Directory.CreateDirectory(dir);
    //    new DirectoryInfo(dir).Attributes = FileAttributes.System | FileAttributes.Archive;
    //  }

    //  var filename = Path.Combine(relpath, key);
    //  File.WriteAllBytes(Path.Combine(_fullcollectionpath, filename), doc);
    //  return filename;
    //}

    //private byte[] ReadDocument(string position)
    //{
    //  return File.ReadAllBytes(Path.Combine(_fullcollectionpath, position));
    //}

    //private void RemoveDocument(string position)
    //{
    //  File.Delete(Path.Combine(_fullcollectionpath, position));
    //}

    [Description("Internal Use ONLY")]
    internal void CloseCollection()
    {
      LockForWrite(() =>
      {
        if (_primarystore != null)
        {
          //_primarystore.Commit();
          _primarystore.UnloadCache();
          _primarystore.Commit();
          _primarystore.Dispose();
          _primarystore = null;
        }
      });
    }

    internal void OpenCollection(string database, string collection, string basepath = null)
    {
      var dir = Path.Combine(basepath ?? Configuration.GetBasePath(), database);
      _fullcollectionpath = Path.Combine(dir, collection + ".coll");
      var options = new BPlusTree<string, byte[]>.Options(PrimitiveSerializer.String, PrimitiveSerializer.Bytes);

      if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);

      options.CreateFile = CreatePolicy.IfNeeded;
      options.FileName = _fullcollectionpath;
      //options.StoragePerformance = StoragePerformance.Fastest;
      options.FileGrowthRate = 1024;
      options.FileBlockSize = 4096;
      //options.TransactionLogFileName = _fullcollectionpath + ".tlog";
      options.CachePolicy = CachePolicy.Recent;
      //options.TransactionLogLimit = 640 * 1024 * 1024; // 64MByte


      _primarystore = new CSharpTest.Net.Collections.BPlusTree<string, byte[]>(options);
      _primarystore.EnableCount();

      _StoreReady.Set();
    }
  }
}

