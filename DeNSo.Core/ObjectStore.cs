using DeNSo;
using DeNSo.Core;
using DeNSo.Core.Filters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.IO.IsolatedStorage;
using System.ComponentModel;

namespace DeNSo
{

  public class ObjectStore : IObjectStore
  {
    private int _indexpossibleincoerences = 0;
    private volatile BloomFilter<string> _bloomfilter = new BloomFilter<string>(Configuration.DictionarySplitSize * 2);

    internal volatile C5.TreeDictionary<string, byte[]> _primarystore = new C5.TreeDictionary<string, byte[]>();
    private string _fullcollectionpath = string.Empty;

    public int ChangesFromLastSave { get; set; }
    public long LastEventSN { get; internal set; }

    public int Count()
    {
      return _primarystore.Count;
    }

    public int Count(Func<JObject, bool> filter)
    {
      //int count = 0;
      //if (filter != null)
      //  foreach (var d in _primarystore.Values)
      //  {
      //    if (!string.IsNullOrEmpty(d) && filter(JObject.Parse(d)))
      //      count++;
      //  }

      //return count;
      return Count();
    }

    public IEnumerable<byte[]> GetAll()
    {
      lock (_primarystore)
        return _primarystore.Values;
    }

    public IEnumerable<string> GetAllKeys()
    {
      lock (_primarystore)
        return _primarystore.Keys.AsEnumerable();
    }

    public byte[] GetById(string key)
    {
      return InternalDictionaryGet(key);
    }

    public void Flush()
    {
      _primarystore.Clear();
    }

    public float IncoerenceIndexRatio()
    {
      return (((float)Math.Max(this.Count() - _bloomfilter.Size, 0) + (float)_indexpossibleincoerences) / (float)this.Count()) * 100;
    }

    public void Remove(string key)
    {
      if (InternalDictionaryContains(key))
        if (InternalDictionaryRemove(key))
          ChangesFromLastSave++;
    }

    public void Reindex()
    {
      lock (_bloomfilter)
      {
        var newsize = this.Count() + Configuration.DictionarySplitSize * 2;
        var newbloom = new BloomFilter<string>(newsize);
        foreach (var k in _primarystore.Keys)
          newbloom.Add(k);

        _bloomfilter = newbloom;
        _indexpossibleincoerences = 0;
      }
    }

    public void Set(string key, byte[] document)
    {
      ChangesFromLastSave++;

      if (!string.IsNullOrEmpty(key))
      {
        if (InternalDictionaryContains(key))
        {
          InternalDictionaryUpdate(key, document);
          return;
        }
        InternalDictionarySet(key, document);
      }
    }

    //public IEnumerable<byte[]> Where(Func<JObject, bool> filter)
    //{
    //  if (filter != null)
    //    lock (_primarystore)
    //      return _primarystore.Values.Where(v => !string.IsNullOrEmpty(v) && filter(JObject.Parse(v))).ToList();

    //  return null;
    //}

    private byte[] InternalDictionaryGet(string key)
    {
      if (BloomFilterCheck(key))
        if (_primarystore.Contains(key))
          return _primarystore[key];
      return null;
    }

    internal void InternalDictionaryInsert(string key, byte[] doc)
    {
      lock (_primarystore)
        if (!_primarystore.Contains(key))
          _primarystore.Add(key, doc);

      BloomFilterAdd(key);
    }

    private void InternalDictionarySet(string key, byte[] doc)
    {
      lock (_primarystore)
        if (_primarystore.Contains(key))
        {
          _primarystore[key] = doc;
          return;
        }

      lock (_primarystore)
        _primarystore.Add(key, doc);

      BloomFilterAdd(key);
    }

    private bool InternalDictionaryUpdate(string key, byte[] doc)
    {
      lock (_primarystore)
        if (_primarystore.Contains(key))
        {
          _primarystore[key] = doc;
          return true;
        }
      return false;
    }

    private bool InternalDictionaryContains(string key)
    {
      if (!string.IsNullOrEmpty(key))
        if (BloomFilterCheck(key))
          return _primarystore.Contains(key);
      return false;
    }

    private bool InternalDictionaryRemove(string key)
    {
      lock (_primarystore)
      {
        if (_primarystore.Contains(key))
        {
          _primarystore.Remove(key);
          _indexpossibleincoerences++;
          return true;
        }
      }
      return false;
    }

    private bool BloomFilterCheck(string key)
    {
      lock (_bloomfilter)
        return _bloomfilter.Contains(key);
    }

    private void BloomFilterAdd(string key)
    {
      lock (_bloomfilter)
        _bloomfilter.Add(key);
    }

    [Description("Internal Use ONLY")]
    public void SaveCollection()
    {
      if (ChangesFromLastSave > 0)
      {
        if (!Directory.Exists(Path.GetDirectoryName(_fullcollectionpath)))
          Directory.CreateDirectory(Path.GetDirectoryName(_fullcollectionpath));

        using (var file = File.Open(_fullcollectionpath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
        {
          using (var writer = new BinaryWriter(file))
          {
            lock (_primarystore)
              foreach (var item in _primarystore)
              {
                writer.Write((int)item.Value.Length); // Data Lenght
                //writer.Write((byte)item.Key.Length);
                writer.Write(item.Key); // Database _id
                writer.Write(item.Value); // Data
              }
            writer.Flush();
            file.Flush();
            file.SetLength(file.Position);
          }
        }
      }
    }

    [Description("Internal Use ONLY")]
    public void LoadCollection(string database, string collection, string basepath = null)
    {
      _fullcollectionpath = Path.Combine(Path.Combine(basepath ?? Configuration.GetBasePath(), database), collection + ".coll");
      if (File.Exists(_fullcollectionpath))
        using (var fs = File.Open(_fullcollectionpath, FileMode.Open, FileAccess.Read, FileShare.Read))
          try
          {
            using (var br = new BinaryReader(fs))
              while (fs.Position < fs.Length)
              {
                var len = br.ReadInt32();
                //var klen = br.ReadByte();
                var id = br.ReadString();
                var data = br.ReadBytes(len);

                this.InternalDictionaryInsert(id, data);
              }
          }
          catch (OutOfMemoryException ex)
          {
            LogWriter.LogException(ex);
          }
    }
  }
}
