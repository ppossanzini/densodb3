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

#if NETFX_CORE
#else
using System.IO.IsolatedStorage;
using System.ComponentModel;
#endif

namespace DeNSo
{
  // Changes list. 
  // 2012-04-23 -> converted storekey from int to byte[] to store multiple types value. 

  public class ObjectStore : IObjectStore
  {
    private int _indexpossibleincoerences = 0;
    private volatile BloomFilter<string> _bloomfilter = new BloomFilter<string>(Configuration.DictionarySplitSize * 2);

    //internal volatile  List<Dictionary<string, string>> _primarystore = new List<Dictionary<string, string>>();
    internal volatile C5.TreeDictionary<string, string> _primarystore = new C5.TreeDictionary<string, string>();
    private string _fullcollectionpath = string.Empty;

    public int ChangesFromLastSave { get; set; }
    public long LastEventSN { get; internal set; }

    public int Count()
    {
      return _primarystore.Count;
      //var result = 0;
      //foreach (var d in _primarystore)
      //  lock (d)
      //    result += d.Count;

      //return result;
    }

    public int Count(Func<JObject, bool> filter)
    {
      int count = 0;
      if (filter != null)
        foreach (var d in _primarystore.Values)
        {
          if (!string.IsNullOrEmpty(d) && filter(JObject.Parse(d)))
            count++;
        }

      return count;
    }

    public IEnumerable<string> GetAll()
    {
      //_primarystore.Values.AsEnumerable();
      lock (_primarystore)
        return _primarystore.Values;
      //  foreach (var v in _primarystore.Values)
      //  {
      //    yield return v;
      //  }
      //yield break;

      //  //  foreach (var v in d.Values)
      //  //  {
      //  //    if (!string.IsNullOrEmpty(v))
      //  yield return v;
      //  //}
      //}

      //yield break;
    }

    public string GetById(string key)
    {
      return InternalDictionaryGet(key);
    }

    public void Flush()
    {
      _primarystore.Clear();
      //foreach (var d in _primarystore)
      //{
      //  lock (d)
      //    d.Clear();
      //}
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
          //foreach (var k in d.Keys)
          newbloom.Add(k);

        _bloomfilter = newbloom;
        _indexpossibleincoerences = 0;
      }
    }

    public void Set(string key, string document)
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

    public IEnumerable<string> Where(Func<JObject, bool> filter)
    {
      if (filter != null)
        lock (_primarystore)
          return _primarystore.Values.Where(v => !string.IsNullOrEmpty(v) && filter(JObject.Parse(v))).ToList();

      return null;
    }

    private string GetEntityUI(JObject document)
    {
      var r = document.Property(DocumentMetadata.IdPropertyName);
      var newkey = ((string)r ?? Guid.NewGuid().ToString());
      document[DocumentMetadata.IdPropertyName] = newkey;
      return newkey;
    }

    private string InternalDictionaryGet(string key)
    {
      if (BloomFilterCheck(key))
        if (_primarystore.Contains(key))
          return _primarystore[key];
      //foreach (var d in _primarystore)
      //{
      //  if (d.ContainsKey(key))
      //    return d[key];
      //}
      return null;
    }

    internal void InternalDictionaryInsert(string key, string doc)
    {
      //Dictionary<string, string> freedictionary = null;
      //foreach (var d in _primarystore)
      //  lock (d)
      //    if (d.Count < Configuration.DictionarySplitSize)
      //    {
      //      freedictionary = d; break;
      //    }

      //if (freedictionary == null)
      //{
      //  freedictionary = new Dictionary<string, string>();
      //  _primarystore.Add(freedictionary);
      //}

      //lock (freedictionary)
      //  if (!freedictionary.ContainsKey(key))
      //    freedictionary.Add(key, doc);

      lock (_primarystore)
        if (!_primarystore.Contains(key))
          _primarystore.Add(key, doc);

      BloomFilterAdd(key);
    }

    private void InternalDictionarySet(string key, string doc)
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

    private bool InternalDictionaryUpdate(string key, string doc)
    {
      //if (BloomFilterCheck(key))
      lock (_primarystore)
        if (_primarystore.Contains(key))
        {
          //foreach (var d in _primarystore)
          //  lock (d)
          //if (d.ContainsKey(key))
          //{
          _primarystore[key] = doc;
          return true;
        }
      //}
      return false;
    }

    private bool InternalDictionaryContains(string key)
    {
      if (!string.IsNullOrEmpty(key))
        if (BloomFilterCheck(key))
          return _primarystore.Contains(key);
      //foreach (var d in _primarystore)
      //{
      //  lock (d)
      //    if (d.ContainsKey(key))
      //      return true;
      //}
      return false;
    }

    private bool InternalDictionaryRemove(string key)
    {
      //if (BloomFilterCheck(key))
      lock (_primarystore)
      {
        if (_primarystore.Contains(key))
        {
          _primarystore.Remove(key);
          _indexpossibleincoerences++;
          return true;
        }
        //Dictionary<string, string> realdictionary = null;
        //foreach (var d in _primarystore)
        //  lock (d)
        //    if (d.ContainsKey(key))
        //    {
        //      realdictionary = d; break;
        //    }

        //lock (realdictionary)
        //  if (realdictionary != null)
        //  {
        //    realdictionary.Remove(key);
        //    _indexpossibleincoerences++;
        //    return true;
        //  }
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
                var data = br.ReadString();

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
