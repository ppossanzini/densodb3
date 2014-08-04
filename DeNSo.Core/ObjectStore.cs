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
using System.Threading;

namespace DeNSo
{

  public class ObjectStore : IObjectStore
  {
    private int _indexpossibleincoerences = 0;
    private volatile BloomFilter<string> _bloomfilter = new BloomFilter<string>(Configuration.DictionarySplitSize * 2);

    internal volatile C5.TreeDictionary<string, long> _primarystore = new C5.TreeDictionary<string, long>();
    private string _fullcollectionpath = string.Empty;

    public int ChangesFromLastSave { get; set; }
    public long LastEventSN { get; internal set; }

    private FileStream _readingStream;
    private BinaryReader _reader;
    private FileStream _writingStream;
    private BinaryWriter _writer;

    private ManualResetEvent _StoreReady = new ManualResetEvent(false);

    public int Count()
    {
      return _primarystore.Count;
    }

    public IEnumerable<string> GetAllKeys()
    {
      _StoreReady.WaitOne();
      lock (_primarystore)
        return _primarystore.Keys.ToArray();
    }

    public byte[] GetById(string key)
    {
      _StoreReady.WaitOne();
      return InternalDictionaryGet(key);
    }

    public void Flush()
    {
      _StoreReady.WaitOne();
      _primarystore.Clear();
    }

    public float IncoerenceIndexRatio()
    {
      return (((float)Math.Max(this.Count() - _bloomfilter.Size, 0) + (float)_indexpossibleincoerences) / (float)this.Count()) * 100;
    }

    public void Remove(string key)
    {
      _StoreReady.WaitOne();
      if (InternalDictionaryContains(key))
        if (InternalDictionaryRemove(key))
          ChangesFromLastSave++;
    }

    public void Reindex()
    {
      _StoreReady.WaitOne();
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
      _StoreReady.WaitOne();
      ChangesFromLastSave++;

      if (!string.IsNullOrEmpty(key))
      {
        InternalDictionarySet(key, document);
      }
    }

    private byte[] InternalDictionaryGet(string key)
    {
      if (BloomFilterCheck(key))
      {
        lock (_primarystore)
          if (_primarystore.Contains(key))
          {
            return ReadDocument(_primarystore[key]);
          }
      }
      return null;
    }

    private void InternalDictionaryInsert(string key, long position)
    {
      lock (_primarystore)
        if (!_primarystore.Contains(key))
          _primarystore.Add(key, position);

      BloomFilterAdd(key);
    }

    private void InternalDictionarySet(string key, byte[] doc)
    {
      lock (_primarystore)
      {
        var result = WriteDocument(key, doc);
        if (_primarystore.Contains(key))
        {
          _primarystore[key] = result;
          return;
        }

        _primarystore.Add(key, result);
      }

      BloomFilterAdd(key);
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
          StoneDocumentAtPosition(_primarystore[key]);
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

    private long WriteDocument(string key, byte[] doc)
    {
      var position = _writingStream.Position;
      _writer.Write('K');
      _writer.Write(doc.Length);
      _writer.Write(key);
      _writer.Write(doc);
      _writer.Flush();
      return position;
    }

    private void StoneDocumentAtPosition(long position)
    {
      _writingStream.Position = position;
      _writer.Write('S');
      _writer.Seek(0, SeekOrigin.End);
    }

    private byte[] ReadDocument(long position)
    {
      _readingStream.Position = position;
      var check = _reader.ReadChar();
      if (check == 'K')
      {
        var len = _reader.ReadInt32();
        var id = _reader.ReadString();
        return _reader.ReadBytes(len);
      }
      return null;
    }

    [Description("Internal Use ONLY")]
    public void SaveCollection()
    {
      _StoreReady.WaitOne();
      lock (_primarystore)
      {
        _writer.Close();
        _reader.Close();
      }
    }

    [Description("Internal Use ONLY")]
    public void LoadCollection(string database, string collection, string basepath = null)
    {
      _fullcollectionpath = Path.Combine(Path.Combine(basepath ?? Configuration.GetBasePath(), database), collection + ".coll");

      _readingStream = File.Open(_fullcollectionpath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Write);
      _reader = new BinaryReader(_readingStream);

      if (_readingStream.Length > 0)
        try
        {
          var dbcheck = _reader.ReadString();
          if (dbcheck == Configuration.Version)
          {
            while (_readingStream.Position < _readingStream.Length)
            {
              var pos = _readingStream.Position;
              var c = _reader.ReadChar();
              var len = _reader.ReadInt32();
              var id = _reader.ReadString();
              _readingStream.Seek(len, SeekOrigin.Current);

              if (c == 'K')
                this.InternalDictionaryInsert(id, pos);
            }
          }
        }
        catch (OutOfMemoryException ex)
        {
          LogWriter.LogException(ex);
        }

      _writingStream = File.Open(_fullcollectionpath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
      _writer = new BinaryWriter(_writingStream);

      if (_writingStream.Length == 0)
      {
        _writer.Write(Configuration.Version);
        _writer.Flush();
      }

      _StoreReady.Set();
    }

    public void ShrinkCollection()
    {
      //return;
      long myposition = 0;
      long writingposition = 0;
      while (myposition < _readingStream.Length)
      {
        lock (_primarystore)
        {
          long originalwriting = _writingStream.Position;

          _readingStream.Position = myposition;
          bool needtowrite = false;

          var check = _reader.ReadChar();
          var len = _reader.ReadInt32();
          var id = _reader.ReadString();
          var buffer = _reader.ReadBytes(len);

          if (check == 'K')
          {
            needtowrite = myposition > writingposition;

            myposition = _readingStream.Position;
            if (needtowrite)
            {
              _writingStream.Position = writingposition;
              _writer.Write('K');
              _writer.Write(len);
              _writer.Write(id);
              _writer.Write(buffer);
              _writer.Flush();

              _primarystore[id] = writingposition;
              writingposition = _writingStream.Position;
              _writingStream.Position = originalwriting;
            }
            else
            {
              writingposition = myposition;
            }
          }
          if (writingposition == originalwriting)
          {
            _readingStream.Position = 0;
            _writingStream.SetLength(originalwriting);
            return;
          }
        }
        Thread.Sleep(0);
      }

    }
  }
}
