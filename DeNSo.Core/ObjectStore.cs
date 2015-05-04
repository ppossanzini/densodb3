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

    private const byte K = 75;
    private const byte S = 83;

    internal volatile C5.TreeDictionary<string, long> _primarystore = new C5.TreeDictionary<string, long>();
    private string _fullcollectionpath = string.Empty;

    public int ChangesFromLastSave { get; set; }
    public long LastEventSN { get; internal set; }

    private FileStream _readingStream;
    private BinaryReader _reader;
    private FileStream _writingStream;
    private BinaryWriter _writer;

    private ManualResetEvent _StoreReady = new ManualResetEvent(false);
    private ManualResetEvent _CanWrite = new ManualResetEvent(true);
    private ReaderWriterLockSlim locker = new ReaderWriterLockSlim();


    public int Count()
    {
      return LockForRead(() => _primarystore.Count);
    }

    public IEnumerable<string> GetAllKeys()
    {
      _StoreReady.WaitOne();

      return LockForRead(() => _primarystore.Keys.ToArray());
    }

    private T LockForRead<T>(Func<T> method)
    {
      locker.EnterUpgradeableReadLock();
      var result = method();
      locker.ExitUpgradeableReadLock();
      return result;
    }

    private void LockForRead(Action method)
    {
      locker.EnterUpgradeableReadLock();
      method();
      locker.ExitUpgradeableReadLock();
    }

    private T LockForWrite<T>(Func<T> method)
    {
      locker.EnterWriteLock();
      var result = method();
      locker.ExitWriteLock();
      return result;
    }

    private void LockForWrite(Action method)
    {
      locker.EnterWriteLock();
      method();
      locker.ExitWriteLock();
    }

    public byte[] GetById(string key)
    {
      _StoreReady.WaitOne();
      return InternalDictionaryGet(key);
    }

    public void Flush()
    {
      _StoreReady.WaitOne();
      _CanWrite.WaitOne();
      LockForWrite(() =>
      {
        _primarystore.Clear();
        _readingStream.Position = 0;
        _writingStream.Position = 0;
        _writingStream.SetLength(0);
        _writer.Write(Configuration.Version);
        _writer.Flush();
      });
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

        LockForRead(() =>
        {
          foreach (var k in _primarystore.Keys)
            newbloom.Add(k);
        });

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
        long position = -1;

        LockForRead(() =>
        {
          if (_primarystore.Contains(key))
            position = _primarystore[key];
        });

        if (position >= 0)
          return ReadDocument(position);
      }
      return null;
    }

    private void InternalDictionaryInsert(string key, long position)
    {
      LockForWrite(() =>
      {
        if (!_primarystore.Contains(key))
          _primarystore.Add(key, position);
      });

      BloomFilterAdd(key);
    }

    private void InternalDictionarySet(string key, byte[] doc)
    {
      _CanWrite.WaitOne();
      LockForWrite(() =>
      {
        var result = WriteDocument(key, doc);
        if (_primarystore.Contains(key))
        {
          _primarystore[key] = result;
          return;
        }

        _primarystore.Add(key, result);
      });

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
      _CanWrite.WaitOne();
      return LockForWrite(() =>
      {
        if (_primarystore.Contains(key))
        {
          StoneDocumentAtPosition(_primarystore[key]);
          _primarystore.Remove(key);

          _indexpossibleincoerences++;
          return true;
        }
        return false;
      });
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
      _writer.Write(K);
      _writer.Write(doc.Length);
      _writer.Write(key);
      _writer.Write(doc);
      _writer.Flush();
      return position;
    }

    private void StoneDocumentAtPosition(long position)
    {
      _writingStream.Position = position;
      _writer.Write(S);
      _writer.Seek(0, SeekOrigin.End);
    }

    private byte[] ReadDocument(long position)
    {
      lock (_readingStream)
      {
        _readingStream.Position = position;
        var check = _reader.ReadByte();
        if (check == K)
        {
          var len = _reader.ReadInt32();
          var id = _reader.ReadString();
          return _reader.ReadBytes(len);
        }
      }
      return null;
    }

    [Description("Internal Use ONLY")]
    public void SaveCollection()
    {
      _StoreReady.WaitOne();
      LockForWrite(() =>
      {
        _writer.Close();

        lock (_readingStream)
          _reader.Close();
      });
    }

    [Description("Internal Use ONLY")]
    public void LoadCollection(string database, string collection, string basepath = null)
    {
      _fullcollectionpath = Path.Combine(Path.Combine(basepath ?? Configuration.GetBasePath(), database), collection + ".coll");

      _readingStream = File.Open(_fullcollectionpath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
      _reader = new BinaryReader(_readingStream);

      if (_readingStream.Length > 0)
        try
        {
          var dbcheck = _reader.ReadByte();
          if (dbcheck == Configuration.Version)
          {
            while (_readingStream.Position < _readingStream.Length)
            {
              var pos = _readingStream.Position;
              var c = _reader.ReadByte();
              var len = _reader.ReadInt32();
              var id = _reader.ReadString();
              _readingStream.Seek(len, SeekOrigin.Current);

              if (c == K)
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
      return;
      long myposition = 1;
      long writingposition = 1;

      while (myposition < _readingStream.Length)
      {
        LockForWrite(() =>
        {
          _CanWrite.Reset();
        });

        byte[] buffer = null;
        string id;
        int len;
        byte check;
        bool needtowrite = false;

        lock (_readingStream)
        {
          _readingStream.Position = myposition;

          check = _reader.ReadByte();
          len = _reader.ReadInt32();
          id = _reader.ReadString();

          needtowrite = check == K && myposition > writingposition;

          if (needtowrite)
            buffer = _reader.ReadBytes(len);
          else
            _readingStream.Seek(len, SeekOrigin.Current);

          myposition = _readingStream.Position;
        }

        if (needtowrite)
        {
          LockForWrite(() =>
          {
            _writingStream.Position = writingposition;
            _writer.Write(K);
            _writer.Write(len);
            _writer.Write(id);
            _writer.Write(buffer);
            _writer.Flush();

            _primarystore[id] = writingposition;
            writingposition = _writingStream.Position;

            if (myposition == _readingStream.Length)
            {
              _readingStream.Position = 0;
              _writingStream.SetLength(_writingStream.Position);
            }

            _writingStream.Seek(0, SeekOrigin.End);
          });
        }
        else
        {
          writingposition = myposition;
        }
        _CanWrite.Set();
      }
      Thread.Sleep(0);
    }
  }
}

