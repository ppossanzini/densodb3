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
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("denso.test, PublicKey=" +
  "0024000004800000940000000602000000240000525341310004000001000100c98f670ec6f0ac" +
  "f9c8529c014464afba633163628ff0813d34e6bff45a4a35bbc439a8aebe6ef2ee4ccf8467e197" +
  "598fd7fb97b574c38a57ed6d853d8d1a95ad1c45030116bdfe87579cf1d44a4c9fb79a7a0296df" +
  "cebde92e0565f1a72cc0644c8089da4dc5645e64f62a035784b14b18b3f4350f20b3902daef4be" +
  "e701aeee")]
namespace DeNSo
{
  public class ObjectStore : IObjectStore
  {
    private int _indexpossibleincoerences = 0;
    private volatile BloomFilter<string> _bloomfilter = new BloomFilter<string>(Configuration.DictionarySplitSize * 2);

    private const byte K = 75;
    private const byte S = 83;
    private const int _blocksize = 1024;
    private const short dataformatversion = 02;
    private byte[] _blocks;

    private long _firstSecureBlockPosition = 0;

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
        _firstSecureBlockPosition = _writer.BaseStream.Position;
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

    internal byte[] InternalDictionaryGet(string key)
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

    internal void InternalDictionaryInsert(string key, long position)
    {
      LockForWrite(() =>
      {
        if (!_primarystore.Contains(key))
          _primarystore.Add(key, position);
      });

      BloomFilterAdd(key);
    }

    internal void InternalDictionarySet(string key, byte[] doc)
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

    internal bool InternalDictionaryContains(string key)
    {
      if (!string.IsNullOrEmpty(key))
        if (BloomFilterCheck(key))
          return _primarystore.Contains(key);
      return false;
    }

    internal bool InternalDictionaryRemove(string key)
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

    internal bool BloomFilterCheck(string key)
    {
      lock (_bloomfilter)
        return _bloomfilter.Contains(key);
    }

    internal void BloomFilterAdd(string key)
    {
      lock (_bloomfilter)
        _bloomfilter.Add(key);
    }

    internal long WriteDocument(string key, byte[] doc)
    {
      var position = _writingStream.Position;
      _writer.Write(K);
      _writer.Write(dataformatversion);
      _writer.Write(doc.Length);
      _writer.Write(key);
      _writer.Write(doc);
      _writer.Flush();
      return position;
    }

    internal void StoneDocumentAtPosition(long position)
    {
      _writingStream.Position = position;
      _writer.Write(S);
      _writer.Seek(0, SeekOrigin.End);
    }

    internal byte[] ReadDocument(long position)
    {
      lock (_readingStream)
      {
        _readingStream.Position = position;
        var check = _reader.ReadByte();
        if (check == K)
        {
          var ver = _reader.ReadInt16();
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
    public void OpenCollection(string database, string collection, string basepath = null)
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
              var ver = _reader.ReadInt16();
              var len = _reader.ReadInt32();
              var id = _reader.ReadString();
              switch (ver)
              {
                case 2:
                case 1: _readingStream.Seek(len, SeekOrigin.Current); break;
              }

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

      if (_writingStream.Length == 0) this.Flush();

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
        short ver;
        int len;
        byte check;
        bool needtowrite = false;

        lock (_readingStream)
        {
          _readingStream.Position = myposition;

          check = _reader.ReadByte();
          ver = _reader.ReadInt16();
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
            _writer.Write(dataformatversion);
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

    internal int CalcDocumentBlocks(int size, int headersize)
    {
      return (int)Math.Ceiling((double)(size + headersize) / _blocksize);
    }

    internal int CalcDocumentDiskSize(int size, int headersize)
    {
      return (int)Math.Ceiling((double)(size + headersize) / _blocksize) * _blocksize;
    }

    internal bool IsBlockUsed(int blockNr)
    {
      var br = (Single)blockNr;
      if (_blocks.Length <= br) throw new IndexOutOfRangeException();

      return (_blocks[(int)Math.Floor(br / 8)] & (128 >> (blockNr % 8))) > 0;
    }

    internal void SignBlock(int blockNr)
    {
      var br = (Single)blockNr;
      if (_blocks.Length <= br) throw new IndexOutOfRangeException();
      _blocks[(int)Math.Floor(br / 8)] = (byte)(_blocks[(int)Math.Floor(br / 8)] | (128 >> (blockNr % 8)));
    }

    internal void ReleaseBlock(int blockNr)
    {
      var br = (Single)blockNr;
      if (_blocks.Length <= br) throw new IndexOutOfRangeException();

      _blocks[(int)Math.Floor(br / 8)] = (byte)(_blocks[(int)Math.Floor(br / 8)] & ~(128 >> (blockNr % 8)));
    }
  }
}

