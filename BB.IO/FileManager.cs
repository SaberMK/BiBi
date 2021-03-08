using BB.IO.Abstract;
using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO
{
    public class FileManager : IFileManager, IDisposable
    {
        private string _filepath;
        private int _blockSize;
        private FileStream _stream;
        private StreamReader _reader;
        private StreamWriter _writer;

        public FileManager(string filepath, int blockSize)
        {
            _filepath = filepath;
            _blockSize = blockSize;

            _stream = new FileStream(
                filepath, 
                FileMode.OpenOrCreate, 
                FileAccess.ReadWrite, 
                FileShare.None, 
                _blockSize, 
                FileOptions.WriteThrough);

            _reader = new StreamReader(_stream);
            _writer = new StreamWriter(_stream);
        }

        public bool Read(int blockId, out Page page)
        {
            var pagePosition = blockId * _blockSize;

            // Out of range
            if (blockId < 0 
                || pagePosition < 0 
                ||_stream.Length < pagePosition)
            {
                page = default(Page);
                return false;
            }

            var data = new byte[_blockSize];

            _stream.Position = pagePosition;
            _stream.Read(data, 0, _blockSize);

            page = new Page(blockId, data);
            return true;
        }

        public bool Write(Page page)
        {
            var pagePosition = page.BlockId * _blockSize;

            // Out of range
            if (page.BlockId < 0 
                || pagePosition < 0 
                || _stream.Length < pagePosition
                || page.PageSize != _blockSize)
            {
                return false;
            }

            lock (page.LockObject)
            {
                _stream.Position = pagePosition;
                _stream.Write(page._data, 0, page.PageSize);
            }

            page._status = PageStatus.Commited;

            return true;
        }

        public Page Append()
        {
            var newBlockId = (int)_stream.Length / _blockSize;
            var data = new byte[_blockSize];
            _stream.Position = newBlockId * _blockSize;
            _stream.Write(data, 0, _blockSize);
            return new Page(newBlockId, data);
        }

        public void Dispose()
        {
            _stream.Dispose();
            _reader.Dispose();

            // It seems that writer should not be disposed?
            // Or it would be disposed automatically because
            // stream is disposed?
            //_writer.Dispose();
        }
    }
}
