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
        private string _filename;
        private int _blockSize;
        private FileStream _stream;

        public FileManager(string filename, int blockSize)
        {
            _filename = filename;
            _blockSize = blockSize;

            _stream = new FileStream(
                filename, 
                FileMode.OpenOrCreate, 
                FileAccess.ReadWrite, 
                FileShare.None, 
                _blockSize, 
                FileOptions.WriteThrough);
        }

        // Should we lock on read?
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

            _stream.Position = pagePosition;
            _stream.Write(page._data, 0, page.PageSize);
            

            return true;
        }

        public Page Append()
        {
            var length = (int)_stream.Length;
            var newBlockId = length == 0 ? 0 : (length/ _blockSize);
            var data = new byte[_blockSize];

            _stream.Position = newBlockId * _blockSize;
            _stream.Write(data, 0, _blockSize);            

            return new Page(newBlockId, data);
        }

        public int Length => (int)_stream.Length;

        public int BlockSize => _blockSize;

        // because there are i.e. THREE blocks but block is SECOND
        public int LastBlockId => Length == 0 ? 0 : (Length / _blockSize) - 1;

        public string Filename => _filename;

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
