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

        public Page Read(int blockId)
        {
            var data = new byte[_blockSize];
            _stream.Position = blockId * _blockSize;
            _stream.Read(data, 0, _blockSize);
            return new Page(blockId, data);
        }

        public void Write(Page page)
        {
            _stream.Position = page.BlockId * _blockSize;
            _stream.Write(page.Data, 0, page.PageSize);
            page._status = PageStatus.Commited;
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
