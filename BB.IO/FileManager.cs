using BB.IO.Abstract;
using BB.IO.Primitives;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO
{
    public class FileManager : IFileManager
    {
        private readonly DirectoryInfo _dbDirectory;
        private readonly bool _isNew;
        private readonly int _blockSize;
        private ConcurrentDictionary<string, FileStream> _openedFiles = new ConcurrentDictionary<string, FileStream>();

        public FileManager(string dbName, string dbsDirectory, int blockSize)
        {
            _blockSize = blockSize;

            if (!Directory.Exists(dbsDirectory))
            {
                Directory.CreateDirectory(dbsDirectory);
            }

            var dbPath = Path.Combine(dbsDirectory, dbName);
            
            _isNew = !Directory.Exists(dbPath);
            if (_isNew)
            {
                _dbDirectory = Directory.CreateDirectory(dbPath);
            }
            else
            {
                _dbDirectory = new DirectoryInfo(dbPath);
            }
        }

        public bool Read(Block block, out byte[] buffer)
        {
            var file = GetFile(block.Filename);

            lock (file)
            {
                var pagePosition = block.Id * _blockSize;

                // Out of range
                if (block.Id < 0
                    || pagePosition < 0
                    || file.Length < pagePosition)
                {
                    buffer = default;
                    return false;
                }

                buffer = new byte[_blockSize];
                file.Position = pagePosition;
                file.Read(buffer, 0, _blockSize);

            }

            return true;
        }

        public bool Write(Block block, byte[] buffer)
        {
            var file = GetFile(block.Filename);

            lock (file)
            {
                var pagePosition = block.Id * _blockSize;

                if (block.Id < 0
                    || pagePosition < 0
                    || file.Length < pagePosition)
                {
                    return false;
                }

                file.Position = pagePosition;
                file.Read(buffer, 0, _blockSize);
            }

            return true;
        }

        public bool Append(string filename, out Block block)
        {
            var file = GetFile(filename);

            lock (file)
            {
                var length = (int)file.Length;
                var newBlockId = length == 0 ? 0 : (length / _blockSize);
                var data = new byte[_blockSize];

                file.Position = newBlockId * _blockSize;
                file.Write(data, 0, _blockSize);

                block = new Block(filename, newBlockId);
            }
            
            return true;
        }

        public int Length(string filename)
        {
            var file = GetFile(filename);
            return (int)file.Length;
        }

        private FileStream GetFile(string filename)
        {
            var hasFile = _openedFiles.TryGetValue(filename, out var file);

            if (hasFile)
                return file;

            file = new FileStream(
                Path.Combine(_dbDirectory.FullName, filename),
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                _blockSize,
                FileOptions.WriteThrough);

            _openedFiles.TryAdd(filename, file);
            return file;
        }

        public Page ResolvePage(Block block)
        {
            return new Page(this, block, _blockSize);
        }

        public Page ResolvePage(Block block, byte[] data)
        {
            return new Page(this, block, data);
        }

        public void Dispose()
        {
            var filesToClose = _openedFiles.Values;
            foreach(var file in filesToClose)
            {
                file.Close();
            }
        }

        public bool IsNew => _isNew;
        public int BlockSize => _blockSize;
    }
}
