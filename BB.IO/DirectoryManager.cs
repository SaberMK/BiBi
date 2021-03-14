using BB.IO.Abstract;
using BB.IO.Primitives;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO
{
    // TODO OPTIMIZE IT!
    public class DirectoryManager : IDirectoryManager
    {
        private readonly int _blockSize;
        private readonly bool _isNew;
        private readonly DirectoryInfo _directory;

        private readonly ConcurrentDictionary<string, IFileManager> _managers = new ConcurrentDictionary<string, IFileManager>();
        public DirectoryManager(string directoryPath, int blockSize)
        {
            _blockSize = blockSize;

            _isNew = (!Directory.Exists(directoryPath));

            if (_isNew)
                _directory = Directory.CreateDirectory(directoryPath);
            else
                _directory = new DirectoryInfo(directoryPath);

            // Removing temp directories is not a task of DirectoryManager!
        }

        public bool Read(Block block, out Page page)
        {
            var manager = GetManager(block.Filename);
            return manager.Read(block.Id, out page);
        }

        public bool Write(Block block, Page page)
        {
            var manager = GetManager(block.Filename);
            return manager.Write(page);
        }

        // TOOD return bool maybe...
        public Page Append(string filename)
        {
            var manager = GetManager(filename);
            return manager.Append();
        }

        public int Length(string filename)
        {
            var manager = GetManager(filename);
            return manager.Length;
        }

        public IFileManager GetManager(string filename)
        {
            var hasManager = _managers.TryGetValue(filename, out var manager);

            if (hasManager)
                return manager;
            
            // TODO: Add to Factory (that passes from DI)
            manager = new FileManager(Path.Combine(_directory.FullName, filename), _blockSize);

            // TODO: Check if cannot
            _managers.TryAdd(filename, manager);
            return manager;
        }

        public void Dispose()
        {
            foreach(var manager in _managers.Values)
            {
                manager.Dispose();
            }
        }
    }
}
