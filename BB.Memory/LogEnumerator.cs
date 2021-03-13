using BB.IO.Abstract;
using BB.IO.Primitives;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory
{
    public class LogEnumerator : IEnumerator<byte[]>
    {
        private readonly IFileManager _fileManager;
        private readonly Block _headBlock;

        private Page _page;
        private Block _block;
        private int _currentPosition;
        private int _boundary;

        public LogEnumerator(IFileManager fileManager, Block block)
        {
            _block = block;
            _fileManager = fileManager;
            _headBlock = block;

            MoveToBlock(block);
        }

        public byte[] Current
        { 
            get
            {
                if (_currentPosition == 0)
                    return default;

                _ = _page.GetBlob(_page.PageSize - _currentPosition, out var bytes);
                return bytes;
            } 
        }

        object IEnumerator.Current => this;

        public bool MoveNext()
        {
            if (_currentPosition == 0
                && _block.Id == 0)
                return false;

            if(_currentPosition == 0)
            {
                _block = new Block(_block.Id - 1, _block.Filename);
                MoveToBlock(_block);
            }

            _ = _page.GetInt(_page.PageSize - _currentPosition, out var length);
            _currentPosition -= sizeof(int) + length;

            if (_currentPosition == 0
                && _block.Id == 0)
                return false;

            if (_currentPosition == 0)
            {
                _block = new Block(_block.Id - 1, _block.Filename);
                MoveToBlock(_block);
            }

            return true;
        }

        public void Reset()
        {
            MoveToBlock(_headBlock);
        }

        public void Dispose()
        {
            // Don't think that IFIleManager should be disposed...
        }

        private void MoveToBlock(Block block)
        {
            _ = _fileManager.Read(block.Id, out _page);
            _ = _page.GetInt(0, out _boundary);
            _currentPosition = _boundary;
        }
    }
}
