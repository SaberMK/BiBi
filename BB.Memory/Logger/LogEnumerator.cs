using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Base;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Logger
{
    public class LogEnumerator : IEnumerator<BasicLogRecord>
    {
        private readonly IFileManager _fileManager;
        private readonly Block _logStartBlock;
        private Block _block;
        private Page _page;
        private int _currentRecord;

        public LogEnumerator(IFileManager fileManager, Block block)
        {
            _logStartBlock = block;
            _fileManager = fileManager;
            _block = block;
            _page = _fileManager.ResolvePage();
            _page.Read(block);

            _ = _page.GetInt(LogManager.LAST_ENTRY_STORAGE_POSITION, out _currentRecord);
        }

        public BasicLogRecord Current
        {
            get
            {
                if (_currentRecord == 0)
                    MoveToNextBlock();

                return new BasicLogRecord(_page, _currentRecord);
            }
        }

        object IEnumerator.Current => this;


        public bool MoveNext()
        {
            if (_currentRecord == 0 && _block.Id == 0)
                return false;

            if (_currentRecord == 0)
                MoveToNextBlock();

            _ = _page.GetInt(LogManager.LAST_ENTRY_STORAGE_POSITION, out _currentRecord);
            return true;
        }

        public void Reset()
        {
            _block = _logStartBlock;
            _page.Read(_block);
            _ = _page.GetInt(LogManager.LAST_ENTRY_STORAGE_POSITION, out _currentRecord);

        }

        public void Dispose()
        {
            // well, don't think that I need to remove something...
        }

        private void MoveToNextBlock()
        {
            _block = new Block(_block.Filename, _block.Id - 1);
            _page.Read(_block);
            _ = _page.GetInt(LogManager.LAST_ENTRY_STORAGE_POSITION, out _currentRecord);
        }
    }
}
