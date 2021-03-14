using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace BB.Memory.Buffers
{
    public class Buffer
    {   
        // Maybe in future log manager would also be changed for a block?
        private readonly ILogManager _logManager;

        // Should not be readonly because it can be changed for a block
        private IFileManager _fileManager;

        private Page _page;
        private Block _block;

        private int _pins = 0;
        private int _transactionNumber = -1;
        private int _lsn = -1;

        public Buffer(ILogManager logManager)
        {
            _logManager = logManager;
        }

        public void SetModified(int transactionNumber, int lsn)
        {
            _transactionNumber = transactionNumber;

            if (lsn >= 0)
            {
                _lsn = lsn;
            }
        }

        public void Pin()
        {
            Interlocked.Increment(ref _pins);
        }

        public void Unpin()
        {
            if (_pins == 0)
                return;

            Interlocked.Decrement(ref _pins);
        }

        internal void AssignToBlock(int blockId, IFileManager fileManager)
        {
            _fileManager = fileManager;
            Flush();
            _block = new Block(blockId, _fileManager.Filename);
            _fileManager.Read(blockId, out _page);
            _pins = 0;
        }

        public void Flush()
        {
            if (_fileManager == null)
                return;

            if (_transactionNumber >=0)
            {
                _logManager.Flush(_lsn);
                _fileManager.Write(_page);
                _transactionNumber = -1;
            }
        }

        public Page Page => _page;
        public Block Block => _block;
        public bool IsPinned => _pins > 0;
        public int ModyfyingTransaction => _transactionNumber;
    }
}
