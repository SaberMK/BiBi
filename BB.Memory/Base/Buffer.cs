using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace BB.Memory.Base
{
    public class Buffer
    {
        private readonly ILogManager _logManager;

        private Page _page;
        private Block _block;
        private int _pins = 0;
        private int _modifiedBy = -1;
        private int _logSequenceNumber = -1;

        public Buffer(ILogManager logManager, IFileManager fileManager)
        {
            _page = new Page(fileManager, fileManager.BlockSize);
            _logManager = logManager;
        }

        public void Pin()
        {
            _pins++;
        }

        public void Unpin()
        {
            _pins--;
        }

        public void Flush()
        {
            if(_modifiedBy >= 0)
            {
                _logManager.Flush(_logSequenceNumber);
                _ = _page.Write(_block);
            }
            _modifiedBy = -1;
        }

        public void AssignToBlock(Block block)
        {
            Flush();
            _block = block;
            _ = _page.Read(_block);
            _pins = 0;
        }

        public void AssignToNew(string filename, IPageFormatter pageFormatter)
        {
            Flush();
            pageFormatter.Format(_page);
            _ = _page.Append(filename, out _block);
            _pins = 0;
        }

        public Block Block => _block;
        
        public bool IsModifiedBy(int transactionNumber)
            => _modifiedBy == transactionNumber;

        public bool IsPinned
            => _pins > 0;

        #region Get Values

        public bool GetInt(int offset, out int value)
        {
            return _page.GetInt(offset, out value);
        }

        public bool GetByte(int offset, out byte value)
        {
            return _page.GetByte(offset, out value);
        }

        public bool GetBool(int offset, out bool value)
        {
            return _page.GetBool(offset, out value);
        }

        public bool GetBlob(int offset, out byte[] value)
        {
            return _page.GetBlob(offset, out value);
        }

        public bool GetString(int offset, out string value)
        {
            return _page.GetString(offset, out value);
        }

        public bool GetDate(int offset, out DateTime value)
        {
            return _page.GetDate(offset, out value);
        }

        #endregion

        #region Set Values

        public bool SetInt(int offset, int value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetInt(offset, value);
        }

        public bool SetByte(int offset, byte value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetByte(offset, value);
        }

        public bool SetBool(int offset, bool value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetBool(offset, value);
        }

        public bool SetBlob(int offset, byte[] value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetBlob(offset, value);
        }

        public bool SetString(int offset, string value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetString(offset, value);
        }

        public bool SetDate(int offset, DateTime value, int transactionNumber, int logSequenceNumber)
        {
            _modifiedBy = transactionNumber;

            if (logSequenceNumber >= 0)
                _logSequenceNumber = logSequenceNumber;

            return _page.SetDate(offset, value);
        }

        #endregion
    }
}
