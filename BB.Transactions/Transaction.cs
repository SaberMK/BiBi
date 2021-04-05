using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Transactions.Abstract;
using BB.Transactions.Helpers;
using BB.Transactions.Recovery;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions
{
    public class Transaction : ITransaction
    {
        private readonly ITransactionNumberDispatcher _numberDispatcher;
        private readonly IRecoveryManager _recoveryManager;
        private readonly IConcurrencyManager _concurrencyManager;
        private readonly TransactionBuffersList _bufferList;
        private readonly IFileManager _fileManager;

        private readonly int _transactionNumber;

        public Transaction(
            ITransactionNumberDispatcher numberDispatcher,
            IBufferManager bufferManager, 
            IConcurrencyManager concurrencyManager,
            IFileManager fileManager,
            ILogManager logManager)
        {
            _numberDispatcher = numberDispatcher;
            _concurrencyManager = concurrencyManager;
            _fileManager = fileManager;
            _bufferList = new TransactionBuffersList(bufferManager);

            _transactionNumber = _numberDispatcher.GetNextTransactionNumber();
            _recoveryManager = new RecoveryManager(bufferManager, logManager, _transactionNumber);
        }

        public void Commit()
        {
            _bufferList.UnpinAll();
            _recoveryManager.Commit();
            _concurrencyManager.Release();
        }

        public void Rollback()
        {
            _bufferList.UnpinAll();
            _recoveryManager.Rollback();
            _concurrencyManager.Release();
        }

        public void Recover()
        {
            _recoveryManager.Recover();
        }

        public void Pin(Block block)
        {
            _bufferList.Pin(block);
        }

        public void Unpin(Block block)
        {
            _bufferList.Unpin(block);
        }

        public int Length(string filename)
        {
            // A dummy block because honestly we do not care about block, we need to get length of this file
            var dummyBlock = new Block(filename, -1);

            _concurrencyManager.SharedLock(dummyBlock);

            return _fileManager.Length(filename);
        }

        public Block Append(string filename, IPageFormatter pageFormatter)
        {
            // A dummy block because honestly we do not care about block, we need to get length of this file
            var dummyBlock = new Block(filename, -1);

            _concurrencyManager.ExclusiveLock(dummyBlock);

            var lastBlock = _bufferList.PinNew(filename, pageFormatter);
            Unpin(lastBlock);

            return lastBlock;
        }

        #region Get Values

        public bool GetInt(Block block, int offset, out int value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetInt(offset, out value);
        }

        public bool GetString(Block block, int offset, out string value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetString(offset, out value);
        }

        public bool GetByte(Block block, int offset, out byte value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetByte(offset, out value);
        }

        public bool GetBool(Block block, int offset, out bool value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetBool(offset, out value);
        }

        public bool GetBlob(Block block, int offset, out byte[] value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetBlob(offset, out value);
        }

        public bool GetDate(Block block, int offset, out DateTime value)
        {
            _concurrencyManager.SharedLock(block);
            var buffer = _bufferList.GetBuffer(block);
            return buffer.GetDate(offset, out value);
        }



        #endregion


        #region Set Values

        public bool SetInt(Block block, int offset, int value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetInt(buffer, offset, value);
            return buffer.SetInt(offset, value, _transactionNumber, lsn);
        }

        public bool SetString(Block block, int offset, string value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetString(buffer, offset, value);
            return buffer.SetString(offset, value, _transactionNumber, lsn);
        }

        public bool SetByte(Block block, int offset, byte value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetByte(buffer, offset, value);
            return buffer.SetByte(offset, value, _transactionNumber, lsn);
        }

        public bool SetBool(Block block, int offset, bool value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetBool(buffer, offset, value);
            return buffer.SetBool(offset, value, _transactionNumber, lsn);
        }

        public bool SetBlob(Block block, int offset, byte[] value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetBlob(buffer, offset, value);
            return buffer.SetBlob(offset, value, _transactionNumber, lsn);
        }

        public bool SetDate(Block block, int offset, DateTime value)
        {
            _concurrencyManager.ExclusiveLock(block);
            var buffer = _bufferList.GetBuffer(block);
            int lsn = _recoveryManager.SetDate(buffer, offset, value);
            return buffer.SetDate(offset, value, _transactionNumber, lsn);
        }

        #endregion
    }
}
