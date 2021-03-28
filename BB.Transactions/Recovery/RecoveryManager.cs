using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;
using BB.Transactions.Records;
using System.Collections.Generic;
using DateTime = System.DateTime;

namespace BB.Transactions.Recovery
{
    public class RecoveryManager : IRecoveryManager
    {
        private readonly IBufferManager _bufferManager;
        private readonly ILogManager _logManager;
        private readonly int _transactionNumber;

        public RecoveryManager(
            IBufferManager bufferManager,
            ILogManager logManager,
            int transactionNumber)
        {
            _bufferManager = bufferManager;
            _logManager = logManager;
            _transactionNumber = transactionNumber;
        }

        public void Commit()
        {
            _bufferManager.FlushAll(_transactionNumber);

            var lsn = new CommitRecord(_logManager, _bufferManager, _transactionNumber)
                .WriteToLog();

            _logManager.Flush(lsn);
        }

        public void Rollback()
        {
            _bufferManager.FlushAll(_transactionNumber);

            DoRollback();

            int lsn = new RollbackRecord(_logManager, _bufferManager, _transactionNumber)
                .WriteToLog();

            _logManager.Flush(lsn);
        }

        public void Recover()
        {
            _bufferManager.FlushAll(_transactionNumber);

            DoRecover();

            // TODO: need to somehow fetch all current transactions and write them
            var lsn = new CheckpointRecord(_logManager, _bufferManager, new int[] { _transactionNumber })
                .WriteToLog();

            _logManager.Flush(lsn);
        }


        public int SetInt(Buffer buffer, int offset, int newValue)
        {
            _ = buffer.GetInt(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetIntRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        public int SetString(Buffer buffer, int offset, string newValue)
        {
            _ = buffer.GetString(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetStringRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        public int SetDate(Buffer buffer, int offset, DateTime newValue)
        {
            _ = buffer.GetDate(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetDateRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        public int SetByte(Buffer buffer, int offset, byte newValue)
        {
            _ = buffer.GetByte(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetByteRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        public int SetBool(Buffer buffer, int offset, bool newValue)
        {
            _ = buffer.GetBool(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetBoolRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        public int SetBlob(Buffer buffer, int offset, byte[] newValue)
        {
            _ = buffer.GetBlob(offset, out var oldValue);

            var block = buffer.Block;

            if (IsTemporaryBlock(block))
                return -1;
            else
                return new SetBlobRecord(_logManager, _bufferManager, _transactionNumber, block, offset, oldValue)
                    .WriteToLog();
        }

        private void DoRollback()
        {
            var enumerator = new LogRecordEnumerator(_logManager, _bufferManager);

            do
            {
                var record = enumerator.Current;

                if(record.TransactionNumber == _transactionNumber)
                {
                    if (record.Type == LogRecordType.Start)
                        return;

                    record.Undo();
                }

            } while (enumerator.MoveNext());
        }

        private void DoRecover()
        {
            var commitedTransactions = new HashSet<int>();
            var enumerator = new LogRecordEnumerator(_logManager, _bufferManager);

            do
            {
                var record = enumerator.Current;

                if (record.Type == LogRecordType.Checkpoint)
                    return;

                if (record.Type == LogRecordType.Commit)
                {
                    commitedTransactions.Add(record.TransactionNumber);
                }
                else if (!commitedTransactions.Contains(record.TransactionNumber))
                {
                    record.Undo();
                }

            } while (enumerator.MoveNext());
        }

        private bool IsTemporaryBlock(Block block)
        {
            return block.Filename.StartsWith("temp");
        }
    }
}
