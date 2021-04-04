using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    public class CommitRecord : LogRecord
    {
        public CommitRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber)
            : base(logManager, bufferManager, LogRecordType.Commit)
        {
            _transactionNumber = transactionNumber;
        }

        public CommitRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record,
            bool needOffset = true)
            : base(logManager, bufferManager, LogRecordType.Commit)
        {
            if (needOffset)
            {
                _ = record.NextInt(out var _);
            }

            _ = record.NextInt(out _transactionNumber);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.Commit,
                _transactionNumber
            };

            _ = _logManager.Append(record, out var lsn);

            return lsn;
        }

        public override void Undo()
        {
            // TODO think about how to undo Start record.
        }

        public override string ToString()
            => $"<COMMIT {_transactionNumber}>";
    }
}
