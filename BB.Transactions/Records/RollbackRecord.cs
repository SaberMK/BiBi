using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    public class RollbackRecord : LogRecord
    {
        public RollbackRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber)
            : base(logManager, bufferManager, LogRecordType.Rollback)
        {
            _transactionNumber = transactionNumber;
        }

        public RollbackRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record)
            : base(logManager, bufferManager, LogRecordType.Rollback)
        {
            _ = record.NextInt(out var _);
            _ = record.NextInt(out _transactionNumber);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.Rollback,
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
            => $"<ROLLBACK {_transactionNumber}>";
    }
}
