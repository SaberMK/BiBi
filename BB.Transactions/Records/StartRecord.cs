using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    public class StartRecord : LogRecord
    {
        public StartRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber)
            : base(logManager, bufferManager, LogRecordType.Start)
        {
            _transactionNumber = transactionNumber;
        }

        public StartRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record)
            : base(logManager, bufferManager, LogRecordType.Start)
        {
            _ = record.NextInt(out var _);
            _ = record.NextInt(out _transactionNumber);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.Start,
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
            => $"<START {_transactionNumber}>";
    }
}
