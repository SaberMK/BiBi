using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    public class CheckpointRecord : LogRecord
    {
        private readonly int[] _transactionNumbers;

        public CheckpointRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int[] transactionsNumbers)
            : base(logManager, bufferManager, LogRecordType.Checkpoint)
        {
            _transactionNumbers = transactionsNumbers;
        }

        public CheckpointRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record)
            : base(logManager, bufferManager, LogRecordType.Checkpoint)
        {
            _ = record.NextInt(out var length);

            _transactionNumbers = new int[length];

            for (int i = 0; i < length; ++i)
            {
                record.NextInt(out _transactionNumbers[i]);
            }
        }

        public override int WriteToLog()
        {
            var record = new object[1 + _transactionNumbers.Length];
            record[0] = LogRecordType.Checkpoint;

            for (int i = 0; i < _transactionNumbers.Length; ++i)
            {
                record[1 + i] = _transactionNumbers[i];
            }

            _ = _logManager.Append(record, out var lsn);

            return lsn;
        }

        public override void Undo()
        {
            // TODO think about how to undo Checkpoint record.
        }

        public override string ToString()
            => $"<CHECKPOINT [{string.Concat(_transactionNumbers)}]>";
    }
}
