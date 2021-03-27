using BB.Memory.Abstract;

namespace BB.Transactions.Abstract
{
    public abstract class LogRecord : ILogRecord
    {
        protected readonly LogRecordType _type;
        protected readonly ILogManager _logManager;
        protected readonly IBufferManager _bufferManager;
        protected int _transactionNumber;

        public LogRecord(ILogManager logManager,
            IBufferManager bufferManager,
            LogRecordType type)
        {
            _logManager = logManager;
            _bufferManager = bufferManager;
            _type = type;
        }

        public abstract int WriteToLog();
        public abstract void Undo();

        public int TransactionNumber => _transactionNumber;
        public LogRecordType Type => _type;
    }

    // TODO store as byte, not as int!
    public enum LogRecordType
    {
        Checkpoint = 0,
        Start = 1,
        Commit = 2,
        Rollback = 3,
        SetInt = 4,
        SetByte = 5,
        SetBool = 6,
        SetBlob = 7,
        SetString = 8,
        SetDate = 9
    }
}
