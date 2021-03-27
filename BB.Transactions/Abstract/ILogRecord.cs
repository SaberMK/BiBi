namespace BB.Transactions.Abstract
{
    public interface ILogRecord
    {
        int WriteToLog();
        void Undo();

        // Think about it: we have no transaction number
        // For commit/rollback, start and checkpoint records.
        // TODO: need another level of abstraction
        public int TransactionNumber { get; }
        public LogRecordType Type { get; }
    }
}
