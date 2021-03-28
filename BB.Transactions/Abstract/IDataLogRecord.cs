using BB.IO.Primitives;

namespace BB.Transactions.Abstract
{
    public interface IDataLogRecord<T>
    {
        public int Offset { get; }
        public T Value { get; }
        public Block Block { get; }
    }
}
