using BB.IO.Primitives;
using BB.Memory.Base;

namespace BB.Memory.Abstract
{
    public interface IBufferPoolStrategy
    {
        void FlushAll(int transactionNumber);
        Buffer Pin(Block block);
        Buffer PinNew(string filename, IPageFormatter pageFormatter);
        void Unpin(Buffer buffer);
        int Available { get; }
    }
}
