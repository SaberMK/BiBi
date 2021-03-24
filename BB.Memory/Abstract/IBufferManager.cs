using BB.IO.Primitives;
using BB.Memory.Base;

namespace BB.Memory.Abstract
{
    public interface IBufferManager
    {
        Buffer Pin(Block block);
        Buffer PinNew(string filename, IPageFormatter pageFormatter);
        void Unpin(Buffer buffer);
        void FlushAll(int transactionNumber);
        int Available { get; }
    }
}
