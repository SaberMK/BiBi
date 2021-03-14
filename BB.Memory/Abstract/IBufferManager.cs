using BB.IO.Primitives;
using BB.Memory.Buffers;

namespace BB.Memory.Abstract
{
    public interface IBufferManager : System.IDisposable
    {
        Buffer Pin(Block block);
        void Unpin(Buffer buffer);
        void FlushAll(int transactionNumber);
        int Available { get; }
    }
}
