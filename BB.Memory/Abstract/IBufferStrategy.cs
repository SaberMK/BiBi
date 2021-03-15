using BB.IO.Primitives;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Abstract
{
    public interface IBufferStrategy
    {
        Buffer Pin(Block block);
        void Unpin(Buffer buffer);
        int Available { get; }
        IEnumerable<Buffer> Buffers { get; }
        StrategyType Type {get;}
    }
}
