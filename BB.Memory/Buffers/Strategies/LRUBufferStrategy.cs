using BB.IO.Primitives;
using BB.Memory.Abstract;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Buffers.Strategies
{
    public class LRUBufferStrategy : IBufferStrategy
    {
        public int Available => throw new NotImplementedException();

        public IEnumerable<Buffer> Buffers => throw new NotImplementedException();

        public StrategyType Type => throw new NotImplementedException();

        public Buffer Pin(Block block)
        {
            throw new NotImplementedException();
        }

        public void Unpin(Buffer buffer)
        {
            throw new NotImplementedException();
        }
    }
}
