using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Buffers.Strategies
{
    public enum StrategyType
    {
        Naive = 0,
        FIFO = 1,
        LRU = 2,
        Clock = 3
    }
}
