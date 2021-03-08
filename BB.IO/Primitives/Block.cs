using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    public struct Block
    {
        public readonly int BlockNumber;
        public bool IsDirty;

        public Block(int blockNumber, bool isDirty)
            => (BlockNumber, IsDirty) = (blockNumber, isDirty);

        public Block(int blockNumber)
            => (BlockNumber, IsDirty) = (blockNumber, true);
    }
}
