using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Entity
{
    public readonly struct RID
    {
        public readonly RID MissedRID => new RID(-1, -1);

        public readonly int BlockNumber;
        public readonly int Id;

        public RID(int blockNumber, int id)
            => (BlockNumber, Id) = (blockNumber, id);
    }
}
