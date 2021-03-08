using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    public enum PageStatus : byte
    {
        New = 0,
        Dirty = 1,
        Commited = 2
    }
}
