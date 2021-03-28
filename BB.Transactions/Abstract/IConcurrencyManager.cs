using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions.Abstract
{
    public interface IConcurrencyManager
    {
        void SharedLock(Block block);
        void ExclusiveLock(Block block);
        void Release();
    }
}
