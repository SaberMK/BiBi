using BB.Memory.Base;
using System.Collections.Generic;

namespace BB.Memory.Abstract
{
    public interface ILogManager
    {
        void Flush(int lsn);
        bool Append(object[] records, out int lsn);
        IEnumerator<BasicLogRecord> GetEnumerator();
    }
}
