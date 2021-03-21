using BB.Memory.Base;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Abstract
{
    public interface ILogManager
    {
        public void Flush(int lsn);
        bool Append(object[] records, out int lsn);
        IEnumerator<BasicLogRecord> GetEnumerator();
    }
}
