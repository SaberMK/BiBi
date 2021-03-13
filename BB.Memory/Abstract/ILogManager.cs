using System;
using System.Collections.Generic;

namespace BB.Memory.Abstract
{
    public interface ILogManager : IDisposable
    {
        bool Append(byte[] data, out int lsn);
        void Flush(int lsn);
        IEnumerator<byte[]> Enumerator();
    }
}
