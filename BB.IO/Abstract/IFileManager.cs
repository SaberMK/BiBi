using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Abstract
{
    public interface IFileManager : IDisposable
    {
        Page Read(int blockId);
        void Write(Page page);
        Page Append();
    }
}
