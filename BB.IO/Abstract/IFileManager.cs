using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Abstract
{
    public interface IFileManager : IDisposable
    {
        bool Read(int blockId, out Page page);
        bool Write(Page page);
        Page Append();

        int Length { get; }
        int BlockSize { get; }
        int LastBlockId { get; }
    }
}
