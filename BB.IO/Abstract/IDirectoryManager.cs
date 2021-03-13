using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Abstract
{
    public interface IDirectoryManager : IDisposable
    {
        bool Read(Block block, out Page page);
        bool Write(Block block, Page page);
        Page Append(string filename);
        int Length(string filename);
        IFileManager GetManager(string filename);
    }
}
