using BB.IO.Primitives;
using System;

namespace BB.IO.Abstract
{
    public interface IFileManager : IDisposable
    {
        bool Read(Block block, out byte[] buffer);
        bool Write(Block block, byte[] buffer);
        bool Append(string filename, out Block block);
        int Length(string filename);
        int LastBlockId(string filename);
        Page ResolvePage(Block block);
        Page ResolvePage(Block block, byte[] data);
        Page ResolvePage();
        bool IsNew { get; }
        int BlockSize { get; }
    }
}