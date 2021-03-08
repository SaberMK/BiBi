using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    public struct Page
    {
        private readonly int _blockId;
        private readonly byte[] _data;
        internal PageStatus _status;

        public int BlockId => _blockId;
        public int PageSize => _data.Length;
        public byte[] Data => _data;
        public PageStatus PageStatus => _status;

        public static readonly Encoding Encoding = Encoding.ASCII;

        public Page(int blockId, int pageSize)
        {
            _blockId = blockId;
            _data = new byte[pageSize];
            _status = PageStatus.New;
        }

        internal Page(int blockId, byte[] data)
        {
            _blockId = blockId;
            _data = data;
            _status = PageStatus.Commited;
        }
    }
}
