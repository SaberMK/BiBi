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

        internal byte[] Data => _data;
        public int BlockId => _blockId;
        public int PageSize => _data.Length;
        public PageStatus PageStatus => _status;

        // I think would add multiple lock objects in future
        public object LockObject { get; private set; }


        public static readonly Encoding Encoding = Encoding.ASCII;

        public Page(int blockId, int pageSize)
        {
            _blockId = blockId;
            _data = new byte[pageSize];
            _status = PageStatus.New;
            LockObject = new object();
        }

        internal Page(int blockId, byte[] data)
        {
            _blockId = blockId;
            _data = data;
            _status = PageStatus.Commited;
            LockObject = new object();
        }
    }
}
