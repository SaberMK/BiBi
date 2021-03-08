using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    public struct Page
    {
        public static readonly Encoding Encoding = Encoding.ASCII;
        private readonly int _pageSize;

        public int PageSize => _pageSize;

        public Page(int pageSize)
        {
            _pageSize = pageSize;
        }
    }
}
