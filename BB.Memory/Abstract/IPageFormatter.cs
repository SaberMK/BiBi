using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Abstract
{
    public interface IPageFormatter
    {
        void Format(Page p);
    }
}
