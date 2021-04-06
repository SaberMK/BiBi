using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Base
{
    public readonly struct FieldInfo
    {
        public readonly FieldType Type;
        public readonly int Length;

        public FieldInfo(FieldType type, int length)
            => (Type, Length) = (type, length);
    }
}
