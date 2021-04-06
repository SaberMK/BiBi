using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Base
{
    public enum FieldType : byte
    {
        Bool = 0,
        Byte = 1,
        Integer = 2,
        Date = 3,
        Blob = 4,
        String = 5
    }
}
