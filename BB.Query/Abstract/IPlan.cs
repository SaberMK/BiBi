using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public interface IPlan
    {
        IScan Open();
        int DistinctValues(string fieldName);
        int BlocksAccessed { get; }
        int RecordsOutput { get; }
        Schema Schema { get; }
    }
}
