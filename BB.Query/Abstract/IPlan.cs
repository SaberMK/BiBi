using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public interface IPlan
    {
        int DistinctValues(string fieldName);
    }
}
