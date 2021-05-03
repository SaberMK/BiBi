using BB.Query.Expressions;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public interface IExpression
    {
        bool IsConstant { get; }
        bool IsFieldName { get; }
        string AsFieldName { get; }
        bool AppliesTo(Schema schema);
        Constant AsConstant { get; }
        Constant Evaluate(IScan scan);
    }
}
