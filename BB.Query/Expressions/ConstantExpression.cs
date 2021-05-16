using BB.Query.Abstract;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Expressions
{
    public class ConstantExpression<T> : IExpression
    {
        private readonly Constant<T> _value;

        public ConstantExpression(Constant<T> value)
        {
            _value = value;
        }

        public bool IsConstant => true;

        public bool IsFieldName => false;

        public Constant AsConstant => _value;

        // TODO maybe do something else here
        public string AsFieldName => string.Empty;

        public Constant Evaluate(IScan scan) => _value;

        public bool AppliesTo(Schema schema) => true;

        public override string ToString() => _value.ToString();
    }
}
