using BB.Query.Abstract;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Expressions
{
    public class FieldNameExpression : IExpression
    {
        private readonly string _fieldName;

        public FieldNameExpression(string fieldName)
        {
            _fieldName = fieldName;
        }

        public bool IsConstant => false;

        public bool IsFieldName => true;

        public Constant AsConstant => null;

        public string AsFieldName => _fieldName;

        public bool AppliesTo(Schema schema) => schema.HasField(_fieldName);

        public Constant Evaluate(IScan scan) => (Constant<string>)scan.GetValue(_fieldName);
    }
}
