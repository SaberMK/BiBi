using BB.Query.Abstract;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Expressions
{
    public class Term
    {
        private readonly IExpression _left, _right;

        public Term(IExpression left, IExpression right)
        {
            _left = left;
            _right = right;
        }

        public int ReductionFactor(IPlan plan)
        {
            string leftName, rightName;

            if(_left.IsFieldName && _right.IsFieldName)
            {
                leftName = _left.AsFieldName;
                rightName = _right.AsFieldName;
                return Math.Max(plan.DistinctValues(leftName), plan.DistinctValues(rightName));
            }

            if(_left.IsFieldName)
            {
                leftName = _left.AsFieldName;
                return plan.DistinctValues(leftName);
            }

            if (_right.IsFieldName)
            {
                rightName = _right.AsFieldName;
                return plan.DistinctValues(rightName);
            }

            if(_left.AsConstant == _right.AsConstant)
            {
                return 1;
            }
            else
            {
                return int.MaxValue;
            }
        }

        public Constant EquatesWithConstant(string fieldName)
        {
            if(_left.IsFieldName && _left.AsFieldName == fieldName && _right.IsConstant)
            {
                return _right.AsConstant;
            }
            else if(_right.IsFieldName && _right.AsFieldName == fieldName && _left.IsConstant)
            {
                return _left.AsConstant;
            }

            return null;
        }

        public string EquatesWithField(string fieldName)
        {
            if(_left.IsFieldName && _left.AsFieldName == fieldName && _right.IsConstant)
            {
                return _right.AsFieldName;
            }
            else if(_right.IsFieldName && _right.AsFieldName == fieldName && _left.IsFieldName)
            {
                return _left.AsFieldName;
            }

            return null;
        }

        public bool AppliesTo(Schema schema)
        {
            return _left.AppliesTo(schema) && _right.AppliesTo(schema);
        }

        public bool IsSatisfied(IScan scan)
        {
            var left = _left.Evaluate(scan);
            var right = _right.Evaluate(scan);

            return right == left;
        }

        public override string ToString()
            => $"{_left} {_right}";
    }
}
