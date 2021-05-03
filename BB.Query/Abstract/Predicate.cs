using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public class Predicate
    {
        public void ConjoinWith(Predicate predicate)
        {

        }

        public bool IsSatisfied(IScan s)
        {
            return default;
        }

        public int ReductionFactor()
        {
            return default;
        }

        public Predicate SelectPredicate(Schema schema)
        {
            return default;
        }

        public Predicate JoinPredicate(Schema schema1, Schema schema2)
        {
            return default;
        }

        public Constant EquaresWithConstant(string fieldName)
        {
            return default;
        }

        public string EquatesWithField(string fieldName)
        {
            return default;
        }
    }
}
